package main

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

type Crawler struct {
	visited     map[string]bool
	visitedMu   sync.RWMutex
	queue       chan string
	wg          sync.WaitGroup
	concurrency int
	delay       time.Duration
	domain      string
}

func NewCrawler(concurrency int, delay time.Duration, domain string) *Crawler {
	return &Crawler{
		visited:     make(map[string]bool),
		visitedMu:   sync.RWMutex{},
		queue:       make(chan string, 100),
		wg:          sync.WaitGroup{},
		concurrency: concurrency,
		delay:       delay,
		domain:      domain,
	}
}

func (c *Crawler) Crawl(starURL string) {
	// start workers
	for i := 0; i < c.concurrency; i++ {
		go c.worker()
	}

	// Add starting url to queue
	c.wg.Add(1)
	c.queue <- starURL

	// wait for all workers to finish
	c.wg.Wait()
	close(c.queue)
}

func (c *Crawler) worker() {
	for jobURL := range c.queue {
		// Rate limiting per domain
		time.Sleep(c.delay)

		//fetch and parse the page
		c.processPage(jobURL)
		c.wg.Done()
	}
}

func (c *Crawler) processPage(pageURL string) {
	// check if already visited
	if c.isVisited(pageURL) {
		return
	}

	// Mark as visited
	c.markVisited(pageURL)

	fmt.Printf("Crawling :%s\n", pageURL)

	// fetch the page with timeout and user-agent
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", pageURL, nil)
	if err != nil {
		fmt.Printf("Error creating request for %s: %v\n", pageURL, err)
		return
	}
	req.Header.Set("User-Agent", "GoCrawler/1.0")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error fetching %s: %v\n", pageURL, err)
		return
	}
	defer resp.Body.Close()

	// check content type
	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/html") {
		return
	}

	// parse HTML and extract links
	links := c.extractLinks(resp, pageURL)

	//Add new links to the queue
	for _, link := range links {
		if c.isVisited(link) {
			continue
		}
		u, err := url.Parse(link)
		if err != nil {
			continue
		}
		if !strings.Contains(u.Hostname(), c.domain) {
			continue
		}
		c.wg.Add(1)
		select {
		case c.queue <- link:
			// added to queue
		default:
			fmt.Printf("Queue full, skipping %s\n", link)
			c.wg.Done()
		}
	}
}
func (c *Crawler) extractLinks(resp *http.Response, baseURL string) []string {
	var links []string

	tokenizer := html.NewTokenizer(resp.Body)
	for {
		tokenType := tokenizer.Next()
		switch {
		case tokenType == html.ErrorToken:
			return links
		case tokenType == html.StartTagToken:
			token := tokenizer.Token()
			if token.Data == "a" {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						normalizeURL := c.normalizeURL(attr.Val, baseURL)
						if normalizeURL != "" {
							links = append(links, normalizeURL)
						}
					}
				}
			}
		}
	}
}

func (c *Crawler) normalizeURL(href, baseURL string) string {
	href = strings.TrimSpace(href)
	if href == "" || strings.HasPrefix(href, "#") || strings.HasPrefix(href, "mailto:") || strings.HasPrefix(href, "javascript:") {
		return ""
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return ""
	}
	u, err := url.Parse(href)
	if err != nil {
		return ""
	}
	resolved := base.ResolveReference(u)
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return ""
	}
	resolved.Fragment = ""
	return resolved.String()
}

func (c *Crawler) isVisited(url string) bool {
	c.visitedMu.RLock()
	defer c.visitedMu.RUnlock()
	return c.visited[url]
}

func (c *Crawler) markVisited(url string) {
	c.visitedMu.Lock()
	defer c.visitedMu.Unlock()
	c.visited[url] = true
}

func main() {
	start := "https://cine-craft-box.lovable.app/"
	if len(os.Args) > 1 {
		start = os.Args[1]
	}
	u, err := url.Parse(start)
	if err != nil {
		fmt.Printf("Invalid start URL: %v\n", err)
		return
	}
	domain := u.Hostname()

	crawler := NewCrawler(
		5,                    // 5 concurrent workers
		100*time.Millisecond, // 100ms delay between requests
		domain,               // only crawl this domain
	)

	crawler.Crawl(start)

	fmt.Printf("\nCrawled %d pages \n", len(crawler.visited))
}
