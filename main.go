package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

type PageData struct {
	URL         string    `json:"url"`
	Title       string    `json:"title"`
	MetaDesc    string    `json:"meta_description"`
	Headers     []string  `json:"headers"`
	Content     string    `json:"content,omitempty"`
	RawHTML     string    `json:"raw_html,omitempty"`
	CrawledAt   time.Time `json:"crawled_at"`
	StatusCode  int       `json:"status_code"`
	ContentType string    `json:"content_type"`
}

type CrawlStats struct {
	StartTime      time.Time `json:"start_time"`
	EndTime        time.Time `json:"end_time"`
	Duration       float64   `json:"duration_seconds"`
	PagesCrawled   int       `json:"pages_crawled"`
	PagesFailed    int       `json:"pages_failed"`
	TotalLinks     int       `json:"total_links"`
	QueueFullCount int       `json:"queue_full_count"`
}

type CrawlState struct {
	VisitedURLs []string  `json:"visited_urls"`
	PendingURLs []string  `json:"pending_urls"`
	Timestamp   time.Time `json:"timestamp"`
}

type Crawler struct {
	visited        map[string]bool
	visitedMu      sync.RWMutex
	queue          chan string
	wg             sync.WaitGroup
	concurrency    int
	delay          time.Duration
	domain         string
	pages          []PageData
	pagesMu        sync.Mutex
	stats          CrawlStats
	statsMu        sync.Mutex
	outputDir      string
	saveContent    bool
	saveRawHTML    bool
	queueFullCount int
	startTime      time.Time
	stateFile      string
}

func NewCrawler(concurrency int, delay time.Duration, domain string, outputDir string, saveContent bool, saveRawHTML bool, stateFile string) *Crawler {
	return &Crawler{
		visited:     make(map[string]bool),
		visitedMu:   sync.RWMutex{},
		queue:       make(chan string, 100),
		wg:          sync.WaitGroup{},
		concurrency: concurrency,
		delay:       delay,
		domain:      domain,
		pages:       make([]PageData, 0),
		pagesMu:     sync.Mutex{},
		stats:       CrawlStats{},
		statsMu:     sync.Mutex{},
		outputDir:   outputDir,
		saveContent: saveContent,
		saveRawHTML: saveRawHTML,
		stateFile:   stateFile,
	}
}

func (c *Crawler) Crawl(startURL string) {
	c.startTime = time.Now()
	c.stats.StartTime = c.startTime

	// Load previous state if available
	if c.stateFile != "" {
		c.loadState()
	}

	// start workers
	for i := 0; i < c.concurrency; i++ {
		go c.worker()
	}

	// Add starting url to queue if not already visited
	if !c.isVisited(startURL) {
		c.wg.Add(1)
		c.queue <- startURL
	}

	// wait for all workers to finish
	c.wg.Wait()
	close(c.queue)

	// Finalize statistics
	c.stats.EndTime = time.Now()
	c.stats.Duration = c.stats.EndTime.Sub(c.stats.StartTime).Seconds()
	c.stats.PagesCrawled = len(c.pages)
	c.stats.TotalLinks = len(c.visited)
	c.stats.QueueFullCount = c.queueFullCount
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
		c.incrementFailedPages()
		return
	}
	req.Header.Set("User-Agent", "GoCrawler/1.0")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error fetching %s: %v\n", pageURL, err)
		c.incrementFailedPages()
		return
	}
	defer resp.Body.Close()

	// check content type
	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/html") {
		return
	}

	// Extract page data
	pageData, links := c.extractPageData(resp, pageURL, contentType)

	// Store page data
	c.pagesMu.Lock()
	c.pages = append(c.pages, pageData)
	c.pagesMu.Unlock()

	// parse HTML and extract links from the already extracted data

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
			c.queueFullCount++
			c.wg.Done()
		}
	}
}

func (c *Crawler) extractPageData(resp *http.Response, pageURL string, contentType string) (PageData, []string) {
	pageData := PageData{
		URL:         pageURL,
		CrawledAt:   time.Now(),
		StatusCode:  resp.StatusCode,
		ContentType: contentType,
	}

	// Read the body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading body for %s: %v\n", pageURL, err)
		return pageData, []string{}
	}

	// Store raw HTML if requested
	if c.saveRawHTML {
		pageData.RawHTML = string(bodyBytes)
	}

	// Parse HTML for metadata and content
	doc, err := html.Parse(strings.NewReader(string(bodyBytes)))
	if err != nil {
		fmt.Printf("Error parsing HTML for %s: %v\n", pageURL, err)
		return pageData, []string{}
	}

	// Extract metadata and content
	c.extractMetadata(doc, &pageData)

	// Extract text content if requested
	if c.saveContent {
		pageData.Content = c.extractTextContent(doc)
	}

	// Extract links from the same document
	links := c.extractLinksFromDoc(doc, pageURL)

	return pageData, links
}

func (c *Crawler) extractMetadata(n *html.Node, pageData *PageData) {
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "title":
				if n.FirstChild != nil {
					pageData.Title = n.FirstChild.Data
				}
			case "meta":
				name := ""
				content := ""
				for _, attr := range n.Attr {
					if attr.Key == "name" {
						name = attr.Val
					}
					if attr.Key == "content" {
						content = attr.Val
					}
					if attr.Key == "property" {
						name = attr.Val
					}
				}
				if name == "description" || name == "og:description" {
					pageData.MetaDesc = content
				}
			case "h1", "h2", "h3":
				if n.FirstChild != nil {
					headerText := strings.TrimSpace(n.FirstChild.Data)
					if headerText != "" {
						pageData.Headers = append(pageData.Headers, headerText)
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(n)
}

func (c *Crawler) extractTextContent(n *html.Node) string {
	var f func(*html.Node)
	var text strings.Builder
	f = func(n *html.Node) {
		if n.Type == html.TextNode {
			text.WriteString(n.Data)
			text.WriteString(" ")
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(n)
	return strings.TrimSpace(text.String())
}

func (c *Crawler) incrementFailedPages() {
	c.statsMu.Lock()
	c.stats.PagesFailed++
	c.statsMu.Unlock()
}

func (c *Crawler) extractLinksFromDoc(doc *html.Node, baseURL string) []string {
	var links []string

	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					normalizedURL := c.normalizeURL(attr.Val, baseURL)
					if normalizedURL != "" {
						links = append(links, normalizedURL)
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)

	return links
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

// Export functionality
func (c *Crawler) ExportToJSON(filename string) error {
	c.pagesMu.Lock()
	defer c.pagesMu.Unlock()

	data := struct {
		Pages []PageData `json:"pages"`
		Stats CrawlStats `json:"stats"`
	}{
		Pages: c.pages,
		Stats: c.stats,
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

func (c *Crawler) ExportToCSV(filename string) error {
	c.pagesMu.Lock()
	defer c.pagesMu.Unlock()

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{"URL", "Title", "MetaDescription", "Headers", "StatusCode", "ContentType", "CrawledAt"}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write rows
	for _, page := range c.pages {
		headersStr := strings.Join(page.Headers, "; ")
		row := []string{
			page.URL,
			page.Title,
			page.MetaDesc,
			headersStr,
			fmt.Sprintf("%d", page.StatusCode),
			page.ContentType,
			page.CrawledAt.Format(time.RFC3339),
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

func (c *Crawler) ExportStats(filename string) error {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(c.stats)
}

// Persistent state management
func (c *Crawler) saveState() error {
	if c.stateFile == "" {
		return nil
	}

	c.visitedMu.RLock()
	defer c.visitedMu.RUnlock()

	state := CrawlState{
		VisitedURLs: make([]string, 0, len(c.visited)),
		PendingURLs: make([]string, 0),
		Timestamp:   time.Now(),
	}

	for url := range c.visited {
		state.VisitedURLs = append(state.VisitedURLs, url)
	}

	// Get pending URLs from queue (this is approximate as queue is being processed)
	for len(c.queue) > 0 {
		select {
		case url := <-c.queue:
			state.PendingURLs = append(state.PendingURLs, url)
		default:
			// Queue is empty, stop draining
		}
	}

	// Put pending URLs back into queue
	for _, url := range state.PendingURLs {
		c.queue <- url
	}

	file, err := os.Create(c.stateFile)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(state)
}

func (c *Crawler) loadState() error {
	if c.stateFile == "" {
		return nil
	}

	file, err := os.Open(c.stateFile)
	if err != nil {
		// File doesn't exist yet, that's ok
		return nil
	}
	defer file.Close()

	var state CrawlState
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&state); err != nil {
		return err
	}

	// Restore visited URLs
	c.visitedMu.Lock()
	for _, url := range state.VisitedURLs {
		c.visited[url] = true
	}
	c.visitedMu.Unlock()

	fmt.Printf("Loaded state: %d visited URLs, %d pending URLs\n", len(state.VisitedURLs), len(state.PendingURLs))

	return nil
}

func main() {
	start := "https://cine-craft-box.lovable.app/"
	if len(os.Args) > 1 {
		start = os.Args[1]
		// Clean the URL by removing brackets and extra whitespace
		start = strings.TrimSpace(start)
		start = strings.Trim(start, "[]")
	}
	u, err := url.Parse(start)
	if err != nil {
		fmt.Printf("Invalid start URL: %v\n", err)
		return
	}
	domain := u.Hostname()

	// Configuration
	outputDir := "./crawl_output"
	saveContent := true
	saveRawHTML := false
	stateFile := "./crawl_state.json"

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		return
	}

	crawler := NewCrawler(
		5,                    // 5 concurrent workers
		100*time.Millisecond, // 100ms delay between requests
		domain,               // only crawl this domain
		outputDir,
		saveContent,
		saveRawHTML,
		stateFile,
	)

	fmt.Printf("Starting crawl of %s (domain: %s)\n", start, domain)
	fmt.Printf("Output directory: %s\n", outputDir)
	fmt.Printf("Save content: %v, Save raw HTML: %v\n", saveContent, saveRawHTML)
	fmt.Printf("State file: %s\n\n", stateFile)

	crawler.Crawl(start)

	// Save state for potential resuming
	if err := crawler.saveState(); err != nil {
		fmt.Printf("Error saving state: %v\n", err)
	}

	// Export results
	jsonFile := fmt.Sprintf("%s/crawl_results.json", outputDir)
	if err := crawler.ExportToJSON(jsonFile); err != nil {
		fmt.Printf("Error exporting to JSON: %v\n", err)
	} else {
		fmt.Printf("Exported results to %s\n", jsonFile)
	}

	csvFile := fmt.Sprintf("%s/crawl_results.csv", outputDir)
	if err := crawler.ExportToCSV(csvFile); err != nil {
		fmt.Printf("Error exporting to CSV: %v\n", err)
	} else {
		fmt.Printf("Exported results to %s\n", csvFile)
	}

	statsFile := fmt.Sprintf("%s/crawl_stats.json", outputDir)
	if err := crawler.ExportStats(statsFile); err != nil {
		fmt.Printf("Error exporting stats: %v\n", err)
	} else {
		fmt.Printf("Exported stats to %s\n", statsFile)
	}

	fmt.Printf("\n=== Crawl Complete ===\n")
	fmt.Printf("Crawled %d unique pages\n", len(crawler.visited))
	fmt.Printf("Successfully processed: %d pages\n", len(crawler.pages))
	fmt.Printf("Failed: %d pages\n", crawler.stats.PagesFailed)
	fmt.Printf("Duration: %.2f seconds\n", crawler.stats.Duration)
	fmt.Printf("Queue full events: %d\n", crawler.stats.QueueFullCount)
}
