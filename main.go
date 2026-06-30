package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
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
	VisitedURLs []string       `json:"visited_urls"`
	PendingURLs []string       `json:"pending_urls"`
	Timestamp   time.Time      `json:"timestamp"`
	URLDepths   map[string]int `json:"url_depths,omitempty"`
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
	client         *http.Client
	pending        map[string]struct{}
	pendingMu      sync.RWMutex
	depths         map[string]int
	depthsMu       sync.RWMutex
	maxDepth       int
	maxPages       int
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
		client:      &http.Client{Timeout: 10 * time.Second},
		pending:     make(map[string]struct{}),
		depths:      make(map[string]int),
		depthsMu:    sync.RWMutex{},
	}
}

func (c *Crawler) Crawl(startURL string) {
	c.startTime = time.Now()
	c.stats.StartTime = c.startTime

	// start workers
	for i := 0; i < c.concurrency; i++ {
		go c.worker()
	}

	// Load previous state if available
	if c.stateFile != "" {
		if err := c.loadState(); err != nil {
			fmt.Printf("Error loading state: %v\n", err)
		}
	}

	c.depthsMu.Lock()
	if _, ok := c.depths[startURL]; !ok {
		c.depths[startURL] = 0
	}
	c.depthsMu.Unlock()

	// Add starting url to queue if not already visited
	c.scheduleURL(startURL)

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
		c.markCompleted(jobURL)
		c.wg.Done()
	}
}

func (c *Crawler) processPage(pageURL string) {
	c.pagesMu.Lock()
	pagesCount := len(c.pages)
	c.pagesMu.Unlock()
	if c.maxPages > 0 && pagesCount >= c.maxPages {
		return
	}

	fmt.Printf("Crawling: %s\n", pageURL)

	// fetch the page with timeout and user-agent
	req, err := http.NewRequest("GET", pageURL, nil)
	if err != nil {
		fmt.Printf("Error creating request for %s: %v\n", pageURL, err)
		c.incrementFailedPages()
		return
	}
	req.Header.Set("User-Agent", "GoCrawler/1.0")
	resp, err := c.client.Do(req)
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
	if c.maxPages > 0 && len(c.pages) >= c.maxPages {
		c.pagesMu.Unlock()
		return
	}
	c.pages = append(c.pages, pageData)
	c.pagesMu.Unlock()

	// Add new links to the queue
	c.depthsMu.RLock()
	parentDepth := c.depths[pageURL]
	c.depthsMu.RUnlock()

	for _, link := range links {
		if c.maxDepth <= 0 || parentDepth < c.maxDepth {
			c.depthsMu.Lock()
			if _, ok := c.depths[link]; !ok {
				c.depths[link] = parentDepth + 1
			}
			c.depthsMu.Unlock()
			c.scheduleURL(link)
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
	doc, err := html.Parse(bytes.NewReader(bodyBytes))
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

func (c *Crawler) scheduleURL(pageURL string) bool {
	if pageURL == "" {
		return false
	}

	u, err := url.Parse(pageURL)
	if err != nil {
		return false
	}
	if !c.isAllowedHostname(u.Hostname()) {
		return false
	}

	c.pagesMu.Lock()
	pagesCount := len(c.pages)
	c.pagesMu.Unlock()
	if c.maxPages > 0 && pagesCount >= c.maxPages {
		return false
	}

	c.visitedMu.Lock()
	if c.visited[pageURL] {
		c.visitedMu.Unlock()
		return false
	}
	c.visited[pageURL] = true
	c.visitedMu.Unlock()

	c.addPending(pageURL)
	c.wg.Add(1)
	c.queue <- pageURL
	return true
}

func (c *Crawler) isAllowedHostname(hostname string) bool {
	if hostname == "" {
		return false
	}
	hostname = strings.ToLower(hostname)
	domain := strings.ToLower(c.domain)
	return hostname == domain || strings.HasSuffix(hostname, "."+domain)
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

func (c *Crawler) addPending(pageURL string) {
	c.pendingMu.Lock()
	c.pending[pageURL] = struct{}{}
	c.pendingMu.Unlock()
}

func (c *Crawler) markCompleted(pageURL string) {
	c.pendingMu.Lock()
	delete(c.pending, pageURL)
	c.pendingMu.Unlock()
}

func (c *Crawler) snapshotPending() []string {
	c.pendingMu.RLock()
	defer c.pendingMu.RUnlock()

	pending := make([]string, 0, len(c.pending))
	for pageURL := range c.pending {
		pending = append(pending, pageURL)
	}
	return pending
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

	writer.Flush()
	return writer.Error()
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
	visited := make([]string, 0, len(c.visited))
	for pageURL := range c.visited {
		visited = append(visited, pageURL)
	}
	c.visitedMu.RUnlock()

	c.depthsMu.RLock()
	depthsCopy := make(map[string]int, len(c.depths))
	for k, v := range c.depths {
		depthsCopy[k] = v
	}
	c.depthsMu.RUnlock()

	state := CrawlState{
		VisitedURLs: visited,
		PendingURLs: c.snapshotPending(),
		Timestamp:   time.Now(),
		URLDepths:   depthsCopy,
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
	for _, pageURL := range state.VisitedURLs {
		c.visited[pageURL] = true
	}
	c.visitedMu.Unlock()

	// Restore depths
	c.depthsMu.Lock()
	if state.URLDepths != nil {
		for k, v := range state.URLDepths {
			c.depths[k] = v
		}
	}
	c.depthsMu.Unlock()

	for _, pageURL := range state.PendingURLs {
		if pageURL == "" {
			continue
		}
		u, err := url.Parse(pageURL)
		if err != nil || !c.isAllowedHostname(u.Hostname()) {
			continue
		}
		c.visitedMu.Lock()
		if !c.visited[pageURL] {
			c.visited[pageURL] = true
		}
		c.visitedMu.Unlock()
		c.addPending(pageURL)
		c.wg.Add(1)
		c.queue <- pageURL
	}

	fmt.Printf("Loaded state: %d visited URLs, %d pending URLs, %d depths mapped\n", len(state.VisitedURLs), len(state.PendingURLs), len(state.URLDepths))

	return nil
}

func main() {
	defaultStart := "https://cine-craft-box.lovable.app/"
	startFlag := flag.String("start-url", defaultStart, "Starting URL for the crawl")
	concurrencyFlag := flag.Int("concurrency", 5, "Number of concurrent workers")
	delayFlag := flag.Duration("delay", 100*time.Millisecond, "Delay between requests")
	outputDirFlag := flag.String("output-dir", "./crawl_output", "Directory for exported crawl results")
	saveContentFlag := flag.Bool("save-content", true, "Save extracted text content")
	saveRawHTMLFlag := flag.Bool("save-raw-html", false, "Save raw HTML for each crawled page")
	stateFileFlag := flag.String("state-file", "./crawl_state.json", "File used to persist crawl state")
	maxDepthFlag := flag.Int("max-depth", 0, "Maximum depth to crawl (0 for unlimited)")
	maxPagesFlag := flag.Int("max-pages", 0, "Maximum pages to crawl (0 for unlimited)")
	interactiveFlag := flag.Bool("interactive", false, "Run in interactive terminal menu mode")
	flag.Parse()

	isDefaultStart := true
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "start-url" {
			isDefaultStart = false
		}
	})

	if *interactiveFlag || (flag.NArg() == 0 && isDefaultStart) {
		runInteractiveMenu()
		return
	}

	start := *startFlag
	if args := flag.Args(); len(args) > 0 {
		start = args[0]
	}
	// Clean the URL by removing brackets and extra whitespace.
	start = strings.TrimSpace(start)
	start = strings.Trim(start, "[]")

	u, err := url.Parse(start)
	if err != nil {
		fmt.Printf("Invalid start URL: %v\n", err)
		return
	}
	domain := strings.ToLower(u.Hostname())

	// Configuration
	outputDir := *outputDirFlag
	saveContent := *saveContentFlag
	saveRawHTML := *saveRawHTMLFlag
	stateFile := *stateFileFlag

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		return
	}

	crawler := NewCrawler(
		*concurrencyFlag,
		*delayFlag,
		domain,
		outputDir,
		saveContent,
		saveRawHTML,
		stateFile,
	)
	crawler.maxDepth = *maxDepthFlag
	crawler.maxPages = *maxPagesFlag

	fmt.Printf("Starting crawl of %s (domain: %s)\n", start, domain)
	fmt.Printf("Output directory: %s\n", outputDir)
	fmt.Printf("Save content: %v, Save raw HTML: %v\n", saveContent, saveRawHTML)
	fmt.Printf("State file: %s\n", stateFile)
	fmt.Printf("Max depth: %d, Max pages: %d\n\n", crawler.maxDepth, crawler.maxPages)

	crawler.Crawl(start)

	finalizeCrawl(crawler, outputDir)
}

func finalizeCrawl(crawler *Crawler, outputDir string) {
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

func runInteractiveMenu() {
	reader := bufio.NewReader(os.Stdin)

	// Default configuration values
	startURL := "https://cine-craft-box.lovable.app/"
	concurrency := 5
	delay := 100 * time.Millisecond
	maxDepth := 0
	maxPages := 0
	saveContent := true
	saveRawHTML := false
	outputDir := "./crawl_output"
	stateFile := "./crawl_state.json"

	for {
		depthLimitDesc := ""
		if maxDepth == 0 {
			depthLimitDesc = "Unlimited"
		} else {
			depthLimitDesc = fmt.Sprintf("Max Depth: %d", maxDepth)
		}

		pagesLimitDesc := ""
		if maxPages == 0 {
			pagesLimitDesc = "Unlimited"
		} else {
			pagesLimitDesc = fmt.Sprintf("Max Pages: %d", maxPages)
		}

		fmt.Println("\n==================================================")
		fmt.Println("         CONCURRENT WEB CRAWLER TUI MENU")
		fmt.Println("==================================================")
		fmt.Printf(" 1. Start URL:         %s\n", startURL)
		fmt.Printf(" 2. Concurrency:       %d workers\n", concurrency)
		fmt.Printf(" 3. Request Delay:     %s\n", delay)
		fmt.Printf(" 4. Max Depth:         %s\n", depthLimitDesc)
		fmt.Printf(" 5. Max Pages:         %s\n", pagesLimitDesc)
		fmt.Printf(" 6. Save Text Content: %t\n", saveContent)
		fmt.Printf(" 7. Save Raw HTML:     %t\n", saveRawHTML)
		fmt.Printf(" 8. Output Directory:  %s\n", outputDir)
		fmt.Printf(" 9. State File Path:   %s\n", stateFile)
		fmt.Println("--------------------------------------------------")
		fmt.Println(" S. START CRAWLING")
		fmt.Println(" R. RESUME FROM PREVIOUS STATE")
		fmt.Println(" E. EXIT")
		fmt.Println("==================================================")
		fmt.Print("Select an option (1-9, S, R, E): ")

		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}
		input = strings.TrimSpace(strings.ToUpper(input))

		switch input {
		case "1":
			fmt.Print("Enter Start URL: ")
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)
			if val != "" {
				startURL = val
			}
		case "2":
			fmt.Print("Enter Concurrency (1-50): ")
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)
			var n int
			if _, err := fmt.Sscanf(val, "%d", &n); err == nil && n >= 1 && n <= 50 {
				concurrency = n
			} else {
				fmt.Println("Invalid concurrency. Must be an integer between 1 and 50.")
			}
		case "3":
			fmt.Print("Enter Request Delay (e.g. 100ms, 1s, 500ms): ")
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)
			if d, err := time.ParseDuration(val); err == nil {
				delay = d
			} else {
				fmt.Println("Invalid duration format. Use e.g. 200ms, 1s.")
			}
		case "4":
			fmt.Print("Enter Max Depth (0 for unlimited): ")
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)
			var d int
			if _, err := fmt.Sscanf(val, "%d", &d); err == nil && d >= 0 {
				maxDepth = d
			} else {
				fmt.Println("Invalid max depth.")
			}
		case "5":
			fmt.Print("Enter Max Pages (0 for unlimited): ")
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)
			var p int
			if _, err := fmt.Sscanf(val, "%d", &p); err == nil && p >= 0 {
				maxPages = p
			} else {
				fmt.Println("Invalid max pages.")
			}
		case "6":
			saveContent = !saveContent
			fmt.Printf("Save Text Content set to: %t\n", saveContent)
		case "7":
			saveRawHTML = !saveRawHTML
			fmt.Printf("Save Raw HTML set to: %t\n", saveRawHTML)
		case "8":
			fmt.Print("Enter Output Directory: ")
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)
			if val != "" {
				outputDir = val
			}
		case "9":
			fmt.Print("Enter State File Path: ")
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)
			if val != "" {
				stateFile = val
			}
		case "S":
			startURL = strings.TrimSpace(startURL)
			startURL = strings.Trim(startURL, "[]")
			u, err := url.Parse(startURL)
			if err != nil || u.Scheme == "" || u.Host == "" {
				fmt.Printf("Invalid start URL: %v. Please set a valid URL first.\n", err)
				break
			}
			domain := strings.ToLower(u.Hostname())

			if err := os.MkdirAll(outputDir, 0755); err != nil {
				fmt.Printf("Error creating output directory: %v\n", err)
				break
			}

			crawler := NewCrawler(concurrency, delay, domain, outputDir, saveContent, saveRawHTML, stateFile)
			crawler.maxDepth = maxDepth
			crawler.maxPages = maxPages

			fmt.Println("\nStarting crawl...")
			fmt.Printf("Target URL: %s\n", startURL)
			fmt.Println("Press Ctrl+C to stop early.")

			crawler.Crawl(startURL)
			finalizeCrawl(crawler, outputDir)

		case "R":
			if _, err := os.Stat(stateFile); os.IsNotExist(err) {
				fmt.Printf("State file %s does not exist. Cannot resume.\n", stateFile)
				break
			}
			startURL = strings.TrimSpace(startURL)
			startURL = strings.Trim(startURL, "[]")
			u, err := url.Parse(startURL)
			if err != nil || u.Scheme == "" || u.Host == "" {
				fmt.Printf("Invalid start URL: %v. Please set a valid URL first.\n", err)
				break
			}
			domain := strings.ToLower(u.Hostname())

			crawler := NewCrawler(concurrency, delay, domain, outputDir, saveContent, saveRawHTML, stateFile)
			crawler.maxDepth = maxDepth
			crawler.maxPages = maxPages

			fmt.Println("\nResuming crawl from saved state...")
			fmt.Println("Press Ctrl+C to stop early.")

			crawler.Crawl(startURL)
			finalizeCrawl(crawler, outputDir)

		case "E":
			fmt.Println("Exiting crawler. Goodbye!")
			return
		default:
			fmt.Println("Invalid option. Please choose between 1-9, S, R, or E.")
		}
	}
}
