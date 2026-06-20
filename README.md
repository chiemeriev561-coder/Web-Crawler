# Web Crawler (Go)

A concurrent web crawler written in Go with comprehensive data storage and export capabilities.

## Features

### Core Crawling
- Concurrent workers with configurable concurrency and per-request delay
- Resolves relative links and stays within the start URL's hostname
- Uses a real HTTP client with timeout and a custom User-Agent
- Skips non-HTML responses and common non-URL schemes (mailto:, javascript:)

### Data & Storage
- **Page Metadata Extraction**: Captures page titles, meta descriptions, and headers (H1-H3)
- **Content Storage**: Saves extracted text content from crawled pages
- **Multiple Export Formats**: Exports results to JSON and CSV formats
- **Statistics Tracking**: Records crawl duration, success/failure rates, and queue metrics
- **Persistent State**: Saves crawl progress to resume interrupted crawls
- **URL Cleaning**: Automatically handles URLs with brackets or extra whitespace

## Requirements

- Go 1.18+

## Build & Run

- Run directly:

  ```bash
  go run main.go [flags] [start-url]
  ```

  Example: `go run main.go -concurrency 8 -delay 200ms https://example.com`

- Build binary:

  ```bash
  go build -o crawler
  ./crawler -output-dir ./crawl_output https://example.com
  ```

## Defaults

- **Default start URL**: `https://cine-craft-box.lovable.app/`
- **Default concurrency**: 5 workers
- **Default delay**: 100ms between requests
- **Queue size**: 100
- **Output directory**: `./crawl_output/`
- **Content saving**: Enabled (text content)
- **Raw HTML saving**: Disabled
- **State file**: `./crawl_state.json`

## Output Files

The crawler creates the following files:

### `crawl_output/crawl_results.json`
Complete crawl data including:
- Page URLs
- Titles and meta descriptions
- Headers (H1, H2, H3)
- Text content (if enabled)
- HTTP status codes and content types
- Timestamps

### `crawl_output/crawl_results.csv`
Tabular format for easy spreadsheet analysis with columns:
- URL, Title, MetaDescription, Headers, StatusCode, ContentType, CrawledAt

### `crawl_output/crawl_stats.json`
Crawl statistics including:
- Start and end times
- Duration in seconds
- Pages crawled/failed counts
- Total unique links discovered
- Queue overflow events

### `crawl_state.json`
Persistent state for resuming interrupted crawls:
- Visited URLs
- Pending URLs (snapshot)
- Timestamp

## Behavior and Limits

- The crawler only follows links whose hostname matches the start URL's hostname or a subdomain
- It does NOT yet respect robots.txt, sitemaps, or crawl-delay directives — use responsibly
- No depth limit currently implemented (may crawl indefinitely on large sites)
- Visited URLs are kept in-memory during crawling, but state is persisted for resuming

## Configuration

The crawler's behavior can be customized with command-line flags:

```bash
go run main.go   -start-url https://example.com   -concurrency 8   -delay 200ms   -output-dir ./crawl_output   -save-content=true   -save-raw-html=false   -state-file ./crawl_state.json
```

## Future Improvements

- Respect robots.txt
- Add configurable depth limit
- Add rate limiting per-host (more sophisticated than fixed delay)
- Parse and respect sitemaps
- Support for authentication/cookies
- Headless browser support for JavaScript-rendered content
- Distributed crawling support
- Link graph visualization

## License

- MIT
