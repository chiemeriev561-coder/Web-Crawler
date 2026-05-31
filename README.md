# Web Crawler (Go)

A simple concurrent web crawler written in Go.

Features
- Concurrent workers with configurable concurrency and per-request delay.
- Resolves relative links and stays within the start URL's hostname.
- Uses a real HTTP client with timeout and a custom User-Agent.
- Skips non-HTML responses and common non-URL schemes (mailto:, javascript:).

Requirements
- Go 1.18+

Build & Run

- Run directly:
  go run main.go <start-url>
  Example: go run main.go https://example.com

- Build binary:
  go build -o crawler
  ./crawler https://example.com

Defaults
- Default start URL: https://cine-craft-box.lovable.app/
- Default concurrency: 5
- Default delay: 100ms between requests
- Queue size: 100

Behavior and limits
- The crawler only follows links whose hostname contains the start URL's hostname.
- It does NOT yet respect robots.txt, sitemaps, or crawl-delay directives — use responsibly.
- No depth limit or storage persistence included (keeps visited URLs in-memory).

Next improvements (suggested)
- Respect robots.txt
- Add depth limit and rate limiting per-host
- Persist results to disk or DB
- Respect and parse sitemaps

License
- MIT
