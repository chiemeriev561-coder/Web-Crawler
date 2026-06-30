package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNormalizeURL(t *testing.T) {
	crawler := NewCrawler(1, 0, "example.com", t.TempDir(), false, false, "")

	got := crawler.normalizeURL(" /about#team ", "https://example.com/index")
	want := "https://example.com/about"
	if got != want {
		t.Fatalf("normalizeURL() = %q, want %q", got, want)
	}

	if got := crawler.normalizeURL("mailto:test@example.com", "https://example.com"); got != "" {
		t.Fatalf("normalizeURL() for mailto returned %q, want empty string", got)
	}

	if got := crawler.normalizeURL("javascript:void(0)", "https://example.com"); got != "" {
		t.Fatalf("normalizeURL() for javascript returned %q, want empty string", got)
	}
}

func TestIsAllowedHostname(t *testing.T) {
	crawler := NewCrawler(1, 0, "example.com", t.TempDir(), false, false, "")

	tests := []struct {
		name     string
		hostname string
		want     bool
	}{
		{name: "exact", hostname: "example.com", want: true},
		{name: "subdomain", hostname: "blog.example.com", want: true},
		{name: "different suffix", hostname: "notexample.com", want: false},
		{name: "other domain", hostname: "example.org", want: false},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := crawler.isAllowedHostname(testCase.hostname); got != testCase.want {
				t.Fatalf("isAllowedHostname(%q) = %v, want %v", testCase.hostname, got, testCase.want)
			}
		})
	}
}

func TestScheduleURL(t *testing.T) {
	crawler := NewCrawler(1, 0, "example.com", t.TempDir(), false, false, "")

	if !crawler.scheduleURL("https://example.com/page") {
		t.Fatal("scheduleURL() = false, want true")
	}

	if crawler.scheduleURL("https://example.com/page") {
		t.Fatal("scheduleURL() accepted a duplicate URL")
	}

	if crawler.scheduleURL("https://evil.com/page") {
		t.Fatal("scheduleURL() accepted an external URL")
	}

	if got := len(crawler.queue); got != 1 {
		t.Fatalf("queue length = %d, want 1", got)
	}

	if got := len(crawler.visited); got != 1 {
		t.Fatalf("visited length = %d, want 1", got)
	}
}

func TestLoadState(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "crawl_state.json")
	state := CrawlState{
		VisitedURLs: []string{"https://example.com", "https://example.com/about"},
		PendingURLs: []string{"https://example.com/contact"},
		Timestamp:   time.Now(),
	}

	file, err := os.Create(stateFile)
	if err != nil {
		t.Fatalf("failed to create state file: %v", err)
	}
	if err := json.NewEncoder(file).Encode(state); err != nil {
		file.Close()
		t.Fatalf("failed to write state file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("failed to close state file: %v", err)
	}

	crawler := NewCrawler(1, 0, "example.com", dir, false, false, stateFile)
	if err := crawler.loadState(); err != nil {
		t.Fatalf("loadState() returned error: %v", err)
	}

	if got := len(crawler.visited); got != 3 {
		t.Fatalf("visited length = %d, want 3", got)
	}

	if got := len(crawler.queue); got != 1 {
		t.Fatalf("queue length = %d, want 1", got)
	}

	if !crawler.isVisited("https://example.com/contact") {
		t.Fatal("pending URL was not restored into visited set")
	}
}

func TestMaxPagesLimit(t *testing.T) {
	crawler := NewCrawler(1, 0, "example.com", t.TempDir(), false, false, "")
	crawler.maxPages = 2

	// Simulate 2 pages already successfully crawled and stored
	crawler.pages = append(crawler.pages, PageData{URL: "https://example.com/1"})
	crawler.pages = append(crawler.pages, PageData{URL: "https://example.com/2"})

	// Scheduling another URL should now return false since we reached maxPages
	if crawler.scheduleURL("https://example.com/3") {
		t.Fatal("scheduleURL accepted a URL after maxPages limit was reached")
	}
}
