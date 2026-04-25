package search

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestGrepSearch(t *testing.T) {
	dir := t.TempDir()
	// Skip dot dirs.
	_ = os.Mkdir(filepath.Join(dir, ".git"), 0755)
	_ = os.WriteFile(filepath.Join(dir, ".git", "HEAD"), []byte("websocket timeout inside .git\n"), 0644)

	_ = os.WriteFile(filepath.Join(dir, "note.md"),
		[]byte("line one\nwebsocket timeout here\nline three\n"), 0644)
	_ = os.WriteFile(filepath.Join(dir, "other.md"),
		[]byte("nothing relevant\n"), 0644)

	g := NewGrep(dir)
	results, err := g.Search(context.Background(), "websocket", 0, 0, "")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("want 1 result (note.md), got %d: %+v", len(results), results)
	}
	if results[0].Path != "note.md" {
		t.Fatalf("path mismatch: %s", results[0].Path)
	}
}

func TestGrepSearchPagination(t *testing.T) {
	dir := t.TempDir()
	// Seed 60 files that all match "common". Names sorted lexically give a
	// stable order so offset/limit assertions are deterministic.
	for i := 0; i < 60; i++ {
		name := filepath.Join(dir, fmt.Sprintf("note-%03d.md", i))
		if err := os.WriteFile(name, []byte("common content here\n"), 0644); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	g := NewGrep(dir)

	page1, err := g.Search(context.Background(), "common", 10, 0, "")
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1) != 10 {
		t.Fatalf("page1: want 10 got %d", len(page1))
	}
	page2, err := g.Search(context.Background(), "common", 10, 10, "")
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2) != 10 {
		t.Fatalf("page2: want 10 got %d", len(page2))
	}
	// Pages must not overlap.
	seen := make(map[string]bool, 20)
	for _, r := range page1 {
		seen[r.Path] = true
	}
	for _, r := range page2 {
		if seen[r.Path] {
			t.Fatalf("page2 contains page1 result %s", r.Path)
		}
	}

	// Default limit caps at 50 even with no explicit limit.
	def, err := g.Search(context.Background(), "common", 0, 0, "")
	if err != nil {
		t.Fatalf("default: %v", err)
	}
	if len(def) != defaultSearchLimit {
		t.Fatalf("default limit: want %d got %d", defaultSearchLimit, len(def))
	}

	// Offset past total yields empty result, not an error.
	empty, err := g.Search(context.Background(), "common", 10, 9999, "")
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("want empty got %d", len(empty))
	}
}
