package search

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kiwifs/kiwifs/internal/storage"
)

var ctxBG = context.Background()

func newTestSQLite(t *testing.T) *SQLite {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	s, err := NewSQLite(dir, store)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestIndexMetaStoresFrontmatter(t *testing.T) {
	s := newTestSQLite(t)

	content := []byte("---\nstatus: published\npriority: high\ntags:\n  - alpha\n  - beta\n---\n# Hello\n")
	if err := s.IndexMeta(ctxBG, "docs/intro.md", content); err != nil {
		t.Fatalf("IndexMeta: %v", err)
	}

	var fm string
	if err := s.readDB.QueryRow(`SELECT frontmatter FROM file_meta WHERE path = ?`, "docs/intro.md").Scan(&fm); err != nil {
		t.Fatalf("query file_meta: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(fm), &parsed); err != nil {
		t.Fatalf("unmarshal: %v (raw=%s)", err, fm)
	}
	if parsed["status"] != "published" {
		t.Fatalf("status mismatch: %+v", parsed)
	}
	if parsed["priority"] != "high" {
		t.Fatalf("priority mismatch: %+v", parsed)
	}
	tags, ok := parsed["tags"].([]any)
	if !ok || len(tags) != 2 || tags[0] != "alpha" || tags[1] != "beta" {
		t.Fatalf("tags mismatch: %+v", parsed["tags"])
	}
}

func TestIndexMetaUpsert(t *testing.T) {
	s := newTestSQLite(t)

	if err := s.IndexMeta(ctxBG, "page.md", []byte("---\nstatus: draft\n---\nbody\n")); err != nil {
		t.Fatalf("first IndexMeta: %v", err)
	}
	if err := s.IndexMeta(ctxBG, "page.md", []byte("---\nstatus: published\n---\nbody\n")); err != nil {
		t.Fatalf("second IndexMeta: %v", err)
	}
	var n int
	if err := s.readDB.QueryRow(`SELECT COUNT(*) FROM file_meta WHERE path = ?`, "page.md").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 row, got %d", n)
	}
	var fm string
	_ = s.readDB.QueryRow(`SELECT frontmatter FROM file_meta WHERE path = ?`, "page.md").Scan(&fm)
	if !contains(fm, `"status":"published"`) {
		t.Fatalf("upsert didn't update: %s", fm)
	}
}

func TestIndexMetaSkipsNonKnowledgeFiles(t *testing.T) {
	s := newTestSQLite(t)
	if err := s.IndexMeta(ctxBG, "assets/diagram.png", []byte("\x89PNG...")); err != nil {
		t.Fatalf("IndexMeta: %v", err)
	}
	var n int
	_ = s.readDB.QueryRow(`SELECT COUNT(*) FROM file_meta`).Scan(&n)
	if n != 0 {
		t.Fatalf("non-markdown should not be indexed, got %d rows", n)
	}
}

func TestRemoveMeta(t *testing.T) {
	s := newTestSQLite(t)
	if err := s.IndexMeta(ctxBG, "a.md", []byte("---\nstatus: draft\n---\n")); err != nil {
		t.Fatalf("IndexMeta: %v", err)
	}
	if err := s.RemoveMeta(ctxBG, "a.md"); err != nil {
		t.Fatalf("RemoveMeta: %v", err)
	}
	var n int
	_ = s.readDB.QueryRow(`SELECT COUNT(*) FROM file_meta WHERE path = ?`, "a.md").Scan(&n)
	if n != 0 {
		t.Fatalf("expected row removed, got %d", n)
	}
}

func TestQueryMeta(t *testing.T) {
	s := newTestSQLite(t)

	files := map[string]string{
		"a.md": "---\nstatus: published\npriority: high\nderived-from:\n  - type: run\n    id: run-249\n---\n",
		"b.md": "---\nstatus: published\npriority: low\nderived-from:\n  - type: run\n    id: run-250\n---\n",
		"c.md": "---\nstatus: draft\npriority: high\n---\n",
		"d.md": "---\nstatus: published\npriority: high\nderived-from:\n  - type: run\n    id: run-249\n  - type: commit\n    id: abc123\n---\n",
	}
	for path, body := range files {
		if err := s.IndexMeta(ctxBG, path, []byte(body)); err != nil {
			t.Fatalf("IndexMeta(%s): %v", path, err)
		}
	}

	// Single filter.
	got, err := s.QueryMeta(ctxBG, []MetaFilter{{Field: "$.status", Op: "=", Value: "published"}}, "", "", 0, 0)
	if err != nil {
		t.Fatalf("single filter: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 published, got %d: %+v", len(got), got)
	}

	// Two filters (AND).
	got, err = s.QueryMeta(ctxBG, []MetaFilter{
		{Field: "$.status", Op: "=", Value: "published"},
		{Field: "$.priority", Op: "=", Value: "high"},
	}, "", "", 0, 0)
	if err != nil {
		t.Fatalf("two filters: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 results, got %d: %+v", len(got), got)
	}

	// Array query — derived-from[*].id = run-249 must match a.md and d.md.
	got, err = s.QueryMeta(ctxBG, []MetaFilter{{Field: "$.derived-from[*].id", Op: "=", Value: "run-249"}}, "", "", 0, 0)
	if err != nil {
		t.Fatalf("array filter: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 matches for run-249, got %d: %+v", len(got), got)
	}

	// Sort asc by priority. "high" < "low" lexically.
	got, err = s.QueryMeta(ctxBG, nil, "$.priority", "asc", 0, 0)
	if err != nil {
		t.Fatalf("sort: %v", err)
	}
	if len(got) < 3 {
		t.Fatalf("expected at least 3, got %d", len(got))
	}
	if got[0].Frontmatter["priority"] != "high" {
		t.Fatalf("sort order wrong: %+v", got[0])
	}

	// Pagination.
	page, err := s.QueryMeta(ctxBG, nil, "", "", 2, 0)
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("page1 expected 2, got %d", len(page))
	}
	page2, err := s.QueryMeta(ctxBG, nil, "", "", 2, 2)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2) != 2 || page2[0].Path == page[0].Path {
		t.Fatalf("pagination overlapped: %+v / %+v", page, page2)
	}

	// Bad field is rejected.
	if _, err := s.QueryMeta(ctxBG, []MetaFilter{{Field: "status", Op: "=", Value: "x"}}, "", "", 0, 0); err == nil {
		t.Fatalf("expected error for missing $. prefix")
	}
	if _, err := s.QueryMeta(ctxBG, []MetaFilter{{Field: "$.status", Op: "DROP", Value: "x"}}, "", "", 0, 0); err == nil {
		t.Fatalf("expected error for invalid op")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestConcurrentSearchAndIndex exercises the dual-pool design: readers
// should never observe SQLITE_BUSY while a writer is active, because WAL
// lets them run against a consistent snapshot instead of contending for
// the write lock.
func TestConcurrentSearchAndIndex(t *testing.T) {
	s := newTestSQLite(t)

	// Seed so the FTS index has something to match against.
	for i := 0; i < 20; i++ {
		if err := s.Index(ctxBG, fmt.Sprintf("seed-%d.md", i), []byte("alpha beta gamma")); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	var (
		wg       sync.WaitGroup
		stop     atomic.Bool
		searchN  atomic.Int64
		writeN   atomic.Int64
		firstErr atomic.Value // string
	)

	// 10 concurrent readers, 1 concurrent writer.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if _, err := s.Search(ctxBG, "alpha", 5, 0); err != nil {
					firstErr.CompareAndSwap(nil, err.Error())
					return
				}
				searchN.Add(1)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for !stop.Load() {
			path := fmt.Sprintf("w/%d.md", i)
			if err := s.Index(ctxBG, path, []byte("alpha beta delta")); err != nil {
				firstErr.CompareAndSwap(nil, err.Error())
				return
			}
			writeN.Add(1)
			i++
		}
	}()

	time.Sleep(500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		t.Fatalf("concurrent op failed: %v", v)
	}
	if searchN.Load() == 0 || writeN.Load() == 0 {
		t.Fatalf("expected some progress on both (reads=%d writes=%d)", searchN.Load(), writeN.Load())
	}
}

// TestReindexBatched commits every reindexBatchSize files so searches are
// unblocked mid-walk. With 1200 files we expect at least one intermediate
// commit — its row count is observable through the readDB during the walk.
func TestReindexBatched(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	// Populate > batchSize files directly on the storage layer before we
	// open SQLite — avoids recursively Index'ing through the writer.
	const total = reindexBatchSize*2 + 100
	for i := 0; i < total; i++ {
		if err := store.Write(context.Background(), fmt.Sprintf("%d.md", i), []byte("content")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	s, err := NewSQLite(dir, store)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer s.Close()

	var n int
	if err := s.readDB.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != total {
		t.Fatalf("expected %d rows after reindex, got %d", total, n)
	}
}

// TestRemoveAllClearsEveryTable verifies docs/links/file_meta all drop in a
// single tx so the three indices never diverge after a delete.
func TestRemoveAllClearsEveryTable(t *testing.T) {
	s := newTestSQLite(t)

	content := []byte("---\nstatus: draft\n---\n# hi [[other]]\n")
	if err := s.Index(ctxBG, "a.md", content); err != nil {
		t.Fatalf("Index: %v", err)
	}
	if err := s.IndexLinks(ctxBG, "a.md", []string{"other"}); err != nil {
		t.Fatalf("IndexLinks: %v", err)
	}
	if err := s.IndexMeta(ctxBG, "a.md", content); err != nil {
		t.Fatalf("IndexMeta: %v", err)
	}
	if err := s.RemoveAll(ctxBG, "a.md"); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}

	for _, q := range []string{
		`SELECT COUNT(*) FROM docs WHERE path = 'a.md'`,
		`SELECT COUNT(*) FROM links WHERE source = 'a.md'`,
		`SELECT COUNT(*) FROM file_meta WHERE path = 'a.md'`,
	} {
		var n int
		if err := s.readDB.QueryRow(q).Scan(&n); err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		if n != 0 {
			t.Fatalf("%q left %d rows", q, n)
		}
	}
}
