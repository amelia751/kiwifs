package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kiwifs/kiwifs/internal/storage"
	_ "modernc.org/sqlite"
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
				if _, err := s.Search(ctxBG, "alpha", 5, 0, ""); err != nil {
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
	if err := s.readDB.QueryRow(`SELECT COUNT(*) FROM doc_paths`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != total {
		t.Fatalf("expected %d rows after reindex, got %d", total, n)
	}
}

// TestResyncReconcilesOutOfBandChanges exercises the startup catch-up:
// files written directly to storage (simulating a git pull or restore from
// backup) must show up in the index; files removed behind the server's
// back must disappear from the index.
func TestResyncReconcilesOutOfBandChanges(t *testing.T) {
	s := newTestSQLite(t)

	ctx := context.Background()
	// Stage stale.md in storage + index it, then drop only from storage
	// to simulate the server having missed a delete while it was down.
	if err := s.store.Write(ctx, "stale.md", []byte("# stale page")); err != nil {
		t.Fatalf("store.Write stale: %v", err)
	}
	if err := s.Index(ctx, "stale.md", []byte("# stale page")); err != nil {
		t.Fatalf("Index: %v", err)
	}
	if err := s.store.Delete(ctx, "stale.md"); err != nil {
		t.Fatalf("store.Delete: %v", err)
	}
	// Write fresh.md directly to storage — bypassing Index — to simulate
	// an out-of-band edit (e.g. git pull, backup restore).
	if err := s.store.Write(ctx, "fresh.md", []byte("# fresh page")); err != nil {
		t.Fatalf("store.Write fresh: %v", err)
	}

	added, removed, err := s.Resync(ctx)
	if err != nil {
		t.Fatalf("Resync: %v", err)
	}
	if added != 1 {
		t.Fatalf("expected 1 added, got %d", added)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}

	var count int
	if err := s.readDB.QueryRow(`SELECT COUNT(*) FROM doc_paths WHERE path = 'fresh.md'`).Scan(&count); err != nil {
		t.Fatalf("count fresh: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected fresh.md to be indexed, got %d rows", count)
	}
	if err := s.readDB.QueryRow(`SELECT COUNT(*) FROM doc_paths WHERE path = 'stale.md'`).Scan(&count); err != nil {
		t.Fatalf("count stale: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected stale.md to be removed, got %d rows", count)
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
		`SELECT COUNT(*) FROM doc_paths WHERE path = 'a.md'`,
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

func TestMigrationFromOldSchema(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	if err := store.Write(context.Background(), "hello.md", []byte("# Hello World\nSome content here")); err != nil {
		t.Fatalf("write: %v", err)
	}

	stateDir := dir + "/.kiwi/state"
	if merr := os.MkdirAll(stateDir, 0755); merr != nil {
		t.Fatalf("mkdir: %v", merr)
	}
	dbPath := stateDir + "/search.db"
	db, err := openTestDB(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err = db.Exec(`CREATE VIRTUAL TABLE docs USING fts5(
		path UNINDEXED, content,
		tokenize = 'porter unicode61 remove_diacritics 1')`)
	if err != nil {
		t.Fatalf("old schema: %v", err)
	}
	_, _ = db.Exec(`INSERT INTO docs(path, content) VALUES ('hello.md', '# Hello World')`)
	db.Close()

	s, err := NewSQLite(dir, store)
	if err != nil {
		t.Fatalf("NewSQLite after migration: %v", err)
	}
	defer s.Close()

	results, err := s.Search(context.Background(), "hello", 10, 0, "")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) == 0 {
		t.Fatalf("expected search results after migration, got 0")
	}
	if results[0].Path != "hello.md" {
		t.Fatalf("expected hello.md, got %s", results[0].Path)
	}
}

func openTestDB(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", path)
	return sql.Open("sqlite", dsn)
}

func TestBenchmarkContentless(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark in short mode")
	}
	const fileCount = 1000
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	var totalRaw int64
	for i := 0; i < fileCount; i++ {
		body := fmt.Sprintf(`---
status: published
tags:
  - tag%d
  - common
---
# Document %d — Architecture Decision Record

This is document number %d with realistic content that simulates a real knowledge base.
It contains multiple paragraphs of text covering various engineering topics.

## Context and Problem Statement

We need to decide on the deployment strategy for the new microservices architecture.
The current monolith deployment takes 45 minutes and requires downtime. Kubernetes
offers rolling deployments but adds operational complexity. The team has experience
with Docker but limited exposure to container orchestration at scale.

## Decision Drivers

- Deployment frequency target: 10+ per day
- Zero-downtime requirement from SLA commitments
- Team skill gap in container orchestration and observability
- Budget constraints on infrastructure spend
- Compliance requirements for audit trails and access control

## Considered Options

### Option A: Blue-Green Deployment with Load Balancer

Traditional approach using two identical environments. The load balancer switches
traffic atomically between blue and green. Simple rollback by switching back.
Infrastructure cost doubles since both environments run simultaneously.

### Option B: Kubernetes Rolling Updates

Gradual pod replacement with health checks. Built-in rollback via ReplicaSet history.
Requires investment in monitoring, alerting, and incident response procedures.
Service mesh adds mutual TLS and traffic management capabilities.

### Option C: Serverless Functions

Event-driven architecture using cloud functions. Pay-per-invocation pricing.
Cold start latency concerns for real-time endpoints. Vendor lock-in risk.
Limited debugging and observability tooling compared to containers.

## Technical Implementation Details

Database migrations require careful schema changes with backward compatibility.
The indexing strategy uses B-tree indexes for equality lookups and GIN indexes
for full-text search. Query optimization includes prepared statements, connection
pooling, and read replica routing for analytics workloads.

Performance tuning techniques include caching at multiple layers: application-level
LRU caches, Redis for session state, CDN for static assets, and database query
result caching. Load testing showed the system handles 5000 concurrent requests
with p99 latency under 200ms when properly tuned.

## References

- [[architecture-overview]] — System architecture diagram
- [[deployment-runbook]] — Step-by-step deployment guide
- [[monitoring-setup]] — Grafana dashboards and alert rules
`, i%50, i, i)
		path := fmt.Sprintf("docs/file-%04d.md", i)
		if err := store.Write(context.Background(), path, []byte(body)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		totalRaw += int64(len(body))
	}

	start := time.Now()
	s, err := NewSQLite(dir, store)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	defer s.Close()
	reindexDur := time.Since(start)

	s.writeDB.ExecContext(context.Background(), `PRAGMA wal_checkpoint(TRUNCATE)`)
	dbPath := filepath.Join(dir, ".kiwi", "state", "search.db")
	var dbSize int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if fi, serr := os.Stat(dbPath + suffix); serr == nil {
			dbSize += fi.Size()
		}
	}
	ratio := float64(dbSize) / float64(totalRaw)

	searchStart := time.Now()
	for i := 0; i < 100; i++ {
		if _, err := s.Search(context.Background(), "kubernetes deployment", 20, 0, ""); err != nil {
			t.Fatalf("search: %v", err)
		}
	}
	searchDur := time.Since(searchStart) / 100

	batchStart := time.Now()
	entries := make([]IndexEntry, 100)
	for i := range entries {
		entries[i] = IndexEntry{
			Path:    fmt.Sprintf("batch/b-%d.md", i),
			Content: []byte(fmt.Sprintf("# Batch %d\nBatch content for testing indexing speed.", i)),
		}
	}
	if err := s.IndexBatch(context.Background(), entries); err != nil {
		t.Fatalf("IndexBatch: %v", err)
	}
	batchDur := time.Since(batchStart)

	t.Logf("=== Contentless FTS5 Benchmark ===")
	t.Logf("Files:          %d", fileCount)
	t.Logf("Raw content:    %.2f MB", float64(totalRaw)/(1024*1024))
	t.Logf("DB size:        %.2f MB", float64(dbSize)/(1024*1024))
	t.Logf("Index/raw:      %.2fx", ratio)
	t.Logf("Reindex time:   %s", reindexDur.Round(time.Millisecond))
	t.Logf("Search latency: %s (avg of 100)", searchDur.Round(time.Microsecond))
	t.Logf("IndexBatch 100: %s", batchDur.Round(time.Millisecond))

	if ratio > 2.0 {
		t.Errorf("index/raw ratio %.2fx exceeds 2.0x target (want <1.5x)", ratio)
	}
}

func TestBuildFTS5Query_BareWildcard(t *testing.T) {
	q := buildFTS5Query("*")
	if q != "" {
		t.Fatalf("bare * should be rejected, got %q", q)
	}
	q2 := buildFTS5Query("***")
	if q2 != "" {
		t.Fatalf("*** should be rejected, got %q", q2)
	}
	q3 := buildFTS5Query("* * *")
	if q3 != "" {
		t.Fatalf("'* * *' should be rejected, got %q", q3)
	}
}

func TestBuildFTS5Query_NormalQuery(t *testing.T) {
	q := buildFTS5Query("hello world")
	if q == "" {
		t.Fatal("normal query should not be empty")
	}
}

func TestBuildFTS5Query_PrefixWildcard(t *testing.T) {
	q := buildFTS5Query("kube*")
	if q == "" {
		t.Fatal("prefix wildcard should be allowed")
	}
	if q != "kube*" {
		t.Fatalf("prefix wildcard should pass through: got %q", q)
	}
}
