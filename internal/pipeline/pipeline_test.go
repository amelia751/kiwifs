package pipeline

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/versioning"
)

// ETag must match `git hash-object` so the ETag is a real git blob
// identifier, not a sha256 prefix.
func TestETagMatchesGitBlobHash(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not on PATH")
	}
	inputs := [][]byte{
		[]byte(""),
		[]byte("hello\n"),
		[]byte("# heading\n\nbody paragraph.\n"),
	}
	for _, in := range inputs {
		cmd := exec.Command("git", "hash-object", "--stdin")
		cmd.Stdin = strings.NewReader(string(in))
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("git hash-object: %v", err)
		}
		want := strings.TrimSpace(string(out))
		got := ETag(in)
		if got != want {
			t.Fatalf("ETag(%q)=%s, want %s", string(in), got, want)
		}
	}
}

func TestPipelineWriteDeleteFansOut(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	ver := versioning.NewNoop()
	searcher := search.NewGrep(dir)
	hub := events.NewHub()
	p := New(store, ver, searcher, nil, hub, nil)

	// Subscribe to SSE so we can verify the broadcast.
	ch, err := hub.Subscribe()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer hub.Unsubscribe(ch)

	ctx := context.Background()
	res, err := p.Write(ctx, "note.md", []byte("# hi\n"), "tester")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if res.ETag == "" {
		t.Fatalf("empty ETag")
	}
	// File ended up on disk.
	if !store.Exists(context.Background(), "note.md") {
		t.Fatalf("file missing")
	}
	// SSE event carries op=write.
	select {
	case msg := <-ch:
		if msg.Op != "write" {
			t.Fatalf("want op=write, got %s", msg.Op)
		}
	default:
		t.Fatalf("no SSE message received")
	}

	if err := p.Delete(ctx, "note.md", "tester"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if store.Exists(context.Background(), "note.md") {
		t.Fatalf("file still present after delete")
	}
}

// TestObserveSkippedAfterWrite verifies the inflight-tracking echo guard:
// a REST write triggers an fsnotify event later, and Observe must not
// re-run every side effect (especially re-enqueueing a vector embedding).
func TestObserveSkippedAfterWrite(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	hub := events.NewHub()
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, hub, nil)

	ch, err := hub.Subscribe()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer hub.Unsubscribe(ch)

	ctx := context.Background()
	if _, err := p.Write(ctx, "note.md", []byte("x"), "rest"); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Drain the one expected SSE event.
	<-ch
	// Simulate the watcher re-observing the same path right after — it
	// should be swallowed by the inflight set so no second SSE fires.
	p.Observe(ctx, "note.md", []byte("x"), "fswatch")
	select {
	case msg := <-ch:
		t.Fatalf("unexpected echo event: %+v", msg)
	default:
	}
}

// TestPipelineIndexesMetaViaSQLite exercises the optional metaIndexer hook.
// Grep search doesn't implement it, so we use the SQLite backend and verify
// that write → delete keeps the file_meta table in sync.
func TestPipelineIndexesMetaViaSQLite(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	sqliteSearcher, err := search.NewSQLite(dir, store)
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer sqliteSearcher.Close()

	p := New(store, versioning.NewNoop(), sqliteSearcher, sqliteSearcher, nil, nil)

	ctx := context.Background()
	content := []byte("---\nstatus: published\npriority: high\n---\n# Hello\n")
	if _, err := p.Write(ctx, "a.md", content, "tester"); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Inspect the meta row via QueryMeta (end-to-end check through the
	// same code path the REST handler will use).
	results, err := sqliteSearcher.QueryMeta(ctx,
		[]search.MetaFilter{{Field: "$.status", Op: "=", Value: "published"}},
		"", "", 0, 0,
	)
	if err != nil {
		t.Fatalf("QueryMeta: %v", err)
	}
	if len(results) != 1 || results[0].Path != "a.md" {
		t.Fatalf("expected 1 result for a.md, got %+v", results)
	}
	if results[0].Frontmatter["priority"] != "high" {
		t.Fatalf("priority missing: %+v", results[0].Frontmatter)
	}

	// Delete should fan out to RemoveMeta.
	if err := p.Delete(ctx, "a.md", "tester"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	results, err = sqliteSearcher.QueryMeta(ctx,
		[]search.MetaFilter{{Field: "$.status", Op: "=", Value: "published"}},
		"", "", 0, 0,
	)
	if err != nil {
		t.Fatalf("QueryMeta after delete: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected no rows after delete, got %+v", results)
	}
}

// TestPipelineConcurrentWritesGitNoDeadlockOrIndexCorruption drives many
// parallel Pipeline.Write calls through a real Git versioner. The pipeline
// holds writeMu, Git no longer holds its own — verifies that one lock is
// enough (no index.lock collisions) and that nobody deadlocks.
func TestPipelineConcurrentWritesGitNoDeadlockOrIndexCorruption(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not on PATH")
	}
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	git, err := versioning.NewGit(dir)
	if err != nil {
		t.Fatalf("git: %v", err)
	}
	p := New(store, git, search.NewGrep(dir), nil, nil, nil)

	const writers = 16
	done := make(chan error, writers)
	start := make(chan struct{})
	ctx := context.Background()
	for i := 0; i < writers; i++ {
		i := i
		go func() {
			<-start
			path := "f" + string(rune('a'+i)) + ".md"
			_, err := p.Write(ctx, path, []byte("body\n"), "tester")
			done <- err
		}()
	}
	close(start)
	for i := 0; i < writers; i++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent write %d: %v", i, err)
		}
	}
	// Every write must have produced a commit reachable by Log.
	for i := 0; i < writers; i++ {
		path := "f" + string(rune('a'+i)) + ".md"
		vs, err := git.Log(context.Background(), path)
		if err != nil || len(vs) == 0 {
			t.Fatalf("log %s: %v %d", path, err, len(vs))
		}
	}
}

// TestPipelineWriteRespectsCancelledContext checks that a caller who has
// already given up (HTTP client disconnect, server-shutdown signal) gets
// context.Canceled back without hitting the storage / versioner / index.
// Phase 1 of context propagation only checks ctx.Err() at the gates of
// each method — once Store/Versioner accept ctx themselves we'll cover the
// mid-flight cancellation case too.
func TestPipelineWriteRespectsCancelledContext(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := p.Write(ctx, "should-not-write.md", []byte("nope"), "tester"); err != context.Canceled {
		t.Fatalf("Write: want context.Canceled, got %v", err)
	}
	if store.Exists(context.Background(), "should-not-write.md") {
		t.Fatalf("Write created the file even though ctx was already cancelled")
	}
	if err := p.Delete(ctx, "should-not-write.md", "tester"); err != context.Canceled {
		t.Fatalf("Delete: want context.Canceled, got %v", err)
	}
	if _, err := p.BulkWrite(ctx, []struct {
		Path    string
		Content []byte
	}{{Path: "a.md", Content: []byte("x")}}, "tester", ""); err != context.Canceled {
		t.Fatalf("BulkWrite: want context.Canceled, got %v", err)
	}
	if store.Exists(context.Background(), "a.md") {
		t.Fatalf("BulkWrite wrote a file under a cancelled ctx")
	}
}

func TestBulkWriteRollbackOnWriteFailure(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil)

	// Seed one existing file.
	if err := store.Write(context.Background(), "existing.md", []byte("before\n")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Make "badslot" a directory so attempting to write a file at that
	// path produces an error mid-batch.
	if err := store.Write(context.Background(), "badslot/placeholder.md", []byte("x")); err != nil {
		t.Fatalf("seed badslot: %v", err)
	}

	files := []struct {
		Path    string
		Content []byte
	}{
		{Path: "existing.md", Content: []byte("after\n")},
		{Path: "badslot", Content: []byte("can't write — path is a directory")},
	}
	_, err = p.BulkWrite(context.Background(), files, "tester", "")
	if err == nil {
		t.Fatalf("expected error when writing over a directory")
	}
	// Original content must be restored by rollback.
	got, _ := store.Read(context.Background(), "existing.md")
	if string(got) != "before\n" {
		t.Fatalf("rollback failed: got %q", got)
	}
}
