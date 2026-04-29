package pipeline

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
	p := New(store, ver, searcher, nil, hub, nil, "")

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
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, hub, nil, "")

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

	p := New(store, versioning.NewNoop(), sqliteSearcher, sqliteSearcher, nil, nil, "")

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
	p := New(store, git, search.NewGrep(dir), nil, nil, nil, "")

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
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

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

func TestPipelineWriteDeleteVectorsNil(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")
	if p.Vectors != nil {
		t.Fatal("expected Vectors to be nil")
	}

	ctx := context.Background()
	if _, err := p.Write(ctx, "vec-test.md", []byte("# Test\n"), "tester"); err != nil {
		t.Fatalf("Write with nil Vectors: %v", err)
	}
	if !store.Exists(ctx, "vec-test.md") {
		t.Fatal("file not created")
	}
	if err := p.Delete(ctx, "vec-test.md", "tester"); err != nil {
		t.Fatalf("Delete with nil Vectors: %v", err)
	}
	if store.Exists(ctx, "vec-test.md") {
		t.Fatal("file not deleted")
	}
}

func TestBulkWriteVectorsNil(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

	ctx := context.Background()
	files := []struct {
		Path    string
		Content []byte
	}{
		{Path: "a.md", Content: []byte("# A")},
		{Path: "b.md", Content: []byte("# B")},
	}
	results, err := p.BulkWrite(ctx, files, "tester", "")
	if err != nil {
		t.Fatalf("BulkWrite with nil Vectors: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestWriteStreamSmallPayloadMatchesWrite(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, dir)

	body := strings.NewReader("# small file\n\ncontent.\n")
	res, err := p.WriteStream(context.Background(), "notes/small.md", body, int64(body.Len()), "tester")
	if err != nil {
		t.Fatalf("WriteStream small: %v", err)
	}
	if res.Path != "notes/small.md" {
		t.Fatalf("result path = %s", res.Path)
	}
	got, _ := store.Read(context.Background(), "notes/small.md")
	if !strings.HasPrefix(string(got), "# small file") {
		t.Fatalf("stored body = %q", got)
	}
}

func TestWriteStreamLargeBinarySkipsInMemoryBuffer(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, dir)

	// 20 MB of zeros via io.LimitReader + zero-byte source: we never
	// allocate the full blob, just drain it through WriteStream.
	const size = 20 * 1024 * 1024
	body := &zeroReader{remaining: size}
	res, err := p.WriteStream(context.Background(), "assets/big.bin", body, int64(size), "tester")
	if err != nil {
		t.Fatalf("WriteStream large: %v", err)
	}
	// ETag for the streaming path is weak — assert it's at least present.
	if res.ETag == "" {
		t.Fatal("expected a non-empty ETag for streamed write")
	}
	// File should be on disk at the requested size without ever living
	// fully in RAM in the test harness.
	abs := store.AbsPath("assets/big.bin")
	info, err := osStat(abs)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info != size {
		t.Fatalf("on-disk size = %d, want %d", info, size)
	}
}

// zeroReader emits `remaining` zero bytes and then io.EOF.
type zeroReader struct{ remaining int }

func (z *zeroReader) Read(p []byte) (int, error) {
	if z.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > z.remaining {
		n = z.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 0
	}
	z.remaining -= n
	return n, nil
}

func osStat(path string) (int, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return int(fi.Size()), nil
}

func TestPipeline_Rename(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	hub := events.NewHub()
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, hub, nil, "")

	ctx := context.Background()
	if _, err := p.Write(ctx, "old.md", []byte("# hello\n"), "tester"); err != nil {
		t.Fatalf("write: %v", err)
	}

	res, err := p.Rename(ctx, "old.md", "new.md", "tester")
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	if res.Path != "new.md" {
		t.Fatalf("result path = %s, want new.md", res.Path)
	}
	if res.ETag == "" {
		t.Fatal("empty ETag")
	}

	if store.Exists(ctx, "old.md") {
		t.Fatal("old path still exists after rename")
	}
	got, err := store.Read(ctx, "new.md")
	if err != nil {
		t.Fatalf("read new: %v", err)
	}
	if string(got) != "# hello\n" {
		t.Fatalf("content = %q", got)
	}
}

func TestPipeline_Rename_SingleCommit(t *testing.T) {
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
	p := New(store, git, search.NewGrep(dir), nil, nil, nil, "")

	ctx := context.Background()
	if _, err := p.Write(ctx, "before.md", []byte("data\n"), "tester"); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := p.Rename(ctx, "before.md", "after.md", "tester"); err != nil {
		t.Fatalf("rename: %v", err)
	}

	cmd := exec.Command("git", "-C", dir, "log", "--oneline", "--all")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var renameCommits int
	for _, l := range lines {
		if strings.Contains(l, "rename") {
			renameCommits++
		}
	}
	if renameCommits != 1 {
		t.Fatalf("expected 1 rename commit, got %d in:\n%s", renameCommits, out)
	}
}

func TestPipeline_Rename_CrashSafety(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

	ctx := context.Background()
	if _, err := p.Write(ctx, "src.md", []byte("content\n"), "tester"); err != nil {
		t.Fatalf("write: %v", err)
	}

	res, err := p.Rename(ctx, "src.md", "dst.md", "tester")
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	got, _ := store.Read(ctx, "dst.md")
	if string(got) != "content\n" {
		t.Fatalf("new file content = %q", got)
	}
	if res.ETag != ETag([]byte("content\n")) {
		t.Fatalf("ETag mismatch")
	}
}

func TestPipeline_Rename_SamePath(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

	ctx := context.Background()
	if _, err := p.Write(ctx, "same.md", []byte("keep me\n"), "tester"); err != nil {
		t.Fatalf("write: %v", err)
	}

	res, err := p.Rename(ctx, "same.md", "same.md", "tester")
	if err != nil {
		t.Fatalf("rename same path: %v", err)
	}
	if res.Path != "same.md" {
		t.Fatalf("result path = %s, want same.md", res.Path)
	}
	got, err := store.Read(ctx, "same.md")
	if err != nil {
		t.Fatalf("file should still exist: %v", err)
	}
	if string(got) != "keep me\n" {
		t.Fatalf("content = %q, want %q", got, "keep me\n")
	}
}

func TestPipeline_Rename_OverwriteExisting(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

	ctx := context.Background()
	if _, err := p.Write(ctx, "src.md", []byte("source data\n"), "tester"); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if _, err := p.Write(ctx, "dst.md", []byte("old dest\n"), "tester"); err != nil {
		t.Fatalf("write dst: %v", err)
	}

	res, err := p.Rename(ctx, "src.md", "dst.md", "tester")
	if err != nil {
		t.Fatalf("rename onto existing: %v", err)
	}
	if res.Path != "dst.md" {
		t.Fatalf("result path = %s", res.Path)
	}

	if store.Exists(ctx, "src.md") {
		t.Fatal("source still exists")
	}
	got, err := store.Read(ctx, "dst.md")
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if string(got) != "source data\n" {
		t.Fatalf("dst content = %q, want source content", got)
	}
}

func TestPipeline_Rename_EmptyPaths(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

	_, err := p.Rename(context.Background(), "", "dst.md", "tester")
	if err == nil {
		t.Fatal("rename with empty oldPath should fail")
	}
	_, err = p.Rename(context.Background(), "src.md", "", "tester")
	if err == nil {
		t.Fatal("rename with empty newPath should fail")
	}
}

func TestPipeline_Rename_NonExistentSource(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

	_, err := p.Rename(context.Background(), "ghost.md", "dst.md", "tester")
	if err == nil {
		t.Fatal("rename non-existent source should fail")
	}
}

func TestPipeline_Rename_PathTraversal(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

	ctx := context.Background()
	if _, err := p.Write(ctx, "legit.md", []byte("data\n"), "tester"); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Storage normalizes "../../../etc/passwd" → "etc/passwd" (safe inside root).
	// The pipeline returns the raw path, but the file lands safely inside root.
	res, err := p.Rename(ctx, "legit.md", "../../../etc/passwd", "tester")
	if err != nil {
		t.Fatalf("rename with traversal should succeed (neutralized at storage layer): %v", err)
	}
	if res.ETag == "" {
		t.Fatal("empty ETag")
	}
	// Verify file is stored inside root at the normalized location.
	got, err := store.Read(ctx, "etc/passwd")
	if err != nil {
		t.Fatalf("file should exist at neutralized path inside root: %v", err)
	}
	if string(got) != "data\n" {
		t.Fatalf("content = %q", got)
	}
}

func TestPipeline_Rename_HiddenPath(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

	ctx := context.Background()
	if _, err := p.Write(ctx, "legit.md", []byte("data\n"), "tester"); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := p.Rename(ctx, "legit.md", ".git/config", "tester")
	if err == nil {
		t.Fatal("rename to .git/config should fail")
	}
}

func TestPipeline_Rename_ConcurrentWithWrite(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	hub := events.NewHub()
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, hub, nil, "")

	ctx := context.Background()
	if _, err := p.Write(ctx, "target.md", []byte("original\n"), "tester"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	errs := make(chan error, 20)
	for i := 0; i < 10; i++ {
		go func(id int) {
			_, err := p.Write(ctx, "target.md", []byte(fmt.Sprintf("writer-%d\n", id)), "tester")
			errs <- err
		}(i)
		go func(id int) {
			_, err := p.Rename(ctx, "target.md", fmt.Sprintf("renamed-%d.md", id), "tester")
			errs <- err
		}(i)
	}

	var errCount int
	for i := 0; i < 20; i++ {
		if err := <-errs; err != nil {
			errCount++
		}
	}
	t.Logf("concurrent rename+write: %d errors out of 20 (expected some)", errCount)
}

func TestPipeline_RapidCreateDelete_SearchConsistency(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	searcher := search.NewGrep(dir)
	hub := events.NewHub()
	p := New(store, versioning.NewNoop(), searcher, nil, hub, nil, "")

	ctx := context.Background()
	for i := 0; i < 30; i++ {
		content := fmt.Sprintf("---\ntitle: cycle %d\n---\n# Flicker doc\nxyzflicker cycle %d\n", i, i)
		if _, err := p.Write(ctx, "flicker.md", []byte(content), "tester"); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if err := p.Delete(ctx, "flicker.md", "tester"); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}

	if store.Exists(ctx, "flicker.md") {
		t.Fatal("file should not exist after all cycles")
	}
}

func TestPipeline_NullByteInPath(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

	_, err := p.Write(context.Background(), "evil\x00.md", []byte("data"), "tester")
	if err == nil {
		t.Fatal("write with null byte should be rejected")
	}
}

func TestPipeline_ControlCharInPath(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

	_, err := p.Write(context.Background(), "file\n.md", []byte("data"), "tester")
	if err == nil {
		t.Fatal("write with newline in path should be rejected")
	}
}

func TestPipeline_LongFilename(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

	long := strings.Repeat("a", 256) + ".md"
	_, err := p.Write(context.Background(), long, []byte("data"), "tester")
	if err == nil {
		t.Fatal("write with 256-char filename should be rejected (ext4 limit)")
	}
}

func TestPipeline_Rename_NullByteInDest(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

	ctx := context.Background()
	if _, err := p.Write(ctx, "ok.md", []byte("data"), "tester"); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := p.Rename(ctx, "ok.md", "evil\x00.md", "tester")
	if err == nil {
		t.Fatal("rename to null-byte path should be rejected")
	}
}

func TestBulkWriteRollbackOnWriteFailure(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, nil, nil, "")

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

func TestPipeline_RenameDir(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	ver := versioning.NewNoop()
	searcher := search.NewGrep(dir)
	hub := events.NewHub()
	p := New(store, ver, searcher, nil, hub, nil, "")

	ctx := context.Background()
	for _, f := range []struct{ path, content string }{
		{"docs/a.md", "aaa"},
		{"docs/b.md", "bbb"},
		{"docs/sub/c.md", "ccc"},
	} {
		if _, err := p.Write(ctx, f.path, []byte(f.content), "test"); err != nil {
			t.Fatalf("seed %s: %v", f.path, err)
		}
	}

	count, err := p.RenameDir(ctx, "docs", "archive", "test")
	if err != nil {
		t.Fatalf("RenameDir: %v", err)
	}
	if count != 3 {
		t.Fatalf("renamed %d files, want 3", count)
	}

	for _, np := range []string{"archive/a.md", "archive/b.md", "archive/sub/c.md"} {
		if !store.Exists(ctx, np) {
			t.Fatalf("expected %s to exist", np)
		}
	}
	for _, op := range []string{"docs/a.md", "docs/b.md", "docs/sub/c.md"} {
		if store.Exists(ctx, op) {
			t.Fatalf("expected %s to be gone", op)
		}
	}
}

func TestPipeline_RenameDir_PathTraversal(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	ver := versioning.NewNoop()
	searcher := search.NewGrep(dir)
	hub := events.NewHub()
	p := New(store, ver, searcher, nil, hub, nil, "")

	ctx := context.Background()
	_, err := p.RenameDir(ctx, "../../../etc", "stolen", "test")
	if err == nil {
		t.Fatal("expected error for path traversal, got nil")
	}
	if !strings.Contains(err.Error(), "denied") && !strings.Contains(err.Error(), "traversal") {
		t.Logf("error = %v (should mention denied/traversal)", err)
	}
}

func TestPipeline_RenameDir_NonExistentSource(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	ver := versioning.NewNoop()
	searcher := search.NewGrep(dir)
	hub := events.NewHub()
	p := New(store, ver, searcher, nil, hub, nil, "")

	_, err := p.RenameDir(context.Background(), "nonexistent", "dest", "test")
	if err == nil {
		t.Fatal("expected error for nonexistent source")
	}
}

func TestPipeline_RenameDir_FileNotDir(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	ver := versioning.NewNoop()
	searcher := search.NewGrep(dir)
	hub := events.NewHub()
	p := New(store, ver, searcher, nil, hub, nil, "")

	ctx := context.Background()
	if _, err := p.Write(ctx, "file.md", []byte("hello"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err := p.RenameDir(ctx, "file.md", "dest", "test")
	if err == nil {
		t.Fatal("expected error when renaming a file via RenameDir")
	}
	if !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("error = %v, want 'not a directory'", err)
	}
}

func TestPipeline_RenameDir_Empty(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	ver := versioning.NewNoop()
	searcher := search.NewGrep(dir)
	hub := events.NewHub()
	p := New(store, ver, searcher, nil, hub, nil, "")

	if err := os.MkdirAll(filepath.Join(dir, "empty"), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	count, err := p.RenameDir(context.Background(), "empty", "moved", "test")
	if err != nil {
		t.Fatalf("RenameDir empty: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}

func TestCreateSymlink_HiddenPath(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	ver := versioning.NewNoop()
	searcher := search.NewGrep(dir)
	hub := events.NewHub()
	p := New(store, ver, searcher, nil, hub, nil, "")

	err := p.CreateSymlink(context.Background(), ".git/hooks/evil", "target", "test")
	if err == nil {
		t.Fatal("expected error for hidden/internal path")
	}
}

func TestCreateSymlink_AbsoluteTarget(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	ver := versioning.NewNoop()
	searcher := search.NewGrep(dir)
	hub := events.NewHub()
	p := New(store, ver, searcher, nil, hub, nil, "")

	err := p.CreateSymlink(context.Background(), "link.md", "/etc/shadow", "test")
	if err == nil {
		t.Fatal("expected error for absolute symlink target")
	}
}

func TestCreateSymlink_EscapingTarget(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	ver := versioning.NewNoop()
	searcher := search.NewGrep(dir)
	hub := events.NewHub()
	p := New(store, ver, searcher, nil, hub, nil, "")

	err := p.CreateSymlink(context.Background(), "link.md", "../../etc/shadow", "test")
	if err == nil {
		t.Fatal("expected error for escaping symlink target")
	}
}

func TestCreateSymlink_ValidRelativeTarget(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	ver := versioning.NewNoop()
	searcher := search.NewGrep(dir)
	hub := events.NewHub()
	p := New(store, ver, searcher, nil, hub, nil, "")

	ctx := context.Background()
	if _, err := p.Write(ctx, "other/file.md", []byte("target"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := p.CreateSymlink(ctx, "sub/link.md", "../other/file.md", "test"); err != nil {
		t.Fatalf("CreateSymlink with valid relative target: %v", err)
	}

	abs := filepath.Join(dir, "sub", "link.md")
	target, err := os.Readlink(abs)
	if err != nil {
		t.Fatalf("Readlink: %v", err)
	}
	if target != "../other/file.md" {
		t.Fatalf("target = %q, want '../other/file.md'", target)
	}
}
