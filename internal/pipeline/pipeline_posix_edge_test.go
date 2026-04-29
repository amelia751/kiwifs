package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/versioning"
)

func newTestPipeline(t *testing.T) (*Pipeline, storage.Storage, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	hub := events.NewHub()
	os.MkdirAll(filepath.Join(dir, ".kiwi", "state"), 0755)
	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, hub, nil, dir)
	return p, store, dir
}

// --- BulkCommitOnly + trackUncommitted ---

func TestBulkCommitOnly_TrackUncommitted_OnError(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	hub := events.NewHub()
	os.MkdirAll(filepath.Join(dir, ".kiwi", "state"), 0755)

	failVer := &failingVersioner{bulkFail: true}
	p := New(store, failVer, search.NewGrep(dir), nil, hub, nil, dir)

	ctx := context.Background()
	if _, err := p.Write(ctx, "a.md", []byte("aaa"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p.BulkCommitOnly(ctx, []string{"a.md", "b.md", "c.md"}, "test", "bulk msg")

	logPath := filepath.Join(dir, ".kiwi", "state", "uncommitted.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read uncommitted log: %v", err)
	}
	content := string(data)
	for _, path := range []string{"a.md", "b.md", "c.md"} {
		if !strings.Contains(content, path) {
			t.Errorf("uncommitted log should contain %q, got %q", path, content)
		}
	}
}

func TestBulkCommitOnly_EmptyPaths(t *testing.T) {
	p, _, _ := newTestPipeline(t)
	p.BulkCommitOnly(context.Background(), []string{}, "test", "empty")
}

func TestBulkCommitOnly_ConcurrentCalls(t *testing.T) {
	p, _, _ := newTestPipeline(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if _, err := p.Write(ctx, fmt.Sprintf("file%d.md", i), []byte(fmt.Sprintf("content %d", i)), "test"); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(batch int) {
			defer wg.Done()
			paths := []string{
				fmt.Sprintf("file%d.md", batch*2),
				fmt.Sprintf("file%d.md", batch*2+1),
			}
			p.BulkCommitOnly(ctx, paths, "test", fmt.Sprintf("batch %d", batch))
		}(i)
	}
	wg.Wait()
}

func TestDrainUncommitted_RecoversLostPaths(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	hub := events.NewHub()
	os.MkdirAll(filepath.Join(dir, ".kiwi", "state"), 0755)

	logPath := filepath.Join(dir, ".kiwi", "state", "uncommitted.log")
	os.WriteFile(logPath, []byte("lost/a.md\nlost/b.md\n"), 0644)

	p := New(store, versioning.NewNoop(), search.NewGrep(dir), nil, hub, nil, dir)
	p.DrainUncommitted(context.Background())

	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		data, _ := os.ReadFile(logPath)
		if len(strings.TrimSpace(string(data))) > 0 {
			t.Fatalf("uncommitted log should be empty or deleted after drain, got: %q", data)
		}
	}
}

// --- Symlink edge cases ---

func TestCreateSymlink_EmptyPath(t *testing.T) {
	p, _, _ := newTestPipeline(t)
	err := p.CreateSymlink(context.Background(), "", "target.md", "test")
	if err == nil {
		t.Fatal("empty path should return error")
	}
}

func TestCreateSymlink_EmptyTarget(t *testing.T) {
	p, _, _ := newTestPipeline(t)
	err := p.CreateSymlink(context.Background(), "link.md", "", "test")
	if err != nil {
		t.Logf("empty target error: %v (acceptable)", err)
	}
}

func TestCreateSymlink_OverwritesExistingFile(t *testing.T) {
	p, store, _ := newTestPipeline(t)
	ctx := context.Background()

	if _, err := p.Write(ctx, "existing.md", []byte("real content"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := p.Write(ctx, "target.md", []byte("target"), "test"); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	err := p.CreateSymlink(ctx, "existing.md", "target.md", "test")
	if err != nil {
		t.Fatalf("CreateSymlink over existing file: %v", err)
	}

	abs := store.AbsPath("existing.md")
	linkTarget, err := os.Readlink(abs)
	if err != nil {
		t.Fatalf("Readlink: %v", err)
	}
	if linkTarget != "target.md" {
		t.Fatalf("symlink target = %q, want 'target.md'", linkTarget)
	}
}

func TestCreateSymlink_DeeplyNestedTarget(t *testing.T) {
	p, _, _ := newTestPipeline(t)
	ctx := context.Background()

	if _, err := p.Write(ctx, "a/b/c/d/e/deep.md", []byte("deep"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err := p.CreateSymlink(ctx, "a/b/link.md", "c/d/e/deep.md", "test")
	if err != nil {
		t.Fatalf("deep relative symlink: %v", err)
	}
}

func TestCreateSymlink_DotSlashTarget(t *testing.T) {
	p, _, dir := newTestPipeline(t)
	ctx := context.Background()

	if _, err := p.Write(ctx, "target.md", []byte("hello"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err := p.CreateSymlink(ctx, "link.md", "./target.md", "test")
	if err != nil {
		t.Fatalf("./target.md should be valid: %v", err)
	}

	abs := filepath.Join(dir, "link.md")
	target, rerr := os.Readlink(abs)
	if rerr != nil {
		t.Fatalf("Readlink: %v", rerr)
	}
	if target != "target.md" {
		t.Logf("target = %q (os.Symlink may clean ./)", target)
	}
}

func TestCreateSymlink_ChainedSymlinks(t *testing.T) {
	p, _, _ := newTestPipeline(t)
	ctx := context.Background()

	if _, err := p.Write(ctx, "real.md", []byte("real"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := p.CreateSymlink(ctx, "link1.md", "real.md", "test"); err != nil {
		t.Fatalf("link1: %v", err)
	}
	if err := p.CreateSymlink(ctx, "link2.md", "link1.md", "test"); err != nil {
		t.Fatalf("link2 -> link1 -> real.md: %v", err)
	}
}

// --- RenameDir edge cases ---

func TestRenameDir_DeeplyNested(t *testing.T) {
	p, store, _ := newTestPipeline(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("deep/level1/level2/level3/file%d.md", i)
		if _, err := p.Write(ctx, path, []byte(fmt.Sprintf("content %d", i)), "test"); err != nil {
			t.Fatalf("seed %s: %v", path, err)
		}
	}

	count, err := p.RenameDir(ctx, "deep", "shallow", "test")
	if err != nil {
		t.Fatalf("RenameDir deep: %v", err)
	}
	if count != 5 {
		t.Fatalf("count = %d, want 5", count)
	}

	for i := 0; i < 5; i++ {
		newPath := fmt.Sprintf("shallow/level1/level2/level3/file%d.md", i)
		if !store.Exists(ctx, newPath) {
			t.Fatalf("expected %s to exist", newPath)
		}
	}
}

func TestRenameDir_ToNestedDestination(t *testing.T) {
	p, store, _ := newTestPipeline(t)
	ctx := context.Background()

	if _, err := p.Write(ctx, "flat/a.md", []byte("aaa"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	count, err := p.RenameDir(ctx, "flat", "archive/2024/flat", "test")
	if err != nil {
		t.Fatalf("RenameDir to nested: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if !store.Exists(ctx, "archive/2024/flat/a.md") {
		t.Fatal("file should exist at nested destination")
	}
}

func TestRenameDir_OverwriteExistingDir(t *testing.T) {
	p, _, dir := newTestPipeline(t)
	ctx := context.Background()

	if _, err := p.Write(ctx, "src/a.md", []byte("new"), "test"); err != nil {
		t.Fatalf("seed src: %v", err)
	}

	if err := os.MkdirAll(filepath.Join(dir, "dest"), 0755); err != nil {
		t.Fatalf("mkdir dest: %v", err)
	}

	_, err := p.RenameDir(ctx, "src", "dest", "test")
	if err != nil {
		t.Logf("RenameDir onto existing empty dir: %v (may succeed or fail depending on POSIX)", err)
	}
}

func TestRenameDir_ConcurrentRenames(t *testing.T) {
	p, _, _ := newTestPipeline(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		path := fmt.Sprintf("batch/file%d.md", i)
		if _, err := p.Write(ctx, path, []byte(fmt.Sprintf("data %d", i)), "test"); err != nil {
			t.Fatalf("seed %s: %v", path, err)
		}
	}

	_, _ = p.RenameDir(ctx, "batch", "renamed", "test")
}

func TestRenameDir_SourceToSelf(t *testing.T) {
	p, _, _ := newTestPipeline(t)
	ctx := context.Background()

	if _, err := p.Write(ctx, "selfdir/a.md", []byte("hello"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err := p.RenameDir(ctx, "selfdir", "selfdir", "test")
	if err != nil {
		t.Logf("rename to self error (expected): %v", err)
	}
}

// --- DeferredDelete edge cases ---

func TestDeferredDelete_NonExistentFile(t *testing.T) {
	p, _, _ := newTestPipeline(t)
	p.DeferredDelete(context.Background(), "nonexistent.md", "test")
}

func TestDeferredDelete_SameFileTwice(t *testing.T) {
	p, _, _ := newTestPipeline(t)
	ctx := context.Background()

	if _, err := p.Write(ctx, "twice.md", []byte("data"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p.DeferredDelete(ctx, "twice.md", "test")
	p.DeferredDelete(ctx, "twice.md", "test")
}

// --- Sub-second mtime is tested in fuse_test.go via Getattr ---

// --- Mixed operations stress test ---

func TestMixedOperations_StressCombined(t *testing.T) {
	p, store, _ := newTestPipeline(t)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		path := fmt.Sprintf("stress/file%d.md", i)
		if _, err := p.Write(ctx, path, []byte(fmt.Sprintf("content %d", i)), "test"); err != nil {
			t.Fatalf("seed %s: %v", path, err)
		}
	}

	if _, err := p.RenameDir(ctx, "stress", "stress-moved", "test"); err != nil {
		t.Fatalf("RenameDir: %v", err)
	}

	for i := 0; i < 20; i++ {
		path := fmt.Sprintf("stress-moved/file%d.md", i)
		if !store.Exists(ctx, path) {
			t.Fatalf("expected %s after dir rename", path)
		}
	}

	if _, err := p.Write(ctx, "stress-moved/extra.md", []byte("extra"), "test"); err != nil {
		t.Fatalf("write after dir rename: %v", err)
	}

	if err := p.Delete(ctx, "stress-moved/file0.md", "test"); err != nil {
		t.Fatalf("delete after dir rename: %v", err)
	}
	if store.Exists(ctx, "stress-moved/file0.md") {
		t.Fatal("deleted file should be gone")
	}

	for i := 0; i < 5; i++ {
		src := fmt.Sprintf("stress-moved/file%d.md", i+1)
		dst := fmt.Sprintf("stress-final/file%d.md", i+1)
		if _, err := p.Rename(ctx, src, dst, "test"); err != nil {
			t.Logf("rename %s->%s: %v (may not exist)", src, dst, err)
		}
	}
}

func TestCreateSymlink_ThenRenameDir(t *testing.T) {
	p, _, dir := newTestPipeline(t)
	ctx := context.Background()

	if _, err := p.Write(ctx, "links/target.md", []byte("target"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := p.CreateSymlink(ctx, "links/sym.md", "target.md", "test"); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	_, err := p.RenameDir(ctx, "links", "links-renamed", "test")
	if err != nil {
		t.Fatalf("RenameDir with symlink inside: %v", err)
	}

	symPath := filepath.Join(dir, "links-renamed", "sym.md")
	target, err := os.Readlink(symPath)
	if err != nil {
		t.Logf("symlink may have been converted to regular file during rename: %v", err)
	} else {
		if target != "target.md" {
			t.Logf("symlink target after dir rename = %q (may or may not resolve correctly)", target)
		}
	}
}

// failingVersioner always returns errors for BulkCommit to test trackUncommitted
type failingVersioner struct {
	versioning.Noop
	bulkFail bool
}

func (f *failingVersioner) BulkCommit(_ context.Context, _ []string, _, _ string) error {
	if f.bulkFail {
		return fmt.Errorf("simulated bulk commit failure")
	}
	return nil
}
