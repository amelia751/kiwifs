package nfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/versioning"
)

func testPipeFS(t *testing.T) (*kiwiFS, storage.Storage) {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	hub := events.NewHub()
	p := pipeline.New(store, versioning.NewNoop(), search.NewGrep(dir), nil, hub, nil, "")
	return &kiwiFS{root: dir, pipe: p}, store
}

// --- Open-unlink edge cases ---

func TestOpenUnlink_WriteAfterUnlink(t *testing.T) {
	kfs, _ := testPipeFS(t)
	ctx := context.Background()

	if _, err := kfs.pipe.Write(ctx, "writable.md", []byte("initial"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	f, err := kfs.OpenFile("writable.md", os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	if err := kfs.Remove("writable.md"); err != nil {
		t.Fatalf("Remove while open: %v", err)
	}

	kf.Write([]byte("written after unlink"))
	err = kf.Close()
	t.Logf("Close after unlink+write: err=%v (deferred delete should fire)", err)
}

func TestOpenUnlink_ReopenAfterUnlink(t *testing.T) {
	kfs, _ := testPipeFS(t)
	ctx := context.Background()

	if _, err := kfs.pipe.Write(ctx, "reopen.md", []byte("data"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	f, err := kfs.Open("reopen.md")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := kfs.Remove("reopen.md"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	_, err = kfs.Open("reopen.md")
	if err == nil {
		t.Logf("reopening unlinked file succeeded (hidden path redirect)")
	} else {
		t.Logf("reopening unlinked file failed (expected): %v", err)
	}

	f.Close()
}

func TestOpenUnlink_ConcurrentRemoveAndRead(t *testing.T) {
	kfs, _ := testPipeFS(t)
	ctx := context.Background()

	if _, err := kfs.pipe.Write(ctx, "race.md", []byte("race condition"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	f, err := kfs.Open("race.md")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		kfs.Remove("race.md")
	}()

	go func() {
		defer wg.Done()
		buf := make([]byte, 64)
		n, _ := f.Read(buf)
		t.Logf("concurrent read during remove: %q", buf[:n])
	}()

	wg.Wait()
	f.Close()
}

func TestOpenUnlink_NestedDirFile(t *testing.T) {
	kfs, _ := testPipeFS(t)
	ctx := context.Background()

	if _, err := kfs.pipe.Write(ctx, "nested/deep/file.md", []byte("deep file"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	f, err := kfs.Open("nested/deep/file.md")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := kfs.Remove("nested/deep/file.md"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	buf := make([]byte, 64)
	n, _ := f.Read(buf)
	if string(buf[:n]) != "deep file" {
		t.Fatalf("read after unlink of nested file = %q, want 'deep file'", buf[:n])
	}

	f.Close()

	fullPath := filepath.Join(kfs.root, "nested/deep/file.md")
	if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
		t.Fatal("file should be gone from disk after close")
	}
}

// --- Symlink on NFS ---

func TestSymlink_NFS(t *testing.T) {
	kfs, _ := testPipeFS(t)
	ctx := context.Background()

	if _, err := kfs.pipe.Write(ctx, "real.md", []byte("real content"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := kfs.Symlink("real.md", "link.md"); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	target, err := kfs.Readlink("link.md")
	if err != nil {
		t.Fatalf("Readlink: %v", err)
	}
	if target != "real.md" {
		t.Fatalf("target = %q, want 'real.md'", target)
	}
}

func TestSymlink_NFS_Lstat(t *testing.T) {
	kfs, _ := testPipeFS(t)
	ctx := context.Background()

	if _, err := kfs.pipe.Write(ctx, "target.md", []byte("content"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := kfs.Symlink("target.md", "sym.md"); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	info, err := kfs.Lstat("sym.md")
	if err != nil {
		t.Fatalf("Lstat: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("Lstat should report ModeSymlink")
	}
}

func TestSymlink_NFS_AbsoluteTargetRejected(t *testing.T) {
	kfs, _ := testPipeFS(t)
	err := kfs.Symlink("/etc/passwd", "evil.md")
	if err == nil {
		t.Fatal("absolute symlink target should be rejected")
	}
}

func TestSymlink_NFS_EscapingTargetRejected(t *testing.T) {
	kfs, _ := testPipeFS(t)
	err := kfs.Symlink("../../../etc/shadow", "escape.md")
	if err == nil {
		t.Fatal("escaping symlink target should be rejected")
	}
}

// --- Directory rename on NFS ---

func TestRenameDir_NFS_WithMixedContent(t *testing.T) {
	kfs, store := testPipeFS(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("project/file%d.md", i)
		if _, err := kfs.pipe.Write(ctx, path, []byte(fmt.Sprintf("# File %d\n", i)), "test"); err != nil {
			t.Fatalf("seed %s: %v", path, err)
		}
	}
	if _, err := kfs.pipe.Write(ctx, "project/sub/nested.md", []byte("nested"), "test"); err != nil {
		t.Fatalf("seed nested: %v", err)
	}

	if err := kfs.Rename("project", "archive"); err != nil {
		t.Fatalf("Rename dir: %v", err)
	}

	for i := 0; i < 5; i++ {
		newPath := fmt.Sprintf("archive/file%d.md", i)
		if !store.Exists(ctx, newPath) {
			t.Fatalf("expected %s to exist", newPath)
		}
	}
	if !store.Exists(ctx, "archive/sub/nested.md") {
		t.Fatal("nested file should exist at new path")
	}
}

func TestRenameDir_NFS_OntoExistingEmptyDir(t *testing.T) {
	kfs, _ := testPipeFS(t)
	ctx := context.Background()

	if _, err := kfs.pipe.Write(ctx, "src/a.md", []byte("aaa"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := os.MkdirAll(filepath.Join(kfs.root, "dst"), 0755); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}

	err := kfs.Rename("src", "dst")
	if err != nil {
		t.Logf("rename onto existing empty dir: %v (platform-dependent)", err)
	}
}

func TestRenameDir_NFS_NonExistent(t *testing.T) {
	kfs, _ := testPipeFS(t)
	err := kfs.Rename("ghost", "moved")
	if err == nil {
		t.Fatal("rename nonexistent dir should fail")
	}
}

// --- WriteAt boundary conditions ---

func TestWriteAt_ExactlyMaxSize(t *testing.T) {
	kfs, _ := testPipeFS(t)
	f, err := kfs.OpenFile("max.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	data := make([]byte, 1024)
	_, err = kf.WriteAt(data, maxFileSize-1024)
	if err != nil {
		t.Fatalf("WriteAt at exactly maxFileSize boundary should succeed: %v", err)
	}

	_, err = kf.WriteAt([]byte("x"), maxFileSize)
	if err == nil {
		t.Fatal("WriteAt one byte past maxFileSize should fail")
	}
	kf.Close()
}

func TestWriteAt_ZeroLength(t *testing.T) {
	kfs, _ := testPipeFS(t)
	f, err := kfs.OpenFile("zero.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	n, err := kf.WriteAt([]byte{}, 100)
	if err != nil {
		t.Fatalf("zero-length WriteAt should succeed: %v", err)
	}
	if n != 0 {
		t.Fatalf("n = %d, want 0", n)
	}
	kf.Close()
}

// --- Truncate boundary conditions ---

func TestTruncate_ExactlyMaxSize(t *testing.T) {
	kfs, _ := testPipeFS(t)
	f, err := kfs.OpenFile("trunc-max.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	if err := kf.Truncate(maxFileSize); err != nil {
		t.Fatalf("Truncate to exactly maxFileSize should succeed: %v", err)
	}

	if err := kf.Truncate(maxFileSize + 1); err == nil {
		t.Fatal("Truncate past maxFileSize should fail")
	}
	kf.Close()
}

func TestTruncate_Zero(t *testing.T) {
	kfs, _ := testPipeFS(t)
	f, err := kfs.OpenFile("trunc-zero.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)
	kf.buffer = []byte("some data")

	if err := kf.Truncate(0); err != nil {
		t.Fatalf("Truncate(0): %v", err)
	}
	if kf.buffer != nil {
		t.Fatalf("buffer should be nil after Truncate(0), got %d bytes", len(kf.buffer))
	}
	kf.Close()
}

// --- Lock contention ---

func TestLock_StressContention(t *testing.T) {
	kfs, _ := testPipeFS(t)
	ctx := context.Background()

	if _, err := kfs.pipe.Write(ctx, "contend.md", []byte("initial"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var wg sync.WaitGroup
	successes := make(chan int, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			f, err := kfs.OpenFile("contend.md", os.O_RDWR, 0644)
			if err != nil {
				return
			}
			kf := f.(*kiwiFile)
			if err := kf.Lock(); err == nil {
				time.Sleep(time.Millisecond)
				successes <- id
				kf.Unlock()
			}
			kf.Close()
		}(i)
	}
	wg.Wait()
	close(successes)

	count := 0
	for range successes {
		count++
	}
	t.Logf("%d/%d goroutines acquired lock (sequential, not parallel — expected 1+)", count, 100)
	if count == 0 {
		t.Fatal("at least one goroutine should have acquired the lock")
	}
}

// --- Rapid create-delete-rename mix ---

func TestRapidMixedOps(t *testing.T) {
	kfs, store := testPipeFS(t)
	ctx := context.Background()

	for round := 0; round < 10; round++ {
		path := fmt.Sprintf("mix/round%d.md", round)
		if _, err := kfs.pipe.Write(ctx, path, []byte(fmt.Sprintf("round %d", round)), "test"); err != nil {
			t.Fatalf("write %d: %v", round, err)
		}

		newPath := fmt.Sprintf("mix/moved%d.md", round)
		if err := kfs.Rename(path, newPath); err != nil {
			t.Logf("rename %s -> %s: %v", path, newPath, err)
			continue
		}

		if err := kfs.Remove(newPath); err != nil {
			t.Logf("remove %s: %v", newPath, err)
		}
	}

	for round := 0; round < 10; round++ {
		path := fmt.Sprintf("mix/round%d.md", round)
		if store.Exists(ctx, path) {
			t.Fatalf("%s should not exist", path)
		}
	}
}

// --- Hidden files filtered from ReadDir ---

func TestReadDir_HidesInternalDirs(t *testing.T) {
	kfs, _ := testPipeFS(t)
	ctx := context.Background()

	if _, err := kfs.pipe.Write(ctx, "visible.md", []byte("yes"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	os.MkdirAll(filepath.Join(kfs.root, ".git"), 0755)
	os.MkdirAll(filepath.Join(kfs.root, ".kiwi"), 0755)
	os.WriteFile(filepath.Join(kfs.root, ".hidden"), []byte("no"), 0644)

	entries, err := kfs.ReadDir("/")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			t.Fatalf("dot-prefixed entry leaked: %q", e.Name())
		}
	}
}

// --- Stable handles ---

func TestStableHandles_DeterministicAcrossInstances(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, ".kiwi"), 0755)

	h1 := newStableHandles(dir)
	h2 := newStableHandles(dir)

	store, _ := storage.NewLocal(dir)
	fs := &kiwiFS{root: dir, pipe: nil}
	_ = store
	_ = fs

	handle1 := h1.deriveHandle("concepts/auth.md")
	handle2 := h2.deriveHandle("concepts/auth.md")

	if string(handle1) != string(handle2) {
		t.Fatal("same namespace + same path should produce identical handles across instances")
	}
}

func TestStableHandles_DifferentPathsDifferentHandles(t *testing.T) {
	dir := t.TempDir()
	h := newStableHandles(dir)

	ha := h.deriveHandle("a.md")
	hb := h.deriveHandle("b.md")

	if string(ha) == string(hb) {
		t.Fatal("different paths should produce different handles")
	}
}

func TestStableHandles_FromHandle_Stale(t *testing.T) {
	dir := t.TempDir()
	h := newStableHandles(dir)

	_, _, err := h.FromHandle([]byte("deadbeef12345678901234567890ab"))
	if err == nil {
		t.Fatal("unknown handle should return stale error")
	}
}

func TestStableHandles_InvalidateHandle(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	h := newStableHandles(dir)

	kfs := &kiwiFS{root: dir}
	_ = store

	handle := h.ToHandle(kfs, []string{"test", "file.md"})

	fs, path, err := h.FromHandle(handle)
	if err != nil {
		t.Fatalf("FromHandle before invalidate: %v", err)
	}
	if fs == nil || len(path) != 2 {
		t.Fatalf("unexpected result: fs=%v path=%v", fs, path)
	}

	h.InvalidateHandle(kfs, handle)

	_, _, err = h.FromHandle(handle)
	if err == nil {
		t.Fatal("handle should be stale after invalidation")
	}
}
