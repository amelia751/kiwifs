package nfs

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/versioning"
)

func TestParseAllowAcceptsCIDRsAndBareIPs(t *testing.T) {
	got, err := ParseAllow("127.0.0.1, 10.0.0.0/8,  ::1")
	if err != nil {
		t.Fatalf("ParseAllow: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 nets, got %d", len(got))
	}
}

func TestParseAllowEmpty(t *testing.T) {
	got, err := ParseAllow("")
	if err != nil || got != nil {
		t.Fatalf("empty spec should return nil, got %v/%v", got, err)
	}
}

func TestParseAllowInvalid(t *testing.T) {
	if _, err := ParseAllow("not-an-ip"); err == nil {
		t.Fatal("expected error for invalid entry")
	}
}

func TestIPAllowed(t *testing.T) {
	_, lan, _ := net.ParseCIDR("10.0.0.0/8")
	allow := []*net.IPNet{lan}
	cases := map[string]struct {
		addr net.Addr
		ok   bool
	}{
		"in-range":  {&net.TCPAddr{IP: net.ParseIP("10.1.2.3"), Port: 2049}, true},
		"outside":   {&net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 2049}, false},
		"nil-addr":  {&net.TCPAddr{IP: nil, Port: 2049}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ipAllowed(tc.addr, allow); got != tc.ok {
				t.Fatalf("ipAllowed(%v) = %v, want %v", tc.addr, got, tc.ok)
			}
		})
	}
}

func TestDefaultAllowCoversLoopback(t *testing.T) {
	allow := DefaultAllow()
	if !ipAllowed(&net.TCPAddr{IP: net.ParseIP("127.0.0.1")}, allow) {
		t.Fatal("127.0.0.1 should be in DefaultAllow")
	}
	if !ipAllowed(&net.TCPAddr{IP: net.ParseIP("::1")}, allow) {
		t.Fatal("::1 should be in DefaultAllow")
	}
	if ipAllowed(&net.TCPAddr{IP: net.ParseIP("8.8.8.8")}, allow) {
		t.Fatal("8.8.8.8 should not be in DefaultAllow")
	}
}

// testFS builds a kiwiFS backed by a real storage + noop versioner for testing.
func testFS(t *testing.T) (*kiwiFS, storage.Storage) {
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

func TestRenameFile(t *testing.T) {
	fs, store := testFS(t)
	ctx := context.Background()

	if _, err := fs.pipe.Write(ctx, "old.md", []byte("# rename me\n"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := fs.Rename("old.md", "new.md"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	if store.Exists(ctx, "old.md") {
		t.Fatal("old path still exists")
	}
	got, err := store.Read(ctx, "new.md")
	if err != nil {
		t.Fatalf("read new: %v", err)
	}
	if string(got) != "# rename me\n" {
		t.Fatalf("content = %q", got)
	}
}

func TestRenameFile_Atomic(t *testing.T) {
	fs, _ := testFS(t)
	ctx := context.Background()

	if _, err := fs.pipe.Write(ctx, "src.md", []byte("data\n"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := fs.Rename("src.md", "dst.md"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	// With Noop versioner we can't check git log, but the rename went
	// through Pipeline.Rename which uses a single BulkCommit — verified
	// by TestPipeline_Rename_SingleCommit in the pipeline package.
}

func TestRenameDirectory(t *testing.T) {
	fs, store := testFS(t)
	ctx := context.Background()

	if _, err := fs.pipe.Write(ctx, "docs/a.md", []byte("aaa\n"), "test"); err != nil {
		t.Fatalf("seed a: %v", err)
	}
	if _, err := fs.pipe.Write(ctx, "docs/sub/b.md", []byte("bbb\n"), "test"); err != nil {
		t.Fatalf("seed b: %v", err)
	}

	if err := fs.Rename("docs", "renamed"); err != nil {
		t.Fatalf("Rename dir: %v", err)
	}

	if store.Exists(ctx, "docs/a.md") {
		t.Fatal("old docs/a.md still exists")
	}
	if store.Exists(ctx, "docs/sub/b.md") {
		t.Fatal("old docs/sub/b.md still exists")
	}

	gotA, err := store.Read(ctx, "renamed/a.md")
	if err != nil {
		t.Fatalf("read renamed/a.md: %v", err)
	}
	if string(gotA) != "aaa\n" {
		t.Fatalf("a.md content = %q", gotA)
	}

	gotB, err := store.Read(ctx, "renamed/sub/b.md")
	if err != nil {
		t.Fatalf("read renamed/sub/b.md: %v", err)
	}
	if string(gotB) != "bbb\n" {
		t.Fatalf("b.md content = %q", gotB)
	}
}

func TestWriteAt(t *testing.T) {
	fs, _ := testFS(t)

	f, err := fs.OpenFile("test.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	kf.buffer = []byte("hello world")
	n, err := kf.WriteAt([]byte("XY"), 5)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n != 2 {
		t.Fatalf("n = %d", n)
	}
	if string(kf.buffer) != "helloXYorld" {
		t.Fatalf("buffer = %q", kf.buffer)
	}
	kf.Close()
}

func TestWriteAt_Extend(t *testing.T) {
	fs, _ := testFS(t)

	f, err := fs.OpenFile("ext.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	kf.buffer = []byte("abc")
	n, err := kf.WriteAt([]byte("XY"), 5)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n != 2 {
		t.Fatalf("n = %d", n)
	}
	if len(kf.buffer) != 7 {
		t.Fatalf("len = %d, want 7", len(kf.buffer))
	}
	if kf.buffer[3] != 0 || kf.buffer[4] != 0 {
		t.Fatal("gap not zero-filled")
	}
	if string(kf.buffer[5:]) != "XY" {
		t.Fatalf("tail = %q", kf.buffer[5:])
	}
	kf.Close()
}

func TestTruncate_NonZero(t *testing.T) {
	fs, _ := testFS(t)
	f, err := fs.OpenFile("trunc.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	kf.buffer = make([]byte, 100)
	for i := range kf.buffer {
		kf.buffer[i] = 'A'
	}

	if err := kf.Truncate(50); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if len(kf.buffer) != 50 {
		t.Fatalf("len = %d, want 50", len(kf.buffer))
	}
	kf.Close()
}

func TestTruncate_Extend(t *testing.T) {
	fs, _ := testFS(t)
	f, err := fs.OpenFile("trunc2.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	kf.buffer = []byte("0123456789")
	if err := kf.Truncate(100); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if len(kf.buffer) != 100 {
		t.Fatalf("len = %d, want 100", len(kf.buffer))
	}
	for i := 10; i < 100; i++ {
		if kf.buffer[i] != 0 {
			t.Fatalf("byte[%d] = %d, want 0", i, kf.buffer[i])
		}
	}
	kf.Close()
}

func TestLock_Exclusive(t *testing.T) {
	fs, _ := testFS(t)

	f1, _ := fs.OpenFile("locked.md", os.O_RDWR|os.O_CREATE, 0644)
	f2, _ := fs.OpenFile("locked.md", os.O_RDWR|os.O_CREATE, 0644)
	kf1 := f1.(*kiwiFile)
	kf2 := f2.(*kiwiFile)

	if err := kf1.Lock(); err != nil {
		t.Fatalf("Lock f1: %v", err)
	}
	if err := kf2.Lock(); err == nil {
		t.Fatal("expected Lock f2 to fail while f1 holds lock")
	}

	kf1.Close()
	kf2.Close()
}

func TestLock_Release(t *testing.T) {
	fs, _ := testFS(t)

	f1, _ := fs.OpenFile("relock.md", os.O_RDWR|os.O_CREATE, 0644)
	kf1 := f1.(*kiwiFile)

	if err := kf1.Lock(); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := kf1.Unlock(); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	f2, _ := fs.OpenFile("relock.md", os.O_RDWR|os.O_CREATE, 0644)
	kf2 := f2.(*kiwiFile)
	if err := kf2.Lock(); err != nil {
		t.Fatalf("re-Lock after Unlock: %v", err)
	}
	kf1.Close()
	kf2.Close()
}

func TestLock_AutoRelease(t *testing.T) {
	fs, _ := testFS(t)

	f1, _ := fs.OpenFile("autorel.md", os.O_RDWR|os.O_CREATE, 0644)
	kf1 := f1.(*kiwiFile)
	if err := kf1.Lock(); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	kf1.Close()

	f2, _ := fs.OpenFile("autorel.md", os.O_RDWR|os.O_CREATE, 0644)
	kf2 := f2.(*kiwiFile)
	if err := kf2.Lock(); err != nil {
		t.Fatalf("Lock after Close should succeed: %v", err)
	}
	kf2.Close()
}

func TestSync(t *testing.T) {
	fs, store := testFS(t)
	ctx := context.Background()

	f, _ := fs.OpenFile("synced.md", os.O_RDWR|os.O_CREATE, 0644)
	kf := f.(*kiwiFile)
	kf.Write([]byte("# synced content\n"))

	if err := kf.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	got, err := store.Read(ctx, "synced.md")
	if err != nil {
		t.Fatalf("read after Sync: %v", err)
	}
	if string(got) != "# synced content\n" {
		t.Fatalf("content = %q", got)
	}
	kf.Close()
}

func TestClose_SkipAfterSync(t *testing.T) {
	fs, store := testFS(t)
	ctx := context.Background()

	f, _ := fs.OpenFile("skipdup.md", os.O_RDWR|os.O_CREATE, 0644)
	kf := f.(*kiwiFile)
	kf.Write([]byte("data\n"))
	kf.Sync()

	// Close should not re-write since buffer hasn't changed since Sync.
	kf.Close()

	got, _ := store.Read(ctx, "skipdup.md")
	if string(got) != "data\n" {
		t.Fatalf("content = %q", got)
	}
}

func TestRemove_Directory(t *testing.T) {
	fs, _ := testFS(t)

	dirPath := filepath.Join(fs.root, "emptydir")
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	if err := fs.Remove("emptydir"); err != nil {
		t.Fatalf("Remove dir: %v", err)
	}

	if _, err := os.Stat(dirPath); !os.IsNotExist(err) {
		t.Fatal("directory still exists after Remove")
	}
}

func TestRemove_File(t *testing.T) {
	fs, store := testFS(t)
	ctx := context.Background()

	if _, err := fs.pipe.Write(ctx, "deleteme.md", []byte("bye\n"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := fs.Remove("deleteme.md"); err != nil {
		t.Fatalf("Remove file: %v", err)
	}
	if store.Exists(ctx, "deleteme.md") {
		t.Fatal("file still exists after Remove")
	}
}

func TestRenameFile_OverwriteExisting(t *testing.T) {
	fs, store := testFS(t)
	ctx := context.Background()

	if _, err := fs.pipe.Write(ctx, "src.md", []byte("new content\n"), "test"); err != nil {
		t.Fatalf("seed src: %v", err)
	}
	if _, err := fs.pipe.Write(ctx, "dst.md", []byte("old content\n"), "test"); err != nil {
		t.Fatalf("seed dst: %v", err)
	}

	if err := fs.Rename("src.md", "dst.md"); err != nil {
		t.Fatalf("Rename onto existing: %v", err)
	}
	if store.Exists(ctx, "src.md") {
		t.Fatal("src should be gone")
	}
	got, err := store.Read(ctx, "dst.md")
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if string(got) != "new content\n" {
		t.Fatalf("dst content = %q, want source content", got)
	}
}

func TestWriteAt_FreshFile(t *testing.T) {
	fs, _ := testFS(t)
	f, err := fs.OpenFile("fresh.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	n, err := kf.WriteAt([]byte("hello"), 10)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n != 5 {
		t.Fatalf("n = %d", n)
	}
	if len(kf.buffer) != 15 {
		t.Fatalf("buffer len = %d, want 15", len(kf.buffer))
	}
	for i := 0; i < 10; i++ {
		if kf.buffer[i] != 0 {
			t.Fatalf("gap at %d = %d, want 0", i, kf.buffer[i])
		}
	}
	if string(kf.buffer[10:]) != "hello" {
		t.Fatalf("written = %q", kf.buffer[10:])
	}
	kf.Close()
}

func TestSync_SkipsDuplicate(t *testing.T) {
	fs, store := testFS(t)
	ctx := context.Background()

	if _, err := fs.pipe.Write(ctx, "sync.md", []byte("original\n"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	f, err := fs.OpenFile("sync.md", os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)
	kf.buffer = []byte("updated\n")

	if err := kf.Sync(); err != nil {
		t.Fatalf("Sync 1: %v", err)
	}
	got, _ := store.Read(ctx, "sync.md")
	if string(got) != "updated\n" {
		t.Fatalf("after sync 1: %q", got)
	}

	if err := kf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got2, _ := store.Read(ctx, "sync.md")
	if string(got2) != "updated\n" {
		t.Fatalf("after close (should skip re-flush): %q", got2)
	}
}

func TestLock_DoubleUnlock(t *testing.T) {
	fs, _ := testFS(t)
	f, err := fs.OpenFile("dbl.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	if err := kf.Lock(); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := kf.Unlock(); err != nil {
		t.Fatalf("Unlock 1: %v", err)
	}
	if err := kf.Unlock(); err != nil {
		t.Fatalf("Unlock 2 (double): %v", err)
	}
}

func TestTruncate_Negative(t *testing.T) {
	fs, _ := testFS(t)
	f, err := fs.OpenFile("neg.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)
	if err := kf.Truncate(-1); err == nil {
		t.Fatal("Truncate(-1) should return error")
	}
}

func TestClose_WritesBeforeUnlock(t *testing.T) {
	fs, store := testFS(t)
	ctx := context.Background()

	f, err := fs.OpenFile("locked.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	if err := kf.Lock(); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	kf.Write([]byte("locked write"))

	if err := kf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := store.Read(ctx, "locked.md")
	if err != nil {
		t.Fatalf("read after close: %v", err)
	}
	if string(got) != "locked write" {
		t.Fatalf("content = %q, want %q (data should be flushed before unlock)", got, "locked write")
	}

	fileLocksMu.Lock()
	_, held := fileLocks["locked.md"]
	fileLocksMu.Unlock()
	if held {
		t.Fatal("lock should be released after Close")
	}
}

func TestConcurrentLockWrite(t *testing.T) {
	fs, store := testFS(t)
	ctx := context.Background()

	if _, err := fs.pipe.Write(ctx, "concurrent.md", []byte("initial"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			f, err := fs.OpenFile("concurrent.md", os.O_RDWR, 0644)
			if err != nil {
				done <- err
				return
			}
			kf := f.(*kiwiFile)
			if err := kf.Lock(); err != nil {
				kf.Close()
				done <- nil
				return
			}
			kf.Write([]byte(fmt.Sprintf("writer-%d", id)))
			done <- kf.Close()
		}(i)
	}

	var errs []error
	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			errs = append(errs, err)
		}
	}

	got, err := store.Read(ctx, "concurrent.md")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("file should have content after concurrent writes")
	}
	t.Logf("final content: %q (one writer won), errors: %v", got, errs)
}

func TestOpen_DeletedFile_Write(t *testing.T) {
	fs, store := testFS(t)
	ctx := context.Background()

	if _, err := fs.pipe.Write(ctx, "ephemeral.md", []byte("exists"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	f, err := fs.OpenFile("ephemeral.md", os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	if err := fs.pipe.Delete(ctx, "ephemeral.md", "test"); err != nil {
		t.Fatalf("delete while open: %v", err)
	}
	if store.Exists(ctx, "ephemeral.md") {
		t.Fatal("file should be deleted on disk")
	}

	kf.Write([]byte("written after delete"))
	err = kf.Close()
	if err != nil {
		t.Logf("Close after delete returned error (acceptable): %v", err)
	} else {
		got, rerr := store.Read(ctx, "ephemeral.md")
		if rerr != nil {
			t.Logf("file not readable after re-creation (pipeline delete removed it): %v", rerr)
		} else {
			t.Logf("file recreated with content: %q", got)
		}
	}
}

func TestRapidCreateDelete(t *testing.T) {
	fs, store := testFS(t)
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		if _, err := fs.pipe.Write(ctx, "flicker.md", []byte(fmt.Sprintf("v%d", i)), "test"); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if err := fs.pipe.Delete(ctx, "flicker.md", "test"); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}

	if store.Exists(ctx, "flicker.md") {
		t.Fatal("file should be deleted after all cycles")
	}
}

func TestRename_NonExistent(t *testing.T) {
	fs, _ := testFS(t)
	err := fs.Rename("ghost.md", "moved.md")
	if err == nil {
		t.Fatal("rename non-existent should fail")
	}
}

func TestWriteAt_LargeOffset(t *testing.T) {
	fs, _ := testFS(t)
	f, err := fs.OpenFile("sparse.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	n, err := kf.WriteAt([]byte("end"), 1024*1024)
	if err != nil {
		t.Fatalf("WriteAt 1MB offset: %v", err)
	}
	if n != 3 {
		t.Fatalf("n = %d", n)
	}
	if len(kf.buffer) != 1024*1024+3 {
		t.Fatalf("buffer len = %d, want %d", len(kf.buffer), 1024*1024+3)
	}
	for i := 0; i < 1024*1024; i++ {
		if kf.buffer[i] != 0 {
			t.Fatalf("gap not zero-filled at %d", i)
			break
		}
	}
	kf.Close()
}

func TestWriteAt_HugeOffset_OOM_Prevention(t *testing.T) {
	fs, _ := testFS(t)
	f, err := fs.OpenFile("huge.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	_, err = kf.WriteAt([]byte("boom"), 1<<62)
	if err == nil {
		t.Fatal("WriteAt at huge offset should return error, not OOM")
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Fatalf("error should mention limit: %v", err)
	}
	kf.Close()
}

func TestWriteAt_NegativeOffset(t *testing.T) {
	fs, _ := testFS(t)
	f, err := fs.OpenFile("neg.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	_, err = kf.WriteAt([]byte("data"), -1)
	if err == nil {
		t.Fatal("negative offset should return error")
	}
	kf.Close()
}

func TestTruncate_HugeSize(t *testing.T) {
	fs, _ := testFS(t)
	f, err := fs.OpenFile("huge-trunc.md", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	kf := f.(*kiwiFile)

	err = kf.Truncate(1 << 62)
	if err == nil {
		t.Fatal("truncate to huge size should fail, not OOM")
	}
	kf.Close()
}

func TestOpenUnlink(t *testing.T) {
	kfs, _ := testFS(t)
	ctx := context.Background()

	if _, err := kfs.pipe.Write(ctx, "victim.md", []byte("doomed"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	f, err := kfs.Open("victim.md")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := kfs.Remove("victim.md"); err != nil {
		t.Fatalf("Remove while open: %v", err)
	}

	buf := make([]byte, 64)
	n, _ := f.Read(buf)
	if string(buf[:n]) != "doomed" {
		t.Fatalf("read after unlink = %q, want 'doomed'", buf[:n])
	}

	f.Close()

	fullPath := filepath.Join(kfs.root, "victim.md")
	if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
		t.Fatal("file should be gone from disk after close")
	}
}

func TestOpenUnlink_MultipleHandles(t *testing.T) {
	kfs, _ := testFS(t)
	ctx := context.Background()

	if _, err := kfs.pipe.Write(ctx, "multi.md", []byte("shared"), "test"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	f1, err := kfs.Open("multi.md")
	if err != nil {
		t.Fatalf("open f1: %v", err)
	}
	f2, err := kfs.Open("multi.md")
	if err != nil {
		t.Fatalf("open f2: %v", err)
	}

	if err := kfs.Remove("multi.md"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	f1.Close()

	pendingUnlinksMu.Lock()
	stillPending := pendingUnlinks["multi.md"]
	pendingUnlinksMu.Unlock()
	if !stillPending {
		t.Fatal("file should still be pending unlink after closing one handle")
	}

	f2.Close()

	pendingUnlinksMu.Lock()
	gone := !pendingUnlinks["multi.md"]
	pendingUnlinksMu.Unlock()
	if !gone {
		t.Fatal("pending unlink should be cleared after closing all handles")
	}
}

func TestOpenUnlink_ReadDir(t *testing.T) {
	kfs, _ := testFS(t)
	ctx := context.Background()

	if _, err := kfs.pipe.Write(ctx, "visible.md", []byte("keep"), "test"); err != nil {
		t.Fatalf("seed visible: %v", err)
	}
	if _, err := kfs.pipe.Write(ctx, "hidden.md", []byte("gone"), "test"); err != nil {
		t.Fatalf("seed hidden: %v", err)
	}

	f, err := kfs.Open("hidden.md")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := kfs.Remove("hidden.md"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	entries, err := kfs.ReadDir("/")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), "hidden") || strings.Contains(e.Name(), "kiwi-unlinked") {
			t.Fatalf("ReadDir should not show unlinked file, got %q", e.Name())
		}
	}

	f.Close()
}
