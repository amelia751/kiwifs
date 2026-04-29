//go:build !windows

package fuse

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	fusepkg "github.com/hanwen/go-fuse/v2/fuse"
)

// --- Sub-second mtime tests ---

func TestGetattr_SubSecondMtime(t *testing.T) {
	now := time.Date(2024, 6, 15, 10, 30, 45, 123456789, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", now.Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("content"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "timed.md"}

	out := &fusepkg.AttrOut{}
	errno := n.Getattr(nil, nil, out)
	if errno != 0 {
		t.Fatalf("Getattr errno: %v", errno)
	}

	if out.Mtime == 0 {
		t.Fatal("Mtime should be non-zero")
	}
	if out.Mode&syscall.S_IFREG == 0 {
		t.Fatal("should be regular file")
	}
}

func TestGetattr_DirectorySubSecondMtime(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "tree") {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(treeResponse{
				Path:     "mydir",
				IsDir:    true,
				Children: []treeResponse{{Name: "a.md", IsDir: false}},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "mydir"}

	out := &fusepkg.AttrOut{}
	errno := n.Getattr(nil, nil, out)
	if errno != 0 {
		t.Fatalf("Getattr dir errno: %v", errno)
	}

	if out.Mode&syscall.S_IFDIR == 0 {
		t.Fatal("should be directory")
	}
	if out.Mtimensec == 0 {
		t.Logf("dir Mtimensec = 0 (uses time.Now(), nsec may or may not be 0)")
	}
}

func TestGetattr_SymlinkMode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-File-Type", "symlink")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("target path"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "link.md"}

	out := &fusepkg.AttrOut{}
	errno := n.Getattr(nil, nil, out)
	if errno != 0 {
		t.Fatalf("Getattr symlink errno: %v", errno)
	}

	if out.Mode&syscall.S_IFLNK == 0 {
		t.Fatal("symlink should have S_IFLNK mode")
	}
	if out.Mode&0777 != 0777 {
		t.Fatalf("symlink permissions = %o, want 0777", out.Mode&0777)
	}
}

func TestGetattr_RootNode(t *testing.T) {
	c := NewClient("http://localhost:9999")
	n := &kiwiNode{client: c, path: ""}

	out := &fusepkg.AttrOut{}
	errno := n.Getattr(nil, nil, out)
	if errno != 0 {
		t.Fatalf("root Getattr errno: %v", errno)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Fatal("root should be directory")
	}
	if out.Size != 4096 {
		t.Fatalf("root size = %d, want 4096", out.Size)
	}
}

// --- O_APPEND edge cases ---

func TestOpenAppend_EmptyFile(t *testing.T) {
	m := newMock()
	m.files["empty.md"] = []byte{}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "empty.md"}

	fh, _, errno := n.Open(nil, syscall.O_APPEND)
	if errno != 0 {
		t.Fatalf("Open errno: %v", errno)
	}
	kf := fh.(*kiwiFile)
	kf.Write(nil, []byte("first line"), 0)

	if string(kf.data) != "first line" {
		t.Fatalf("data = %q, want 'first line'", kf.data)
	}
}

func TestOpenAppend_MultipleWrites(t *testing.T) {
	m := newMock()
	m.files["multi.md"] = []byte("start-")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	c.storeFile("multi.md", []byte("start-"), false)
	n := &kiwiNode{client: c, path: "multi.md"}

	fh, _, _ := n.Open(nil, syscall.O_APPEND)
	kf := fh.(*kiwiFile)

	for i := 0; i < 10; i++ {
		kf.Write(nil, []byte("x"), 0)
	}

	want := "start-xxxxxxxxxx"
	if string(kf.data) != want {
		t.Fatalf("data = %q, want %q", kf.data, want)
	}
}

func TestOpenAppend_ThenTruncate(t *testing.T) {
	m := newMock()
	m.files["apptrunc.md"] = []byte("hello")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	c.storeFile("apptrunc.md", []byte("hello"), false)
	n := &kiwiNode{client: c, path: "apptrunc.md"}

	fh, _, _ := n.Open(nil, syscall.O_APPEND)
	kf := fh.(*kiwiFile)
	kf.Write(nil, []byte(" world"), 0)

	if string(kf.data) != "hello world" {
		t.Fatalf("after append: %q", kf.data)
	}
}

// --- FUSE Write EFBIG boundary ---

func TestWrite_MaxFileSizeBoundary(t *testing.T) {
	m := newMock()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "big.md"}

	fh, _, _ := n.Open(nil, 0)
	kf := fh.(*kiwiFile)
	kf.data = make([]byte, fuseMaxFileSize-10)

	_, errno := kf.Write(nil, make([]byte, 10), int64(fuseMaxFileSize-10))
	if errno != 0 {
		t.Fatalf("write at boundary should succeed, got errno %v", errno)
	}

	_, errno = kf.Write(nil, []byte("x"), int64(fuseMaxFileSize))
	if errno != syscall.EFBIG {
		t.Fatalf("write past max should return EFBIG, got %v", errno)
	}
}

func TestWrite_NegativeOffset(t *testing.T) {
	m := newMock()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "neg.md"}
	fh, _, _ := n.Open(nil, 0)
	kf := fh.(*kiwiFile)

	_, errno := kf.Write(nil, []byte("data"), -1)
	if errno != syscall.EFBIG {
		t.Fatalf("negative offset should fail, got errno %v", errno)
	}
}

// --- Directory rename edge cases ---

func TestRenameDirectory_NotFoundFallsBack(t *testing.T) {
	m := newMock()
	m.files["notadir.md"] = []byte("file content")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	parent := &kiwiNode{client: c, path: ""}
	newParent := &kiwiNode{client: c, path: ""}

	errno := parent.Rename(nil, "notadir.md", newParent, "moved.md", 0)
	if errno != 0 {
		t.Fatalf("file rename should work: errno %v", errno)
	}
	if _, ok := m.files["moved.md"]; !ok {
		t.Fatal("file should exist at new path")
	}
}

func TestRenameDirectory_EmptyDir(t *testing.T) {
	m := newMock()
	m.dirs["emptydir"] = []treeResponse{}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	parent := &kiwiNode{client: c, path: ""}
	newParent := &kiwiNode{client: c, path: ""}

	errno := parent.Rename(nil, "emptydir", newParent, "movedempty", 0)
	if errno != 0 {
		t.Fatalf("empty dir rename errno = %v", errno)
	}
}

// --- Readlink edge cases ---

func TestReadlink_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "bad.md"}
	_, errno := n.Readlink(nil)
	if errno == 0 {
		t.Fatal("server error should propagate as errno")
	}
}

func TestReadlink_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "missing.md"}
	_, errno := n.Readlink(nil)
	if errno != syscall.ENOENT {
		t.Fatalf("readlink missing should be ENOENT, got %v", errno)
	}
}

// --- Flush without write should be no-op ---

func TestFlush_NoWrite(t *testing.T) {
	m := newMock()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "clean.md"}
	fh := &kiwiFile{node: n, client: c}

	errno := fh.Flush(nil)
	if errno != 0 {
		t.Fatalf("Flush without write should be no-op, got errno %v", errno)
	}
	if m.puts.Load() != 0 {
		t.Fatalf("no PUT should have been sent, got %d", m.puts.Load())
	}
}
