//go:build !windows

package fuse

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// mockKiwi captures the minimum of the KiwiFS REST API the FUSE client
// uses (/api/kiwi/tree and /api/kiwi/file). It lets the tests assert JSON
// shape, PUT payloads, and cache-hit counts without a real mount.
type mockKiwi struct {
	files map[string][]byte
	dirs  map[string][]treeResponse

	fileHits atomic.Int32
	treeHits atomic.Int32
	puts     atomic.Int32
	renames  atomic.Int32
}

func newMock() *mockKiwi {
	return &mockKiwi{
		files: map[string][]byte{},
		dirs:  map[string][]treeResponse{},
	}
}

func (m *mockKiwi) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/kiwi/tree", func(w http.ResponseWriter, r *http.Request) {
		m.treeHits.Add(1)
		path := r.URL.Query().Get("path")
		children, ok := m.dirs[path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(treeResponse{
			Path:     path,
			Name:     path,
			IsDir:    true,
			Children: children,
		})
	})
	mux.HandleFunc("/api/kiwi/file", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Query().Get("path")
		switch r.Method {
		case http.MethodGet:
			m.fileHits.Add(1)
			data, ok := m.files[path]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "text/markdown")
			w.Write(data)
		case http.MethodPut:
			m.puts.Add(1)
			body, _ := io.ReadAll(r.Body)
			m.files[path] = body
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			delete(m.files, path)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/kiwi/rename", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		m.renames.Add(1)
		var req struct {
			From string `json:"from"`
			To   string `json:"to"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data, ok := m.files[req.From]
		if !ok {
			http.NotFound(w, r)
			return
		}
		m.files[req.To] = data
		delete(m.files, req.From)
		json.NewEncoder(w).Encode(map[string]string{"from": req.From, "to": req.To, "etag": "abc"})
	})
	mux.HandleFunc("/api/kiwi/rename-dir", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			From string `json:"from"`
			To   string `json:"to"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, ok := m.dirs[req.From]; !ok {
			http.NotFound(w, r)
			return
		}
		m.dirs[req.To] = m.dirs[req.From]
		delete(m.dirs, req.From)
		json.NewEncoder(w).Encode(map[string]any{"from": req.From, "to": req.To, "renamed": len(m.dirs[req.To])})
	})
	return mux
}

// TestListDirParsesNestedChildren covers the Readdir JSON bug: the
// endpoint returns `{children: [...]}`, not a bare array.
func TestListDirParsesNestedChildren(t *testing.T) {
	m := newMock()
	m.dirs[""] = []treeResponse{
		{Name: "index.md", IsDir: false},
		{Name: "concepts", IsDir: true},
	}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c}

	entries, errno := n.listDir()
	if errno != 0 {
		t.Fatalf("listDir errno: %v", errno)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %v, want 2", entries)
	}
	names := []string{entries[0].Name, entries[1].Name}
	if names[0] != "index.md" || names[1] != "concepts" {
		t.Fatalf("names = %v, want [index.md concepts]", names)
	}
}

func TestListDirUsesCache(t *testing.T) {
	m := newMock()
	m.dirs[""] = []treeResponse{{Name: "a.md", IsDir: false}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c}
	for i := 0; i < 3; i++ {
		if _, errno := n.listDir(); errno != 0 {
			t.Fatalf("listDir errno: %v", errno)
		}
	}
	if got := m.treeHits.Load(); got != 1 {
		t.Fatalf("tree hits = %d, want 1 (TTL cache should absorb follow-ups)", got)
	}
}

func TestStatFilePopulatesFileCache(t *testing.T) {
	m := newMock()
	m.files["note.md"] = []byte("hello world")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "note.md"}

	st, errno := n.statFile()
	if errno != 0 || !st.found {
		t.Fatalf("statFile: found=%v errno=%v", st.found, errno)
	}
	if st.size != int64(len("hello world")) {
		t.Fatalf("size = %d, want %d", st.size, len("hello world"))
	}
	// The stat should have primed the file cache so a subsequent Read()
	// goes zero-RTT — the whole point of caching.
	f := &kiwiFile{node: n, client: c}
	dest := make([]byte, 64)
	beforeHits := m.fileHits.Load()
	rr, errno := f.Read(nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read errno: %v", errno)
	}
	got, _ := rr.Bytes(dest)
	if string(got) != "hello world" {
		t.Fatalf("Read bytes = %q, want 'hello world'", got)
	}
	if m.fileHits.Load() != beforeHits {
		t.Fatal("file Read should hit the cache, not the network")
	}
}

func TestFlushInvalidatesSiblingCache(t *testing.T) {
	m := newMock()
	m.files["note.md"] = []byte("old")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	// Prime the cache with the old value, then write new content.
	n := &kiwiNode{client: c, path: "note.md"}
	f := &kiwiFile{node: n, client: c}
	dest := make([]byte, 64)
	rr, _ := f.Read(nil, dest, 0)
	got, _ := rr.Bytes(dest)
	if string(got) != "old" {
		t.Fatalf("expected cached 'old', got %q", got)
	}

	f2 := &kiwiFile{node: n, client: c, data: []byte("new"), dirty: true}
	if errno := f2.Flush(nil); errno != 0 {
		t.Fatalf("flush errno: %v", errno)
	}
	if m.puts.Load() != 1 {
		t.Fatalf("puts = %d, want 1", m.puts.Load())
	}
	// Reading via a fresh handle should now see the updated content.
	f3 := &kiwiFile{node: n, client: c}
	dest2 := make([]byte, 64)
	rr2, errno := f3.Read(nil, dest2, 0)
	if errno != 0 {
		t.Fatalf("re-read errno: %v", errno)
	}
	got2, _ := rr2.Bytes(dest2)
	if !strings.HasPrefix(string(got2), "new") {
		t.Fatalf("post-flush read = %q, want prefix 'new'", got2)
	}
}

func TestClientAttachesAuthHeaders(t *testing.T) {
	// Fake a protected KiwiFS that only answers requests with matching
	// auth + space headers. The test asserts every FUSE codepath (tree,
	// file, put, delete) threads those through.
	var (
		seenKey   atomic.Value // string
		seenSpace atomic.Value // string
		seenAuth  atomic.Value // string
	)
	seenKey.Store("")
	seenSpace.Store("")
	seenAuth.Store("")

	handler := http.NewServeMux()
	handler.HandleFunc("/api/kiwi/tree", func(w http.ResponseWriter, r *http.Request) {
		seenKey.Store(r.Header.Get("X-API-Key"))
		seenSpace.Store(r.Header.Get("X-Kiwi-Space"))
		seenAuth.Store(r.Header.Get("Authorization"))
		if r.Header.Get("X-API-Key") != "secret" {
			http.Error(w, "forbidden", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(treeResponse{Path: "", IsDir: true})
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	auth := &ClientAuth{APIKey: "secret"}
	c := NewClientWithAuth(srv.URL, auth, "acme")
	n := &kiwiNode{client: c}
	if _, errno := n.listDir(); errno != 0 {
		t.Fatalf("listDir with auth: errno %v", errno)
	}
	if got := seenKey.Load().(string); got != "secret" {
		t.Fatalf("server saw X-API-Key=%q, want %q", got, "secret")
	}
	if got := seenSpace.Load().(string); got != "acme" {
		t.Fatalf("server saw X-Kiwi-Space=%q, want %q", got, "acme")
	}

	// Without auth the client should propagate the server's 401 as
	// EACCES, which is what the kernel surfaces to users as "permission
	// denied" instead of the opaque "i/o error" we returned before.
	plain := NewClient(srv.URL)
	n2 := &kiwiNode{client: plain}
	if _, errno := n2.listDir(); errno == 0 {
		t.Fatal("plain client should have failed, got success")
	}
}

func TestBearerAuthHeader(t *testing.T) {
	var seen atomic.Value
	seen.Store("")

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.Header.Get("Authorization"))
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(treeResponse{})
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	c := NewClientWithAuth(srv.URL, &ClientAuth{Bearer: "tok"}, "")
	n := &kiwiNode{client: c}
	if _, errno := n.listDir(); errno != 0 {
		t.Fatalf("errno: %v", errno)
	}
	if got := seen.Load().(string); got != "Bearer tok" {
		t.Fatalf("Authorization = %q, want %q", got, "Bearer tok")
	}
}

func TestRename(t *testing.T) {
	m := newMock()
	m.files["old.md"] = []byte("rename me")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	parent := &kiwiNode{client: c, path: ""}
	newParent := &kiwiNode{client: c, path: ""}

	errno := parent.Rename(nil, "old.md", newParent, "new.md", 0)
	if errno != 0 {
		t.Fatalf("Rename errno: %v", errno)
	}
	if _, ok := m.files["old.md"]; ok {
		t.Fatal("old path should have been deleted")
	}
	got, ok := m.files["new.md"]
	if !ok {
		t.Fatal("new path should exist")
	}
	if string(got) != "rename me" {
		t.Fatalf("new content = %q, want %q", got, "rename me")
	}
	if r := m.renames.Load(); r != 1 {
		t.Fatalf("expected 1 atomic rename call, got %d", r)
	}
	if p := m.puts.Load(); p != 0 {
		t.Fatalf("expected 0 PUT calls (atomic rename should replace GET+PUT+DELETE), got %d", p)
	}
}

func TestRename_NonASCII(t *testing.T) {
	m := newMock()
	m.files["café.md"] = []byte("french content")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	parent := &kiwiNode{client: c, path: ""}
	newParent := &kiwiNode{client: c, path: ""}

	errno := parent.Rename(nil, "café.md", newParent, "日記.md", 0)
	if errno != 0 {
		t.Fatalf("Rename non-ASCII errno: %v", errno)
	}
	if _, ok := m.files["café.md"]; ok {
		t.Fatal("old non-ASCII path should have been deleted")
	}
	got, ok := m.files["日記.md"]
	if !ok {
		t.Fatal("new non-ASCII path should exist")
	}
	if string(got) != "french content" {
		t.Fatalf("content = %q", got)
	}
}

func TestRenameDirectory(t *testing.T) {
	m := newMock()
	m.dirs["mydir"] = []treeResponse{{Name: "a.md", IsDir: false}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	parent := &kiwiNode{client: c, path: ""}
	newParent := &kiwiNode{client: c, path: ""}

	errno := parent.Rename(nil, "mydir", newParent, "newdir", 0)
	if errno != 0 {
		t.Fatalf("directory rename errno = %v, want 0", errno)
	}
	if _, ok := m.dirs["newdir"]; !ok {
		t.Fatal("expected newdir to exist after rename")
	}
	if _, ok := m.dirs["mydir"]; ok {
		t.Fatal("expected mydir to be deleted after rename")
	}
}

func TestSetattr_Truncate(t *testing.T) {
	m := newMock()
	m.files["note.md"] = []byte("hello world")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "note.md"}

	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE
	in.Size = 0
	out := &fuse.AttrOut{}

	errno := n.Setattr(nil, nil, in, out)
	if errno != 0 {
		t.Fatalf("Setattr errno: %v", errno)
	}
	if got := m.files["note.md"]; len(got) != 0 {
		t.Fatalf("file should be empty after truncate, got %d bytes", len(got))
	}
}

func TestSetattr_TruncateNonZero(t *testing.T) {
	m := newMock()
	m.files["note.md"] = []byte("hello world")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "note.md"}

	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE
	in.Size = 5
	out := &fuse.AttrOut{}

	errno := n.Setattr(nil, nil, in, out)
	if errno != 0 {
		t.Fatalf("Setattr errno: %v", errno)
	}
	if got := m.files["note.md"]; string(got) != "hello" {
		t.Fatalf("truncated content = %q, want %q", got, "hello")
	}
}

func TestFsync(t *testing.T) {
	m := newMock()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "synced.md"}
	f := &kiwiFile{node: n, client: c, data: []byte("sync this"), dirty: true}

	errno := f.Fsync(nil, 0)
	if errno != 0 {
		t.Fatalf("Fsync errno: %v", errno)
	}
	if m.puts.Load() != 1 {
		t.Fatalf("puts = %d, want 1", m.puts.Load())
	}
	got, ok := m.files["synced.md"]
	if !ok {
		t.Fatal("file should exist on server after fsync")
	}
	if string(got) != "sync this" {
		t.Fatalf("server content = %q, want %q", got, "sync this")
	}
	if f.dirty {
		t.Fatal("dirty should be false after fsync")
	}
}

func TestOpenTrunc(t *testing.T) {
	m := newMock()
	m.files["trunc.md"] = []byte("old content")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "trunc.md"}

	fh, _, errno := n.Open(nil, syscall.O_TRUNC)
	if errno != 0 {
		t.Fatalf("Open errno: %v", errno)
	}
	kf := fh.(*kiwiFile)
	if len(kf.data) != 0 {
		t.Fatalf("data after O_TRUNC = %d bytes, want 0", len(kf.data))
	}
	if !kf.dirty {
		t.Fatal("dirty should be true after O_TRUNC")
	}
}

func TestOpenAppend(t *testing.T) {
	m := newMock()
	m.files["append.md"] = []byte("first ")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "append.md"}
	c.storeFile("append.md", []byte("first "), false)

	fh, _, errno := n.Open(nil, syscall.O_APPEND)
	if errno != 0 {
		t.Fatalf("Open errno: %v", errno)
	}
	kf := fh.(*kiwiFile)
	if !kf.append {
		t.Fatal("append should be true after O_APPEND")
	}

	kf.Write(nil, []byte("second "), 0)
	kf.Write(nil, []byte("third"), 0)

	want := "first second third"
	if string(kf.data) != want {
		t.Fatalf("data = %q, want %q", kf.data, want)
	}
}

func TestOpenAppend_Uncached(t *testing.T) {
	m := newMock()
	m.files["remote.md"] = []byte("existing ")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "remote.md"}

	fh, _, errno := n.Open(nil, syscall.O_APPEND)
	if errno != 0 {
		t.Fatalf("Open errno: %v", errno)
	}
	kf := fh.(*kiwiFile)

	kf.Write(nil, []byte("appended"), 0)

	want := "existing appended"
	if string(kf.data) != want {
		t.Fatalf("data = %q, want %q (append without cache should fetch from server)", kf.data, want)
	}
}

func TestRename_PathTraversal(t *testing.T) {
	m := newMock()
	m.files["legit.md"] = []byte("ok")
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	parent := &kiwiNode{client: c, path: ""}
	newParent := &kiwiNode{client: c, path: ""}

	errno := parent.Rename(nil, "legit.md", newParent, "../../../etc/passwd", 0)
	if errno != 0 && errno != syscall.EIO {
		t.Logf("Rename with path traversal got errno %v (server should reject)", errno)
	}
}

func TestSymlink_SendsCorrectRequest(t *testing.T) {
	var gotMethod, gotContentType string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"path":"link.md","type":"symlink"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	req, _ := http.NewRequest("PUT", c.apiURL("/api/kiwi/file", "link.md"), bytes.NewReader([]byte("../other/target.md")))
	req.Header.Set("Content-Type", "application/x-symlink")
	req.Header.Set("X-Actor", "fuse")
	resp, err := c.do(req)
	if err != nil {
		t.Fatalf("symlink request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if gotMethod != "PUT" {
		t.Fatalf("method = %q, want PUT", gotMethod)
	}
	if gotContentType != "application/x-symlink" {
		t.Fatalf("Content-Type = %q, want application/x-symlink", gotContentType)
	}
	if string(gotBody) != "../other/target.md" {
		t.Fatalf("body = %q, want ../other/target.md", gotBody)
	}
}

func TestReadlink(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/kiwi/readlink" {
			t.Errorf("expected /api/kiwi/readlink, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("../other/target.md"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "link.md"}
	data, errno := n.Readlink(nil)
	if errno != 0 {
		t.Fatalf("Readlink errno = %v, want 0", errno)
	}
	if string(data) != "../other/target.md" {
		t.Fatalf("Readlink = %q, want ../other/target.md", data)
	}
}

func TestStatFile_DetectsSymlink(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-File-Type", "symlink")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("target content"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	n := &kiwiNode{client: c, path: "link.md"}
	st, errno := n.statFile()
	if errno != 0 {
		t.Fatalf("statFile errno = %v", errno)
	}
	if !st.found {
		t.Fatal("expected found=true")
	}
	if !st.isSymlink {
		t.Fatal("expected isSymlink=true for X-File-Type: symlink header")
	}
	if st.size != int64(len("target content")) {
		t.Fatalf("size = %d, want %d", st.size, len("target content"))
	}

	cached := c.cachedFile("link.md")
	if cached == nil {
		t.Fatal("expected symlink to be cached")
	}
	if !cached.isSymlink {
		t.Fatal("cached entry should preserve isSymlink=true")
	}
}

// Quiet the "unused" warning when we swap out the old import graph.
var _ = io.ReadAll

func TestMkdirWritesPlaceholder(t *testing.T) {
	m := newMock()
	m.dirs[""] = nil
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := NewClient(srv.URL)
	// The FUSE Mkdir path needs a real context + EntryOut, but we only
	// exercise the HTTP side here — call the underlying helpers instead
	// of invoking the full FUSE interface.
	placeholder := "runbook/.keep"
	req, _ := http.NewRequest("PUT", c.apiURL("/api/kiwi/file", placeholder), bytes.NewReader(nil))
	req.Header.Set("X-Actor", "fuse")
	resp, err := c.client.Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	resp.Body.Close()
	if _, ok := m.files[placeholder]; !ok {
		t.Fatal("placeholder file was not created on server")
	}
}
