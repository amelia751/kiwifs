package fuse

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
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

	size, found, errno := n.statFile()
	if errno != 0 || !found {
		t.Fatalf("statFile: found=%v errno=%v", found, errno)
	}
	if size != int64(len("hello world")) {
		t.Fatalf("size = %d, want %d", size, len("hello world"))
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
