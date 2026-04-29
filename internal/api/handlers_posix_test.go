package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPOSIX_ReadAfterWrite(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "raft.md", "read-after-write content")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=raft.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET: %d", rec.Code)
	}
	if got := rec.Body.String(); got != "read-after-write content" {
		t.Fatalf("content mismatch: %q", got)
	}
}

func TestPOSIX_DeleteThenRead(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "todelete.md", "delete me")

	req := httptest.NewRequest(http.MethodDelete, "/api/kiwi/file?path=todelete.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE: %d %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=todelete.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET after delete: want 404, got %d", rec.Code)
	}
}

func TestPOSIX_RenameAtomicity(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)
	mustPutFile(t, s, "alpha.md", "atomic rename content")

	body := `{"from":"alpha.md","to":"beta.md"}`
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rename: %d %s", rec.Code, rec.Body.String())
	}

	// Old path should 404.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=alpha.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("old path: want 404, got %d", rec.Code)
	}

	// New path should have content.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=beta.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("new path: %d", rec.Code)
	}
	if got := rec.Body.String(); got != "atomic rename content" {
		t.Fatalf("content: %q", got)
	}

	// Search should return new path.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/search?q=atomic+rename", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("search: %d", rec.Code)
	}
	searchBody := rec.Body.String()
	if strings.Contains(searchBody, "alpha.md") {
		t.Fatalf("search still returns old path: %s", searchBody)
	}
}

func TestPOSIX_DirectoryListing(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "docs/a.md", "a")
	mustPutFile(t, s, "docs/b.md", "b")
	mustPutFile(t, s, "docs/sub/c.md", "c")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/tree?path=docs", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("tree: %d %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "a.md") || !strings.Contains(body, "b.md") {
		t.Fatalf("missing files in tree: %s", body)
	}
	if strings.Contains(body, ".git") || strings.Contains(body, ".kiwi") {
		t.Fatalf("internal dirs leaked in tree: %s", body)
	}
}

func TestPOSIX_StatMetadata(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "stat.md", "version1")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=stat.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET: %d", rec.Code)
	}
	etag1 := rec.Header().Get("ETag")
	if etag1 == "" {
		t.Fatal("no ETag on first read")
	}

	// Modify and verify ETag changed.
	mustPutFile(t, s, "stat.md", "version2")

	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=stat.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET: %d", rec.Code)
	}
	etag2 := rec.Header().Get("ETag")
	if etag2 == etag1 {
		t.Fatal("ETag did not change after modification")
	}
}

func TestPOSIX_ConditionalWrite(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "cond.md", "original")

	// Read the ETag.
	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=cond.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	etag := strings.Trim(rec.Header().Get("ETag"), `"`)

	// Conditional write with correct ETag should succeed.
	req = httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path=cond.md", strings.NewReader("updated"))
	req.Header.Set("If-Match", etag)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conditional PUT with valid ETag: %d %s", rec.Code, rec.Body.String())
	}

	// Conditional write with stale ETag should 409.
	req = httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path=cond.md", strings.NewReader("conflict"))
	req.Header.Set("If-Match", etag)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("conditional PUT with stale ETag: want 409, got %d", rec.Code)
	}

	// Unconditional write should always succeed.
	req = httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path=cond.md", strings.NewReader("overwrite"))
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unconditional PUT: %d", rec.Code)
	}
}

func TestPOSIX_AtomicWrite(t *testing.T) {
	s := buildTestServer(t)

	// Write a file and verify exact content.
	mustPutFile(t, s, "atomic.md", "exact content here")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=atomic.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET: %d", rec.Code)
	}
	if got := rec.Body.String(); got != "exact content here" {
		t.Fatalf("torn write? got %q", got)
	}

	// Rapid sequential writes — final read should be one of the written values.
	for i := 0; i < 100; i++ {
		mustPutFile(t, s, "rapid.md", strings.Repeat("x", i+1))
	}
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=rapid.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET rapid: %d", rec.Code)
	}
	got := rec.Body.String()
	valid := false
	for i := 0; i < 100; i++ {
		if got == strings.Repeat("x", i+1) {
			valid = true
			break
		}
	}
	if !valid {
		t.Fatalf("content is not any of the written values (len=%d)", len(got))
	}
}

func TestPOSIX_RenameETagMatch(t *testing.T) {
	s := buildTestServer(t)
	content := "etag-test-content"
	mustPutFile(t, s, "etag-src.md", content)

	body := `{"from":"etag-src.md","to":"etag-dst.md"}`
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rename: %d", rec.Code)
	}
	var resp map[string]string
	json.Unmarshal(rec.Body.Bytes(), &resp)

	// Read the renamed file and verify ETag matches.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=etag-dst.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	readETag := strings.Trim(rec.Header().Get("ETag"), `"`)
	if readETag != resp["etag"] {
		t.Fatalf("ETag mismatch: rename returned %q, read returned %q", resp["etag"], readETag)
	}
}

func mustCreateSymlink(t *testing.T, s *Server, path, target string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path="+path, strings.NewReader(target))
	req.Header.Set("Content-Type", "application/x-symlink")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT symlink %s: %d %s", path, rec.Code, rec.Body.String())
	}
}

func TestReadlink(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "target.md", "real content")
	mustCreateSymlink(t, s, "link.md", "target.md")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/readlink?path=link.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /readlink: %d %s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "target.md" {
		t.Fatalf("readlink = %q, want 'target.md'", got)
	}
}

func TestReadlink_NotSymlink(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "regular.md", "not a symlink")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/readlink?path=regular.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("readlink on regular file: want 400, got %d", rec.Code)
	}
}

func TestReadlink_NotFound(t *testing.T) {
	s := buildTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/readlink?path=missing.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("readlink on missing file: want 404, got %d", rec.Code)
	}
}

func TestReadlink_PathTraversal(t *testing.T) {
	s := buildTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/readlink?path=.git/config", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("readlink internal path: want 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestReadFile_SymlinkHeader(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "real.md", "actual content")
	mustCreateSymlink(t, s, "sym.md", "real.md")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=sym.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET file via symlink: %d", rec.Code)
	}
	if ft := rec.Header().Get("X-File-Type"); ft != "symlink" {
		t.Fatalf("X-File-Type = %q, want 'symlink'", ft)
	}
}
