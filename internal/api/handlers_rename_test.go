package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRenameFile(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	mustPutFile(t, s, "concepts/old.md", "hello")

	body := `{"from":"concepts/old.md","to":"concepts/new.md"}`
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("POST /rename: %d %s", rec.Code, rec.Body.String())
	}
	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["to"] != "concepts/new.md" {
		t.Fatalf("unexpected to: %s", resp["to"])
	}
	if resp["etag"] == "" {
		t.Fatal("expected non-empty etag")
	}

	// Old path should 404.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=concepts/old.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET old path: want 404, got %d", rec.Code)
	}

	// New path should return the content.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=concepts/new.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET new path: %d %s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "hello" {
		t.Fatalf("content mismatch: %q", got)
	}

	// Search should find the new path, not the old.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/search?q=hello", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("search: %d", rec.Code)
	}
	searchBody := rec.Body.String()
	if strings.Contains(searchBody, "old.md") {
		t.Fatalf("search still returns old path: %s", searchBody)
	}
	if !strings.Contains(searchBody, "new.md") {
		t.Fatalf("search doesn't return new path: %s", searchBody)
	}
}

func TestRenameFile_NotFound(t *testing.T) {
	s := buildTestServer(t)

	body := `{"from":"nonexistent.md","to":"dest.md"}`
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("want 404 for missing source, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRenameFile_MissingParams(t *testing.T) {
	s := buildTestServer(t)

	tests := []struct {
		name string
		body string
	}{
		{"empty from", `{"from":"","to":"b.md"}`},
		{"empty to", `{"from":"a.md","to":""}`},
		{"both empty", `{"from":"","to":""}`},
		{"no body", `{}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			s.echo.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestRenameFile_PathTraversal(t *testing.T) {
	s := buildTestServer(t)

	body := `{"from":"../escape.md","to":"dest.md"}`
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatal("path traversal should not succeed")
	}
}

func TestRenameDir(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "docs/a.md", "aaa")
	mustPutFile(t, s, "docs/b.md", "bbb")

	body := `{"from":"docs","to":"archive"}`
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename-dir", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("POST /rename-dir: %d %s", rec.Code, rec.Body.String())
	}

	for _, np := range []string{"archive/a.md", "archive/b.md"} {
		req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path="+np, nil)
		rec = httptest.NewRecorder()
		s.echo.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s: %d", np, rec.Code)
		}
	}

	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=docs/a.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("old path should 404, got %d", rec.Code)
	}
}

func TestRenameDir_NotFound(t *testing.T) {
	s := buildTestServer(t)

	body := `{"from":"nonexistent","to":"dest"}`
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename-dir", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatal("expected non-200 for missing source dir")
	}
}

func TestRenameDir_PathTraversal(t *testing.T) {
	s := buildTestServer(t)

	cases := []struct {
		name string
		from string
	}{
		{"dot-prefixed path", ".git/objects"},
		{"null byte", "docs\x00/evil"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"from":%q,"to":"stolen"}`, tc.from)
			req := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename-dir", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			s.echo.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for %q, got %d: %s", tc.from, rec.Code, rec.Body.String())
			}
		})
	}

	t.Run("relative traversal neutralized to 404", func(t *testing.T) {
		body := `{"from":"../../../etc","to":"stolen"}`
		req := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename-dir", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s.echo.ServeHTTP(rec, req)

		if rec.Code == http.StatusOK {
			t.Fatal("path traversal should not succeed")
		}
	})
}

func TestRenameDir_MissingParams(t *testing.T) {
	s := buildTestServer(t)

	tests := []struct {
		name string
		body string
	}{
		{"empty from", `{"from":"","to":"b"}`},
		{"empty to", `{"from":"a","to":""}`},
		{"both empty", `{"from":"","to":""}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename-dir", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			s.echo.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestRenameFile_SameSourceAndDest(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "same.md", "content")

	body := `{"from":"same.md","to":"same.md"}`
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("same src/dest should return 200, got %d: %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=same.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("file gone after same-path rename: %d", rec.Code)
	}
	if got := rec.Body.String(); got != "content" {
		t.Fatalf("content changed after same-path rename: %q", got)
	}
}
