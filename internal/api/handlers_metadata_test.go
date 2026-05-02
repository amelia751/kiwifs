package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReadFile_MetadataOnly(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "doc.md", "---\nstatus: published\ntags:\n  - go\n---\n# Hello\n\nBody text here.\n")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=doc.md&metadata_only=true", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET metadata_only: %d %s", rec.Code, rec.Body.String())
	}

	ct := rec.Header().Get("Content-Type")
	if ct == "" || ct[:16] != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	var fm map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &fm); err != nil {
		t.Fatalf("unmarshal: %v (body=%s)", err, rec.Body.String())
	}
	if fm["status"] != "published" {
		t.Fatalf("status = %v, want published", fm["status"])
	}
	// Body text should NOT appear
	if _, ok := fm["Body"]; ok {
		t.Fatal("body text should not appear in metadata_only response")
	}
}

func TestReadFile_MetadataOnly_NoFrontmatter(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "plain.md", "# No Frontmatter\n\nJust body text.\n")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=plain.md&metadata_only=true", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}

	var fm map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &fm); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(fm) != 0 {
		t.Fatalf("expected empty JSON, got %v", fm)
	}
}

func TestReadFile_MetadataOnly_StillHasETag(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "doc.md", "---\nstatus: draft\n---\n# Doc\n")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=doc.md&metadata_only=true", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if etag := rec.Header().Get("ETag"); etag == "" {
		t.Fatal("expected ETag header on metadata_only response")
	}
}

func TestReadFile_MetadataOnly_BinaryFile(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "data.json", `{"key": "value"}`)

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=data.json&metadata_only=true", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	var fm map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &fm); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(fm) != 0 {
		t.Fatalf("expected empty frontmatter for non-markdown, got %v", fm)
	}
}

func TestReadFile_MetadataOnly_WithIfNoneMatch(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "doc.md", "---\nstatus: draft\n---\n# Doc\n")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=doc.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	etag := rec.Header().Get("ETag")

	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=doc.md&metadata_only=true", nil)
	req.Header.Set("If-None-Match", etag)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("expected 304 on metadata_only with matching etag, got %d", rec.Code)
	}
}
