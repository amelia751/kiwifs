package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReadFile_IfNoneMatch_Hit(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "doc.md", "# Hello\n")

	// First read to get ETag
	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=doc.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first read: %d", rec.Code)
	}
	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag header")
	}

	// Second read with If-None-Match
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=doc.md", nil)
	req.Header.Set("If-None-Match", etag)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("expected 304, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != "" {
		t.Fatalf("expected empty body on 304, got %q", body)
	}
}

func TestReadFile_IfNoneMatch_Miss(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "doc.md", "# Version 1\n")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=doc.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	oldETag := rec.Header().Get("ETag")

	// Modify the file
	mustPutFile(t, s, "doc.md", "# Version 2\n")

	// Read with old ETag
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=doc.md", nil)
	req.Header.Set("If-None-Match", oldETag)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != "# Version 2\n" {
		t.Fatalf("expected new content, got %q", body)
	}
}

func TestReadFile_IfNoneMatch_NoHeader(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "doc.md", "# Hello\n")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=doc.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != "# Hello\n" {
		t.Fatalf("expected full content, got %q", body)
	}
}

func TestReadFile_IfNoneMatch_UnquotedETag(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "doc.md", "# Hello\n")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=doc.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	etag := rec.Header().Get("ETag")
	// Strip quotes and send unquoted
	unquoted := etag[1 : len(etag)-1]

	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=doc.md", nil)
	req.Header.Set("If-None-Match", unquoted)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("expected 304 with unquoted etag, got %d", rec.Code)
	}
}

func TestReadFile_IfNoneMatch_Wildcard(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "doc.md", "# Hello\n")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=doc.md", nil)
	req.Header.Set("If-None-Match", "*")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("expected 304 with wildcard If-None-Match, got %d", rec.Code)
	}
}
