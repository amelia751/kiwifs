package api

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kiwifs/kiwifs/internal/comments"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/versioning"
)

func mustPutFile(t *testing.T, s *Server, path, body string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path="+path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT %s: %d %s", path, rec.Code, rec.Body.String())
	}
}

func buildTestPipeline(t *testing.T) (string, *pipeline.Pipeline, *comments.Store) {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	searcher := search.NewGrep(dir)
	ver := versioning.NewNoop()
	hub := events.NewHub()
	pipe := pipeline.New(store, ver, searcher, nil, hub, nil, "")
	cstore, err := comments.New(dir)
	if err != nil {
		t.Fatalf("comments: %v", err)
	}
	return dir, pipe, cstore
}

func buildTestServer(t *testing.T) *Server {
	t.Helper()
	dir, pipe, cstore := buildTestPipeline(t)
	cfg := &config.Config{}
	cfg.Storage.Root = dir
	return NewServer(cfg, pipe, nil, cstore, nil, nil)
}

func buildSQLiteTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	searcher, err := search.NewSQLite(dir, store)
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	t.Cleanup(func() { _ = searcher.Close() })
	ver := versioning.NewNoop()
	hub := events.NewHub()
	pipe := pipeline.New(store, ver, searcher, searcher, hub, nil, "")
	cstore, err := comments.New(dir)
	if err != nil {
		t.Fatalf("comments: %v", err)
	}
	cfg := &config.Config{}
	cfg.Storage.Root = dir
	return NewServer(cfg, pipe, nil, cstore, nil, nil), dir
}

func buildTestServerWithAssets(t *testing.T, assets config.AssetsConfig) *Server {
	t.Helper()
	dir, pipe, cstore := buildTestPipeline(t)
	cfg := &config.Config{Assets: assets}
	cfg.Storage.Root = dir
	return NewServer(cfg, pipe, nil, cstore, nil, nil)
}

func buildTestServerWithPublicURL(t *testing.T, publicURL string) *Server {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	searcher := search.NewGrep(dir)
	ver := versioning.NewNoop()
	hub := events.NewHub()
	pipe := pipeline.New(store, ver, searcher, nil, hub, nil, "")
	cstore, err := comments.New(dir)
	if err != nil {
		t.Fatalf("comments: %v", err)
	}
	lr := links.NewResolver(func(ctx context.Context, fn func(path string)) error {
		return storage.Walk(ctx, store, "/", func(e storage.Entry) error {
			fn(e.Path)
			return nil
		})
	})
	pipe.OnInvalidate = func() { lr.MarkDirty() }
	cfg := &config.Config{}
	cfg.Storage.Root = dir
	cfg.Server.PublicURL = publicURL
	return NewServer(cfg, pipe, nil, cstore, nil, lr)
}

var pngMagic = []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 0, 0, 0, 0}

func buildMultipart(t *testing.T, fieldName, filename string, content []byte) (*bytes.Buffer, string) {
	t.Helper()
	buf := &bytes.Buffer{}
	w := multipart.NewWriter(buf)
	part, err := w.CreateFormFile(fieldName, filename)
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatalf("write part: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close multipart: %v", err)
	}
	return buf, w.FormDataContentType()
}
