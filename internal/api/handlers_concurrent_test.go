package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestConcurrentWrites_ETagConflict(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "race.md", "v0")

	// Read the initial ETag.
	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=race.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET: %d", rec.Code)
	}
	staleETag := strings.Trim(rec.Header().Get("ETag"), `"`)
	if staleETag == "" {
		t.Fatal("no ETag returned")
	}

	const N = 10
	var wg sync.WaitGroup
	codes := make([]int, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := fmt.Sprintf("writer-%d", idx)
			r := httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path=race.md", strings.NewReader(body))
			r.Header.Set("If-Match", staleETag)
			w := httptest.NewRecorder()
			s.echo.ServeHTTP(w, r)
			codes[idx] = w.Code
		}(i)
	}
	wg.Wait()

	okCount := 0
	conflictCount := 0
	for _, code := range codes {
		switch code {
		case http.StatusOK:
			okCount++
		case http.StatusConflict:
			conflictCount++
		default:
			t.Errorf("unexpected status code %d", code)
		}
	}

	if okCount != 1 {
		t.Errorf("expected exactly 1 OK, got %d (conflicts: %d)", okCount, conflictCount)
	}
}

func TestConcurrentRename_NoDataLoss(t *testing.T) {
	s := buildTestServer(t)
	mustPutFile(t, s, "source.md", "original content")

	var wg sync.WaitGroup
	codes := make([]int, 2)
	targets := []string{"dest-b.md", "dest-c.md"}

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"from":"source.md","to":"%s"}`, targets[idx])
			r := httptest.NewRequest(http.MethodPost, "/api/kiwi/rename", strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			s.echo.ServeHTTP(w, r)
			codes[idx] = w.Code
		}(i)
	}
	wg.Wait()

	// Source should not exist.
	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=source.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatal("source still exists after rename")
	}

	// Exactly one target should exist with correct content.
	found := 0
	for _, target := range targets {
		req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path="+target, nil)
		rec = httptest.NewRecorder()
		s.echo.ServeHTTP(rec, req)
		if rec.Code == http.StatusOK {
			found++
			if got := rec.Body.String(); got != "original content" {
				t.Errorf("target %s has wrong content: %q", target, got)
			}
		}
	}
	if found == 0 {
		t.Fatal("no target file exists — data lost")
	}
}

func TestConcurrentBulkWrite(t *testing.T) {
	s := buildTestServer(t)

	const goroutines = 5
	const filesPerBatch = 10
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(batch int) {
			defer wg.Done()
			var files []map[string]string
			for i := 0; i < filesPerBatch; i++ {
				files = append(files, map[string]string{
					"path":    fmt.Sprintf("batch%d/file%d.md", batch, i),
					"content": fmt.Sprintf("batch %d file %d", batch, i),
				})
			}
			bodyJSON, _ := json.Marshal(map[string]any{"files": files})
			r := httptest.NewRequest(http.MethodPost, "/api/kiwi/bulk", strings.NewReader(string(bodyJSON)))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			s.echo.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Errorf("bulk write batch %d: %d %s", batch, w.Code, w.Body.String())
			}
		}(g)
	}
	wg.Wait()

	// Verify all files exist.
	for g := 0; g < goroutines; g++ {
		for i := 0; i < filesPerBatch; i++ {
			path := fmt.Sprintf("batch%d/file%d.md", g, i)
			req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path="+path, nil)
			rec := httptest.NewRecorder()
			s.echo.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("missing %s: %d", path, rec.Code)
			}
		}
	}
}
