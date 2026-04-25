package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAnalyticsEndpoint(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	mustPutFile(t, s, "doc1.md", "---\nstatus: active\n---\n# Doc 1\nSome content here.\n")
	mustPutFile(t, s, "doc2.md", "---\nstatus: draft\n---\n# Doc 2\nMore words.\n")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/analytics", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /analytics: %d %s", rec.Code, rec.Body.String())
	}

	var resp AnalyticsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v (body=%s)", err, rec.Body.String())
	}
	if resp.TotalPages < 2 {
		t.Fatalf("expected at least 2 pages, got %d", resp.TotalPages)
	}
	if resp.TopUpdated == nil {
		t.Fatal("top_updated should not be nil")
	}
	if resp.Health.Stale.Paths == nil {
		t.Fatal("stale paths should not be nil (should be empty slice)")
	}
}

func TestAnalyticsScopeFiltering(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	mustPutFile(t, s, "students/alice.md", "---\nstatus: active\n---\n# Alice\n")
	mustPutFile(t, s, "students/bob.md", "---\nstatus: active\n---\n# Bob\n")
	mustPutFile(t, s, "topics/math.md", "---\nsubject: math\n---\n# Math\n")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/analytics?scope=students/", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /analytics?scope=students/: %d %s", rec.Code, rec.Body.String())
	}

	var resp AnalyticsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.TotalPages != 2 {
		t.Fatalf("scoped to students/ expected 2 pages, got %d", resp.TotalPages)
	}
}

func TestAnalyticsEmptyKB(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/analytics", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /analytics: %d %s", rec.Code, rec.Body.String())
	}

	var resp AnalyticsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.TotalPages != 0 {
		t.Fatalf("empty KB expected 0 pages, got %d", resp.TotalPages)
	}
	if resp.TotalWords != 0 {
		t.Fatalf("empty KB expected 0 words, got %d", resp.TotalWords)
	}
	if resp.TopUpdated == nil {
		t.Fatal("top_updated should be empty slice, not nil")
	}
	if len(resp.TopUpdated) != 0 {
		t.Fatalf("expected no top updated, got %d", len(resp.TopUpdated))
	}
}

func TestHealthCheckEndpoint(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	mustPutFile(t, s, "doc.md", "---\nstatus: active\n---\n# Doc\nSome words for counting.\n")

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/health-check?path=doc.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /health-check: %d %s", rec.Code, rec.Body.String())
	}

	var resp HealthCheckResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Path != "doc.md" {
		t.Fatalf("expected path=doc.md, got %s", resp.Path)
	}
	if resp.Issues == nil {
		t.Fatal("issues should not be nil")
	}
}

func TestHealthCheckMissingPath(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/health-check", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing path, got %d", rec.Code)
	}
}
