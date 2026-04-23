package spaces

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/kiwifs/kiwifs/internal/config"
)

func minimalCfg() *config.Config {
	return &config.Config{
		Versioning: config.VersioningConfig{Strategy: "none"},
		Search:     config.SearchConfig{Engine: "grep"},
	}
}

func TestAddSpaceAndList(t *testing.T) {
	m := NewManager(nil)
	dir := t.TempDir()

	if err := m.AddSpace("alpha", dir, minimalCfg()); err != nil {
		t.Fatalf("AddSpace(alpha): %v", err)
	}
	dir2 := t.TempDir()
	if err := m.AddSpace("beta", dir2, minimalCfg()); err != nil {
		t.Fatalf("AddSpace(beta): %v", err)
	}
	defer m.Close()

	names := m.ListSpaces()
	if len(names) != 2 || names[0] != "alpha" || names[1] != "beta" {
		t.Fatalf("ListSpaces = %v, want [alpha beta]", names)
	}
}

func TestAddSpaceDuplicateRejects(t *testing.T) {
	m := NewManager(nil)
	dir := t.TempDir()

	if err := m.AddSpace("dup", dir, minimalCfg()); err != nil {
		t.Fatalf("first AddSpace: %v", err)
	}
	defer m.Close()

	if err := m.AddSpace("dup", dir, minimalCfg()); err == nil {
		t.Fatal("duplicate AddSpace should fail")
	}
}

func TestGetSpace(t *testing.T) {
	m := NewManager(nil)
	dir := t.TempDir()
	if err := m.AddSpace("one", dir, minimalCfg()); err != nil {
		t.Fatalf("AddSpace: %v", err)
	}
	defer m.Close()

	if sp, ok := m.GetSpace("one"); !ok || sp == nil {
		t.Fatal("GetSpace(one) should succeed")
	}
	if _, ok := m.GetSpace("nonexistent"); ok {
		t.Fatal("GetSpace(nonexistent) should fail")
	}
}

func TestResolveSpaceDefaultsToFirst(t *testing.T) {
	m := NewManager(nil)
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	m.AddSpace("first", dir1, minimalCfg())
	m.AddSpace("second", dir2, minimalCfg())
	defer m.Close()

	r := &http.Request{URL: &url.URL{Path: "/api/kiwi/tree"}}
	sp := m.resolveSpace(r)
	if sp == nil || sp.Name != "first" {
		t.Fatalf("resolveSpace default = %v, want first", sp)
	}
}

func TestResolveSpaceByName(t *testing.T) {
	m := NewManager(nil)
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	m.AddSpace("alpha", dir1, minimalCfg())
	m.AddSpace("beta", dir2, minimalCfg())
	defer m.Close()

	r := &http.Request{URL: &url.URL{Path: "/api/kiwi/beta/tree"}}
	sp := m.resolveSpace(r)
	if sp == nil || sp.Name != "beta" {
		t.Fatalf("resolveSpace(beta) = %v, want beta", sp)
	}
	if r.URL.Path != "/api/kiwi/tree" {
		t.Fatalf("path rewrite = %s, want /api/kiwi/tree", r.URL.Path)
	}
}

func TestResolveSpaceTrailingSlash(t *testing.T) {
	m := NewManager(nil)
	dir := t.TempDir()
	m.AddSpace("docs", dir, minimalCfg())
	defer m.Close()

	r := &http.Request{URL: &url.URL{Path: "/api/kiwi/docs"}}
	sp := m.resolveSpace(r)
	if sp == nil || sp.Name != "docs" {
		t.Fatalf("resolveSpace(docs no trailing slash) = %v, want docs", sp)
	}
}

func TestResolveSpaceEmpty(t *testing.T) {
	m := NewManager(nil)
	r := &http.Request{URL: &url.URL{Path: "/api/kiwi/tree"}}
	sp := m.resolveSpace(r)
	if sp != nil {
		t.Fatal("resolveSpace on empty manager should return nil")
	}
}

func TestHandlerHealthEndpoint(t *testing.T) {
	m := NewManager(nil)
	h := m.Handler()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/health status = %d, want 200", rec.Code)
	}
}

func TestHandlerSpacesEndpoint(t *testing.T) {
	m := NewManager(nil)
	dir := t.TempDir()
	m.AddSpace("wiki", dir, minimalCfg())
	defer m.Close()

	h := m.Handler()
	req := httptest.NewRequest(http.MethodGet, "/api/spaces", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/api/spaces status = %d, want 200", rec.Code)
	}
}

func TestCloseSkipsRegisterServerSpaces(t *testing.T) {
	m := NewManager(nil)
	dir := t.TempDir()

	if err := m.AddSpace("built", dir, minimalCfg()); err != nil {
		t.Fatalf("AddSpace: %v", err)
	}
	m.RegisterServer("prebuilt", dir, nil)

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestListSpacesPreservesOrder(t *testing.T) {
	m := NewManager(nil)
	names := []string{"charlie", "alpha", "bravo"}
	for _, name := range names {
		dir := t.TempDir()
		m.AddSpace(name, dir, minimalCfg())
	}
	defer m.Close()

	got := m.ListSpaces()
	for i, want := range names {
		if got[i] != want {
			t.Fatalf("ListSpaces[%d] = %s, want %s", i, got[i], want)
		}
	}
}

// ─── HTTP handler tests for space CRUD ──────────────────────────────────────

func TestHTTPCreateSpace(t *testing.T) {
	baseCfg := minimalCfg()
	baseCfg.Storage.Root = t.TempDir()
	m := NewManager(baseCfg)
	dir := t.TempDir()
	m.AddSpace("seed", dir, minimalCfg())
	defer m.Close()

	h := m.Handler()

	body := `{"name":"newspace"}`
	req := httptest.NewRequest(http.MethodPost, "/api/spaces", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /api/spaces status = %d, want 201; body: %s", rec.Code, rec.Body.String())
	}

	var meta SpaceMeta
	if err := json.NewDecoder(rec.Body).Decode(&meta); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if meta.Name != "newspace" {
		t.Fatalf("created space name = %q, want %q", meta.Name, "newspace")
	}

	names := m.ListSpaces()
	found := false
	for _, n := range names {
		if n == "newspace" {
			found = true
		}
	}
	if !found {
		t.Fatalf("newspace not found in ListSpaces: %v", names)
	}
}

func TestHTTPCreateSpaceDuplicate(t *testing.T) {
	m := NewManager(minimalCfg())
	dir := t.TempDir()
	m.AddSpace("existing", dir, minimalCfg())
	defer m.Close()

	h := m.Handler()

	body := `{"name":"existing"}`
	req := httptest.NewRequest(http.MethodPost, "/api/spaces", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("POST duplicate status = %d, want 409; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHTTPGetSpace(t *testing.T) {
	m := NewManager(nil)
	dir := t.TempDir()
	m.AddSpace("docs", dir, minimalCfg())
	defer m.Close()

	h := m.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/spaces/docs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/spaces/docs status = %d, want 200", rec.Code)
	}

	var meta SpaceMeta
	if err := json.NewDecoder(rec.Body).Decode(&meta); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if meta.Name != "docs" {
		t.Fatalf("space name = %q, want %q", meta.Name, "docs")
	}
}

func TestHTTPGetSpaceNotFound(t *testing.T) {
	m := NewManager(nil)
	h := m.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/spaces/nonexistent", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET nonexistent space status = %d, want 404", rec.Code)
	}
}

func TestHTTPDeleteSpace(t *testing.T) {
	m := NewManager(nil)
	dir := t.TempDir()
	m.AddSpace("ephemeral", dir, minimalCfg())
	defer m.Close()

	h := m.Handler()

	req := httptest.NewRequest(http.MethodDelete, "/api/spaces/ephemeral", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE /api/spaces/ephemeral status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}

	if _, ok := m.GetSpace("ephemeral"); ok {
		t.Fatal("space should be removed after DELETE")
	}
}

func TestHTTPDeleteSpaceNotFound(t *testing.T) {
	m := NewManager(nil)
	h := m.Handler()

	req := httptest.NewRequest(http.MethodDelete, "/api/spaces/ghost", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("DELETE nonexistent space status = %d, want 404", rec.Code)
	}
}

func TestHTTPListSpacesReturnsAll(t *testing.T) {
	m := NewManager(nil)
	for _, name := range []string{"alpha", "beta", "gamma"} {
		m.AddSpace(name, t.TempDir(), minimalCfg())
	}
	defer m.Close()

	h := m.Handler()
	req := httptest.NewRequest(http.MethodGet, "/api/spaces", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/spaces status = %d, want 200", rec.Code)
	}

	var resp struct {
		Spaces []SpaceMeta `json:"spaces"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Spaces) != 3 {
		t.Fatalf("expected 3 spaces, got %d", len(resp.Spaces))
	}
	names := make([]string, len(resp.Spaces))
	for i, s := range resp.Spaces {
		names[i] = s.Name
	}
	want := []string{"alpha", "beta", "gamma"}
	for i, w := range want {
		if names[i] != w {
			t.Fatalf("space[%d] = %q, want %q", i, names[i], w)
		}
	}
}
