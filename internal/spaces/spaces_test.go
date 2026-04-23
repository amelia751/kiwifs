package spaces

import (
	"net/http"
	"net/http/httptest"
	"net/url"
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
