package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestChanges_Empty(t *testing.T) {
	s := buildTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/changes", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	// Without git init, might get empty or an error; with noop versioner there's no git.
	// This test validates the endpoint is wired up and doesn't panic.
	if rec.Code != http.StatusOK && rec.Code != http.StatusInternalServerError {
		t.Fatalf("GET /changes: unexpected status %d", rec.Code)
	}
}

func TestChanges_InvalidSince(t *testing.T) {
	s := buildTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/changes?since=not-a-hash!", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("GET /changes with invalid since: got %d, want 400", rec.Code)
	}
}

func TestParseCommitSubject(t *testing.T) {
	tests := []struct {
		subject    string
		wantAction string
		wantPath   string
	}{
		{"agent: write concepts/auth.md", "write", "concepts/auth.md"},
		{"kiwifs: delete reports/old.md", "delete", "reports/old.md"},
		{"agent: rename old.md → new.md", "rename", "new.md"},
		{"agent: bulk write — 5 files", "write", ""},
		{"write concepts/auth.md", "write", "concepts/auth.md"},
	}
	for _, tt := range tests {
		action, path := parseCommitSubject(tt.subject)
		if action != tt.wantAction {
			t.Errorf("parseCommitSubject(%q) action = %q, want %q", tt.subject, action, tt.wantAction)
		}
		if path != tt.wantPath {
			t.Errorf("parseCommitSubject(%q) path = %q, want %q", tt.subject, path, tt.wantPath)
		}
	}
}

func TestIsHexHash(t *testing.T) {
	if !isHexHash("a1b2c3d4e5f6") {
		t.Error("expected valid hex hash")
	}
	if isHexHash("xyz") {
		t.Error("expected invalid hex hash")
	}
	if isHexHash("ab") {
		t.Error("expected too-short hash to be invalid")
	}
	if isHexHash("a1b2c3!") {
		t.Error("expected hash with special chars to be invalid")
	}
}

func TestChanges_ResponseShape(t *testing.T) {
	// Validate changesResponse JSON shape
	resp := changesResponse{
		Changes: []changeEntry{
			{Seq: "abc123", Path: "test.md", Action: "write", Actor: "agent", Timestamp: "2026-01-01T00:00:00Z"},
		},
		LastSeq: "abc123",
	}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got changesResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.LastSeq != "abc123" {
		t.Errorf("last_seq = %q, want %q", got.LastSeq, "abc123")
	}
	if len(got.Changes) != 1 {
		t.Errorf("changes count = %d, want 1", len(got.Changes))
	}
}
