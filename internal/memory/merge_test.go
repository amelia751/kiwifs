package memory

import (
	"strings"
	"testing"
)

func TestInjectMergedFrom_idempotent(t *testing.T) {
	s := `---
title: Auth
---
# Auth
`
	entries := []MergedFromEntry{
		{Type: "episode", ID: "ep-1", Note: "first"},
	}
	out, err := InjectMergedFrom([]byte(s), entries)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "merged-from") {
		t.Fatalf("expected merged-from in output:\n%s", out)
	}
	out2, err := InjectMergedFrom(out, entries)
	if err != nil {
		t.Fatal(err)
	}
	// second inject should not duplicate
	c := strings.Count(string(out2), "ep-1")
	if c != 1 {
		t.Fatalf("expected one ep-1 ref, got %d\n%s", c, out2)
	}
}

func TestInjectMergedFrom_acceptsPathOnly(t *testing.T) {
	b := []byte("---\n---\n# X\n")
	out, err := InjectMergedFrom(b, []MergedFromEntry{{
		Type: "episode",
		Path: "episodes/2026/a.md",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "episodes/2026/a.md") {
		t.Fatalf("expected path: %s", out)
	}
}
