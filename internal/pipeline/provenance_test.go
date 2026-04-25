package pipeline

import (
	"strings"
	"testing"

	"github.com/kiwifs/kiwifs/internal/markdown"
	"gopkg.in/yaml.v3"
)

func TestInjectProvenanceCreatesFrontmatter(t *testing.T) {
	in := []byte("# Hello\n\nbody\n")
	out, err := InjectProvenance(in, "run", "run-249", "agent:exec_abc")
	if err != nil {
		t.Fatalf("InjectProvenance: %v", err)
	}
	if !strings.HasPrefix(string(out), "---\n") {
		t.Fatalf("expected frontmatter prefix, got:\n%s", out)
	}
	fm, body := mustSplit(t, out)
	var parsed struct {
		DerivedFrom []map[string]any `yaml:"derived-from"`
	}
	if err := yaml.Unmarshal(fm, &parsed); err != nil {
		t.Fatalf("unmarshal yaml: %v\nfm=%q", err, fm)
	}
	if len(parsed.DerivedFrom) != 1 {
		t.Fatalf("want 1 derived-from entry, got %d (%+v)", len(parsed.DerivedFrom), parsed.DerivedFrom)
	}
	entry := parsed.DerivedFrom[0]
	if entry["type"] != "run" || entry["id"] != "run-249" || entry["actor"] != "agent:exec_abc" {
		t.Fatalf("entry mismatch: %+v", entry)
	}
	if !strings.Contains(string(body), "# Hello") {
		t.Fatalf("body lost: %q", body)
	}
}

func TestInjectProvenanceAppendsToExistingList(t *testing.T) {
	in := []byte("---\nstatus: published\nderived-from:\n  - type: run\n    id: run-001\n    date: 2026-01-01T00:00:00Z\n---\n# Title\n")
	out, err := InjectProvenance(in, "commit", "abc123", "")
	if err != nil {
		t.Fatalf("InjectProvenance: %v", err)
	}
	fm, body := mustSplit(t, out)
	var parsed struct {
		Status      string           `yaml:"status"`
		DerivedFrom []map[string]any `yaml:"derived-from"`
	}
	if err := yaml.Unmarshal(fm, &parsed); err != nil {
		t.Fatalf("unmarshal: %v (fm=%q)", err, fm)
	}
	if parsed.Status != "published" {
		t.Fatalf("status lost: %+v", parsed)
	}
	if len(parsed.DerivedFrom) != 2 {
		t.Fatalf("want 2 entries, got %d: %+v", len(parsed.DerivedFrom), parsed.DerivedFrom)
	}
	if parsed.DerivedFrom[0]["id"] != "run-001" {
		t.Fatalf("first entry lost: %+v", parsed.DerivedFrom[0])
	}
	if parsed.DerivedFrom[1]["type"] != "commit" || parsed.DerivedFrom[1]["id"] != "abc123" {
		t.Fatalf("new entry wrong: %+v", parsed.DerivedFrom[1])
	}
	if _, ok := parsed.DerivedFrom[1]["actor"]; ok {
		// omitempty — we passed "" for actor.
		t.Fatalf("actor should be omitted when empty, got %+v", parsed.DerivedFrom[1])
	}
	if !strings.Contains(string(body), "# Title") {
		t.Fatalf("body lost: %q", body)
	}
}

func TestInjectProvenanceNoop(t *testing.T) {
	in := []byte("# Hi\n")
	out, err := InjectProvenance(in, "", "", "")
	if err != nil {
		t.Fatalf("InjectProvenance: %v", err)
	}
	if string(out) != string(in) {
		t.Fatalf("empty type/id should be a no-op, got %q", out)
	}
}

func TestInjectProvenancePreservesExistingFrontmatterFields(t *testing.T) {
	in := []byte("---\nstatus: draft\npriority: high\n---\n\nbody text\n")
	out, err := InjectProvenance(in, "manual", "ticket-42", "alice")
	if err != nil {
		t.Fatalf("InjectProvenance: %v", err)
	}
	fm, body := mustSplit(t, out)
	var parsed map[string]any
	if err := yaml.Unmarshal(fm, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed["status"] != "draft" || parsed["priority"] != "high" {
		t.Fatalf("existing fields lost: %+v", parsed)
	}
	derived, ok := parsed["derived-from"].([]any)
	if !ok || len(derived) != 1 {
		t.Fatalf("expected single derived-from entry, got %+v", parsed["derived-from"])
	}
	if !strings.Contains(string(body), "body text") {
		t.Fatalf("body lost: %q", body)
	}
}

func TestParseProvenanceHeader(t *testing.T) {
	cases := map[string]struct {
		in      string
		wantT   string
		wantID  string
		wantOk  bool
	}{
		"empty":        {in: "", wantOk: false},
		"no-colon":     {in: "runid", wantOk: false},
		"empty-id":     {in: "run:", wantOk: false},
		"empty-type":   {in: ":run-1", wantOk: false},
		"ok":           {in: "run:run-249", wantT: "run", wantID: "run-249", wantOk: true},
		"trim-spaces":  {in: "  run  :  run-249 ", wantT: "run", wantID: "run-249", wantOk: true},
		"colon-in-id":  {in: "run:prefix:suffix", wantT: "run", wantID: "prefix:suffix", wantOk: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gt, gid, ok := ParseProvenanceHeader(tc.in)
			if ok != tc.wantOk || gt != tc.wantT || gid != tc.wantID {
				t.Fatalf("ParseProvenanceHeader(%q) = (%q, %q, %v); want (%q, %q, %v)",
					tc.in, gt, gid, ok, tc.wantT, tc.wantID, tc.wantOk)
			}
		})
	}
}

// mustSplit is a test helper that pulls the two halves of a frontmattered
// markdown file apart so each test can assert on the YAML independently.
func mustSplit(t *testing.T, content []byte) (fm, body []byte) {
	t.Helper()
	f, b, err := markdown.SplitFrontmatter(content)
	if err != nil {
		t.Fatalf("SplitFrontmatter: %v\ncontent=%q", err, content)
	}
	if f == nil {
		t.Fatalf("expected frontmatter, got nil (body=%q)", b)
	}
	return f, b
}
