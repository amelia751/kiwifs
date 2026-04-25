package workflow

import (
	"strings"
	"testing"
)

const sampleWorkflow = `---
title: "Onboarding"
type: onboarding
owner: alice
status: draft
tasks:
  - id: t1
    title: "Create email"
    status: todo
  - id: t2
    title: "Grant access"
    status: in-progress
approval:
  status: pending
tags: [onboarding]
---

# Welcome

Body content.
`

func TestParseWorkflow_ComputesProgress(t *testing.T) {
	wf, err := ParseWorkflow([]byte(sampleWorkflow))
	if err != nil {
		t.Fatalf("ParseWorkflow: %v", err)
	}
	if wf == nil {
		t.Fatalf("expected workflow, got nil")
	}
	if len(wf.Tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(wf.Tasks))
	}
	if wf.Progress != 0 {
		t.Fatalf("expected progress 0 with no tasks done, got %v", wf.Progress)
	}
}

func TestParseWorkflow_NoTasks(t *testing.T) {
	wf, err := ParseWorkflow([]byte("---\ntitle: hi\n---\n\n# body\n"))
	if err != nil {
		t.Fatalf("ParseWorkflow: %v", err)
	}
	if wf != nil {
		t.Fatalf("expected nil for page without tasks, got %+v", wf)
	}
}

func TestUpdateTask_TransitionToDoneStampsActor(t *testing.T) {
	out, err := UpdateTask([]byte(sampleWorkflow), "t1", TaskDone, "alice@team.io")
	if err != nil {
		t.Fatalf("UpdateTask: %v", err)
	}
	if !strings.Contains(string(out), "status: done") {
		t.Fatalf("expected 'status: done' in output\n%s", out)
	}
	if !strings.Contains(string(out), "completed-by: alice@team.io") {
		t.Fatalf("expected completed-by stamp\n%s", out)
	}
	// Body preserved.
	if !strings.Contains(string(out), "# Welcome") {
		t.Fatalf("body was dropped\n%s", out)
	}

	// Re-parse should now report 1/2 done.
	wf, err := ParseWorkflow(out)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	if wf.Progress != 0.5 {
		t.Fatalf("expected progress 0.5, got %v", wf.Progress)
	}
}

func TestUpdateTask_UnknownID(t *testing.T) {
	_, err := UpdateTask([]byte(sampleWorkflow), "nope", TaskDone, "alice")
	if err == nil {
		t.Fatalf("expected error for unknown task id")
	}
}

func TestUpdateApproval_CreatesFieldWhenMissing(t *testing.T) {
	// Page with no approval block at all.
	src := `---
title: hi
tasks:
  - id: t1
    title: x
    status: todo
---

body
`
	out, err := UpdateApproval([]byte(src), ApprovalApproved, "bob", "LGTM")
	if err != nil {
		t.Fatalf("UpdateApproval: %v", err)
	}
	if !strings.Contains(string(out), "approval:") {
		t.Fatalf("expected approval block to be added\n%s", out)
	}
	if !strings.Contains(string(out), "status: approved") {
		t.Fatalf("expected status: approved\n%s", out)
	}
	if !strings.Contains(string(out), "approver: bob") {
		t.Fatalf("expected approver: bob\n%s", out)
	}
	if !strings.Contains(string(out), `comment: LGTM`) {
		t.Fatalf("expected comment field\n%s", out)
	}
}

func TestMergeFrontmatter_AddsUpdatesAndRemoves(t *testing.T) {
	src := `---
title: hi
status: draft
owner: alice
---

body
`
	out, err := MergeFrontmatter([]byte(src), map[string]any{
		"status":     "verified",
		"owner":      "bob",
		"confidence": 0.9,
		"tags":       []string{"api", "docs"},
	})
	if err != nil {
		t.Fatalf("MergeFrontmatter: %v", err)
	}
	s := string(out)
	for _, want := range []string{"status: verified", "owner: bob", "confidence: 0.9", "- api", "- docs"} {
		if !strings.Contains(s, want) {
			t.Fatalf("expected %q in output:\n%s", want, s)
		}
	}
	if !strings.Contains(s, "body") {
		t.Fatalf("body dropped\n%s", s)
	}

	// Removal via nil.
	out2, err := MergeFrontmatter(out, map[string]any{"owner": nil})
	if err != nil {
		t.Fatalf("MergeFrontmatter remove: %v", err)
	}
	if strings.Contains(string(out2), "owner:") {
		t.Fatalf("owner should have been removed\n%s", out2)
	}
}

func TestMergeFrontmatter_NoFrontmatter(t *testing.T) {
	src := "# just a heading\n\nbody\n"
	out, err := MergeFrontmatter([]byte(src), map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("MergeFrontmatter: %v", err)
	}
	s := string(out)
	if !strings.HasPrefix(s, "---\n") {
		t.Fatalf("expected frontmatter to be prepended:\n%s", s)
	}
	if !strings.Contains(s, "# just a heading") {
		t.Fatalf("body was dropped:\n%s", s)
	}
}
