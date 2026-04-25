package janitor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kiwifs/kiwifs/internal/storage"
)

// buildStore creates a temp local storage seeded with the provided files.
func buildStore(t *testing.T, files map[string]string) (storage.Storage, string) {
	t.Helper()
	root := t.TempDir()
	for rel, content := range files {
		abs := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
	s, err := storage.NewLocal(root)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	return s, root
}

func issuesByKind(issues []Issue) map[string][]Issue {
	out := map[string][]Issue{}
	for _, is := range issues {
		out[is.Kind] = append(out[is.Kind], is)
	}
	return out
}

func TestScan_FlagsMissingMetadataAndEmptyPages(t *testing.T) {
	store, root := buildStore(t, map[string]string{
		"index.md": "# Index\n\nThis index has plenty of real content to stay above the empty-page threshold, and links to [[empty]] and [[ghost]].\n",
		// no frontmatter, short body — missing-owner/status + empty-page
		"empty.md": "x",
	})

	sc := New(root, store, nil, 90)
	res, err := sc.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	byKind := issuesByKind(res.Issues)
	if len(byKind[IssueEmptyPage]) != 1 || byKind[IssueEmptyPage][0].Path != "empty.md" {
		t.Fatalf("expected exactly empty.md flagged empty, got %+v", byKind[IssueEmptyPage])
	}
	if len(byKind[IssueMissingOwner]) < 1 {
		t.Fatalf("expected at least 1 missing-owner, got %+v", byKind[IssueMissingOwner])
	}
	if len(byKind[IssueBrokenLink]) != 1 {
		t.Fatalf("expected 1 broken-link (ghost), got %+v", byKind[IssueBrokenLink])
	}
}

func TestScan_DetectsStalePage(t *testing.T) {
	store, root := buildStore(t, map[string]string{
		"old.md": `---
title: Old
owner: alice
next-review: 2020-01-01
---

Some content here that is long enough to not be empty and pass the 50 char threshold for the test to behave nicely.
`,
	})
	sc := New(root, store, nil, 30)
	res, err := sc.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	by := issuesByKind(res.Issues)
	if len(by[IssueStale]) == 0 {
		t.Fatalf("expected stale issue, got %+v", res.Issues)
	}
	if !strings.Contains(by[IssueStale][0].Message, "2020-01-01") {
		t.Fatalf("stale message should reference the date, got %q", by[IssueStale][0].Message)
	}
}

func TestScan_DetectsDuplicateTitles(t *testing.T) {
	store, root := buildStore(t, map[string]string{
		"a/auth.md": `---
title: Auth
owner: alice
status: verified
reviewed: 2030-01-01
next-review: 2040-01-01
---

Content long enough to avoid empty-page flag and cover the minimum threshold.
`,
		"b/auth.md": `---
title: Auth
owner: bob
status: verified
reviewed: 2030-01-01
next-review: 2040-01-01
---

Another doc with the same title, long enough to avoid the empty-page flag.
`,
	})
	sc := New(root, store, nil, 90)
	res, err := sc.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	by := issuesByKind(res.Issues)
	if len(by[IssueDuplicate]) == 0 {
		t.Fatalf("expected duplicate, got %+v", res.Issues)
	}
}

func TestScan_DetectsContradictionBetweenSoTPages(t *testing.T) {
	store, root := buildStore(t, map[string]string{
		"a.md": `---
title: Billing A
owner: alice
status: verified
source-of-truth: true
tags: [billing, payments]
reviewed: 2030-01-01
next-review: 2040-01-01
---

Content long enough to avoid empty-page flag and hit fifty chars of body text here.
`,
		"b.md": `---
title: Billing B
owner: bob
status: verified
source-of-truth: true
tags: [billing, payments]
reviewed: 2030-01-01
next-review: 2040-01-01
---

Conflicting source of truth content, long enough to avoid the empty-page threshold.
`,
	})
	sc := New(root, store, nil, 90)
	res, err := sc.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	by := issuesByKind(res.Issues)
	if len(by[IssueContradiction]) == 0 {
		t.Fatalf("expected contradiction, got %+v", res.Issues)
	}
}

func TestScan_HealthyCount(t *testing.T) {
	store, root := buildStore(t, map[string]string{
		"index.md": `---
title: Index
owner: alice
status: verified
reviewed: 2030-01-01
next-review: 2040-01-01
---

This content is long enough to avoid being flagged as an empty page entirely.
`,
	})
	sc := New(root, store, nil, 90)
	res, err := sc.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if res.Scanned != 1 {
		t.Fatalf("expected 1 scanned, got %d", res.Scanned)
	}
	if res.Healthy != 1 {
		t.Fatalf("expected 1 healthy (index.md is exempt from orphan check), got %d; issues=%+v", res.Healthy, res.Issues)
	}
}
