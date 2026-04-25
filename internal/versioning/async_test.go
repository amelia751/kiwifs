package versioning

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAsyncGit_BatchesCommits(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	g, err := NewGit(dir)
	if err != nil {
		t.Fatalf("NewGit: %v", err)
	}

	ag := NewAsyncGit(g, WithBatchWindow(100*time.Millisecond), WithBatchMaxSize(50))
	ctx := context.Background()

	const n = 20
	for i := 0; i < n; i++ {
		name := filepath.Join("files", "f"+string(rune('a'+i))+".md")
		writeRoot(t, dir, name, "body")
		if err := ag.Commit(ctx, name, "tester", "write "+name); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}

	if err := ag.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Count commits — should be significantly fewer than 20.
	out, err := exec.Command("git", "-C", dir, "log", "--oneline").CombinedOutput()
	if err != nil {
		t.Fatalf("git log: %v\n%s", err, out)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	commitCount := len(lines)
	if commitCount >= n {
		t.Fatalf("expected fewer than %d commits (batching), got %d", n, commitCount)
	}
	t.Logf("batched %d writes into %d commits", n, commitCount)

	// All 20 files must be committed (none lost).
	for i := 0; i < n; i++ {
		name := filepath.Join("files", "f"+string(rune('a'+i))+".md")
		status, err := exec.Command("git", "-C", dir, "status", "--porcelain", "--", name).CombinedOutput()
		if err != nil {
			t.Fatalf("git status %s: %v", name, err)
		}
		if strings.TrimSpace(string(status)) != "" {
			t.Fatalf("file %s not committed: %s", name, status)
		}
	}
}

func TestAsyncGit_ReadThrough(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	g, err := NewGit(dir)
	if err != nil {
		t.Fatalf("NewGit: %v", err)
	}

	ctx := context.Background()
	writeRoot(t, dir, "doc.md", "hello world")
	if err := g.Commit(ctx, "doc.md", "tester", "init"); err != nil {
		t.Fatalf("sync commit: %v", err)
	}

	ag := NewAsyncGit(g)
	defer ag.Close()

	vs, err := ag.Log(ctx, "doc.md")
	if err != nil || len(vs) != 1 {
		t.Fatalf("Log via async: err=%v, len=%d", err, len(vs))
	}

	content, err := ag.Show(ctx, "doc.md", vs[0].Hash)
	if err != nil {
		t.Fatalf("Show: %v", err)
	}
	if string(content) != "hello world" {
		t.Fatalf("Show content = %q", content)
	}
}

func TestAsyncGit_CloseFlushes(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	g, err := NewGit(dir)
	if err != nil {
		t.Fatalf("NewGit: %v", err)
	}

	ag := NewAsyncGit(g, WithBatchWindow(10*time.Second))
	ctx := context.Background()

	writeRoot(t, dir, "flush.md", "content")
	ag.Commit(ctx, "flush.md", "tester", "test")

	// Close should flush without waiting for the 10s window.
	ag.Close()

	status, _ := exec.Command("git", "-C", dir, "status", "--porcelain", "--", "flush.md").CombinedOutput()
	if strings.TrimSpace(string(status)) != "" {
		t.Fatalf("file not flushed on Close: %s", status)
	}
}
