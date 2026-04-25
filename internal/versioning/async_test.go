package versioning

import (
	"context"
	"os"
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

	ag.Close()

	status, _ := exec.Command("git", "-C", dir, "status", "--porcelain", "--", "flush.md").CombinedOutput()
	if strings.TrimSpace(string(status)) != "" {
		t.Fatalf("file not flushed on Close: %s", status)
	}
}

func TestAsyncGit_ActorSeparation(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	g, err := NewGit(dir)
	if err != nil {
		t.Fatalf("NewGit: %v", err)
	}

	ag := NewAsyncGit(g, WithBatchWindow(500*time.Millisecond), WithBatchMaxSize(100))
	ctx := context.Background()

	writeRoot(t, dir, "a.md", "alice content")
	ag.Commit(ctx, "a.md", "alice", "alice: a.md")

	writeRoot(t, dir, "b.md", "bob content")
	ag.Commit(ctx, "b.md", "bob", "bob: b.md")

	writeRoot(t, dir, "c.md", "alice again")
	ag.Commit(ctx, "c.md", "alice", "alice: c.md")

	ag.Close()

	out, err := exec.Command("git", "-C", dir, "log", "--format=%an|%s").CombinedOutput()
	if err != nil {
		t.Fatalf("git log: %v\n%s", err, out)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, line := range lines {
		parts := strings.SplitN(line, "|", 2)
		if len(parts) != 2 {
			continue
		}
		actor := parts[0]
		if actor != "alice" && actor != "bob" {
			t.Fatalf("unexpected actor %q in commit %q", actor, line)
		}
	}
	t.Logf("commits: %s", strings.Join(lines, " / "))
}

func TestAsyncGit_PreservesMessage(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	g, err := NewGit(dir)
	if err != nil {
		t.Fatalf("NewGit: %v", err)
	}

	ag := NewAsyncGit(g, WithBatchWindow(100*time.Millisecond), WithBatchMaxSize(100))
	ctx := context.Background()

	writeRoot(t, dir, "doc.md", "single file")
	ag.Commit(ctx, "doc.md", "tester", "my custom message")
	ag.Close()

	out, err := exec.Command("git", "-C", dir, "log", "-1", "--format=%s").CombinedOutput()
	if err != nil {
		t.Fatalf("git log: %v\n%s", err, out)
	}
	msg := strings.TrimSpace(string(out))
	if msg != "my custom message" {
		t.Fatalf("expected original message, got %q", msg)
	}
}

func TestAsyncGit_JournalAndClear(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	g, err := NewGit(dir)
	if err != nil {
		t.Fatalf("NewGit: %v", err)
	}

	journal := filepath.Join(dir, ".kiwi-uncommitted.log")
	ag := NewAsyncGit(g,
		WithBatchWindow(100*time.Millisecond),
		WithUncommittedLog(journal),
	)
	ctx := context.Background()

	writeRoot(t, dir, "j.md", "journal test")
	ag.Commit(ctx, "j.md", "tester", "journaled")

	// Journal file must exist before flush completes.
	time.Sleep(10 * time.Millisecond)
	data, err := os.ReadFile(journal)
	if err != nil {
		t.Fatalf("journal should exist after Commit: %v", err)
	}
	if !strings.Contains(string(data), "j.md") {
		t.Fatalf("journal should contain path, got: %q", data)
	}

	ag.Close()

	// After successful flush, journal must be cleared.
	if _, err := os.Stat(journal); !os.IsNotExist(err) {
		t.Fatalf("journal should be removed after successful flush, err=%v", err)
	}
}
