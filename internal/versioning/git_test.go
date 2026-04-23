package versioning

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func requireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not in PATH")
	}
}

func TestGitCommitAndLog(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	g, err := NewGit(dir)
	if err != nil {
		t.Fatalf("NewGit: %v", err)
	}
	writeRoot(t, dir, "note.md", "body v1")
	ctx := context.Background()
	if err := g.Commit(ctx, "note.md", "tester", "initial"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	vs, err := g.Log(ctx, "note.md")
	if err != nil || len(vs) != 1 {
		t.Fatalf("log: %v, %d", err, len(vs))
	}
	if vs[0].Author != "tester" {
		t.Fatalf("author: %s", vs[0].Author)
	}
}

// TestGitCallerContextCancellationPropagates verifies that a cancelled
// caller context kills the in-flight subprocess immediately rather than
// waiting for gitCmdTimeout. Pipeline.writeMu funnels every write
// through one goroutine, so a single hung commit holding a cancelled
// ctx is the worst-case scenario for tail latency on a busy server —
// the cancel must take effect before the 30s ceiling.
func TestGitCallerContextCancellationPropagates(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	g, err := NewGit(dir)
	if err != nil {
		t.Fatalf("NewGit: %v", err)
	}
	shim := filepath.Join(t.TempDir(), "hang.sh")
	if err := os.WriteFile(shim, []byte("#!/bin/sh\nexec sleep 600\n"), 0o755); err != nil {
		t.Fatalf("write shim: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err = g.run(ctx, shim)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected error from cancelled subprocess")
	}
	// Caller cancel should land well under 5s — it goes through
	// exec.CommandContext which sends SIGKILL on ctx.Done().
	if elapsed > 5*time.Second {
		t.Fatalf("subprocess took %s after cancel — ctx didn't propagate", elapsed)
	}
}

// TestGitSubprocessTimeoutKillsHangingChild verifies the timeout actually
// fires by invoking a hung subprocess. We call run() with a non-existent
// git command name pointing to a shim script that sleeps forever, with
// gitCmdTimeout dropped to 100ms so the test stays fast.
func TestGitSubprocessTimeoutKillsHangingChild(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	g, err := NewGit(dir)
	if err != nil {
		t.Fatalf("NewGit: %v", err)
	}

	// Stand up a shim that sleeps forever — invoked by absolute path so
	// we don't have to mutate PATH (which trips Go's race detector across
	// parallel tests).
	shim := filepath.Join(t.TempDir(), "hang.sh")
	if err := os.WriteFile(shim, []byte("#!/bin/sh\nexec sleep 600\n"), 0o755); err != nil {
		t.Fatalf("write shim: %v", err)
	}

	prev := gitCmdTimeout
	gitCmdTimeoutForTest(100 * time.Millisecond)
	defer gitCmdTimeoutForTest(prev)

	start := time.Now()
	err = g.run(context.Background(), shim)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected timeout error from hanging shim")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected timeout error, got: %v", err)
	}
	// Real production timeout is 30s; we set 100ms — anything taking
	// >5s means the kill didn't propagate.
	if elapsed > 5*time.Second {
		t.Fatalf("subprocess took %s to die — kill didn't propagate", elapsed)
	}
}

// TestGitConcurrentCommitsRequireExternalSerialisation documents the new
// Git contract: the type does NOT serialise its own writes, callers must.
// The test wraps every Commit in an external mutex (the role
// Pipeline.writeMu plays in production) and verifies the index stays
// healthy. Without external serialisation `git add` collisions on the
// single index.lock would surface as the "index.lock: File exists" errors
// the old internal mutex used to hide.
func TestGitConcurrentCommitsRequireExternalSerialisation(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	g, err := NewGit(dir)
	if err != nil {
		t.Fatalf("NewGit: %v", err)
	}
	var serialise sync.Mutex
	var wg sync.WaitGroup
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := filepath.Join("files", "f"+string(rune('a'+i))+".md")
			writeRoot(t, dir, name, "body")
			serialise.Lock()
			defer serialise.Unlock()
			if err := g.Commit(ctx, name, "tester", "concurrent"); err != nil {
				t.Errorf("commit %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < 8; i++ {
		name := filepath.Join("files", "f"+string(rune('a'+i))+".md")
		if _, err := g.Log(ctx, name); err != nil {
			t.Fatalf("log %s: %v", name, err)
		}
	}
}
