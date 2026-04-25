package bootstrap

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/versioning"
)

func newCfg(strategy, engine string) *config.Config {
	return &config.Config{
		Versioning: config.VersioningConfig{Strategy: strategy},
		Search:     config.SearchConfig{Engine: engine},
	}
}

// Build must produce a server and wire every required component — the
// fields the HTTP handlers and alt-protocol servers reach for must not
// be nil when their corresponding feature is on.
func TestBuildWithGrepAndNoop(t *testing.T) {
	dir := t.TempDir()
	stack, err := Build("default", dir, newCfg("none", "grep"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer stack.Close()

	if stack.Server == nil {
		t.Fatal("Server is nil")
	}
	if stack.Pipeline == nil {
		t.Fatal("Pipeline is nil")
	}
	if stack.Searcher == nil {
		t.Fatal("Searcher is nil")
	}
	if _, ok := stack.Versioner.(*versioning.Noop); !ok {
		t.Fatalf("Versioner = %T, want *versioning.Noop", stack.Versioner)
	}
	if stack.Vectors != nil {
		t.Fatal("Vectors should be nil when [search.vector] is disabled")
	}
}

// Git versioning should succeed when the binary is available; if it
// isn't, skip (the missing-git fallback is tested separately).
func TestBuildWithGit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not on PATH")
	}
	dir := t.TempDir()
	stack, err := Build("default", dir, newCfg("git", "grep"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer stack.Close()

	if _, ok := stack.Versioner.(*versioning.AsyncGit); !ok {
		t.Fatalf("Versioner = %T, want *versioning.AsyncGit (async is default for git)", stack.Versioner)
	}
	// NewGit runs `git init` when .git is missing — verify it did.
	if _, err := os.Stat(filepath.Join(dir, ".git")); err != nil {
		t.Fatalf(".git not created: %v", err)
	}
}

// CoW versioning must honour config.Versioning.MaxVersions. The pre-
// bootstrap copy of this wiring in spaces.AddSpace silently dropped this
// setting; the refactor's main behaviour fix depends on it being applied
// through the single Build path.
func TestBuildCowAppliesMaxVersions(t *testing.T) {
	dir := t.TempDir()
	cfg := newCfg("cow", "grep")
	cfg.Versioning.MaxVersions = 7

	stack, err := Build("default", dir, cfg)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer stack.Close()

	cow, ok := stack.Versioner.(*versioning.Cow)
	if !ok {
		t.Fatalf("Versioner = %T, want *versioning.Cow", stack.Versioner)
	}
	if cow.MaxVersions != 7 {
		t.Fatalf("MaxVersions = %d, want 7", cow.MaxVersions)
	}
}

// When strategy=git but git is missing, Build must not fail — the server
// still has to come up, just without versioning.
func TestBuildDegradesWhenGitMissing(t *testing.T) {
	t.Setenv("PATH", "")
	dir := t.TempDir()

	stack, err := Build("default", dir, newCfg("git", "grep"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer stack.Close()

	if _, ok := stack.Versioner.(*versioning.Noop); !ok {
		t.Fatalf("Versioner = %T, want *versioning.Noop after git-unavailable fallback", stack.Versioner)
	}
}

// SQLite FTS is the production default — it must wire through Build and
// plug into the Linker interface (the SQLite searcher implements both).
func TestBuildWithSQLiteSearchWiresLinker(t *testing.T) {
	dir := t.TempDir()
	stack, err := Build("default", dir, newCfg("none", "sqlite"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer stack.Close()

	if stack.Linker == nil {
		t.Fatal("Linker is nil — SQLite searcher should satisfy links.Linker")
	}
}

// Close must be idempotent-safe for callers that defer it and then
// explicitly shut down. Double-close shouldn't panic or error loudly.
func TestStackCloseIsSafeToCallTwice(t *testing.T) {
	dir := t.TempDir()
	stack, err := Build("default", dir, newCfg("none", "sqlite"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := stack.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second Close: searcher/vectors may error on already-closed handles,
	// which is fine — the point is that it doesn't panic.
	_ = stack.Close()
}
