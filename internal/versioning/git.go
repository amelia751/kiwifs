package versioning

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// gitCmdTimeout caps every git subprocess. A corrupt .git, runaway auto-gc,
// or a stuck git-lfs filter has historically been able to hang `git commit`
// for minutes, holding Pipeline.writeMu and blocking every other writer on
// the server. 30s is comfortably above any healthy local commit (sub-second)
// while bounded enough that one broken repo can't pin the process forever.
//
// Mutable as a `var` so tests can shrink it via gitCmdTimeoutForTest —
// production code never assigns it after init.
var gitCmdTimeout = 30 * time.Second

// gitCmdTimeoutForTest swaps in a shorter timeout and returns the previous
// value so the caller can restore it. Test-only — there's no production
// reason to vary the timeout at runtime.
func gitCmdTimeoutForTest(d time.Duration) time.Duration {
	prev := gitCmdTimeout
	gitCmdTimeout = d
	return prev
}

// Git implements Versioner using the system git binary.
//
// Write methods (Commit, BulkCommit, CommitDelete) are NOT internally
// serialised — the caller (Pipeline.writeMu) already serialises every
// path that reaches us, and an inner lock here would create a second
// mutex that future code could acquire in the wrong order and deadlock.
// Read paths (Log/Show/Diff/Blame) remain lock-free as before.
type Git struct {
	root string
	// baseEnv is a snapshot of os.Environ() taken at init. Commit methods
	// build per-call env slices by appending GIT_AUTHOR_*/GIT_COMMITTER_*
	// to a copy of this rather than calling os.Environ() on every write.
	baseEnv []string
}

// NewGit initialises (or opens) a git repo at root.
func NewGit(root string) (*Git, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	// Verify git is available.
	if _, err := exec.LookPath("git"); err != nil {
		return nil, fmt.Errorf("git not found in PATH: %w", err)
	}

	g := &Git{root: abs, baseEnv: os.Environ()}

	// Init if no .git directory. Init runs at construction so we have no
	// caller context — use Background; gitCmdTimeout still bounds it.
	if _, err := os.Stat(filepath.Join(abs, ".git")); os.IsNotExist(err) {
		if err := g.run(context.Background(), "git", "init"); err != nil {
			return nil, fmt.Errorf("git init: %w", err)
		}
		if err := g.run(context.Background(), "git", "config", "user.email", "kiwifs@internal"); err != nil {
			return nil, err
		}
		if err := g.run(context.Background(), "git", "config", "user.name", "KiwiFS"); err != nil {
			return nil, err
		}
	}

	return g, nil
}

// run executes a subcommand with a hard 30s deadline derived from the
// caller's context — exec.CommandContext kills the process when the
// context expires, and Setpgid: true puts the child in its own process
// group so kill propagates to anything it spawned (ssh, git-lfs,
// credential helpers) instead of leaving orphans behind.
//
// The timeout context wraps the caller's ctx so a caller-cancelled
// request takes effect immediately while still being capped at
// gitCmdTimeout for callers that pass context.Background().
func (g *Git) run(ctx context.Context, name string, args ...string) error {
	ctx, cancel := context.WithTimeout(ctx, gitCmdTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = g.root
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("%s %v: timed out after %s\n%s", name, args, gitCmdTimeout, stderr.String())
		}
		return fmt.Errorf("%s %v: %w\n%s", name, args, err, stderr.String())
	}
	return nil
}

// commitEnv returns an env slice with git author/committer set to actor.
// Appends to a copy of the cached baseEnv so os.Environ() isn't called on
// every commit — avoids ~200 allocations per call on a typical host.
func (g *Git) commitEnv(actor string) []string {
	env := make([]string, len(g.baseEnv), len(g.baseEnv)+4)
	copy(env, g.baseEnv)
	return append(env,
		"GIT_AUTHOR_NAME="+actor,
		"GIT_AUTHOR_EMAIL=kiwifs@internal",
		"GIT_COMMITTER_NAME="+actor,
		"GIT_COMMITTER_EMAIL=kiwifs@internal",
	)
}

// commit invokes `git commit -m <message>` with the actor's identity.
// Same 30s timeout / Setpgid:true contract as run/output — needed here
// because commit hooks (pre-commit lint, gpg sign) can hang independently
// of the git binary itself.
func (g *Git) commit(ctx context.Context, actor, message string) error {
	ctx, cancel := context.WithTimeout(ctx, gitCmdTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "commit", "-m", message)
	cmd.Dir = g.root
	cmd.Env = g.commitEnv(actor)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("git commit: timed out after %s\n%s", gitCmdTimeout, stderr.String())
		}
		return fmt.Errorf("git commit: %w\n%s", err, stderr.String())
	}
	return nil
}

// output is run's sibling for commands whose stdout we capture. Same 30s
// timeout + Setpgid:true contract as run; see its comment for rationale.
func (g *Git) output(ctx context.Context, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, gitCmdTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = g.root
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "", fmt.Errorf("%s %v: timed out after %s\n%s", name, args, gitCmdTimeout, stderr.String())
		}
		return "", fmt.Errorf("%s %v: %w\n%s", name, args, err, stderr.String())
	}
	return stdout.String(), nil
}

// Commit stages and commits a single path. Caller must serialise — the
// pipeline's writeMu funnels every Write/Delete/BulkWrite through one
// goroutine at a time, so Git holds no inner lock.
func (g *Git) Commit(ctx context.Context, path, actor, message string) error {
	if err := g.run(ctx, "git", "add", "--", path); err != nil {
		return err
	}
	// Check if there's anything staged.
	status, err := g.output(ctx, "git", "status", "--porcelain", "--", path)
	if err != nil {
		return err
	}
	if strings.TrimSpace(status) == "" {
		return nil // nothing changed, skip commit
	}
	return g.commit(ctx, actor, message)
}

// BulkCommit stages many paths and commits them under one message.
// Caller must serialise (Pipeline.writeMu).
func (g *Git) BulkCommit(ctx context.Context, paths []string, actor, message string) error {
	if len(paths) == 0 {
		return nil
	}

	addArgs := append([]string{"add", "--"}, paths...)
	if err := g.run(ctx, "git", addArgs...); err != nil {
		// If the add itself fails partway, unstage whatever made it in
		// so we don't contaminate the next commit with half of this
		// bulk write.
		_ = g.Unstage(ctx, paths)
		return err
	}

	statusArgs := append([]string{"status", "--porcelain", "--"}, paths...)
	status, err := g.output(ctx, "git", statusArgs...)
	if err != nil {
		_ = g.Unstage(ctx, paths)
		return err
	}
	if strings.TrimSpace(status) == "" {
		return nil
	}
	if err := g.commit(ctx, actor, message); err != nil {
		// The commit failed — reset the index so the staged files don't
		// linger. Without this, the next REST write picks them up.
		_ = g.Unstage(ctx, paths)
		return err
	}
	return nil
}

// Unstage removes paths from the git index without touching the working
// tree. Called by Pipeline.BulkWrite on rollback to keep the staging area
// consistent with the working-tree restore — otherwise a failed bulk
// commit leaves the index claiming those files were modified, and the
// next unrelated commit picks them up.
func (g *Git) Unstage(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	// `git reset HEAD -- <paths>` is the classic "unstage" recipe. On a
	// brand-new repo with no commits it fails; in that case we fall
	// back to `git rm --cached -r --force` which works regardless of
	// whether HEAD exists.
	args := append([]string{"reset", "HEAD", "--"}, paths...)
	if err := g.run(ctx, "git", args...); err != nil {
		args = append([]string{"rm", "--cached", "-r", "--force", "--ignore-unmatch", "--"}, paths...)
		return g.run(ctx, "git", args...)
	}
	return nil
}

// CommitDelete records a path's removal. Caller must serialise
// (Pipeline.writeMu).
func (g *Git) CommitDelete(ctx context.Context, path, actor string) error {
	// `git rm --force` stages the deletion. It may fail if the path is
	// already untracked (e.g. this is a trailing filesystem event after an
	// API-side delete that already ran both rm and commit) — in that case
	// `git add -A` still captures a pending working-tree removal.
	if err := g.run(ctx, "git", "rm", "--force", "--", path); err != nil {
		if err := g.run(ctx, "git", "add", "-A", "--", path); err != nil {
			return err
		}
	}

	status, err := g.output(ctx, "git", "status", "--porcelain", "--", path)
	if err != nil {
		return err
	}
	if strings.TrimSpace(status) == "" {
		return nil
	}
	return g.commit(ctx, actor, fmt.Sprintf("delete: %s", path))
}

func (g *Git) Log(ctx context.Context, path string) ([]Version, error) {
	out, err := g.output(ctx, "git", "log",
		"--pretty=format:%H\t%ai\t%an\t%s",
		"--", path)
	if err != nil {
		return nil, err
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return nil, nil
	}

	lines := strings.Split(out, "\n")
	versions := make([]Version, 0, len(lines))
	for _, line := range lines {
		parts := strings.SplitN(line, "\t", 4)
		if len(parts) < 4 {
			continue
		}
		versions = append(versions, Version{
			Hash:    parts[0],
			Date:    parts[1],
			Author:  parts[2],
			Message: parts[3],
		})
	}
	return versions, nil
}

func (g *Git) Show(ctx context.Context, path, hash string) ([]byte, error) {
	if strings.ContainsAny(path, "\n\r:") {
		return nil, fmt.Errorf("invalid path: must not contain newlines or colons")
	}
	out, err := g.output(ctx, "git", "show", fmt.Sprintf("%s:%s", hash, path))
	if err != nil {
		return nil, err
	}
	return []byte(out), nil
}

func (g *Git) Diff(ctx context.Context, path, fromHash, toHash string) (string, error) {
	return g.output(ctx, "git", "diff", fromHash, toHash, "--", path)
}

func (g *Git) Blame(ctx context.Context, path string) ([]BlameLine, error) {
	out, err := g.output(ctx, "git", "blame", "--porcelain", "--", path)
	if err != nil {
		// Untracked or empty file — no blame data yet.
		return nil, nil
	}
	return parseBlame(out), nil
}

// parseBlame parses the output of `git blame --porcelain`.
func parseBlame(out string) []BlameLine {
	type commitInfo struct {
		author string
		date   string
	}
	commits := make(map[string]*commitInfo)
	var lines []BlameLine

	rawLines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	i := 0
	for i < len(rawLines) {
		line := rawLines[i]
		// Hash header: 40-char hex followed by space and line numbers.
		if len(line) < 41 || line[40] != ' ' {
			i++
			continue
		}
		hash := line[:40]
		parts := strings.Fields(line[41:])
		if len(parts) < 2 {
			i++
			continue
		}
		finalLine, err := strconv.Atoi(parts[1])
		if err != nil {
			i++
			continue
		}
		i++

		if _, known := commits[hash]; !known {
			commits[hash] = &commitInfo{}
		}
		ci := commits[hash]

		// Read metadata + content until the \t-prefixed content line.
		var content string
		for i < len(rawLines) {
			h := rawLines[i]
			i++
			if strings.HasPrefix(h, "\t") {
				content = h[1:]
				break
			}
			if strings.HasPrefix(h, "author ") && !strings.HasPrefix(h, "author-") {
				ci.author = h[7:]
			} else if strings.HasPrefix(h, "author-time ") {
				ts, _ := strconv.ParseInt(h[12:], 10, 64)
				if ts != 0 {
					ci.date = time.Unix(ts, 0).UTC().Format(time.RFC3339)
				}
			}
		}

		lines = append(lines, BlameLine{
			Line:   finalLine,
			Author: ci.author,
			Date:   ci.date,
			Hash:   hash,
			Text:   content,
		})
	}
	return lines
}
