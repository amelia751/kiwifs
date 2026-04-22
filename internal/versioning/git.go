package versioning

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Git implements Versioner using the system git binary.
type Git struct {
	root string
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

	g := &Git{root: abs}

	// Init if no .git directory.
	if _, err := os.Stat(filepath.Join(abs, ".git")); os.IsNotExist(err) {
		if err := g.run("git", "init"); err != nil {
			return nil, fmt.Errorf("git init: %w", err)
		}
		if err := g.run("git", "config", "user.email", "kiwifs@internal"); err != nil {
			return nil, err
		}
		if err := g.run("git", "config", "user.name", "KiwiFS"); err != nil {
			return nil, err
		}
	}

	return g, nil
}

func (g *Git) run(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Dir = g.root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %v: %w\n%s", name, args, err, stderr.String())
	}
	return nil
}

func (g *Git) output(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = g.root
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s %v: %w\n%s", name, args, err, stderr.String())
	}
	return stdout.String(), nil
}

func (g *Git) Commit(path, actor, message string) error {
	if err := g.run("git", "add", "--", path); err != nil {
		return err
	}
	// Check if there's anything staged.
	status, err := g.output("git", "status", "--porcelain", "--", path)
	if err != nil {
		return err
	}
	if strings.TrimSpace(status) == "" {
		return nil // nothing changed, skip commit
	}

	env := append(os.Environ(),
		fmt.Sprintf("GIT_AUTHOR_NAME=%s", actor),
		"GIT_AUTHOR_EMAIL=kiwifs@internal",
		fmt.Sprintf("GIT_COMMITTER_NAME=%s", actor),
		"GIT_COMMITTER_EMAIL=kiwifs@internal",
	)
	cmd := exec.Command("git", "commit", "-m", message)
	cmd.Dir = g.root
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git commit: %w\n%s", err, stderr.String())
	}
	return nil
}

func (g *Git) CommitDelete(path, actor string) error {
	if err := g.run("git", "rm", "--force", "--", path); err != nil {
		return err
	}
	return g.Commit(path, actor, fmt.Sprintf("delete: %s", path))
}

func (g *Git) Log(path string) ([]Version, error) {
	out, err := g.output("git", "log",
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

func (g *Git) Show(path, hash string) ([]byte, error) {
	out, err := g.output("git", "show", fmt.Sprintf("%s:%s", hash, path))
	if err != nil {
		return nil, err
	}
	return []byte(out), nil
}

func (g *Git) Diff(path, fromHash, toHash string) (string, error) {
	return g.output("git", "diff", fromHash, toHash, "--", path)
}
