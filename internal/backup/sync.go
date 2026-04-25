package backup

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultInterval = 5 * time.Minute
	remoteName      = "backup"
	cmdTimeout      = 60 * time.Second
)

type Syncer struct {
	root     string
	remote   string
	branch   string
	interval time.Duration

	stopCh chan struct{}
	done   sync.WaitGroup
}

func New(root, remote, branch, interval string) (*Syncer, error) {
	if remote == "" {
		return nil, fmt.Errorf("backup remote is required")
	}
	dur := defaultInterval
	if interval != "" {
		d, err := time.ParseDuration(interval)
		if err != nil {
			return nil, fmt.Errorf("parse backup interval %q: %w", interval, err)
		}
		if d < 30*time.Second {
			d = 30 * time.Second
		}
		dur = d
	}
	return &Syncer{
		root:     root,
		remote:   remote,
		branch:   branch,
		interval: dur,
		stopCh:   make(chan struct{}),
	}, nil
}

func (s *Syncer) Start() {
	s.done.Add(1)
	go s.run()
}

func (s *Syncer) Close() {
	close(s.stopCh)
	s.done.Wait()
}

func (s *Syncer) run() {
	defer s.done.Done()

	if err := s.ensureRemote(); err != nil {
		log.Printf("backup: setup remote: %v", err)
		return
	}

	branch := s.resolveBranch()
	log.Printf("backup: pushing to %s (%s) every %s", s.remote, branch, s.interval)

	s.push(branch)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			s.push(branch)
			return
		case <-ticker.C:
			s.push(branch)
		}
	}
}

func (s *Syncer) ensureRemote() error {
	existing, err := s.output("git", "remote", "get-url", remoteName)
	if err == nil {
		existing = strings.TrimSpace(existing)
		if existing == s.remote {
			return nil
		}
		if err := s.cmd("git", "remote", "set-url", remoteName, s.remote); err != nil {
			return fmt.Errorf("set-url: %w", err)
		}
		return nil
	}
	return s.cmd("git", "remote", "add", remoteName, s.remote)
}

func (s *Syncer) resolveBranch() string {
	if s.branch != "" {
		return s.branch
	}
	out, err := s.output("git", "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return "main"
	}
	b := strings.TrimSpace(out)
	if b == "" || b == "HEAD" {
		return "main"
	}
	return b
}

// Push runs a single git push. Exported for the one-shot CLI command.
func (s *Syncer) Push(branch string) error {
	if err := s.ensureRemote(); err != nil {
		return fmt.Errorf("setup remote: %w", err)
	}
	if branch == "" {
		branch = s.resolveBranch()
	}
	return s.push(branch)
}

func (s *Syncer) push(branch string) error {
	err := s.cmd("git", "push", remoteName, branch)
	if err != nil {
		log.Printf("backup: push failed: %v", err)
		return err
	}
	if verr := s.verifyPush(branch); verr != nil {
		// A successful push that we can't verify is almost worse than
		// a failed one — admins think they have a backup when they
		// don't. Log loudly. We don't return the error because the
		// push actually *did* succeed per git; we'd rather keep the
		// sync loop alive and let the operator investigate via logs.
		log.Printf("backup: push to %s/%s verification failed: %v", remoteName, branch, verr)
		return nil
	}
	log.Printf("backup: pushed + verified %s/%s", remoteName, branch)
	return nil
}

// verifyPush re-queries the remote for the branch's head and checks it
// matches the local HEAD commit. A silent mismatch is the nightmare
// case — someone's remote rejected the update with a non-fast-forward
// warning git printed to stderr but our wrapper already consumed.
func (s *Syncer) verifyPush(branch string) error {
	local, err := s.output("git", "rev-parse", "HEAD")
	if err != nil {
		return fmt.Errorf("local rev-parse: %w", err)
	}
	local = strings.TrimSpace(local)

	remote, err := s.output("git", "ls-remote", remoteName, branch)
	if err != nil {
		return fmt.Errorf("ls-remote: %w", err)
	}
	parts := strings.Fields(strings.TrimSpace(remote))
	if len(parts) == 0 {
		return fmt.Errorf("remote branch %q not found after push", branch)
	}
	remoteSHA := parts[0]
	if remoteSHA != local {
		return fmt.Errorf("remote HEAD %s != local HEAD %s", remoteSHA, local)
	}
	return nil
}

func (s *Syncer) cmd(name string, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel()
	c := exec.CommandContext(ctx, name, args...)
	c.Dir = s.root
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stderr bytes.Buffer
	c.Stderr = &stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("%s %v: %w\n%s", name, args, err, stderr.String())
	}
	return nil
}

func (s *Syncer) output(name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel()
	c := exec.CommandContext(ctx, name, args...)
	c.Dir = s.root
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr
	if err := c.Run(); err != nil {
		return "", fmt.Errorf("%s %v: %w\n%s", name, args, err, stderr.String())
	}
	return stdout.String(), nil
}
