//go:build !windows

package lockdir

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// Lock represents an exclusive advisory lock held on a KiwiFS data directory.
// The lock is automatically released by the kernel when the process exits
// (including SIGKILL), so there is no stale-lock problem — this is why flock
// is preferred over PID files per Prometheus, etcd, and Grafana Loki.
type Lock struct {
	f *os.File
}

// Acquire takes an exclusive, non-blocking flock(2) on
// <root>/.kiwi/server.lock. Returns an error if another process already
// holds the lock.
func Acquire(root string) (*Lock, error) {
	dir := filepath.Join(root, ".kiwi")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create lock dir: %w", err)
	}

	p := filepath.Join(dir, "server.lock")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another kiwifs process is already serving %s — only one instance per data directory is safe", root)
	}

	// Write PID for diagnostics only — the actual guard is the flock.
	f.Truncate(0)
	f.Seek(0, 0)
	fmt.Fprintf(f, "%d\n", os.Getpid())
	f.Sync()

	return &Lock{f: f}, nil
}

// Release drops the flock. Called on clean shutdown; the kernel also
// releases it on any form of process exit.
func (l *Lock) Release() {
	if l != nil && l.f != nil {
		syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
		l.f.Close()
		l.f = nil
	}
}
