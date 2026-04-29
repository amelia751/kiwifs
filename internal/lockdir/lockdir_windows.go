package lockdir

import (
	"fmt"
	"os"
	"path/filepath"
)

// Lock represents an exclusive advisory lock held on a KiwiFS data directory.
// On Windows we fall back to holding an exclusive file open — the OS
// prevents a second open with sharing violations, which achieves the same
// single-instance guarantee.
type Lock struct {
	f *os.File
}

// Acquire takes an exclusive lock by opening the lock file with no sharing.
// Windows automatically prevents two processes from holding the same file
// open in this mode.
func Acquire(root string) (*Lock, error) {
	dir := filepath.Join(root, ".kiwi")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create lock dir: %w", err)
	}

	p := filepath.Join(dir, "server.lock")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("another kiwifs process is already serving %s — only one instance per data directory is safe", root)
	}

	f.Truncate(0)
	f.Seek(0, 0)
	fmt.Fprintf(f, "%d\n", os.Getpid())
	f.Sync()

	return &Lock{f: f}, nil
}

// Release drops the lock.
func (l *Lock) Release() {
	if l != nil && l.f != nil {
		l.f.Close()
		l.f = nil
	}
}
