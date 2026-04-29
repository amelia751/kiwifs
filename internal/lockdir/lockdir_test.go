//go:build !windows

package lockdir

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAcquireAndRelease(t *testing.T) {
	dir := t.TempDir()
	l, err := Acquire(dir)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer l.Release()

	lockFile := filepath.Join(dir, ".kiwi", "server.lock")
	if _, err := os.Stat(lockFile); err != nil {
		t.Fatalf("lock file not created: %v", err)
	}
}

func TestDoubleAcquireFails(t *testing.T) {
	dir := t.TempDir()
	l1, err := Acquire(dir)
	if err != nil {
		t.Fatalf("Acquire 1: %v", err)
	}
	defer l1.Release()

	_, err = Acquire(dir)
	if err == nil {
		t.Fatal("second Acquire should fail but succeeded")
	}
	t.Logf("expected error: %v", err)
}

func TestReleaseAllowsReacquire(t *testing.T) {
	dir := t.TempDir()
	l1, err := Acquire(dir)
	if err != nil {
		t.Fatalf("Acquire 1: %v", err)
	}
	l1.Release()

	l2, err := Acquire(dir)
	if err != nil {
		t.Fatalf("Acquire 2 after release: %v", err)
	}
	l2.Release()
}
