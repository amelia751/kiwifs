package versioning

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeRoot(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestCowSnapshotsOnCommit(t *testing.T) {
	root := t.TempDir()
	writeRoot(t, root, "note.md", "v1")
	c, err := NewCow(root)
	if err != nil {
		t.Fatalf("NewCow: %v", err)
	}
	ctx := context.Background()
	// First commit snapshots v1.
	if err := c.Commit(ctx, "note.md", "", ""); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Change the file and commit again — a tiny sleep so the nanosecond
	// timestamp differs from the first snapshot.
	time.Sleep(2 * time.Millisecond)
	writeRoot(t, root, "note.md", "v2")
	if err := c.Commit(ctx, "note.md", "", ""); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	versions, err := c.Log(ctx, "note.md")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("want 2 versions, got %d", len(versions))
	}
}

func TestCowPrunesToMaxVersions(t *testing.T) {
	root := t.TempDir()
	writeRoot(t, root, "note.md", "start")
	c, _ := NewCow(root)
	c.MaxVersions = 3

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		writeRoot(t, root, "note.md", string(rune('a'+i)))
		if err := c.Commit(ctx, "note.md", "", ""); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
		time.Sleep(time.Millisecond)
	}
	versions, _ := c.Log(ctx, "note.md")
	if len(versions) != 3 {
		t.Fatalf("want 3 versions after pruning, got %d", len(versions))
	}
}

func TestCowBlameReturnsUnsupportedError(t *testing.T) {
	c, _ := NewCow(t.TempDir())
	_, err := c.Blame(context.Background(), "anything.md")
	if !errors.Is(err, ErrBlameUnsupported) {
		t.Fatalf("want ErrBlameUnsupported, got %v", err)
	}
}

func TestCowPreDeleteSnapshotPreservesContent(t *testing.T) {
	root := t.TempDir()
	writeRoot(t, root, "gone.md", "final-content")
	c, _ := NewCow(root)
	if err := c.PreDeleteSnapshot("gone.md"); err != nil {
		t.Fatalf("pre-delete: %v", err)
	}
	// Delete the on-disk file as pipeline.Delete would.
	if err := os.Remove(filepath.Join(root, "gone.md")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	versions, err := c.Log(context.Background(), "gone.md")
	if err != nil || len(versions) != 1 {
		t.Fatalf("log after delete: %v %d", err, len(versions))
	}
	body, err := c.Show(context.Background(), "gone.md", versions[0].Hash)
	if err != nil || string(body) != "final-content" {
		t.Fatalf("show: %q %v", body, err)
	}
}
