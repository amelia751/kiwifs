package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalReadWriteDelete(t *testing.T) {
	dir := t.TempDir()
	l, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := l.Write(context.Background(), "a/b.md", []byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := l.Read(context.Background(), "a/b.md")
	if err != nil || string(got) != "x" {
		t.Fatalf("read: %q %v", got, err)
	}
	if !l.Exists(context.Background(), "a/b.md") {
		t.Fatalf("Exists false")
	}
	if err := l.Delete(context.Background(), "a/b.md"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if l.Exists(context.Background(), "a/b.md") {
		t.Fatalf("still exists")
	}
}

func TestLocalListHidesDotDirs(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLocal(dir)
	// Put a file in the root and a .git subtree.
	if err := os.Mkdir(filepath.Join(dir, ".git"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".git", "HEAD"), []byte(""), 0644); err != nil {
		t.Fatal(err)
	}
	if err := l.Write(context.Background(), "visible.md", []byte("hi")); err != nil {
		t.Fatal(err)
	}
	entries, err := l.List(context.Background(), "/")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, e := range entries {
		if e.Name == ".git" || e.Name == ".kiwi" {
			t.Fatalf("hidden dir leaked: %s", e.Name)
		}
	}
}

func TestLocalConfinesTraversalToRoot(t *testing.T) {
	// filepath.Clean("/" + "../escape") → "/escape", so traversal attempts
	// collapse onto root rather than escaping it. Verify the path the
	// storage layer resolves stays within root.
	dir := t.TempDir()
	l, _ := NewLocal(dir)
	abs := l.AbsPath("../escape.md")
	if !strings.HasPrefix(abs, l.root) {
		t.Fatalf("traversal escaped root: %s not under %s", abs, l.root)
	}
}
