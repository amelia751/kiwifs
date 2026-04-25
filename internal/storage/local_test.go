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

func TestLocalRejectsSymlinkEscape(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLocal(dir)

	// Create a symlink inside root that points outside it.
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, "secret.txt"), []byte("sensitive"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "escape")); err != nil {
		t.Skipf("symlinks not supported: %v", err)
	}

	// Reading through the symlink should be rejected.
	_, err := l.Read(context.Background(), "escape/secret.txt")
	if err == nil {
		t.Fatal("expected error reading through escaping symlink, got nil")
	}
	if !strings.Contains(err.Error(), "outside root") {
		t.Fatalf("expected 'outside root' error, got: %v", err)
	}

	// Writing through the symlink should also be rejected.
	err = l.Write(context.Background(), "escape/new.md", []byte("injected"))
	if err == nil {
		t.Fatal("expected error writing through escaping symlink, got nil")
	}
}

func TestLocalAllowsInternalSymlink(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLocal(dir)

	// Create a subdirectory and a symlink that stays inside root.
	subdir := filepath.Join(dir, "real")
	if err := os.MkdirAll(subdir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subdir, "ok.md"), []byte("safe"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(subdir, filepath.Join(dir, "alias")); err != nil {
		t.Skipf("symlinks not supported: %v", err)
	}

	// Reading through an internal symlink should succeed.
	data, err := l.Read(context.Background(), "alias/ok.md")
	if err != nil {
		t.Fatalf("expected internal symlink read to succeed: %v", err)
	}
	if string(data) != "safe" {
		t.Fatalf("unexpected content: %q", data)
	}
}

func TestLocalWriteAtomic(t *testing.T) {
	dir := t.TempDir()
	l, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()
	path := "atomic/test.md"

	if err := l.Write(ctx, path, []byte("first")); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	got, err := l.Read(ctx, path)
	if err != nil || string(got) != "first" {
		t.Fatalf("read 1: %q %v", got, err)
	}

	if err := l.Write(ctx, path, []byte("second")); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	got, err = l.Read(ctx, path)
	if err != nil || string(got) != "second" {
		t.Fatalf("read 2: want %q got %q err %v", "second", got, err)
	}

	// Verify no temp files remain after a successful write.
	entries, err := os.ReadDir(filepath.Join(dir, "atomic"))
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".kiwi-write-") {
			t.Fatalf("leftover temp file: %s", e.Name())
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
