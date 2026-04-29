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
	dir := t.TempDir()
	l, _ := NewLocal(dir)
	abs := l.AbsPath("../escape.md")
	if !strings.HasPrefix(abs, l.root) {
		t.Fatalf("traversal escaped root: %s not under %s", abs, l.root)
	}
}

func TestGuardPath_NullByte(t *testing.T) {
	dir := t.TempDir()
	_, err := GuardPath(dir, "file\x00.md")
	if err == nil {
		t.Fatal("null byte in path should be rejected")
	}
	if !strings.Contains(err.Error(), "null byte") {
		t.Fatalf("error should mention null byte: %v", err)
	}
}

func TestGuardPath_ControlChars(t *testing.T) {
	dir := t.TempDir()
	for _, c := range []byte{0x01, 0x0A, 0x0D, 0x1F} {
		path := "file" + string(c) + ".md"
		_, err := GuardPath(dir, path)
		if err == nil {
			t.Fatalf("control char 0x%02x should be rejected", c)
		}
	}
}

func TestGuardPath_TabAllowed(t *testing.T) {
	dir := t.TempDir()
	_, err := GuardPath(dir, "notes\ttab.md")
	if err != nil {
		t.Fatalf("tab should be allowed in paths: %v", err)
	}
}

func TestGuardPath_LongSegment(t *testing.T) {
	dir := t.TempDir()
	long := strings.Repeat("a", 256) + ".md"
	_, err := GuardPath(dir, long)
	if err == nil {
		t.Fatal("256-byte filename should be rejected")
	}
	if !strings.Contains(err.Error(), "255 bytes") {
		t.Fatalf("error should mention 255 bytes: %v", err)
	}
}

func TestGuardPath_255ByteSegmentAllowed(t *testing.T) {
	dir := t.TempDir()
	name := strings.Repeat("a", 252) + ".md"
	_, err := GuardPath(dir, name)
	if err != nil {
		t.Fatalf("255-byte filename should be allowed: %v", err)
	}
}

func TestGuardPath_LeadingTrailingSpaces(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLocal(dir)
	ctx := context.Background()

	if err := l.Write(ctx, " leading.md", []byte("data")); err != nil {
		t.Fatalf("write leading space: %v", err)
	}
	got, err := l.Read(ctx, " leading.md")
	if err != nil {
		t.Fatalf("read leading space: %v", err)
	}
	if string(got) != "data" {
		t.Fatalf("content = %q", got)
	}

	if err := l.Write(ctx, "trailing.md ", []byte("data2")); err != nil {
		t.Fatalf("write trailing space: %v", err)
	}
	got2, err := l.Read(ctx, "trailing.md ")
	if err != nil {
		t.Fatalf("read trailing space: %v", err)
	}
	if string(got2) != "data2" {
		t.Fatalf("content = %q", got2)
	}
}

func TestWrite_NullByteRejected(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLocal(dir)
	err := l.Write(context.Background(), "evil\x00.md", []byte("data"))
	if err == nil {
		t.Fatal("write with null byte should fail")
	}
}

func TestRead_NullByteRejected(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLocal(dir)
	_, err := l.Read(context.Background(), "evil\x00.md")
	if err == nil {
		t.Fatal("read with null byte should fail")
	}
}

func TestDelete_NullByteRejected(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLocal(dir)
	err := l.Delete(context.Background(), "evil\x00.md")
	if err == nil {
		t.Fatal("delete with null byte should fail")
	}
}

func TestGuardPath_GitattributesBlocked(t *testing.T) {
	dir := t.TempDir()
	// Root-level .gitattributes — caught by hidden component check
	_, err := GuardPath(dir, ".gitattributes")
	if err == nil {
		t.Fatal(".gitattributes should be blocked")
	}

	// Subdirectory .gitattributes — also caught by hidden component check
	_, err2 := GuardPath(dir, "subdir/.gitattributes")
	if err2 == nil {
		t.Fatal("subdir/.gitattributes should be blocked")
	}

	// Verify isDangerousFile catches the basename independently
	if !isDangerousFile(".gitattributes") {
		t.Fatal("isDangerousFile should flag .gitattributes")
	}
	if !isDangerousFile("any/path/.gitmodules") {
		t.Fatal("isDangerousFile should flag .gitmodules")
	}
	if isDangerousFile("normal.md") {
		t.Fatal("isDangerousFile should not flag normal.md")
	}
}

func TestGuardPath_GitmodulesBlocked(t *testing.T) {
	dir := t.TempDir()
	_, err := GuardPath(dir, "subdir/.gitmodules")
	if err == nil {
		t.Fatal(".gitmodules should be blocked")
	}
}

func TestGuardPath_NormalGitFilesAllowed(t *testing.T) {
	dir := t.TempDir()
	_, err := GuardPath(dir, "gitattributes-info.md")
	if err != nil {
		t.Fatalf("gitattributes-info.md should be allowed: %v", err)
	}
}
