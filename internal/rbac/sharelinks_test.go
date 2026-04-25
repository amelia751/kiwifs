package rbac

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *ShareStore {
	t.Helper()
	dir := t.TempDir()
	store, err := NewShareStore(dir)
	if err != nil {
		t.Fatalf("NewShareStore: %v", err)
	}
	return store
}

func TestCreateAndResolve_NoPassword(t *testing.T) {
	s := newTestStore(t)

	link, err := s.Create("notes/hello.md", "alice", 0, "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if link.Token == "" || link.ID == "" {
		t.Fatalf("expected non-empty token/id, got %+v", link)
	}
	if link.PasswordHash != "" || link.PasswordSalt != "" {
		t.Fatalf("link should not carry secrets in API response")
	}

	got, err := s.Resolve(link.Token, "")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got == nil || got.Path != "notes/hello.md" {
		t.Fatalf("unexpected resolve result: %+v", got)
	}
	if got.ViewCount != 1 {
		t.Fatalf("view count should be 1 after first resolve, got %d", got.ViewCount)
	}
}

func TestResolve_PasswordProtected(t *testing.T) {
	s := newTestStore(t)
	link, err := s.Create("a.md", "alice", 0, "sesame")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, err := s.Resolve(link.Token, "wrong"); !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("expected ErrInvalidPassword, got %v", err)
	}
	if _, err := s.Resolve(link.Token, ""); !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("expected ErrInvalidPassword for empty, got %v", err)
	}
	got, err := s.Resolve(link.Token, "sesame")
	if err != nil {
		t.Fatalf("correct password should resolve, got %v", err)
	}
	if got == nil {
		t.Fatalf("expected link, got nil")
	}
}

func TestResolve_Expired(t *testing.T) {
	s := newTestStore(t)
	link, err := s.Create("a.md", "alice", time.Millisecond, "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	got, err := s.Resolve(link.Token, "")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for expired link, got %+v", got)
	}
}

func TestRevoke(t *testing.T) {
	s := newTestStore(t)
	link, _ := s.Create("a.md", "alice", 0, "")
	if err := s.Revoke(link.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	got, _ := s.Resolve(link.Token, "")
	if got != nil {
		t.Fatalf("revoked link should not resolve, got %+v", got)
	}
	if err := s.Revoke("does-not-exist"); err == nil {
		t.Fatalf("Revoke of unknown id should error")
	}
}

func TestListForPath_RedactsSecrets(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Create("a.md", "alice", 0, "super-secret"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	links, err := s.ListForPath("a.md")
	if err != nil {
		t.Fatalf("ListForPath: %v", err)
	}
	if len(links) != 1 {
		t.Fatalf("expected 1 link, got %d", len(links))
	}
	if links[0].Password != "" || links[0].PasswordHash != "" || links[0].PasswordSalt != "" {
		t.Fatalf("secrets leaked in list response: %+v", links[0])
	}
}

func TestPersistence_MigratesPlainPassword(t *testing.T) {
	dir := t.TempDir()
	// Write a legacy file that stored the password in plaintext.
	s1, _ := NewShareStore(dir)
	legacy := &ShareLink{
		ID:        "abc",
		Path:      "a.md",
		Token:     "tok-legacy",
		Password:  "legacy",
		CreatedBy: "alice",
		CreatedAt: time.Now(),
	}
	s1.links["tok-legacy"] = legacy
	if err := s1.save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Reload: load() should upgrade the plaintext to a salted hash.
	s2, err := NewShareStore(dir)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, err := s2.Resolve("tok-legacy", "legacy")
	if err != nil {
		t.Fatalf("Resolve after migration: %v", err)
	}
	if got == nil {
		t.Fatalf("expected resolved link")
	}
	if _, err := s2.Resolve("tok-legacy", "wrong"); !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("migrated password should still be enforced, got %v", err)
	}
	// Confirm the on-disk file no longer contains the plaintext.
	data, err := os.ReadFile(filepath.Join(dir, ".kiwi", "state", "sharelinks.json"))
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if strings.Contains(string(data), `"password":"legacy"`) {
		t.Fatalf("plaintext password survived migration: %s", data)
	}
}
