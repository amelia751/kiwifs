package storage

import (
	"context"
	"path/filepath"
	"strings"
	"time"
)

// Entry represents a file or directory in the knowledge base.
type Entry struct {
	Path    string    `json:"path"`
	Name    string    `json:"name"`
	IsDir   bool      `json:"isDir"`
	Size    int64     `json:"size,omitempty"`
	ModTime time.Time `json:"modTime,omitempty"`
}

// Storage is the abstract interface over any POSIX-like filesystem.
//
// Every I/O method takes context.Context as its first parameter. The local
// backend currently ignores it — Go's stdlib file ops aren't ctx-aware —
// but the signature is in place so future remote backends (S3, WebDAV
// passthrough, network FS) can honour cancellation without a breaking
// interface change. AbsPath is a pure path computation and stays
// ctx-free.
type Storage interface {
	Read(ctx context.Context, path string) ([]byte, error)
	Write(ctx context.Context, path string, content []byte) error
	Delete(ctx context.Context, path string) error
	List(ctx context.Context, path string) ([]Entry, error)
	Stat(ctx context.Context, path string) (*Entry, error)
	Exists(ctx context.Context, path string) bool
	// AbsPath resolves a relative knowledge path to an absolute filesystem path.
	AbsPath(path string) string
}

// WalkFunc is invoked for every entry Walk encounters. Return a non-nil
// error to stop the traversal and propagate it back to the caller.
type WalkFunc func(entry Entry) error

// Walk recursively visits every entry under root, respecting the storage
// layer's hidden-file filter (anything List skips stays skipped). It's a
// thin wrapper around repeated List calls — uses no OS-specific APIs so
// non-local backends (future S3/Postgres storage) can plug in without
// re-implementing walk semantics. Only .md files are yielded; directories
// are descended into but not emitted. This consolidates the "walk root,
// skip dot-dirs, filter .md" pattern previously copy-pasted across
// search, vectorstore, graph, lint, and watcher.
func Walk(ctx context.Context, s Storage, root string, fn WalkFunc) error {
	entries, err := s.List(ctx, root)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.IsDir {
			if err := Walk(ctx, s, e.Path, fn); err != nil {
				return err
			}
			continue
		}
		if !strings.HasSuffix(strings.ToLower(e.Name), ".md") {
			continue
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// IsKnowledgeFile reports whether path names a user-facing knowledge file
// (currently: .md, not under .git/ or .kiwi/). Used by both the search
// indexer and the vector store to decide what to index.
func IsKnowledgeFile(path string) bool {
	if !strings.HasSuffix(path, ".md") {
		return false
	}
	clean := filepath.ToSlash(filepath.Clean(path))
	return !strings.HasPrefix(clean, ".git/") && !strings.HasPrefix(clean, ".kiwi/")
}
