// Package comments stores inline annotations on markdown files.
//
// One JSON file per document lives at .kiwi/comments/<path>.json and holds
// an ordered array of Comment records. The directory mirrors the knowledge
// tree so a single path-to-JSON mapping is predictable and easy to git-log.
//
// Each comment carries an "anchor" — a chunk of text with short prefix and
// suffix context — so the UI can re-locate the range in the rendered page
// even after nearby edits. This mirrors the W3C Web Annotation TextQuote
// selector pattern.
package comments

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Anchor is the text-quote selector used to re-locate a comment range.
// Quote is the selected text; Prefix and Suffix provide disambiguation when
// the same Quote appears multiple times. Offset is a hint for fast lookup.
type Anchor struct {
	Quote  string `json:"quote"`
	Prefix string `json:"prefix,omitempty"`
	Suffix string `json:"suffix,omitempty"`
	Offset int    `json:"offset,omitempty"`
}

// Comment is a single annotation attached to a markdown file.
type Comment struct {
	ID        string    `json:"id"`
	Path      string    `json:"path"`
	Anchor    Anchor    `json:"anchor"`
	Body      string    `json:"body"`
	Author    string    `json:"author"`
	CreatedAt time.Time `json:"createdAt"`
	Resolved  bool      `json:"resolved,omitempty"`
}

// Store persists comments to disk. Safe for concurrent use — each
// markdown path has its own RWMutex so concurrent List() calls for the
// same file don't serialise on a single mutex. Writes still take the
// exclusive lock so Add/Delete see a consistent snapshot.
type Store struct {
	root string
	// mapMu guards the lock map itself (insert-on-first-use).
	mapMu sync.Mutex
	locks map[string]*sync.RWMutex
}

// New returns a store rooted at the knowledge root. The comments directory
// (.kiwi/comments) is created lazily on first write.
func New(root string) (*Store, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root: %w", err)
	}
	return &Store{root: abs, locks: make(map[string]*sync.RWMutex)}, nil
}

// lockFor returns the RWMutex guarding a single markdown path. Inserted on
// first use under mapMu. Entries are never removed — the lock set is
// bounded by the number of distinct paths that ever received a comment,
// which is negligible compared to file-count concerns.
func (s *Store) lockFor(path string) *sync.RWMutex {
	s.mapMu.Lock()
	defer s.mapMu.Unlock()
	if m, ok := s.locks[path]; ok {
		return m
	}
	m := &sync.RWMutex{}
	s.locks[path] = m
	return m
}

// FilePath returns the repo-relative path of the JSON file that stores
// comments for the given markdown path. Exported so API handlers can hand
// the path to the versioner for git commits.
func (s *Store) FilePath(path string) string {
	return filepath.ToSlash(filepath.Join(".kiwi", "comments", path+".json"))
}

// absPath returns the absolute path of the comments JSON file and guards
// against traversal outside <root>/.kiwi/comments.
func (s *Store) absPath(path string) (string, error) {
	if path == "" {
		return "", errors.New("path is required")
	}
	base := filepath.Join(s.root, ".kiwi", "comments")
	abs := filepath.Join(base, filepath.FromSlash(path)+".json")
	rel, err := filepath.Rel(base, abs)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("path outside comments dir: %s", path)
	}
	return abs, nil
}

// List returns all comments for a markdown path, oldest first. A missing
// file yields an empty slice rather than an error.
func (s *Store) List(path string) ([]Comment, error) {
	abs, err := s.absPath(path)
	if err != nil {
		return nil, err
	}
	m := s.lockFor(path)
	m.RLock()
	defer m.RUnlock()
	return readFile(abs)
}

// Add appends a new comment. ID, Path, and CreatedAt are filled in; the
// caller may leave them zero. Returns the fully-populated record so the
// HTTP handler can echo it back.
func (s *Store) Add(path string, c Comment) (Comment, error) {
	abs, err := s.absPath(path)
	if err != nil {
		return Comment{}, err
	}
	if strings.TrimSpace(c.Body) == "" {
		return Comment{}, errors.New("body is required")
	}
	if strings.TrimSpace(c.Anchor.Quote) == "" {
		return Comment{}, errors.New("anchor.quote is required")
	}

	m := s.lockFor(path)
	m.Lock()
	defer m.Unlock()

	list, err := readFile(abs)
	if err != nil {
		return Comment{}, err
	}

	c.ID = newID()
	c.Path = path
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	list = append(list, c)

	if err := writeFile(abs, list); err != nil {
		return Comment{}, err
	}
	return c, nil
}

// Delete removes the comment with the given id from the file at path.
// Returns os.ErrNotExist when the id is not found.
func (s *Store) Delete(path, id string) error {
	if id == "" {
		return errors.New("id is required")
	}
	abs, err := s.absPath(path)
	if err != nil {
		return err
	}

	m := s.lockFor(path)
	m.Lock()
	defer m.Unlock()

	list, err := readFile(abs)
	if err != nil {
		return err
	}
	idx := -1
	for i, c := range list {
		if c.ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return os.ErrNotExist
	}
	list = append(list[:idx], list[idx+1:]...)

	// Remove the JSON file entirely when empty — keeps git diffs clean and
	// the .kiwi/comments tree free of stub files.
	if len(list) == 0 {
		if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	return writeFile(abs, list)
}

func readFile(abs string) ([]Comment, error) {
	data, err := os.ReadFile(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return []Comment{}, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return []Comment{}, nil
	}
	var list []Comment
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parse %s: %w", abs, err)
	}
	return list, nil
}

func writeFile(abs string, list []Comment) error {
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(abs, data, 0644)
}

func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
