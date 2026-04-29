package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Local implements Storage over a local directory.
//
// Methods accept context.Context to satisfy the interface but currently
// don't honour it — Go's stdlib file ops aren't cancellable. That's fine
// for a local FS where every op completes in microseconds; the parameter
// is here so a swap to a network-backed Storage doesn't churn callers.
type Local struct {
	root string
}

// NewLocal creates a local storage rooted at the given directory.
// It creates the directory if it doesn't exist.
func NewLocal(root string) (*Local, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root: %w", err)
	}
	if err := os.MkdirAll(abs, 0755); err != nil {
		return nil, fmt.Errorf("create root: %w", err)
	}
	// Resolve symlinks on root itself so guardPath's post-resolution Rel
	// check works correctly (e.g. macOS /var -> /private/var).
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, fmt.Errorf("resolve root symlinks: %w", err)
	}
	return &Local{root: resolved}, nil
}

// hidden returns true for internal dirs that should not be exposed via the
// API. Any leading-dot name is hidden, which already covers .git and .kiwi.
func hidden(name string) bool {
	return strings.HasPrefix(name, ".")
}

func (l *Local) AbsPath(path string) string {
	clean := filepath.Clean("/" + path)
	return filepath.Join(l.root, clean)
}

// GuardPath resolves userPath against root and rejects any result that
// escapes root via ".." or other traversal. It also blocks access to
// internal directories (.git, .kiwi, and any other dot-prefixed dirs)
// that must never be exposed through the API. Returns the absolute path.
func GuardPath(root, userPath string) (string, error) {
	clean := filepath.Clean("/" + userPath)
	abs := filepath.Join(root, clean)
	rel, err := filepath.Rel(root, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal denied: %s", userPath)
	}
	if hasHiddenComponent(rel) {
		return "", fmt.Errorf("access to internal path denied: %s", userPath)
	}
	return abs, nil
}

// hasHiddenComponent reports whether any segment of a slash-separated
// relative path starts with a dot (e.g. ".git/config", "a/.kiwi/state").
// The lone "." (current directory) is not considered hidden.
func hasHiddenComponent(rel string) bool {
	for _, seg := range strings.Split(filepath.ToSlash(rel), "/") {
		if seg != "." && strings.HasPrefix(seg, ".") {
			return true
		}
	}
	return false
}

func (l *Local) guardPath(path string) (string, error) {
	abs, err := GuardPath(l.root, path)
	if err != nil {
		return "", err
	}
	evaluated, err := filepath.EvalSymlinks(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return l.guardAncestor(abs)
		}
		return "", fmt.Errorf("eval symlinks: %w", err)
	}
	rel, err := filepath.Rel(l.root, evaluated)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("path outside root (symlink): %s", path)
	}
	return abs, nil
}

// guardAncestor walks up from abs until it finds an existing ancestor,
// evaluates its symlinks, and checks that the ancestor is inside root.
// Used when the target path doesn't exist yet (new file writes).
func (l *Local) guardAncestor(abs string) (string, error) {
	dir := abs
	for {
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
		evaluated, err := filepath.EvalSymlinks(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", fmt.Errorf("eval symlinks: %w", err)
		}
		rel, err := filepath.Rel(l.root, evaluated)
		if err != nil || strings.HasPrefix(rel, "..") {
			return "", fmt.Errorf("path outside root (symlink ancestor): %s", abs)
		}
		return abs, nil
	}
	return abs, nil
}

func (l *Local) Read(_ context.Context, path string) ([]byte, error) {
	abs, err := l.guardPath(path)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(abs)
}

func (l *Local) Write(_ context.Context, path string, content []byte) error {
	abs, err := l.guardPath(path)
	if err != nil {
		return err
	}
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create parent dirs: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".kiwi-write-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		// Clean up the temp file on any failure path.
		if tmpName != "" {
			os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("fsync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Chmod(tmpName, 0644); err != nil {
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if err := os.Rename(tmpName, abs); err != nil {
		return fmt.Errorf("rename temp to target: %w", err)
	}
	tmpName = "" // rename succeeded — don't remove in defer

	// fsync the parent directory so the new directory entry is durable.
	d, err := os.Open(dir)
	if err != nil {
		return nil // file is already in place; dir sync failure is non-fatal
	}
	d.Sync()
	d.Close()
	return nil
}

func (l *Local) Delete(_ context.Context, path string) error {
	abs, err := l.guardPath(path)
	if err != nil {
		return err
	}
	return os.Remove(abs)
}

func (l *Local) List(_ context.Context, path string) ([]Entry, error) {
	abs, err := l.guardPath(path)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(abs)
	if err != nil {
		return nil, err
	}

	// Normalize the dir path: strip leading/trailing slashes for consistent joining.
	cleanDir := strings.Trim(path, "/")

	result := make([]Entry, 0, len(entries))
	for _, e := range entries {
		if hidden(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		var relPath string
		if cleanDir == "" {
			relPath = e.Name()
		} else {
			relPath = cleanDir + "/" + e.Name()
		}
		if e.IsDir() {
			relPath += "/"
		}
		result = append(result, Entry{
			Path:    relPath,
			Name:    e.Name(),
			IsDir:   e.IsDir(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
		})
	}
	return result, nil
}

func (l *Local) Stat(_ context.Context, path string) (*Entry, error) {
	abs, err := l.guardPath(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, err
	}
	return &Entry{
		Path:    path,
		Name:    info.Name(),
		IsDir:   info.IsDir(),
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}, nil
}

func (l *Local) Exists(_ context.Context, path string) bool {
	abs, err := l.guardPath(path)
	if err != nil {
		return false
	}
	_, err = os.Stat(abs)
	return err == nil
}
