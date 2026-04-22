package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Local implements Storage over a local directory.
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
	return &Local{root: abs}, nil
}

// hidden returns true for internal dirs that should not be exposed via the API.
func hidden(name string) bool {
	return strings.HasPrefix(name, ".") || name == ".git" || name == ".kiwi"
}

func (l *Local) AbsPath(path string) string {
	clean := filepath.Clean("/" + path)
	return filepath.Join(l.root, clean)
}

func (l *Local) guardPath(path string) (string, error) {
	abs := l.AbsPath(path)
	// Prevent path traversal outside root
	rel, err := filepath.Rel(l.root, abs)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("path outside root: %s", path)
	}
	return abs, nil
}

func (l *Local) Read(path string) ([]byte, error) {
	abs, err := l.guardPath(path)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(abs)
}

func (l *Local) Write(path string, content []byte) error {
	abs, err := l.guardPath(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		return fmt.Errorf("create parent dirs: %w", err)
	}
	return os.WriteFile(abs, content, 0644)
}

func (l *Local) Delete(path string) error {
	abs, err := l.guardPath(path)
	if err != nil {
		return err
	}
	return os.Remove(abs)
}

func (l *Local) List(path string) ([]Entry, error) {
	abs, err := l.guardPath(path)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(abs)
	if err != nil {
		return nil, fmt.Errorf("read dir: %w", err)
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

func (l *Local) Stat(path string) (*Entry, error) {
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

func (l *Local) Exists(path string) bool {
	abs, err := l.guardPath(path)
	if err != nil {
		return false
	}
	_, err = os.Stat(abs)
	return err == nil
}
