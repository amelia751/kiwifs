package storage

import "time"

// Entry represents a file or directory in the knowledge base.
type Entry struct {
	Path    string  `json:"path"`
	Name    string  `json:"name"`
	IsDir   bool    `json:"isDir"`
	Size    int64   `json:"size,omitempty"`
	ModTime time.Time `json:"modTime,omitempty"`
}

// Storage is the abstract interface over any POSIX-like filesystem.
type Storage interface {
	Read(path string) ([]byte, error)
	Write(path string, content []byte) error
	Delete(path string) error
	List(path string) ([]Entry, error)
	Stat(path string) (*Entry, error)
	Exists(path string) bool
	// AbsPath resolves a relative knowledge path to an absolute filesystem path.
	AbsPath(path string) string
}
