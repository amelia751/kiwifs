package storage

import (
	"context"
	"path/filepath"
	"strings"
)

type TreeEntry struct {
	Path     string       `json:"path"`
	Name     string       `json:"name"`
	IsDir    bool         `json:"isDir"`
	Size     int64        `json:"size,omitempty"`
	Children []*TreeEntry `json:"children,omitempty"`
}

func BuildTree(ctx context.Context, store Storage, path string, depth int) (*TreeEntry, error) {
	entries, err := store.List(ctx, path)
	if err != nil {
		return nil, err
	}

	cleanPath := strings.Trim(path, "/")
	displayName := filepath.Base(cleanPath)
	if cleanPath == "" {
		displayName = "/"
	}
	root := &TreeEntry{
		Path:  cleanPath,
		Name:  displayName,
		IsDir: true,
	}

	for _, e := range entries {
		child := &TreeEntry{
			Path:  e.Path,
			Name:  e.Name,
			IsDir: e.IsDir,
			Size:  e.Size,
		}
		if e.IsDir && depth > 0 {
			sub, err := BuildTree(ctx, store, e.Path, depth-1)
			if err == nil {
				child.Children = sub.Children
			}
		}
		root.Children = append(root.Children, child)
	}
	return root, nil
}
