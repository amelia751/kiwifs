package mcpserver

import (
	"context"
	"encoding/json"
)

type SearchResult struct {
	Path    string  `json:"path"`
	Snippet string  `json:"snippet,omitempty"`
	Score   float64 `json:"score,omitempty"`
}

type MetaResult struct {
	Path       string          `json:"path"`
	Frontmatter json.RawMessage `json:"frontmatter"`
}

type Version struct {
	Hash    string `json:"hash"`
	Date    string `json:"date"`
	Author  string `json:"author"`
	Message string `json:"message"`
}

type Backlink struct {
	Path  string `json:"path"`
	Count int    `json:"count"`
}

type BulkFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

var (
	_ Backend = (*RemoteBackend)(nil)
	_ Backend = (*LocalBackend)(nil)
)

type Backend interface {
	ReadFile(ctx context.Context, path string) (content string, etag string, err error)
	WriteFile(ctx context.Context, path, content, actor string, provenance string) (etag string, err error)
	DeleteFile(ctx context.Context, path, actor string) error
	Tree(ctx context.Context, path string) (json.RawMessage, error)
	Search(ctx context.Context, query string, limit, offset int, pathPrefix string) ([]SearchResult, error)
	SearchSemantic(ctx context.Context, query string, limit int) ([]SearchResult, error)
	QueryMeta(ctx context.Context, filters []string, sort, order string, limit, offset int) ([]MetaResult, error)
	Versions(ctx context.Context, path string) ([]Version, error)
	BulkWrite(ctx context.Context, files []BulkFile, actor, provenance string) (map[string]string, error)
	Backlinks(ctx context.Context, path string) ([]Backlink, error)
	ResolveWikiLinks(ctx context.Context, content string) string
	PublicURL() string
	Health(ctx context.Context) error
	Close() error
}
