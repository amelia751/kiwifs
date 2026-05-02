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

// QueryResult is the response from a DQL query via the dataview engine.
type QueryResult struct {
	Columns []string           `json:"columns"`
	Rows    []map[string]any   `json:"rows"`
	Total   int                `json:"total"`
	HasMore bool               `json:"has_more"`
	Groups  []GroupResult      `json:"groups,omitempty"`
}

// GroupResult mirrors dataview.GroupResult for MCP transport.
type GroupResult struct {
	Key   string `json:"key"`
	Count int    `json:"count"`
}

type Change struct {
	Seq       string `json:"seq"`
	Path      string `json:"path"`
	Action    string `json:"action"`
	Actor     string `json:"actor"`
	Timestamp string `json:"timestamp"`
}

type ChangesResult struct {
	Changes []Change `json:"changes"`
	LastSeq string   `json:"last_seq"`
}

type Backend interface {
	Changes(ctx context.Context, since string, limit int) (*ChangesResult, error)
	ReadFile(ctx context.Context, path string) (content string, etag string, err error)
	WriteFile(ctx context.Context, path, content, actor string, provenance string) (etag string, err error)
	DeleteFile(ctx context.Context, path, actor string) error
	Tree(ctx context.Context, path string) (json.RawMessage, error)
	Search(ctx context.Context, query string, limit, offset int, pathPrefix string) ([]SearchResult, error)
	SearchSemantic(ctx context.Context, query string, limit int) ([]SearchResult, error)
	QueryMeta(ctx context.Context, filters []string, sort, order string, limit, offset int) ([]MetaResult, error)
	QueryMetaOr(ctx context.Context, andFilters, orFilters []string, sort, order string, limit, offset int, paths ...string) ([]MetaResult, error)
	QueryDQL(ctx context.Context, dql string, limit, offset int) (*QueryResult, error)
	ViewRefresh(ctx context.Context, path string) (changed bool, err error)
	Versions(ctx context.Context, path string) ([]Version, error)
	BulkWrite(ctx context.Context, files []BulkFile, actor, provenance string) (map[string]string, error)
	Aggregate(ctx context.Context, groupBy, calc, where, pathPrefix string) (map[string]map[string]any, error)
	Analytics(ctx context.Context, scope string, staleThreshold int) (json.RawMessage, error)
	MemoryReport(ctx context.Context, episodesPrefix string) (json.RawMessage, error)
	HealthCheckPage(ctx context.Context, path string) (json.RawMessage, error)
	Append(ctx context.Context, path, content, separator, actor string) (string, error)
	Rename(ctx context.Context, from, to, actor string) (string, error)
	RenameWithLinks(ctx context.Context, from, to, actor string, updateLinks bool) (string, []string, error)
	Backlinks(ctx context.Context, path string) ([]Backlink, error)
	ResolveWikiLinks(ctx context.Context, content string) string
	PublicURL() string
	Health(ctx context.Context) error
	Close() error
}
