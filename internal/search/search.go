package search

import "context"

// Match is a single line match within a file.
type Match struct {
	Line int    `json:"line"`
	Text string `json:"text"`
}

// Result is all matches within a single file.
type Result struct {
	Path    string  `json:"path"`
	Matches []Match `json:"matches"`
	// Score is the BM25 relevance score (lower = more relevant in FTS5 ordering,
	// but we flip the sign at the API boundary so "higher is better" for clients).
	// Zero for engines without ranking (grep).
	Score float64 `json:"score,omitempty"`
	// Snippet is a short highlighted excerpt around the best match.
	// Empty for engines that don't produce snippets (grep).
	Snippet string `json:"snippet,omitempty"`
}

// DefaultSearchLimit is the default number of results returned when the
// caller doesn't ask for a specific page size.
const DefaultSearchLimit = 50

// MaxSearchLimit caps pagination so a malicious ?limit=999999 can't force
// the server to build a huge JSON response in memory.
const MaxSearchLimit = 200

// Searcher searches across all knowledge files and (for index-backed engines)
// keeps the index in sync with filesystem writes.
//
// Every method takes context.Context as the first parameter. SQLite-backed
// implementations forward it to QueryContext/ExecContext so a cancelled
// HTTP request frees the DB connection immediately. Grep checks it
// between files during the walk so a long search bows out on cancel.
type Searcher interface {
	// Search runs a full-text query. limit == 0 means "use the engine
	// default" (DefaultSearchLimit). Negative values are treated as zero.
	// offset < 0 is treated as zero. Engines should cap limit at
	// MaxSearchLimit even if the caller forgets to.
	Search(ctx context.Context, query string, limit, offset int) ([]Result, error)
	// Index upserts a file into the search index. No-op for stateless engines.
	Index(ctx context.Context, path string, content []byte) error
	// Remove drops a file from the search index. No-op for stateless engines.
	Remove(ctx context.Context, path string) error
	// Reindex rebuilds the index from scratch by walking the knowledge root.
	// Returns the number of files indexed. No-op for stateless engines (returns 0).
	Reindex(ctx context.Context) (int, error)
	// Close releases any resources (open DB handles, etc.). No-op is fine.
	Close() error
}

// NormalizeLimit clamps a caller-supplied limit into [1, MaxSearchLimit].
// Zero (or negative) means "use the default".
func NormalizeLimit(limit int) int {
	if limit <= 0 {
		return DefaultSearchLimit
	}
	if limit > MaxSearchLimit {
		return MaxSearchLimit
	}
	return limit
}

// NormalizeOffset clamps a caller-supplied offset to be non-negative.
func NormalizeOffset(offset int) int {
	if offset < 0 {
		return 0
	}
	return offset
}
