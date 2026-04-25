package search

import (
	"context"
	"time"
)

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
	// TrustScore is the BM25 score adjusted by trust signals (status, confidence,
	// source-of-truth). Only populated by TrustSearcher.SearchVerified.
	TrustScore float64 `json:"trustScore,omitempty"`
}

const defaultSearchLimit = 50

const maxSearchLimit = 200

// Searcher searches across all knowledge files and (for index-backed engines)
// keeps the index in sync with filesystem writes.
//
// Every method takes context.Context as the first parameter. SQLite-backed
// implementations forward it to QueryContext/ExecContext so a cancelled
// HTTP request frees the DB connection immediately. Grep checks it
// between files during the walk so a long search bows out on cancel.
type Searcher interface {
	// Search runs a full-text query. limit == 0 means "use the engine
	// default" (defaultSearchLimit). Negative values are treated as zero.
	// offset < 0 is treated as zero. Engines should cap limit at
	// maxSearchLimit even if the caller forgets to.
	// pathPrefix, when non-empty, restricts results to paths starting with
	// that prefix (server-side filtering, not post-fetch).
	Search(ctx context.Context, query string, limit, offset int, pathPrefix string) ([]Result, error)
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

// DateFilterer is an optional interface that searchers can implement to
// filter result paths by modification date using indexed data instead of
// per-file stat calls.
type DateFilterer interface {
	FilterByDate(ctx context.Context, paths []string, after time.Time) ([]string, error)
}

// TrustSearcher is implemented by search backends that support trust-boosted ranking.
type TrustSearcher interface {
	// SearchVerified returns only pages whose trust signals clear a
	// high bar (status == verified, source-of-truth == true, or a
	// confidence > 0.8). Zero results is normal: the caller explicitly
	// asked for the "only canonical" view.
	SearchVerified(ctx context.Context, query string, limit, offset int, pathPrefix string) ([]Result, error)
	// SearchBoosted returns the same set of hits as a plain BM25
	// search, but re-ranks them with a soft trust multiplier so
	// verified pages bubble up. Unlike SearchVerified, a page with no
	// trust metadata still appears in results — it just doesn't get
	// the boost.
	SearchBoosted(ctx context.Context, query string, limit, offset int, pathPrefix string) ([]Result, error)
}

// StaleDetector is implemented by search backends that can identify pages
// past their review date or not reviewed within a given window.
type StaleDetector interface {
	StalePages(ctx context.Context, staleDays int) ([]MetaResult, error)
}

// ContradictionDetector is implemented by search backends that can find
// pages with overlapping topics but conflicting trust signals.
type ContradictionDetector interface {
	FindContradictions(ctx context.Context, path string) ([]string, error)
}

// Resyncer is implemented by search backends that support an incremental
// reconciliation with underlying storage (used at startup to catch
// out-of-band filesystem changes made while the server was down).
type Resyncer interface {
	Resync(ctx context.Context) (added, removed int, err error)
}

// NormalizeLimit clamps a caller-supplied limit into [1, maxSearchLimit].
// Zero (or negative) means "use the default".
func NormalizeLimit(limit int) int {
	if limit <= 0 {
		return defaultSearchLimit
	}
	if limit > maxSearchLimit {
		return maxSearchLimit
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
