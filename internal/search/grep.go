package search

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
)

// Grep implements Searcher using a pure-Go line scanner.
// Zero external deps — works everywhere, fast for up to ~5000 files.
type Grep struct {
	root string
}

const (
	// maxMatchesPerFile prevents a single large file from monopolising the
	// result set. Elasticsearch and Solr both use similar per-document caps.
	maxMatchesPerFile = 20
)

func NewGrep(root string) *Grep {
	return &Grep{root: root}
}

func (g *Grep) Search(ctx context.Context, query string, limit, offset int) ([]Result, error) {
	if query == "" {
		return nil, nil
	}
	lower := strings.ToLower(query)
	limit = NormalizeLimit(limit)
	offset = NormalizeOffset(offset)
	// Collect up to (limit + offset) matches so we can skip the first
	// `offset` and still return `limit` results. Walking further would
	// just discard them.
	walkCap := limit + offset

	var results []Result

	err := filepath.WalkDir(g.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		// Per-file ctx check so an abandoned request abandons the walk
		// promptly — each file is cheap, but a 5000-file root still
		// costs hundreds of ms if we ignore cancellation.
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if len(results) >= walkCap {
			return filepath.SkipAll
		}
		name := d.Name()
		if d.IsDir() && strings.HasPrefix(name, ".") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(name, ".md") {
			return nil
		}

		matches, err := searchFile(path, lower)
		if err != nil || len(matches) == 0 {
			return nil
		}
		if len(matches) > maxMatchesPerFile {
			matches = matches[:maxMatchesPerFile]
		}

		rel, err := filepath.Rel(g.root, path)
		if err != nil {
			return nil
		}
		results = append(results, Result{Path: rel, Matches: matches})
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Walk order is lexical (filepath.WalkDir docs), so pagination here is
	// stable across calls. Apply offset/limit now.
	if offset >= len(results) {
		return []Result{}, nil
	}
	results = results[offset:]
	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

// Index is a no-op — grep reads files directly at query time, no index to keep.
func (g *Grep) Index(_ context.Context, _ string, _ []byte) error { return nil }

// Remove is a no-op for the same reason.
func (g *Grep) Remove(_ context.Context, _ string) error { return nil }

// Reindex is a no-op.
func (g *Grep) Reindex(_ context.Context) (int, error) { return 0, nil }

// Close is a no-op.
func (g *Grep) Close() error { return nil }

func searchFile(path, lowerQuery string) ([]Match, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var matches []Match
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if strings.Contains(strings.ToLower(line), lowerQuery) {
			matches = append(matches, Match{
				Line: lineNum,
				Text: strings.TrimSpace(line),
			})
		}
	}
	return matches, scanner.Err()
}
