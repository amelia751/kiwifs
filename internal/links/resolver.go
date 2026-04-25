package links

import (
	"context"
	"strings"
	"sync"
)

// PathWalker lists all file paths under a root. Matches the signature of
// storage.Walk's callback collector without importing the storage package.
type PathWalker func(ctx context.Context, fn func(path string)) error

// Resolver maintains a cached index of every file path in the knowledge
// base (expanded into all fuzzy target forms) and resolves wiki-link
// targets to canonical paths. The index is rebuilt lazily on first use
// after a write invalidation, so reads are O(1) and writes only set a
// dirty flag.
type Resolver struct {
	walker PathWalker
	mu     sync.RWMutex
	index  map[string]string // lower(targetForm) → canonical path
	dirty  bool
}

// NewResolver creates a resolver backed by the given walker function.
// The walker is called to collect all file paths when the index needs
// rebuilding.
func NewResolver(walker PathWalker) *Resolver {
	return &Resolver{walker: walker, dirty: true}
}

// MarkDirty signals that the file set has changed. The next Resolve call
// will rebuild the index. This is cheap — O(1), no I/O.
func (r *Resolver) MarkDirty() {
	r.mu.Lock()
	r.dirty = true
	r.mu.Unlock()
}

// Resolve rewrites [[wiki-links]] in content to full permalink markdown
// links. Returns content unchanged if publicURL is empty or the content
// has no wiki links.
func (r *Resolver) Resolve(ctx context.Context, content, publicURL string) string {
	if publicURL == "" {
		return content
	}

	r.mu.RLock()
	needsRebuild := r.dirty || r.index == nil
	r.mu.RUnlock()

	if needsRebuild {
		r.mu.Lock()
		if r.dirty || r.index == nil {
			r.rebuild(ctx)
		}
		r.mu.Unlock()
	}

	r.mu.RLock()
	idx := r.index
	r.mu.RUnlock()

	resolver := func(target string) string {
		return idx[strings.ToLower(target)]
	}
	return ResolveWikiLinksToMarkdown(content, publicURL, resolver)
}

func (r *Resolver) rebuild(ctx context.Context) {
	var allPaths []string
	_ = r.walker(ctx, func(path string) {
		allPaths = append(allPaths, path)
	})
	idx := make(map[string]string, len(allPaths)*4)
	for _, p := range allPaths {
		for _, form := range TargetForms(p) {
			lower := strings.ToLower(form)
			if _, exists := idx[lower]; !exists {
				idx[lower] = p
			}
		}
	}
	r.index = idx
	r.dirty = false
}
