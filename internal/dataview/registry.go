package dataview

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"

	"github.com/kiwifs/kiwifs/internal/storage"
)

// Registry tracks computed view files and their staleness.
type Registry struct {
	mu    sync.RWMutex
	views map[string]*QueryPlan // path → parsed query plan
	stale map[string]bool       // paths marked stale
	db    *sql.DB
	store storage.Storage
	exec  *Executor
}

// NewRegistry creates a view registry backed by the given executor and store.
// The executor's limits (max_scan_rows, query_timeout) apply to all view
// regeneration queries.
func NewRegistry(exec *Executor, store storage.Storage) *Registry {
	return &Registry{
		views: make(map[string]*QueryPlan),
		stale: make(map[string]bool),
		db:    exec.db,
		store: store,
		exec:  exec,
	}
}

// Scan discovers all computed views in file_meta where kiwi-view = true.
func (r *Registry) Scan(ctx context.Context) error {
	rows, err := r.db.QueryContext(ctx,
		`SELECT path, json_extract(frontmatter, '$."kiwi-query"')
		 FROM file_meta
		 WHERE json_extract(frontmatter, '$."kiwi-view"') = 1`)
	if err != nil {
		return err
	}
	defer rows.Close()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.views = make(map[string]*QueryPlan)
	r.stale = make(map[string]bool)

	for rows.Next() {
		var path string
		var query sql.NullString
		if err := rows.Scan(&path, &query); err != nil {
			continue
		}
		if !query.Valid || strings.TrimSpace(query.String) == "" {
			continue
		}
		plan, err := ParseQuery(strings.TrimSpace(query.String))
		if err != nil {
			log.Printf("dataview: view %s has invalid query: %v", path, err)
			continue
		}
		r.views[path] = plan
	}
	return rows.Err()
}

// OnWrite checks if any registered view's FROM scope overlaps the written path.
// If so, marks those views as stale. Tag-scoped views are always invalidated
// since we can't cheaply check if the written file has matching tags.
func (r *Registry) OnWrite(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for viewPath, plan := range r.views {
		if plan.From == "" || strings.HasPrefix(path, plan.From) || len(plan.FromTags) > 0 {
			r.stale[viewPath] = true
		}
	}
}

// IsStale reports whether a view is marked stale.
func (r *Registry) IsStale(path string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stale[path]
}

// IsView reports whether the path is a registered computed view.
func (r *Registry) IsView(path string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.views[path]
	return ok
}

// MarkFresh clears the stale flag for a view after regeneration.
func (r *Registry) MarkFresh(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.stale, path)
}

// ListViews returns all registered view paths.
func (r *Registry) ListViews() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.views))
	for path := range r.views {
		out = append(out, path)
	}
	return out
}

// RegenerateIfStale regenerates a view if it's marked stale.
func (r *Registry) RegenerateIfStale(ctx context.Context, path string) (bool, error) {
	if !r.IsStale(path) && !r.IsView(path) {
		return false, nil
	}
	changed, err := RegenerateView(ctx, r.store, r.exec, path)
	if err != nil {
		return false, err
	}
	if changed {
		r.MarkFresh(path)
	}
	return changed, nil
}

// Register adds or updates a view in the registry.
func (r *Registry) Register(path string, plan *QueryPlan) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.views[path] = plan
	r.stale[path] = true
}

// Unregister removes a view from the registry.
func (r *Registry) Unregister(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.views, path)
	delete(r.stale, path)
}
