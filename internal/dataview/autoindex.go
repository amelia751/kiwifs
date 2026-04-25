package dataview

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
)

const maxAutoIndexes = 20

// AutoIndexer creates generated columns and indexes for frequently queried
// frontmatter fields. When a query uses a field in WHERE/SORT for the first
// time, a VIRTUAL generated column + index are created so subsequent queries
// hit an index scan instead of a full-table json_extract scan.
type AutoIndexer struct {
	mu      sync.RWMutex
	writeDB *sql.DB
	readDB  *sql.DB
	indexed map[string]bool // field → true
	limit   int
}

// NewAutoIndexer creates an auto-indexer. writeDB is used for DDL (ALTER TABLE,
// CREATE INDEX); readDB is used for lookups.
func NewAutoIndexer(writeDB, readDB *sql.DB, limit int) *AutoIndexer {
	if limit <= 0 {
		limit = maxAutoIndexes
	}
	ai := &AutoIndexer{
		writeDB: writeDB,
		readDB:  readDB,
		indexed: make(map[string]bool),
		limit:   limit,
	}
	ai.loadExisting()
	return ai
}

func (ai *AutoIndexer) loadExisting() {
	ai.ensureTable()
	rows, err := ai.readDB.Query(`SELECT field FROM dataview_indexes`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var field string
		if rows.Scan(&field) == nil {
			ai.indexed[field] = true
		}
	}
}

func (ai *AutoIndexer) ensureTable() {
	ai.writeDB.Exec(`CREATE TABLE IF NOT EXISTS dataview_indexes (
		field TEXT PRIMARY KEY,
		created_at TEXT NOT NULL DEFAULT (datetime('now'))
	)`)
}

// EnsureIndex creates a generated column + index for the given field if one
// doesn't already exist and the limit hasn't been reached.
func (ai *AutoIndexer) EnsureIndex(ctx context.Context, field string) {
	if field == "" || strings.HasPrefix(field, "_") {
		return
	}
	if err := validateFieldPath(field); err != nil {
		return
	}

	ai.mu.Lock()
	defer ai.mu.Unlock()

	if ai.indexed[field] {
		return
	}
	if len(ai.indexed) >= ai.limit {
		return
	}

	colName := "_idx_" + strings.NewReplacer(".", "_", "-", "_", "[", "", "]", "", "*", "").Replace(field)

	_, err := ai.writeDB.ExecContext(ctx, fmt.Sprintf(
		`ALTER TABLE file_meta ADD COLUMN %s TEXT GENERATED ALWAYS AS (json_extract(frontmatter, '$.%s')) VIRTUAL`,
		colName, field,
	))
	if err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			log.Printf("dataview: auto-index column %s: %v", field, err)
			return
		}
	}

	_, err = ai.writeDB.ExecContext(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS idx_dv_%s ON file_meta(%s)`,
		strings.NewReplacer(".", "_", "-", "_").Replace(field), colName,
	))
	if err != nil {
		log.Printf("dataview: auto-index index %s: %v", field, err)
		return
	}

	ai.writeDB.ExecContext(ctx,
		`INSERT OR IGNORE INTO dataview_indexes(field) VALUES (?)`, field)
	ai.indexed[field] = true
	log.Printf("dataview: auto-indexed field %q as column %s", field, colName)
}

// IndexedColumn returns the generated column name for a field if it exists.
func (ai *AutoIndexer) IndexedColumn(field string) (string, bool) {
	ai.mu.RLock()
	defer ai.mu.RUnlock()
	if !ai.indexed[field] {
		return "", false
	}
	colName := "_idx_" + strings.NewReplacer(".", "_", "-", "_", "[", "", "]", "", "*", "").Replace(field)
	return colName, true
}

// CollectFields extracts field paths used in WHERE and SORT of a query plan.
func CollectFields(plan *QueryPlan) []string {
	var fields []string
	if plan.Sort != "" {
		if _, implicit := resolveField(plan.Sort); !implicit {
			fields = append(fields, plan.Sort)
		}
	}
	for _, ss := range plan.Sorts {
		if _, implicit := resolveField(ss.Field); !implicit {
			fields = append(fields, ss.Field)
		}
	}
	if plan.Where != nil {
		fields = append(fields, collectExprFields(plan.Where)...)
	}
	return fields
}

func collectExprFields(expr Expr) []string {
	switch e := expr.(type) {
	case *BinaryExpr:
		var out []string
		out = append(out, collectExprFields(e.Left)...)
		out = append(out, collectExprFields(e.Right)...)
		return out
	case *UnaryExpr:
		return collectExprFields(e.Expr)
	case *FieldRef:
		if _, implicit := resolveField(e.Path); !implicit {
			return []string{e.Path}
		}
	case *FuncCall:
		for _, arg := range e.Args {
			if fr, ok := arg.(*FieldRef); ok {
				if _, implicit := resolveField(fr.Path); !implicit {
					return []string{fr.Path}
				}
			}
		}
	case *BetweenExpr:
		return collectExprFields(e.Expr)
	case *IsNullExpr:
		return collectExprFields(e.Expr)
	}
	return nil
}
