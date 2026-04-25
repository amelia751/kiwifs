package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/markdown"
	"github.com/kiwifs/kiwifs/internal/storage"
	_ "modernc.org/sqlite"
)

func placeholders(n int) string {
	s := make([]string, n)
	for i := range s {
		s[i] = "?"
	}
	return strings.Join(s, ",")
}

func scanMetaRows(rows *sql.Rows) ([]MetaResult, error) {
	var out []MetaResult
	for rows.Next() {
		var path, raw string
		if err := rows.Scan(&path, &raw); err != nil {
			return nil, err
		}
		fm := map[string]any{}
		if raw != "" {
			_ = json.Unmarshal([]byte(raw), &fm)
		}
		out = append(out, MetaResult{Path: path, Frontmatter: fm})
	}
	return out, rows.Err()
}

type trustCoeffs struct {
	verified, deprecated, outdated, sot, confScale float64
}

func trustBoost(fm map[string]any, c trustCoeffs) float64 {
	boost := 1.0
	if status, _ := fm["status"].(string); status != "" {
		switch strings.ToLower(status) {
		case "verified":
			boost *= c.verified
		case "deprecated":
			boost *= c.deprecated
		case "outdated":
			boost *= c.outdated
		}
	}
	if sot, ok := fm["source-of-truth"]; ok {
		if b, isBool := sot.(bool); isBool && b {
			boost *= c.sot
		}
	}
	if conf, ok := fm["confidence"]; ok {
		var cv float64
		switch v := conf.(type) {
		case float64:
			cv = v
		case int:
			cv = float64(v)
		}
		if cv > 0 && cv <= 1 {
			boost *= 1.0 + c.confScale*cv
		}
	}
	return boost
}

var (
	hardCoeffs = trustCoeffs{2.0, 0.1, 0.3, 3.0, 1.0}
	softCoeffs = trustCoeffs{1.2, 0.5, 0.7, 1.4, 0.3}
)

// SQLite is a Searcher backed by SQLite FTS5. Pure-Go (no CGo).
// The index lives at <root>/.kiwi/state/search.db and is fully rebuildable
// from the files — if it ever drifts, `Reindex()` wipes and re-populates.
//
// Two pools share the same underlying DB file (WAL makes this safe):
//
//   writeDB — SetMaxOpenConns(1). Serialises every writer at the Go pool
//             boundary, so there's never more than one writer in the SQLite
//             engine at once. FTS5 and the regular tables see each DML/DDL
//             run to completion before the next one starts, which is what
//             the engine wants anyway, and we avoid the SQLITE_BUSY retries
//             that crop up when two writers race for the WAL write lock.
//
//   readDB —  SetMaxOpenConns(N). WAL readers are MVCC-style: they see a
//             consistent snapshot even while a writer is mid-transaction,
//             so searches never wait on indexing to finish. Opened with
//             `query_only=1` so a bug in this layer can't silently mutate
//             through the read pool.
type SQLite struct {
	root           string
	store          storage.Storage // reindex source; keeps the search layer storage-agnostic
	writeDB        *sql.DB         // MaxOpenConns=1 — every write/DDL
	readDB         *sql.DB         // MaxOpenConns=N — read-only snapshot reads
	computedFields bool            // when true, _word_count etc. are injected into frontmatter
}

// NewSQLite opens (or creates) the FTS5 index at <root>/.kiwi/state/search.db.
// The Storage is used as the reindex source so non-local backends (future
// S3-backed or network-FS storage) don't need to reimplement the walk.
// If the index is empty on open, it reindexes everything the store yields.
func NewSQLite(root string, store storage.Storage) (*SQLite, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root: %w", err)
	}

	stateDir := filepath.Join(abs, ".kiwi", "state")
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	dbPath := filepath.Join(stateDir, "search.db")

	// _pragma args are honoured by modernc.org/sqlite on connection open.
	// WAL + NORMAL synchronous is the standard "fast + durable enough" combo;
	// busy_timeout covers the rare case where the writer is upgrading the
	// WAL and a reader briefly contends for the shared lock.
	const basePragmas = "_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	writeDSN := fmt.Sprintf("file:%s?%s", dbPath, basePragmas)
	writeDB, err := sql.Open("sqlite", writeDSN)
	if err != nil {
		return nil, fmt.Errorf("open sqlite write pool: %w", err)
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)

	// query_only makes the engine reject INSERT/UPDATE/DELETE/DDL on this
	// connection — useful as a correctness guard, but more importantly it's
	// how we signal that these connections are only ever in the "reader"
	// role of the WAL protocol.
	readDSN := fmt.Sprintf("file:%s?%s&_pragma=query_only(1)", dbPath, basePragmas)
	readDB, err := sql.Open("sqlite", readDSN)
	if err != nil {
		writeDB.Close()
		return nil, fmt.Errorf("open sqlite read pool: %w", err)
	}
	readers := runtime.NumCPU()
	if readers < 4 {
		readers = 4
	}
	readDB.SetMaxOpenConns(readers)
	readDB.SetMaxIdleConns(readers)

	s := &SQLite{root: abs, store: store, writeDB: writeDB, readDB: readDB, computedFields: true}

	// Construction has no caller ctx — the schema bootstrap and initial
	// reindex run with Background. Production calls pass a real ctx.
	ctx := context.Background()
	if err := s.createSchema(ctx); err != nil {
		writeDB.Close()
		readDB.Close()
		return nil, err
	}

	empty, err := s.isEmpty(ctx)
	if err != nil {
		writeDB.Close()
		readDB.Close()
		return nil, err
	}
	if empty {
		start := time.Now()
		n, err := s.reindexLocked(ctx)
		if err != nil {
			log.Printf("kiwifs search: initial reindex failed: %v", err)
		} else {
			log.Printf("kiwifs search: indexed %d files in %s", n, time.Since(start).Round(time.Millisecond))
		}
	}

	return s, nil
}

func (s *SQLite) createSchema(ctx context.Context) error {
	// `porter` applies English stemming on top of the `unicode61` tokenizer.
	// `remove_diacritics 1` folds é → e etc. so queries match regardless of accents.
	const ddl = `
CREATE VIRTUAL TABLE IF NOT EXISTS docs USING fts5(
	path UNINDEXED,
	content,
	tokenize = 'porter unicode61 remove_diacritics 1'
);
CREATE TABLE IF NOT EXISTS links (
	source TEXT NOT NULL,
	target TEXT NOT NULL,
	target_lc TEXT NOT NULL,
	PRIMARY KEY (source, target_lc)
);
CREATE INDEX IF NOT EXISTS idx_links_target_lc ON links(target_lc);
CREATE TABLE IF NOT EXISTS file_meta (
	path TEXT PRIMARY KEY,
	frontmatter TEXT NOT NULL DEFAULT '{}',
	tasks TEXT NOT NULL DEFAULT '[]',
	updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_meta_status ON file_meta(json_extract(frontmatter, '$.status'));
CREATE INDEX IF NOT EXISTS idx_meta_owner ON file_meta(json_extract(frontmatter, '$.owner'));
CREATE INDEX IF NOT EXISTS idx_meta_confidence ON file_meta(json_extract(frontmatter, '$.confidence'));
CREATE INDEX IF NOT EXISTS idx_meta_source_of_truth ON file_meta(json_extract(frontmatter, '$.source-of-truth'));
CREATE INDEX IF NOT EXISTS idx_meta_reviewed ON file_meta(json_extract(frontmatter, '$.reviewed'));
CREATE INDEX IF NOT EXISTS idx_meta_next_review ON file_meta(json_extract(frontmatter, '$.next-review'));
CREATE INDEX IF NOT EXISTS idx_meta_visibility ON file_meta(json_extract(frontmatter, '$.visibility'));`
	_, err := s.writeDB.ExecContext(ctx, ddl)
	if err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	// Migration: add tasks column if it doesn't exist (for pre-v0.2 databases)
	s.writeDB.ExecContext(ctx, `ALTER TABLE file_meta ADD COLUMN tasks TEXT NOT NULL DEFAULT '[]'`)

	return nil
}

func (s *SQLite) isEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM docs`).Scan(&n); err != nil {
		return false, fmt.Errorf("count docs: %w", err)
	}
	return n == 0, nil
}

func (s *SQLite) Search(ctx context.Context, query string, limit, offset int, pathPrefix string) ([]Result, error) {
	q := buildFTS5Query(query)
	if q == "" {
		return nil, nil
	}
	limit = NormalizeLimit(limit)
	offset = NormalizeOffset(offset)

	sqlQ := `SELECT path, snippet(docs, 1, '<mark>', '</mark>', '…', 16) AS snip, bm25(docs) AS score FROM docs WHERE docs MATCH ?`
	args := []any{q}
	if pathPrefix != "" {
		sqlQ += ` AND path LIKE ?`
		args = append(args, pathPrefix+"%")
	}
	sqlQ += ` ORDER BY rank LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := s.readDB.QueryContext(ctx, sqlQ, args...)
	if err != nil {
		fallback := phraseFallback(query)
		if fallback == "" || fallback == q {
			return nil, fmt.Errorf("search: %w", err)
		}
		if pathPrefix != "" {
			args[0] = fallback
		} else {
			args[0] = fallback
		}
		rows, err = s.readDB.QueryContext(ctx, sqlQ, args...)
		if err != nil {
			return nil, fmt.Errorf("search (fallback): %w", err)
		}
	}
	defer rows.Close()

	var results []Result
	for rows.Next() {
		var path, snip string
		var score float64
		if err := rows.Scan(&path, &snip, &score); err != nil {
			return nil, err
		}
		results = append(results, Result{
			Path: path,
			// Flip sign so "higher = more relevant" at the API boundary.
			// FTS5's bm25() returns negative numbers for hits.
			Score:   -score,
			Snippet: snip,
			Matches: []Match{{Line: 0, Text: snip}},
		})
	}
	return results, rows.Err()
}

func (s *SQLite) Index(ctx context.Context, path string, content []byte) error {
	if !storage.IsKnowledgeFile(path) {
		return nil
	}
	// writeDB's single connection serialises every writer, so no per-struct
	// mutex is needed here.
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM docs WHERE path = ?`, path); err != nil {
		tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO docs(path, content) VALUES (?, ?)`, path, string(content)); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *SQLite) Remove(ctx context.Context, path string) error {
	_, err := s.writeDB.ExecContext(ctx, `DELETE FROM docs WHERE path = ?`, path)
	return err
}

// RemoveAll drops every index entry for a path in a single transaction so
// the FTS row, link edges, and frontmatter record are either all present or
// all gone — partial state after a mid-delete error left the indices
// drifting previously.
func (s *SQLite) RemoveAll(ctx context.Context, path string) error {
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM docs WHERE path = ?`, path); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM links WHERE source = ?`, path); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_meta WHERE path = ?`, path); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLite) Reindex(ctx context.Context) (int, error) {
	return s.reindexLocked(ctx)
}

func (s *SQLite) Resync(ctx context.Context) (added, removed int, err error) {
	rows, err := s.readDB.QueryContext(ctx, `SELECT path FROM docs`)
	if err != nil {
		return 0, 0, fmt.Errorf("list indexed: %w", err)
	}
	indexed := make(map[string]struct{})
	for rows.Next() {
		var p string
		if serr := rows.Scan(&p); serr != nil {
			rows.Close()
			return 0, 0, serr
		}
		indexed[p] = struct{}{}
	}
	rows.Close()
	if rerr := rows.Err(); rerr != nil {
		return 0, 0, rerr
	}

	onDisk := make(map[string]struct{})
	walkErr := storage.Walk(ctx, s.store, "/", func(e storage.Entry) error {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		onDisk[e.Path] = struct{}{}
		if _, ok := indexed[e.Path]; ok {
			return nil
		}
		content, rerr := s.store.Read(ctx, e.Path)
		if rerr != nil {
			return nil
		}
		if ierr := s.Index(ctx, e.Path, content); ierr != nil {
			log.Printf("kiwifs search: resync index %s: %v", e.Path, ierr)
			return nil
		}
		added++
		return nil
	})
	if walkErr != nil {
		return added, 0, walkErr
	}
	for p := range indexed {
		if _, ok := onDisk[p]; ok {
			continue
		}
		if rerr := s.Remove(ctx, p); rerr != nil {
			log.Printf("kiwifs search: resync remove %s: %v", p, rerr)
			continue
		}
		removed++
	}
	return added, removed, nil
}

func (s *SQLite) ReadDB() *sql.DB  { return s.readDB }
func (s *SQLite) WriteDB() *sql.DB { return s.writeDB }

func (s *SQLite) Close() error {
	// Close the read pool first so any in-flight reader returns before the
	// underlying file is released. A reversed order can leave a reader
	// holding a shared lock against a DB that's already been closed.
	rerr := s.readDB.Close()
	werr := s.writeDB.Close()
	if werr != nil {
		return werr
	}
	return rerr
}

// IndexLinks replaces every link row emitted by `source`. Atomic: either all
// old rows for this source are gone and all new rows are in, or neither.
func (s *SQLite) IndexLinks(ctx context.Context, source string, targets []string) error {
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM links WHERE source = ?`, source); err != nil {
		return err
	}
	if len(targets) > 0 {
		stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO links(source, target, target_lc) VALUES (?, ?, ?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, t := range links.Unique(targets) {
			if _, err := stmt.ExecContext(ctx, source, t, strings.ToLower(t)); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (s *SQLite) RemoveLinks(ctx context.Context, source string) error {
	_, err := s.writeDB.ExecContext(ctx, `DELETE FROM links WHERE source = ?`, source)
	return err
}

// IndexMeta upserts a file's frontmatter into the file_meta table so
// structured queries (status = "published", derived-from[*].id = "run-249",
// …) can hit a JSON column instead of re-parsing every document.
// Non-markdown files are skipped — they never carry frontmatter.
func (s *SQLite) IndexMeta(ctx context.Context, path string, content []byte) error {
	if !storage.IsKnowledgeFile(path) {
		return nil
	}
	parsed, err := markdown.Parse(content)
	if err != nil {
		return fmt.Errorf("parse markdown: %w", err)
	}
	fm := parsed.Frontmatter
	if fm == nil {
		fm = map[string]any{}
	}
	if s.computedFields {
		body := []byte(markdown.BodyAfterFrontmatter(content))
		fm["_word_count"] = len(strings.Fields(string(body)))
		fm["_link_count"] = len(links.Extract(content))
		fm["_heading_count"] = len(parsed.Headings)
		fm["_has_frontmatter"] = len(parsed.Frontmatter) > 0

		forms := links.TargetForms(path)
		if len(forms) > 0 {
			args := make([]any, len(forms))
			for i, f := range forms {
				args[i] = strings.ToLower(f)
			}
			var blCount int
			_ = s.readDB.QueryRowContext(ctx,
				fmt.Sprintf(`SELECT COUNT(DISTINCT source) FROM links WHERE target_lc IN (%s)`,
					placeholders(len(forms))),
				args...,
			).Scan(&blCount)
			fm["_backlink_count"] = blCount
		}
	}
	encodable := toJSONSafe(fm)
	payload, err := json.Marshal(encodable)
	if err != nil {
		return fmt.Errorf("marshal frontmatter: %w", err)
	}

	tasks := markdown.Tasks(content)
	tasksPayload := "[]"
	if len(tasks) > 0 {
		tb, err := json.Marshal(tasks)
		if err == nil {
			tasksPayload = string(tb)
		}
	}

	_, err = s.writeDB.ExecContext(ctx,
		`INSERT OR REPLACE INTO file_meta(path, frontmatter, tasks, updated_at) VALUES (?, ?, ?, ?)`,
		path, string(payload), tasksPayload, time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

// RemoveMeta drops the file_meta row for a path — called from the pipeline's
// delete fan-out so queries don't keep returning stale metadata.
func (s *SQLite) RemoveMeta(ctx context.Context, path string) error {
	_, err := s.writeDB.ExecContext(ctx, `DELETE FROM file_meta WHERE path = ?`, path)
	return err
}

// MetaFilter is one predicate against a frontmatter JSON path. Field must be
// a JSON-path starting with "$.", and Op must be one of the values validated
// by validMetaOp — these restrictions let QueryMeta build safe SQL without
// trusting the caller.
type MetaFilter struct {
	Field string // e.g. "$.status" or "$.derived-from[*].id"
	Op    string // "=", "!=", "LIKE", ">", ">=", "<", "<="
	Value string
}

// MetaResult is one row returned by QueryMeta.
type MetaResult struct {
	Path        string         `json:"path"`
	Frontmatter map[string]any `json:"frontmatter"`
}

// validMetaOp is the allowlist of SQL operators that can be spliced into
// QueryMeta's generated SQL. Anything outside this set is rejected so the
// caller can't smuggle `;` or subqueries through the Op field.
var validMetaOp = map[string]bool{
	"=":  true,
	"!=": true,
	"<>": true,
	"<":  true,
	"<=": true,
	">":  true,
	">=": true,
}

// validMetaOpCI contains case-insensitive operators (LIKE/NOT LIKE) that we
// normalise to upper case before emitting.
var validMetaOpCI = map[string]string{
	"like":     "LIKE",
	"not like": "NOT LIKE",
}

// normaliseMetaOp returns the canonical SQL form of op, or ("", false) if
// op isn't one of the allowed operators. This is the only place operator
// text crosses from caller input into the generated SQL.
func normaliseMetaOp(op string) (string, bool) {
	if validMetaOp[op] {
		return op, true
	}
	if out, ok := validMetaOpCI[strings.ToLower(op)]; ok {
		return out, true
	}
	return "", false
}

// validMetaField checks that a caller-supplied JSON path looks like one
// `json_extract` or `json_each` can consume — we only allow "$." prefixes
// plus the character set ordinary frontmatter keys use. This keeps the SQL
// builder from needing to quote the path: the path itself is still passed
// as a parameter, but we refuse inputs that could confuse the parser or
// look like SQL injection attempts if logs surface them.
func validMetaField(field string) bool {
	if !strings.HasPrefix(field, "$.") {
		return false
	}
	for _, r := range field[2:] {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_' || r == '-' || r == '.' || r == '[' || r == ']' || r == '*':
		default:
			return false
		}
	}
	return true
}

// arrayPathPrefix, if field ends in "[*].<sub>" or "[*]", returns the parent
// path plus the sub-path used inside json_each. For "$.derived-from[*].id"
// it returns ("$.derived-from", "$.id", true). For paths without a [*] it
// returns false.
func arrayPathPrefix(field string) (parent, sub string, ok bool) {
	i := strings.Index(field, "[*]")
	if i < 0 {
		return "", "", false
	}
	parent = field[:i]
	remainder := field[i+3:] // skip "[*]"
	switch {
	case remainder == "":
		sub = "$"
	case strings.HasPrefix(remainder, "."):
		sub = "$" + remainder
	default:
		return "", "", false
	}
	return parent, sub, true
}

// QueryMeta runs a structured query against file_meta. filters are AND-ed;
// sort is an optional JSON path ("$.priority" etc.), order is "asc"/"desc".
// limit ≤ 0 falls back to 50 (maxSearchLimit caps at 200); negative offset
// is treated as zero.
func (s *SQLite) QueryMeta(ctx context.Context, filters []MetaFilter, sort, order string, limit, offset int) ([]MetaResult, error) {
	return s.QueryMetaOr(ctx, filters, nil, sort, order, limit, offset)
}

// QueryMetaOr extends QueryMeta with OR-group support. andFilters are AND-ed,
// orFilters are OR-ed together, and the two groups are AND-ed with each other.
// When filters are empty (both nil), all rows are returned (subject to limit).
func (s *SQLite) QueryMetaOr(ctx context.Context, andFilters, orFilters []MetaFilter, sort, order string, limit, offset int) ([]MetaResult, error) {
	limit = NormalizeLimit(limit)
	offset = NormalizeOffset(offset)

	var (
		conditions []string
		args       []any
	)

	compileFilter := func(i int, f MetaFilter) (string, []any, error) {
		if !validMetaField(f.Field) {
			return "", nil, fmt.Errorf("filter[%d]: invalid field %q (must start with $. and use letters/digits/_-./[*])", i, f.Field)
		}
		op, ok := normaliseMetaOp(f.Op)
		if !ok {
			return "", nil, fmt.Errorf("filter[%d]: invalid op %q", i, f.Op)
		}
		if parent, sub, isArr := arrayPathPrefix(f.Field); isArr {
			return fmt.Sprintf(
				"EXISTS (SELECT 1 FROM json_each(frontmatter, ?) AS j WHERE json_extract(j.value, ?) %s ?)",
				op,
			), []any{parent, sub, f.Value}, nil
		}
		return fmt.Sprintf("json_extract(frontmatter, ?) %s ?", op), []any{f.Field, f.Value}, nil
	}

	for i, f := range andFilters {
		clause, fArgs, err := compileFilter(i, f)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, clause)
		args = append(args, fArgs...)
	}

	if len(orFilters) > 0 {
		var orParts []string
		for i, f := range orFilters {
			clause, fArgs, err := compileFilter(i, f)
			if err != nil {
				return nil, err
			}
			orParts = append(orParts, clause)
			args = append(args, fArgs...)
		}
		conditions = append(conditions, "("+strings.Join(orParts, " OR ")+")")
	}

	sb := strings.Builder{}
	sb.WriteString(`SELECT path, frontmatter FROM file_meta`)
	if len(conditions) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conditions, " AND "))
	}
	if sort != "" {
		if !validMetaField(sort) {
			return nil, fmt.Errorf("invalid sort field %q", sort)
		}
		dir := "ASC"
		if strings.EqualFold(order, "desc") {
			dir = "DESC"
		}
		sb.WriteString(" ORDER BY json_extract(frontmatter, ?) ")
		sb.WriteString(dir)
		args = append(args, sort)
	} else {
		sb.WriteString(" ORDER BY path ASC")
	}
	sb.WriteString(" LIMIT ? OFFSET ?")
	args = append(args, limit, offset)

	rows, err := s.readDB.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query meta: %w", err)
	}
	defer rows.Close()
	return scanMetaRows(rows)
}

// toJSONSafe converts YAML-parsed maps (which can use map[any]any under
// goldmark-meta) into the map[string]any shape encoding/json requires.
// Non-map values are returned as-is; keys that can't be stringified are
// dropped to keep marshalling total instead of erroring halfway through.
func toJSONSafe(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = toJSONSafe(val)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			ks, ok := k.(string)
			if !ok {
				ks = fmt.Sprint(k)
			}
			out[ks] = toJSONSafe(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = toJSONSafe(x)
		}
		return out
	default:
		return v
	}
}

// AllEdges dumps the entire links table as raw (source, target) pairs. Used
// by the graph view, which resolves target strings to paths client-side via
// the same fuzzy rules used for in-page wiki-link rendering.
func (s *SQLite) AllEdges(ctx context.Context) ([]links.Edge, error) {
	rows, err := s.readDB.QueryContext(ctx, `SELECT source, target FROM links ORDER BY source, target`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []links.Edge
	for rows.Next() {
		var src, tgt string
		if err := rows.Scan(&src, &tgt); err != nil {
			return nil, err
		}
		out = append(out, links.Edge{Source: src, Target: tgt})
	}
	return out, rows.Err()
}

// Backlinks returns every source that refers to `target` via any of the
// common [[…]] forms (full path, stem, basename, stem-of-basename).
func (s *SQLite) Backlinks(ctx context.Context, target string) ([]links.Entry, error) {
	forms := links.TargetForms(target)
	if len(forms) == 0 {
		return nil, nil
	}
	args := make([]any, len(forms))
	for i, f := range forms {
		args[i] = strings.ToLower(f)
	}
	q := fmt.Sprintf(
		`SELECT source, COUNT(*) FROM links WHERE target_lc IN (%s) GROUP BY source ORDER BY source`,
		placeholders(len(forms)),
	)
	rows, err := s.readDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []links.Entry
	for rows.Next() {
		var src string
		var count int
		if err := rows.Scan(&src, &count); err != nil {
			return nil, err
		}
		out = append(out, links.Entry{Path: src, Count: count})
	}
	return out, rows.Err()
}

// FilterByDate returns the subset of paths whose file_meta.updated_at is after
// the given cutoff. Single query against the indexed table — no stat calls.
func (s *SQLite) FilterByDate(ctx context.Context, paths []string, after time.Time) ([]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	cutoff := after.UTC().Format(time.RFC3339)
	args := make([]any, len(paths)+1)
	for i, p := range paths {
		args[i] = p
	}
	args[len(paths)] = cutoff
	q := fmt.Sprintf(
		`SELECT path FROM file_meta WHERE path IN (%s) AND updated_at > ?`,
		placeholders(len(paths)),
	)
	rows, err := s.readDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SearchBoosted runs a normal FTS5 search and then applies a *soft*
// trust re-rank: verified / source-of-truth / high-confidence pages get
// nudged up, deprecated pages nudged down, but every BM25 hit stays in
// the result set. This is the default ranker used by the main /search
// endpoint — a silent boost is more useful than a toggle nobody checks.
//
// SearchVerified below does the same lookup but with aggressive
// multipliers so verified/source-of-truth pages dominate the ranking
// while deprecated pages drop near the bottom.
func (s *SQLite) SearchBoosted(ctx context.Context, query string, limit, offset int, pathPrefix string) ([]Result, error) {
	return s.searchTrust(ctx, query, limit, offset, pathPrefix, softTrustBoost)
}

// SearchVerified runs a normal FTS5 search and then aggressively re-ranks
// results using hard trust multipliers. Unlike SearchBoosted (soft nudge),
// verified/source-of-truth pages get 2–3x boosts while deprecated pages
// are pushed to 0.1x — effectively burying them. Note: this re-ranks
// the full result set rather than filtering; all BM25 hits are retained.
func (s *SQLite) SearchVerified(ctx context.Context, query string, limit, offset int, pathPrefix string) ([]Result, error) {
	return s.searchTrust(ctx, query, limit, offset, pathPrefix, hardTrustBoost)
}

// searchTrust is the shared re-ranking path for SearchVerified (hard
// multipliers, intended for the "Verified" chip) and SearchBoosted
// (soft multipliers, used as the default sort). The boost callback
// returns the multiplier for a given frontmatter map; the search path
// otherwise uses the same BM25 candidates, frontmatter lookup, and
// paging logic to keep trust-ranked results comparable to the plain
// /search list.
func (s *SQLite) searchTrust(ctx context.Context, query string, limit, offset int, pathPrefix string, boostFn func(map[string]any) float64) ([]Result, error) {
	// Over-fetch so we have enough candidates after re-ranking and slicing.
	fetchLimit := limit * 3
	if fetchLimit < 60 {
		fetchLimit = 60
	}
	results, err := s.Search(ctx, query, fetchLimit, 0, pathPrefix)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return results, nil
	}

	paths := make([]string, len(results))
	for i, r := range results {
		paths[i] = r.Path
	}

	args := make([]any, len(paths))
	for i, p := range paths {
		args[i] = p
	}
	q := fmt.Sprintf(
		`SELECT path, frontmatter FROM file_meta WHERE path IN (%s)`,
		placeholders(len(paths)),
	)
	rows, err := s.readDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("search verified meta lookup: %w", err)
	}
	defer rows.Close()

	fmMap := make(map[string]map[string]any, len(paths))
	for rows.Next() {
		var path, raw string
		if err := rows.Scan(&path, &raw); err != nil {
			return nil, err
		}
		fm := map[string]any{}
		if raw != "" {
			_ = json.Unmarshal([]byte(raw), &fm)
		}
		fmMap[path] = fm
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		fm := fmMap[results[i].Path]
		boost := boostFn(fm)
		if boost <= 0 {
			boost = 1.0
		}
		results[i].TrustScore = results[i].Score * boost
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].TrustScore > results[j].TrustScore
	})

	if offset >= len(results) {
		return nil, nil
	}
	results = results[offset:]
	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func hardTrustBoost(fm map[string]any) float64 { return trustBoost(fm, hardCoeffs) }
func softTrustBoost(fm map[string]any) float64 { return trustBoost(fm, softCoeffs) }

// StalePages returns pages that are past their next-review date, or haven't
// been reviewed within staleDays. Excludes deprecated/archived pages.
func (s *SQLite) StalePages(ctx context.Context, staleDays int) ([]MetaResult, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -staleDays).Format("2006-01-02")
	now := time.Now().UTC().Format("2006-01-02")

	// Pages with an explicit next-review date that has passed.
	const nextReviewQ = `
SELECT path, frontmatter FROM file_meta
WHERE json_extract(frontmatter, '$.next-review') IS NOT NULL
  AND json_extract(frontmatter, '$.next-review') < ?
  AND COALESCE(json_extract(frontmatter, '$.status'), '') NOT IN ('deprecated', 'archived')
ORDER BY json_extract(frontmatter, '$.next-review') ASC`

	// Fallback: pages where reviewed < cutoff and next-review is not set.
	const reviewedFallbackQ = `
SELECT path, frontmatter FROM file_meta
WHERE json_extract(frontmatter, '$.next-review') IS NULL
  AND json_extract(frontmatter, '$.reviewed') IS NOT NULL
  AND json_extract(frontmatter, '$.reviewed') < ?
  AND COALESCE(json_extract(frontmatter, '$.status'), '') NOT IN ('deprecated', 'archived')
ORDER BY json_extract(frontmatter, '$.reviewed') ASC`

	seen := map[string]bool{}
	var out []MetaResult

	for _, qr := range []struct {
		sql string
		arg string
	}{
		{nextReviewQ, now},
		{reviewedFallbackQ, cutoff},
	} {
		rows, err := s.readDB.QueryContext(ctx, qr.sql, qr.arg)
		if err != nil {
			return nil, fmt.Errorf("stale pages: %w", err)
		}
		for rows.Next() {
			var path, raw string
			if err := rows.Scan(&path, &raw); err != nil {
				rows.Close()
				return nil, err
			}
			if seen[path] {
				continue
			}
			seen[path] = true
			fm := map[string]any{}
			if raw != "" {
				_ = json.Unmarshal([]byte(raw), &fm)
			}
			out = append(out, MetaResult{Path: path, Frontmatter: fm})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// FindContradictions looks for pages that share tags/topics with the given
// path but have conflicting trust signals (different status or competing
// source-of-truth claims).
func (s *SQLite) FindContradictions(ctx context.Context, path string) ([]string, error) {
	var raw string
	err := s.readDB.QueryRowContext(ctx,
		`SELECT frontmatter FROM file_meta WHERE path = ?`, path,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		// Page is not in the meta index yet (e.g. no frontmatter, or just
		// written). That's expected — treat it as "nothing to contradict".
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find contradictions: %w", err)
	}
	if raw == "" {
		return nil, nil
	}
	var fm map[string]any
	if err := json.Unmarshal([]byte(raw), &fm); err != nil {
		return nil, fmt.Errorf("parse frontmatter: %w", err)
	}

	tags := extractStringSlice(fm, "tags")
	tags = append(tags, extractStringSlice(fm, "topics")...)
	if len(tags) == 0 {
		return nil, nil
	}

	status, _ := fm["status"].(string)
	isSoT := false
	if sot, ok := fm["source-of-truth"]; ok {
		isSoT, _ = sot.(bool)
	}

	args := make([]any, len(tags))
	for i, t := range tags {
		args[i] = strings.ToLower(t)
	}
	ph := placeholders(len(tags))

	q := fmt.Sprintf(`
SELECT DISTINCT fm.path, fm.frontmatter FROM file_meta fm
WHERE fm.path != ?
  AND (
    EXISTS (
      SELECT 1 FROM json_each(fm.frontmatter, '$.tags') jt
      WHERE LOWER(jt.value) IN (%s)
    )
    OR EXISTS (
      SELECT 1 FROM json_each(fm.frontmatter, '$.topics') jt
      WHERE LOWER(jt.value) IN (%s)
    )
  )`,
		ph, ph,
	)

	// args order: tags... tags... path
	// But the query has path first (fm.path != ?), then tags twice.
	// Reorder: path, tags..., tags...
	queryArgs := make([]any, 0, 1+2*len(tags))
	queryArgs = append(queryArgs, path)
	queryArgs = append(queryArgs, args[:len(tags)]...)
	queryArgs = append(queryArgs, args[:len(tags)]...)

	rows, err := s.readDB.QueryContext(ctx, q, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("find contradictions query: %w", err)
	}
	defer rows.Close()

	var contradictions []string
	for rows.Next() {
		var otherPath, otherRaw string
		if err := rows.Scan(&otherPath, &otherRaw); err != nil {
			return nil, err
		}
		var otherFM map[string]any
		if err := json.Unmarshal([]byte(otherRaw), &otherFM); err != nil {
			continue
		}

		otherStatus, _ := otherFM["status"].(string)
		otherSoT := false
		if sot, ok := otherFM["source-of-truth"]; ok {
			otherSoT, _ = sot.(bool)
		}

		conflicting := false
		if status != "" && otherStatus != "" && !strings.EqualFold(status, otherStatus) {
			conflicting = true
		}
		if isSoT && otherSoT {
			conflicting = true
		}
		if conflicting {
			contradictions = append(contradictions, otherPath)
		}
	}
	return contradictions, rows.Err()
}

func extractStringSlice(fm map[string]any, key string) []string {
	val, ok := fm[key]
	if !ok {
		return nil
	}
	switch v := val.(type) {
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		if v != "" {
			return []string{v}
		}
	}
	return nil
}

// reindexBatchSize caps the number of files per reindex transaction. A
// single megabatch holds the write lock for the whole walk — at 10k files
// that's seconds of blocked search. 500 keeps per-batch latency low while
// amortising the commit's fsync cost.
const reindexBatchSize = 500

// reindexLocked drops and rebuilds the search, link, and metadata indices
// from disk. Serialised at the writeDB pool boundary (MaxOpenConns=1) —
// no per-struct mutex needed.
//
// The truncate runs in its own short transaction; the repopulate is split
// into reindexBatchSize-row chunks so concurrent readers (served by readDB
// over the WAL) see progress and aren't stuck behind a multi-second
// megatransaction.
func (s *SQLite) reindexLocked(ctx context.Context) (int, error) {
	if _, err := s.writeDB.ExecContext(ctx, `DELETE FROM docs`); err != nil {
		return 0, fmt.Errorf("truncate docs: %w", err)
	}
	if _, err := s.writeDB.ExecContext(ctx, `DELETE FROM links`); err != nil {
		return 0, fmt.Errorf("truncate links: %w", err)
	}
	if _, err := s.writeDB.ExecContext(ctx, `DELETE FROM file_meta`); err != nil {
		return 0, fmt.Errorf("truncate file_meta: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	count := 0

	var (
		tx                          *sql.Tx
		docStmt, linkStmt, metaStmt *sql.Stmt
	)
	// Reusable open-batch helper. Keeping it closure-local makes the "begin
	// tx + prepare three statements" sequence atomic at the source level —
	// any new prepared statement has to go through this one place.
	openBatch := func() error {
		var perr error
		tx, perr = s.writeDB.BeginTx(ctx, nil)
		if perr != nil {
			return perr
		}
		if docStmt, perr = tx.PrepareContext(ctx, `INSERT INTO docs(path, content) VALUES (?, ?)`); perr != nil {
			return perr
		}
		if linkStmt, perr = tx.PrepareContext(ctx, `INSERT OR IGNORE INTO links(source, target, target_lc) VALUES (?, ?, ?)`); perr != nil {
			return perr
		}
		if metaStmt, perr = tx.PrepareContext(ctx, `INSERT OR REPLACE INTO file_meta(path, frontmatter, tasks, updated_at) VALUES (?, ?, ?, ?)`); perr != nil {
			return perr
		}
		return nil
	}
	closeBatch := func() {
		if docStmt != nil {
			docStmt.Close()
			docStmt = nil
		}
		if linkStmt != nil {
			linkStmt.Close()
			linkStmt = nil
		}
		if metaStmt != nil {
			metaStmt.Close()
			metaStmt = nil
		}
	}
	if err := openBatch(); err != nil {
		return 0, err
	}
	// A mid-walk return path (error on exec) needs to leave nothing leaked;
	// rollback is only a no-op if the batch was already committed, so the
	// deferred closure is safe either way.
	defer func() {
		closeBatch()
		if tx != nil {
			tx.Rollback()
		}
	}()

	// Walk via storage.Storage so any future non-local backend plugs in
	// automatically — the previous filepath.WalkDir baked a local-FS
	// assumption directly into the search layer, undoing the "storage-
	// agnostic" claim the Storage interface is supposed to give us.
	walkErr := storage.Walk(ctx, s.store, "/", func(e storage.Entry) error {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		content, err := s.store.Read(ctx, e.Path)
		if err != nil {
			return nil
		}
		if _, err := docStmt.ExecContext(ctx, e.Path, string(content)); err != nil {
			return fmt.Errorf("insert doc %s: %w", e.Path, err)
		}
		for _, t := range links.Unique(links.Extract(content)) {
			if _, err := linkStmt.ExecContext(ctx, e.Path, t, strings.ToLower(t)); err != nil {
				return fmt.Errorf("insert link %s→%s: %w", e.Path, t, err)
			}
		}
		if parsed, perr := markdown.Parse(content); perr == nil {
			fm := parsed.Frontmatter
			if fm == nil {
				fm = map[string]any{}
			}
			if s.computedFields {
				body := []byte(markdown.BodyAfterFrontmatter(content))
				fm["_word_count"] = len(strings.Fields(string(body)))
				fm["_link_count"] = len(links.Extract(content))
				fm["_heading_count"] = len(parsed.Headings)
				fm["_has_frontmatter"] = len(parsed.Frontmatter) > 0
			}
			payload, jerr := json.Marshal(toJSONSafe(fm))
			if jerr == nil {
				fileTasks := markdown.Tasks(content)
				tasksJSON := "[]"
				if len(fileTasks) > 0 {
					if tb, terr := json.Marshal(fileTasks); terr == nil {
						tasksJSON = string(tb)
					}
				}
				if _, err := metaStmt.ExecContext(ctx, e.Path, string(payload), tasksJSON, now); err != nil {
					return fmt.Errorf("insert meta %s: %w", e.Path, err)
				}
			}
		}
		count++
		if count%reindexBatchSize == 0 {
			closeBatch()
			if cerr := tx.Commit(); cerr != nil {
				tx = nil
				return fmt.Errorf("commit batch: %w", cerr)
			}
			tx = nil
			if oerr := openBatch(); oerr != nil {
				return oerr
			}
		}
		return nil
	})
	if walkErr != nil {
		return 0, walkErr
	}
	closeBatch()
	if tx != nil {
		if err := tx.Commit(); err != nil {
			tx = nil
			return 0, err
		}
		tx = nil
	}

	if s.computedFields {
		if err := s.updateBacklinkCounts(ctx); err != nil {
			log.Printf("kiwifs search: backlink count pass failed: %v", err)
		}
	}

	return count, nil
}

func (s *SQLite) updateBacklinkCounts(ctx context.Context) error {
	// TargetForms generates up to 4 forms: full path, stem, basename, basename stem.
	// Match all of them so [[alice]] resolves to students/alice.md.
	_, err := s.writeDB.ExecContext(ctx, `
		UPDATE file_meta
		SET frontmatter = json_set(frontmatter, '$._backlink_count',
			(SELECT COUNT(DISTINCT source) FROM links
			 WHERE target_lc = LOWER(file_meta.path)
			    OR target_lc = LOWER(REPLACE(file_meta.path, '.md', ''))
			    OR target_lc = LOWER(REPLACE(file_meta.path, RTRIM(file_meta.path, REPLACE(file_meta.path, '/', '')), ''))
			    OR target_lc = LOWER(REPLACE(REPLACE(file_meta.path, RTRIM(file_meta.path, REPLACE(file_meta.path, '/', '')), ''), '.md', ''))))
	`)
	return err
}

// buildFTS5Query turns a user-supplied search string into a well-formed FTS5
// MATCH expression. If the user is clearly using FTS5 syntax (operators,
// quotes, wildcards, parens), pass it through verbatim. Otherwise, tokenize
// on whitespace and quote each token — this avoids syntax errors on hyphens,
// apostrophes, colons, etc., while giving implicit-AND behaviour.
func buildFTS5Query(q string) string {
	q = strings.TrimSpace(q)
	if q == "" {
		return ""
	}
	if looksLikeFTS5(q) {
		return q
	}
	return phraseFallback(q)
}

// phraseFallback tokenizes on whitespace and returns a space-joined list of
// double-quoted phrases (implicit AND in FTS5).
func phraseFallback(q string) string {
	q = strings.TrimSpace(q)
	if q == "" {
		return ""
	}
	tokens := strings.FieldsFunc(q, func(r rune) bool {
		return unicode.IsSpace(r)
	})
	parts := make([]string, 0, len(tokens))
	for _, t := range tokens {
		t = strings.ReplaceAll(t, `"`, `""`)
		// Strip characters that mean something to FTS5 even inside a phrase
		// (none do — quotes are literal inside a phrase — but strip NULs).
		t = strings.ReplaceAll(t, "\x00", "")
		if t == "" {
			continue
		}
		parts = append(parts, `"`+t+`"`)
	}
	return strings.Join(parts, " ")
}

// looksLikeFTS5 guesses whether the user is intentionally using FTS5 operator
// syntax. If yes we pass the string through; if no we tokenize.
func looksLikeFTS5(q string) bool {
	if strings.ContainsAny(q, `"()*^:`) {
		return true
	}
	// Boolean operators must be UPPERCASE whole words to be operators.
	fields := strings.Fields(q)
	for _, f := range fields {
		switch f {
		case "AND", "OR", "NOT", "NEAR":
			return true
		}
		if strings.HasPrefix(f, "NEAR(") {
			return true
		}
	}
	return false
}
