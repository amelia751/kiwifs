package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"unicode"

	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/markdown"
	"github.com/kiwifs/kiwifs/internal/storage"
	_ "modernc.org/sqlite"
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
	root    string
	store   storage.Storage // reindex source; keeps the search layer storage-agnostic
	writeDB *sql.DB         // MaxOpenConns=1 — every write/DDL
	readDB  *sql.DB         // MaxOpenConns=N — read-only snapshot reads
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

	s := &SQLite{root: abs, store: store, writeDB: writeDB, readDB: readDB}

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
	updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_meta_status ON file_meta(json_extract(frontmatter, '$.status'));`
	_, err := s.writeDB.ExecContext(ctx, ddl)
	if err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	return nil
}

func (s *SQLite) isEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM docs`).Scan(&n); err != nil {
		return false, fmt.Errorf("count docs: %w", err)
	}
	return n == 0, nil
}

// ─── Searcher interface ─────────────────────────────────────────────────────

func (s *SQLite) Search(ctx context.Context, query string, limit, offset int) ([]Result, error) {
	q := buildFTS5Query(query)
	if q == "" {
		return nil, nil
	}
	limit = NormalizeLimit(limit)
	offset = NormalizeOffset(offset)

	const sqlQ = `
SELECT path,
       snippet(docs, 1, '<mark>', '</mark>', '…', 16) AS snip,
       bm25(docs) AS score
FROM docs
WHERE docs MATCH ?
ORDER BY rank
LIMIT ? OFFSET ?;`

	rows, err := s.readDB.QueryContext(ctx, sqlQ, q, limit, offset)
	if err != nil {
		// Malformed FTS5 query — retry with a pure phrase form as a safety net.
		fallback := phraseFallback(query)
		if fallback == "" || fallback == q {
			return nil, fmt.Errorf("search: %w", err)
		}
		rows, err = s.readDB.QueryContext(ctx, sqlQ, fallback, limit, offset)
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

// ─── Linker interface ───────────────────────────────────────────────────────

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

// ─── Metadata interface ─────────────────────────────────────────────────────

// IndexMeta upserts a file's frontmatter into the file_meta table so
// structured queries (status = "published", derived-from[*].id = "run-249",
// …) can hit a JSON column instead of re-parsing every document.
// Non-markdown files are skipped — they never carry frontmatter.
func (s *SQLite) IndexMeta(ctx context.Context, path string, content []byte) error {
	if !storage.IsKnowledgeFile(path) {
		return nil
	}
	fm, err := markdown.Frontmatter(content)
	if err != nil {
		return fmt.Errorf("parse frontmatter: %w", err)
	}
	if fm == nil {
		fm = map[string]any{}
	}
	// Normalise to JSON-safe types. goldmark-meta yields map[any]any for
	// nested YAML mappings, which `encoding/json` refuses; converting up
	// front keeps the persisted JSON clean.
	encodable := toJSONSafe(fm)
	payload, err := json.Marshal(encodable)
	if err != nil {
		return fmt.Errorf("marshal frontmatter: %w", err)
	}
	_, err = s.writeDB.ExecContext(ctx,
		`INSERT OR REPLACE INTO file_meta(path, frontmatter, updated_at) VALUES (?, ?, ?)`,
		path, string(payload), time.Now().UTC().Format(time.RFC3339),
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
// limit ≤ 0 falls back to 50 (MaxSearchLimit caps at 200); negative offset
// is treated as zero.
func (s *SQLite) QueryMeta(ctx context.Context, filters []MetaFilter, sort, order string, limit, offset int) ([]MetaResult, error) {
	limit = NormalizeLimit(limit)
	offset = NormalizeOffset(offset)

	var (
		whereParts []string
		args       []any
	)
	for i, f := range filters {
		if !validMetaField(f.Field) {
			return nil, fmt.Errorf("filter[%d]: invalid field %q (must start with $. and use letters/digits/_-./[*])", i, f.Field)
		}
		op, ok := normaliseMetaOp(f.Op)
		if !ok {
			return nil, fmt.Errorf("filter[%d]: invalid op %q", i, f.Op)
		}
		if parent, sub, isArr := arrayPathPrefix(f.Field); isArr {
			// Array predicate: EXISTS a json_each row whose value at `sub`
			// matches. This handles `$.derived-from[*].id = run-249` and
			// plain `$.tags[*] = alpha`.
			whereParts = append(whereParts,
				fmt.Sprintf(
					"EXISTS (SELECT 1 FROM json_each(frontmatter, ?) AS j WHERE json_extract(j.value, ?) %s ?)",
					op,
				))
			args = append(args, parent, sub, f.Value)
		} else {
			whereParts = append(whereParts,
				fmt.Sprintf("json_extract(frontmatter, ?) %s ?", op))
			args = append(args, f.Field, f.Value)
		}
	}

	sb := strings.Builder{}
	sb.WriteString(`SELECT path, frontmatter FROM file_meta`)
	if len(whereParts) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(whereParts, " AND "))
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

	out := []MetaResult{}
	for rows.Next() {
		var (
			path string
			raw  string
		)
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
	placeholders := make([]string, len(forms))
	args := make([]any, len(forms))
	for i, f := range forms {
		placeholders[i] = "?"
		args[i] = strings.ToLower(f)
	}
	q := fmt.Sprintf(
		`SELECT source, COUNT(*) FROM links WHERE target_lc IN (%s) GROUP BY source ORDER BY source`,
		strings.Join(placeholders, ","),
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
	placeholders := make([]string, len(paths))
	args := make([]any, len(paths)+1)
	for i, p := range paths {
		placeholders[i] = "?"
		args[i] = p
	}
	args[len(paths)] = cutoff
	q := fmt.Sprintf(
		`SELECT path FROM file_meta WHERE path IN (%s) AND updated_at > ?`,
		strings.Join(placeholders, ","),
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

// ─── Internals ──────────────────────────────────────────────────────────────

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
		if metaStmt, perr = tx.PrepareContext(ctx, `INSERT OR REPLACE INTO file_meta(path, frontmatter, updated_at) VALUES (?, ?, ?)`); perr != nil {
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
		if fm, ferr := markdown.Frontmatter(content); ferr == nil && fm != nil {
			payload, jerr := json.Marshal(toJSONSafe(fm))
			if jerr == nil {
				if _, err := metaStmt.ExecContext(ctx, e.Path, string(payload), now); err != nil {
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
	return count, nil
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
