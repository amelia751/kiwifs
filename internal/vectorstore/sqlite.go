package vectorstore

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	_ "modernc.org/sqlite"
)

// SQLite is a pure-Go Store. Vectors are serialised as little-endian float32
// BLOBs and stored in a regular SQLite table — no sqlite-vec extension
// required, no CGO. Nearest-neighbour is a linear scan in Go.
//
// That's fine for knowledge bases up to ~50k chunks on commodity hardware;
// beyond that, swap in a proper ANN store at the Store-interface layer.
//
// All vectors are L2-normalised on write so that "nearest neighbour" at
// query time collapses to a dot product.
//
// Two pools share the DB file (safe under WAL): writeDB with MaxOpenConns(1)
// serialises upserts/deletes at the Go pool boundary so the FTS / B-tree
// doesn't see two writers at once; readDB lets semantic searches run
// concurrently against a consistent snapshot without waiting for an
// in-flight upsert.
type SQLite struct {
	path    string
	writeDB *sql.DB
	readDB  *sql.DB
}

// NewSQLite opens (or creates) the vector DB at <root>/.kiwi/state/vectors.db.
func NewSQLite(root string) (*SQLite, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root: %w", err)
	}
	stateDir := filepath.Join(abs, ".kiwi", "state")
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	dbPath := filepath.Join(stateDir, "vectors.db")

	const basePragmas = "_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	writeDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?%s", dbPath, basePragmas))
	if err != nil {
		return nil, fmt.Errorf("open sqlite write pool: %w", err)
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)

	readDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?%s&_pragma=query_only(1)", dbPath, basePragmas))
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

	s := &SQLite{path: dbPath, writeDB: writeDB, readDB: readDB}
	if err := s.createSchema(); err != nil {
		writeDB.Close()
		readDB.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLite) createSchema() error {
	const ddl = `
CREATE TABLE IF NOT EXISTS vectors (
	id        TEXT PRIMARY KEY,
	path      TEXT NOT NULL,
	chunk_idx INTEGER NOT NULL,
	text      TEXT NOT NULL,
	dims      INTEGER NOT NULL,
	vec       BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_vectors_path ON vectors(path);`
	_, err := s.writeDB.Exec(ddl)
	return err
}

func (s *SQLite) Upsert(ctx context.Context, chunks []Chunk) error {
	if len(chunks) == 0 {
		return nil
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT OR REPLACE INTO vectors(id, path, chunk_idx, text, dims, vec) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, c := range chunks {
		norm := normalise(c.Vector)
		blob := floatsToBytes(norm)
		if _, err := stmt.ExecContext(ctx, c.ID, c.Path, c.ChunkIdx, c.Text, len(norm), blob); err != nil {
			return fmt.Errorf("upsert %s: %w", c.ID, err)
		}
	}
	return tx.Commit()
}

func (s *SQLite) RemoveByPath(ctx context.Context, path string) error {
	_, err := s.writeDB.ExecContext(ctx, `DELETE FROM vectors WHERE path = ?`, path)
	return err
}

func (s *SQLite) Reset(ctx context.Context) error {
	_, err := s.writeDB.ExecContext(ctx, `DELETE FROM vectors`)
	return err
}

func (s *SQLite) Count(ctx context.Context) (int, error) {
	var n int
	err := s.readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM vectors`).Scan(&n)
	return n, err
}

func (s *SQLite) Close() error {
	rerr := s.readDB.Close()
	werr := s.writeDB.Close()
	if werr != nil {
		return werr
	}
	return rerr
}

// Search scans every row, scores dot-product against the (already-normalised)
// query vector, and returns the top-k. Linear — fast enough for knowledge
// bases in the thousands of chunks, keeps the binary pure Go with no native
// deps, and avoids an ANN index that would drift out of sync with writes.
func (s *SQLite) Search(ctx context.Context, vector []float32, topK int) ([]Result, error) {
	if topK <= 0 {
		topK = DefaultTopK
	}
	query := normalise(vector)

	rows, err := s.readDB.QueryContext(ctx,
		`SELECT path, chunk_idx, text, dims, vec FROM vectors`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	candidates := make([]Result, 0, 128)
	for rows.Next() {
		var (
			path string
			idx  int
			text string
			dims int
			blob []byte
		)
		if err := rows.Scan(&path, &idx, &text, &dims, &blob); err != nil {
			return nil, err
		}
		row := bytesToFloats(blob, dims)
		if len(row) != len(query) {
			// Mismatched dimensions → likely a stale row from a previous
			// embedding model. Skip; reindex will clean it up.
			continue
		}
		candidates = append(candidates, Result{
			Path:     path,
			ChunkIdx: idx,
			Score:    float64(dot(query, row)),
			Snippet:  snippet(text, defaultSnippetLen),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})
	if len(candidates) > topK {
		candidates = candidates[:topK]
	}
	return candidates, nil
}

func normalise(v []float32) []float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return append([]float32(nil), v...)
	}
	inv := float32(1.0 / math.Sqrt(sum))
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x * inv
	}
	return out
}

func dot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

func floatsToBytes(v []float32) []byte {
	b := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

func bytesToFloats(b []byte, dims int) []float32 {
	if len(b) != 4*dims {
		return nil
	}
	out := make([]float32, dims)
	for i := 0; i < dims; i++ {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out
}

