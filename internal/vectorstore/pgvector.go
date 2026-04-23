package vectorstore

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pgvector is a Store backed by PostgreSQL + the pgvector extension.
//
// The table is created on demand; the caller must have CREATE privileges and
// the `vector` extension must already be installed (CREATE EXTENSION vector).
// We use pgvector's text input format ("[0.1,0.2,...]") which pgx accepts as
// a plain string parameter — keeps the pgx dep minimal (no custom types).
type Pgvector struct {
	pool  *pgxpool.Pool
	table string
	dims  int
}

// NewPgvector opens a connection pool against dsn and ensures the table
// exists. Dims must be known up front — we pass 0 and defer creation until
// the first Upsert so the caller can plumb it through from the embedder.
func NewPgvector(ctx context.Context, dsn, table string, dims int) (*Pgvector, error) {
	if dsn == "" {
		return nil, fmt.Errorf("pgvector: dsn is required")
	}
	if table == "" {
		table = "kiwi_vectors"
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgvector: connect: %w", err)
	}
	return &Pgvector{pool: pool, table: table, dims: dims}, nil
}

func (p *Pgvector) ensureTable(ctx context.Context, dims int) error {
	// Creating the table + index is idempotent.
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	id        TEXT PRIMARY KEY,
	path      TEXT NOT NULL,
	chunk_idx INT  NOT NULL,
	text      TEXT NOT NULL,
	embedding vector(%d) NOT NULL
);
CREATE INDEX IF NOT EXISTS %s_path_idx ON %s (path);`,
		p.table, dims, p.table, p.table)
	_, err := p.pool.Exec(ctx, ddl)
	return err
}

func (p *Pgvector) Upsert(ctx context.Context, chunks []Chunk) error {
	if len(chunks) == 0 {
		return nil
	}
	if p.dims == 0 {
		p.dims = len(chunks[0].Vector)
	}
	if err := p.ensureTable(ctx, p.dims); err != nil {
		return err
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	sql := fmt.Sprintf(`
INSERT INTO %s (id, path, chunk_idx, text, embedding) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (id) DO UPDATE SET
	path = EXCLUDED.path,
	chunk_idx = EXCLUDED.chunk_idx,
	text = EXCLUDED.text,
	embedding = EXCLUDED.embedding`, p.table)

	for _, c := range chunks {
		if _, err := tx.Exec(ctx, sql, c.ID, c.Path, c.ChunkIdx, c.Text, vectorLiteral(c.Vector)); err != nil {
			return fmt.Errorf("upsert %s: %w", c.ID, err)
		}
	}
	return tx.Commit(ctx)
}

func (p *Pgvector) RemoveByPath(ctx context.Context, path string) error {
	sql := fmt.Sprintf(`DELETE FROM %s WHERE path = $1`, p.table)
	_, err := p.pool.Exec(ctx, sql, path)
	return err
}

func (p *Pgvector) Reset(ctx context.Context) error {
	sql := fmt.Sprintf(`TRUNCATE TABLE %s`, p.table)
	_, err := p.pool.Exec(ctx, sql)
	return err
}

func (p *Pgvector) Count(ctx context.Context) (int, error) {
	var n int
	err := p.pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, p.table)).Scan(&n)
	if err != nil {
		return 0, nil // table probably doesn't exist yet
	}
	return n, nil
}

func (p *Pgvector) Search(ctx context.Context, vector []float32, topK int) ([]Result, error) {
	if topK <= 0 {
		topK = DefaultTopK
	}
	// Operator <=> is cosine distance in pgvector (0 = identical, 2 = opposite).
	sql := fmt.Sprintf(`
SELECT path, chunk_idx, text, 1 - (embedding <=> $1::vector) AS score
FROM %s
ORDER BY embedding <=> $1::vector
LIMIT $2`, p.table)

	rows, err := p.pool.Query(ctx, sql, vectorLiteral(vector), topK)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Result, 0, topK)
	for rows.Next() {
		var r Result
		var text string
		if err := rows.Scan(&r.Path, &r.ChunkIdx, &text, &r.Score); err != nil {
			return nil, err
		}
		r.Snippet = snippet(text, defaultSnippetLen)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (p *Pgvector) Close() error {
	p.pool.Close()
	return nil
}

// vectorLiteral formats a slice of float32 as pgvector's text input, e.g.
// [0.1,0.2,0.3]. pgvector casts this to `vector` when the column type is set.
func vectorLiteral(v []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'f', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}
