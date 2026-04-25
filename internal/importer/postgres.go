package importer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// PostgresSource implements Source for PostgreSQL databases.
type PostgresSource struct {
	db      *sql.DB
	table   string
	query   string
	columns []string
	pk      string
}

// NewPostgres creates a PostgreSQL source. If query is provided, it overrides
// the table scan. columns filters which columns to include (empty = all).
func NewPostgres(dsn, table, query string, columns []string) (*PostgresSource, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	src := &PostgresSource{db: db, table: table, query: query, columns: columns}
	if table != "" && query == "" {
		src.pk = src.detectPrimaryKey()
	}
	return src, nil
}

func (s *PostgresSource) Name() string { return s.table }

func (s *PostgresSource) detectPrimaryKey() string {
	var pk string
	row := s.db.QueryRow(`
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
		  AND tc.table_name = $1
		  AND tc.table_schema = current_schema()
		LIMIT 1`, s.table)
	if err := row.Scan(&pk); err != nil {
		return ""
	}
	return pk
}

func (s *PostgresSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		q := s.query
		if q == "" {
			q = fmt.Sprintf("SELECT * FROM %s", quoteIdent(s.table))
		}
		rows, err := s.db.QueryContext(ctx, q)
		if err != nil {
			errs <- fmt.Errorf("query: %w", err)
			return
		}
		defer rows.Close()

		cols, err := rows.ColumnTypes()
		if err != nil {
			errs <- fmt.Errorf("column types: %w", err)
			return
		}

		colNames := make([]string, len(cols))
		for i, c := range cols {
			colNames[i] = c.Name()
		}

		pk := s.pk
		if pk == "" && len(colNames) > 0 {
			pk = colNames[0]
		}

		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				errs <- fmt.Errorf("scan: %w", err)
				return
			}

			fields := make(map[string]any, len(cols))
			var pkVal string
			for i, name := range colNames {
				if len(s.columns) > 0 && !containsStr(s.columns, name) && name != pk {
					continue
				}
				fields[name] = mapPgValue(vals[i])
				if name == pk {
					pkVal = fmt.Sprintf("%v", vals[i])
				}
			}

			rec := Record{
				SourceID:   fmt.Sprintf("pg:%s:%s", s.table, pkVal),
				SourceDSN:  "postgres",
				Table:      s.table,
				Fields:     fields,
				PrimaryKey: pkVal,
			}
			select {
			case records <- rec:
			case <-ctx.Done():
				return
			}
		}
		if err := rows.Err(); err != nil {
			errs <- err
		}
	}()
	return records, errs
}

func (s *PostgresSource) Close() error {
	return s.db.Close()
}

func mapPgValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case time.Time:
		return val.Format(time.RFC3339)
	case []byte:
		return string(val)
	case int64:
		return val
	case float64:
		return val
	case bool:
		return val
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func containsStr(ss []string, s string) bool {
	for _, v := range ss {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}
