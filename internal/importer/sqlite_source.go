package importer

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteSource implements Source for SQLite databases.
type SQLiteSource struct {
	db    *sql.DB
	table string
	query string
}

// NewSQLiteSource creates a SQLite source.
func NewSQLiteSource(dbPath, table, query string) (*SQLiteSource, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	return &SQLiteSource{db: db, table: table, query: query}, nil
}

func (s *SQLiteSource) Name() string { return s.table }

func (s *SQLiteSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
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

		if len(colNames) == 0 {
			errs <- fmt.Errorf("query returned zero columns")
			return
		}

		pk := colNames[0]

		rowIdx := 0
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
				fields[name] = mapSQLiteValue(vals[i])
				if name == pk {
					pkVal = fmt.Sprintf("%v", vals[i])
				}
			}

			rec := Record{
				SourceID:   fmt.Sprintf("sqlite:%s:%s", s.table, pkVal),
				SourceDSN:  "sqlite",
				Table:      s.table,
				Fields:     fields,
				PrimaryKey: pkVal,
			}
			select {
			case records <- rec:
			case <-ctx.Done():
				return
			}
			rowIdx++
		}
		if err := rows.Err(); err != nil {
			errs <- err
		}
	}()
	return records, errs
}

func (s *SQLiteSource) Close() error {
	return s.db.Close()
}

func mapSQLiteValue(v any) any {
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
