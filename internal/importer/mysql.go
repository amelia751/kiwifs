package importer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLSource implements Source for MySQL databases.
type MySQLSource struct {
	db      *sql.DB
	table   string
	query   string
	columns []string
	pk      string
}

// NewMySQL creates a MySQL source. DSN format: user:pass@tcp(host:3306)/dbname
func NewMySQL(dsn, table, query string, columns []string) (*MySQLSource, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	src := &MySQLSource{db: db, table: table, query: query, columns: columns}
	if table != "" && query == "" {
		src.pk = src.detectPrimaryKey()
	}
	return src, nil
}

func (s *MySQLSource) Name() string { return s.table }

func (s *MySQLSource) detectPrimaryKey() string {
	var pk string
	row := s.db.QueryRow(`
		SELECT COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		  AND TABLE_SCHEMA = DATABASE()
		LIMIT 1`, s.table)
	if err := row.Scan(&pk); err != nil {
		return ""
	}
	return pk
}

func (s *MySQLSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		q := s.query
		if q == "" {
			q = fmt.Sprintf("SELECT * FROM `%s`", escapeBacktick(s.table))
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
				fields[name] = mapMySQLValue(vals[i])
				if name == pk {
					pkVal = fmt.Sprintf("%v", vals[i])
				}
			}

			rec := Record{
				SourceID:   fmt.Sprintf("mysql:%s:%s", s.table, pkVal),
				SourceDSN:  "mysql",
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

func (s *MySQLSource) Close() error {
	return s.db.Close()
}

func escapeBacktick(s string) string {
	return strings.ReplaceAll(s, "`", "``")
}

func mapMySQLValue(v any) any {
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
