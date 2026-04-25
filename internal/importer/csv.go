package importer

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// CSVSource implements Source for CSV files.
type CSVSource struct {
	filePath  string
	hasHeader bool
	idColumn  string
}

// NewCSV creates a CSV source. If hasHeader is true, the first row is used
// as column names. idColumn overrides which column to use as the primary key.
func NewCSV(filePath string, hasHeader bool) (*CSVSource, error) {
	if _, err := os.Stat(filePath); err != nil {
		return nil, fmt.Errorf("csv file: %w", err)
	}
	return &CSVSource{filePath: filePath, hasHeader: hasHeader}, nil
}

func (s *CSVSource) Name() string {
	base := s.filePath
	if idx := strings.LastIndex(base, "/"); idx >= 0 {
		base = base[idx+1:]
	}
	return strings.TrimSuffix(base, ".csv")
}

func (s *CSVSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		f, err := os.Open(s.filePath)
		if err != nil {
			errs <- fmt.Errorf("open csv: %w", err)
			return
		}
		defer f.Close()

		reader := csv.NewReader(f)
		reader.LazyQuotes = true
		reader.TrimLeadingSpace = true

		var headers []string
		if s.hasHeader {
			row, err := reader.Read()
			if err != nil {
				errs <- fmt.Errorf("read header: %w", err)
				return
			}
			headers = row
		}

		// Read all rows to auto-detect numeric columns
		allRows, err := reader.ReadAll()
		if err != nil {
			errs <- fmt.Errorf("read csv: %w", err)
			return
		}

		if headers == nil && len(allRows) > 0 {
			headers = make([]string, len(allRows[0]))
			for i := range headers {
				headers[i] = fmt.Sprintf("col_%d", i)
			}
		}

		numericCols := detectNumericColumns(allRows, headers)

		name := s.Name()
		for i, row := range allRows {
			if ctx.Err() != nil {
				return
			}

			fields := make(map[string]any, len(headers))
			for j, h := range headers {
				if j >= len(row) {
					continue
				}
				val := row[j]
				if numericCols[h] {
					if n, err := strconv.ParseFloat(val, 64); err == nil {
						if n == float64(int64(n)) {
							fields[h] = int64(n)
						} else {
							fields[h] = n
						}
						continue
					}
				}
				fields[h] = val
			}

			pk := fmt.Sprintf("%d", i)
			if s.idColumn != "" {
				if v, ok := fields[s.idColumn]; ok {
					pk = fmt.Sprintf("%v", v)
				}
			}

			rec := Record{
				SourceID:   fmt.Sprintf("csv:%s:%d", name, i),
				SourceDSN:  s.filePath,
				Table:      name,
				Fields:     fields,
				PrimaryKey: pk,
			}
			select {
			case records <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()
	return records, errs
}

func (s *CSVSource) Close() error { return nil }

func detectNumericColumns(rows [][]string, headers []string) map[string]bool {
	if len(rows) == 0 {
		return nil
	}
	numeric := make(map[string]bool, len(headers))
	for _, h := range headers {
		numeric[h] = true
	}

	for _, row := range rows {
		for j, h := range headers {
			if j >= len(row) || !numeric[h] {
				continue
			}
			val := strings.TrimSpace(row[j])
			if val == "" {
				continue
			}
			if _, err := strconv.ParseFloat(val, 64); err != nil {
				numeric[h] = false
			}
		}
	}
	return numeric
}
