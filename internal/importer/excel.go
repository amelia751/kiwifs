package importer

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/xuri/excelize/v2"
)

type ExcelSource struct {
	filePath  string
	sheetName string
}

func NewExcel(filePath, sheetName string) (*ExcelSource, error) {
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("open xlsx: %w", err)
	}
	f.Close()

	return &ExcelSource{filePath: filePath, sheetName: sheetName}, nil
}

func (s *ExcelSource) Name() string {
	base := s.filePath
	if idx := strings.LastIndex(base, "/"); idx >= 0 {
		base = base[idx+1:]
	}
	return strings.TrimSuffix(base, ".xlsx")
}

func (s *ExcelSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		f, err := excelize.OpenFile(s.filePath)
		if err != nil {
			errs <- fmt.Errorf("open xlsx: %w", err)
			return
		}
		defer f.Close()

		sheet := s.sheetName
		if sheet == "" {
			sheet = f.GetSheetName(0)
		}

		rows, err := f.GetRows(sheet)
		if err != nil {
			errs <- fmt.Errorf("get rows: %w", err)
			return
		}
		if len(rows) < 1 {
			return
		}

		headers := rows[0]
		dataRows := rows[1:]

		numericCols := detectNumericExcelColumns(dataRows, headers)

		name := s.Name()
		for i, row := range dataRows {
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
			if id, ok := fields["id"]; ok {
				pk = fmt.Sprintf("%v", id)
			}

			rec := Record{
				SourceID:   fmt.Sprintf("excel:%s:%d", name, i),
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

func (s *ExcelSource) Close() error { return nil }

func detectNumericExcelColumns(rows [][]string, headers []string) map[string]bool {
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
