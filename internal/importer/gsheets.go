package importer

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

type GSheetsSource struct {
	spreadsheetID string
	sheetName     string
	svc           *sheets.Service
}

func NewGoogleSheets(spreadsheetID, sheetName, credentialsFile string) (*GSheetsSource, error) {
	ctx := context.Background()

	var opts []option.ClientOption
	if credentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(credentialsFile))
	}

	svc, err := sheets.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("google sheets service: %w", err)
	}

	if sheetName == "" {
		sheetName = "Sheet1"
	}

	return &GSheetsSource{
		spreadsheetID: spreadsheetID,
		sheetName:     sheetName,
		svc:           svc,
	}, nil
}

func (s *GSheetsSource) Name() string { return s.sheetName }

func (s *GSheetsSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		resp, err := s.svc.Spreadsheets.Values.Get(s.spreadsheetID, s.sheetName).Context(ctx).Do()
		if err != nil {
			errs <- fmt.Errorf("sheets get: %w", err)
			return
		}

		if len(resp.Values) < 1 {
			return
		}

		headers := make([]string, len(resp.Values[0]))
		for i, v := range resp.Values[0] {
			headers[i] = fmt.Sprintf("%v", v)
		}

		numericCols := detectNumericSheetColumns(resp.Values[1:], headers)

		name := s.Name()
		for i, row := range resp.Values[1:] {
			if ctx.Err() != nil {
				return
			}

			fields := make(map[string]any, len(headers))
			for j, h := range headers {
				if j >= len(row) {
					continue
				}
				val := fmt.Sprintf("%v", row[j])
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
				SourceID:   fmt.Sprintf("gsheets:%s:%d", name, i),
				SourceDSN:  s.spreadsheetID,
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

func (s *GSheetsSource) Close() error { return nil }

func detectNumericSheetColumns(rows [][]interface{}, headers []string) map[string]bool {
	numeric := make(map[string]bool, len(headers))
	for _, h := range headers {
		numeric[h] = true
	}
	for _, row := range rows {
		for j, h := range headers {
			if j >= len(row) || !numeric[h] {
				continue
			}
			val := strings.TrimSpace(fmt.Sprintf("%v", row[j]))
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
