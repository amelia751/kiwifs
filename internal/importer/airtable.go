package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

var airtableHTTPClient = &http.Client{Timeout: 30 * time.Second}

// AirtableSource implements Source for Airtable bases using the REST API.
type AirtableSource struct {
	apiKey  string
	baseID  string
	tableID string
}

// NewAirtable creates an Airtable source.
func NewAirtable(apiKey, baseID, tableID string) (*AirtableSource, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("airtable API key is required (set AIRTABLE_API_KEY)")
	}
	return &AirtableSource{apiKey: apiKey, baseID: baseID, tableID: tableID}, nil
}

func (s *AirtableSource) Name() string { return s.tableID }

func (s *AirtableSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		offset := ""
		idx := 0
		for {
			u := fmt.Sprintf("https://api.airtable.com/v0/%s/%s", s.baseID, s.tableID)
			if offset != "" {
				u += "?offset=" + url.QueryEscape(offset)
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
			if err != nil {
				errs <- fmt.Errorf("airtable request: %w", err)
				return
			}
			req.Header.Set("Authorization", "Bearer "+s.apiKey)

			resp, err := airtableHTTPClient.Do(req)
			if err != nil {
				errs <- fmt.Errorf("airtable fetch: %w", err)
				return
			}

			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				errs <- fmt.Errorf("airtable read: %w", err)
				return
			}
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("airtable: HTTP %d: %s", resp.StatusCode, string(body))
				return
			}

			var result airtableResponse
			if err := json.Unmarshal(body, &result); err != nil {
				errs <- fmt.Errorf("airtable decode: %w", err)
				return
			}

			for _, rec := range result.Records {
				fields := make(map[string]any, len(rec.Fields))
				for k, v := range rec.Fields {
					fields[k] = v
				}

				pk := rec.ID
				if name, ok := fields["Name"].(string); ok && name != "" {
					pk = sanitizePath(name)
				}

				r := Record{
					SourceID:   fmt.Sprintf("airtable:%s:%s", s.tableID, rec.ID),
					SourceDSN:  s.baseID,
					Table:      s.tableID,
					Fields:     fields,
					PrimaryKey: pk,
				}
				select {
				case records <- r:
				case <-ctx.Done():
					return
				}
				idx++
			}

			if result.Offset == "" {
				break
			}
			offset = result.Offset
		}
	}()
	return records, errs
}

func (s *AirtableSource) Close() error { return nil }

type airtableResponse struct {
	Records []airtableRecord `json:"records"`
	Offset  string           `json:"offset"`
}

type airtableRecord struct {
	ID     string         `json:"id"`
	Fields map[string]any `json:"fields"`
}
