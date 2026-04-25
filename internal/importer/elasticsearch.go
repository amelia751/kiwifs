package importer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var esHTTPClient = &http.Client{Timeout: 30 * time.Second}

type ESSource struct {
	url   string
	index string
	query map[string]any
}

func NewElasticsearch(url, index string, query map[string]any) (*ESSource, error) {
	url = strings.TrimRight(url, "/")
	if query == nil {
		query = map[string]any{"query": map[string]any{"match_all": map[string]any{}}}
	}
	return &ESSource{url: url, index: index, query: query}, nil
}

func (s *ESSource) Name() string { return s.index }

func (s *ESSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		scrollQuery := make(map[string]any, len(s.query)+1)
		for k, v := range s.query {
			scrollQuery[k] = v
		}
		if _, ok := scrollQuery["size"]; !ok {
			scrollQuery["size"] = 500
		}

		body, err := json.Marshal(scrollQuery)
		if err != nil {
			errs <- fmt.Errorf("marshal query: %w", err)
			return
		}

		searchURL := fmt.Sprintf("%s/%s/_search?scroll=1m", s.url, s.index)
		scrollID, hits, err := s.doSearch(ctx, searchURL, body)
		if err != nil {
			errs <- err
			return
		}

		name := s.Name()
		idx := 0

		for len(hits) > 0 {
			for _, hit := range hits {
				if ctx.Err() != nil {
					s.clearScroll(ctx, scrollID)
					return
				}

				fields := make(map[string]any)
				if src, ok := hit["_source"].(map[string]any); ok {
					for k, v := range src {
						fields[k] = v
					}
				}

				pk := fmt.Sprintf("%d", idx)
				if id, ok := hit["_id"].(string); ok {
					pk = id
				}

				rec := Record{
					SourceID:   fmt.Sprintf("es:%s:%s", name, pk),
					SourceDSN:  s.url,
					Table:      name,
					Fields:     fields,
					PrimaryKey: pk,
				}
				select {
				case records <- rec:
				case <-ctx.Done():
					s.clearScroll(ctx, scrollID)
					return
				}
				idx++
			}

			scrollID, hits, err = s.doScroll(ctx, scrollID)
			if err != nil {
				errs <- err
				s.clearScroll(ctx, scrollID)
				return
			}
		}

		s.clearScroll(ctx, scrollID)
	}()
	return records, errs
}

func (s *ESSource) Close() error { return nil }

type esResponse struct {
	ScrollID string `json:"_scroll_id"`
	Hits     struct {
		Hits []map[string]any `json:"hits"`
	} `json:"hits"`
}

func (s *ESSource) doSearch(ctx context.Context, url string, body []byte) (string, []map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := esHTTPClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("es search: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return "", nil, fmt.Errorf("es search status %d: %s", resp.StatusCode, string(respBody))
	}

	var result esResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", nil, fmt.Errorf("decode es response: %w", err)
	}

	return result.ScrollID, result.Hits.Hits, nil
}

func (s *ESSource) doScroll(ctx context.Context, scrollID string) (string, []map[string]any, error) {
	scrollReq := map[string]any{
		"scroll":    "1m",
		"scroll_id": scrollID,
	}
	body, _ := json.Marshal(scrollReq)

	url := fmt.Sprintf("%s/_search/scroll", s.url)
	return s.doSearch(ctx, url, body)
}

func (s *ESSource) clearScroll(ctx context.Context, scrollID string) {
	if scrollID == "" {
		return
	}
	body, _ := json.Marshal(map[string]any{"scroll_id": scrollID})
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, fmt.Sprintf("%s/_search/scroll", s.url), bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := esHTTPClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}
