package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// HTTP is the "bring your own model" escape hatch. Any provider reachable via
// HTTP can be adapted to this contract with a ~10-line shim:
//
//	POST <url>  body={"texts": [...]}  → {"vectors": [[...], [...]]}
type HTTP struct {
	url     string
	headers map[string]string
	dims    int
	client  *http.Client
}

// NewHTTP creates an embedder that calls a user-supplied HTTP URL. headers is
// merged into every request (use it for auth / API keys). Dimensions are
// learned lazily on the first Embed call if not provided.
func NewHTTP(url string, headers map[string]string, dims int) (*HTTP, error) {
	if url == "" {
		return nil, fmt.Errorf("http embedder: url is required")
	}
	return &HTTP{
		url:     url,
		headers: headers,
		dims:    dims,
		client:  &http.Client{Timeout: defaultTimeout},
	}, nil
}

func (e *HTTP) Dimensions() int { return e.dims }

func (e *HTTP) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(map[string]any{"texts": texts})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range e.headers {
		req.Header.Set(k, v)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("http embed: %s: %s", resp.Status, truncate(string(raw), 200))
	}
	var parsed struct {
		Vectors [][]float32 `json:"vectors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("http embed: decode: %w", err)
	}
	if e.dims == 0 && len(parsed.Vectors) > 0 {
		e.dims = len(parsed.Vectors[0])
	}
	return parsed.Vectors, nil
}
