package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Cohere calls the Cohere v2 /embed endpoint.
//
// Wire format: POST /v2/embed  {model, texts, input_type, embedding_types}
// Response:    {embeddings: {float: [[...], ...]}}
type Cohere struct {
	apiKey  string
	model   string
	baseURL string
	dims    int
	client  *http.Client
}

// NewCohere creates an embedder for Cohere. Model defaults to embed-v4.0.
// Dimensions are learned from the first response if not provided.
func NewCohere(apiKey, model, baseURL string, dims int) (*Cohere, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("cohere embedder: api_key is required")
	}
	if model == "" {
		model = "embed-v4.0"
	}
	if baseURL == "" {
		baseURL = "https://api.cohere.com"
	}
	return &Cohere{
		apiKey:  apiKey,
		model:   model,
		baseURL: strings.TrimRight(baseURL, "/"),
		dims:    dims,
		client:  &http.Client{Timeout: defaultTimeout},
	}, nil
}

func (e *Cohere) Dimensions() int { return e.dims }

func (e *Cohere) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(map[string]any{
		"model":           e.model,
		"texts":           texts,
		"input_type":      "search_document",
		"embedding_types": []string{"float"},
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/v2/embed", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+e.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("cohere embed: %s: %s", resp.Status, truncate(string(raw), 200))
	}
	var parsed struct {
		Embeddings struct {
			Float [][]float32 `json:"float"`
		} `json:"embeddings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("cohere embed: decode: %w", err)
	}
	if e.dims == 0 && len(parsed.Embeddings.Float) > 0 {
		e.dims = len(parsed.Embeddings.Float[0])
	}
	return parsed.Embeddings.Float, nil
}
