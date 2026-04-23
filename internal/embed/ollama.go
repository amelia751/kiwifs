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

// Ollama calls a local Ollama /api/embed endpoint.
type Ollama struct {
	baseURL string
	model   string
	dims    int
	client  *http.Client
}

// NewOllama creates an embedder backed by Ollama. Dimensions are discovered
// lazily on the first Embed call (Ollama doesn't advertise them); pass dims>0
// to skip discovery when the model's width is already known.
func NewOllama(baseURL, model string, dims int) (*Ollama, error) {
	if baseURL == "" {
		baseURL = "http://localhost:11434"
	}
	if model == "" {
		model = "nomic-embed-text"
	}
	return &Ollama{
		baseURL: strings.TrimRight(baseURL, "/"),
		model:   model,
		dims:    dims,
		client:  &http.Client{Timeout: defaultTimeout},
	}, nil
}

func (e *Ollama) Dimensions() int { return e.dims }

func (e *Ollama) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(map[string]any{
		"model": e.model,
		"input": texts,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ollama embed: %s: %s", resp.Status, truncate(string(raw), 200))
	}
	var parsed struct {
		Embeddings [][]float32 `json:"embeddings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("ollama embed: decode: %w", err)
	}
	if e.dims == 0 && len(parsed.Embeddings) > 0 {
		e.dims = len(parsed.Embeddings[0])
	}
	return parsed.Embeddings, nil
}
