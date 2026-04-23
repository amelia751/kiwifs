package embed

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

const defaultTimeout = 30 * time.Second

// OpenAI calls the OpenAI /v1/embeddings endpoint. The same wire format works
// for Azure OpenAI (different base URL + headers).
type OpenAI struct {
	apiKey  string
	model   string
	baseURL string
	dims    int
	client  *http.Client
}

// NewOpenAI creates an embedder for OpenAI-compatible endpoints. baseURL
// defaults to https://api.openai.com; model defaults to
// text-embedding-3-small (1536 dims).
func NewOpenAI(apiKey, model, baseURL string, dims int) (*OpenAI, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("openai embedder: api_key is required")
	}
	if model == "" {
		model = "text-embedding-3-small"
	}
	if baseURL == "" {
		baseURL = "https://api.openai.com"
	}
	if dims <= 0 {
		dims = openAIDims(model)
	}
	return &OpenAI{
		apiKey:  apiKey,
		model:   model,
		baseURL: strings.TrimRight(baseURL, "/"),
		dims:    dims,
		client:  &http.Client{Timeout: defaultTimeout},
	}, nil
}

func (e *OpenAI) Dimensions() int { return e.dims }

func (e *OpenAI) Embed(ctx context.Context, texts []string) ([][]float32, error) {
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/v1/embeddings", bytes.NewReader(body))
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
		return nil, fmt.Errorf("openai embed: %s: %s", resp.Status, truncate(string(raw), 200))
	}
	var parsed struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("openai embed: decode: %w", err)
	}
	out := make([][]float32, len(parsed.Data))
	for i, d := range parsed.Data {
		out[i] = d.Embedding
	}
	return out, nil
}

// openAIDims returns the default vector width for well-known OpenAI models.
// Unknown models fall back to 1536 (the small-model default); callers can
// override via the explicit dims parameter.
func openAIDims(model string) int {
	switch model {
	case "text-embedding-3-small":
		return 1536
	case "text-embedding-3-large":
		return 3072
	case "text-embedding-ada-002":
		return 1536
	default:
		return 1536
	}
}
