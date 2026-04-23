package vectorstore

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

// Weaviate is a Store backed by Weaviate's REST v1 API. Uses the "custom
// vector" flow (vectorizer = none) so we supply embeddings produced by our
// own Embedder rather than delegating to Weaviate's module system.
//
// Class defaults to "KiwiChunk". Class is auto-created on first write if it
// doesn't already exist.
type Weaviate struct {
	baseURL string
	apiKey  string
	class   string
	dims    int
	client  *http.Client
	created bool
}

func NewWeaviate(url, apiKey, class string, dims int) (*Weaviate, error) {
	if url == "" {
		url = "http://localhost:8080"
	}
	if class == "" {
		class = "KiwiChunk"
	}
	return &Weaviate{
		baseURL: strings.TrimRight(url, "/"),
		apiKey:  apiKey,
		class:   class,
		dims:    dims,
		client:  &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (w *Weaviate) do(ctx context.Context, method, path string, body any, out any) error {
	var buf io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		buf = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, w.baseURL+path, buf)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if w.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+w.apiKey)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		got, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("weaviate %s %s: %s: %s", method, path, resp.Status, truncate(string(got), 200))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (w *Weaviate) ensureClass(ctx context.Context) error {
	if w.created {
		return nil
	}
	// Idempotent: 200 if class exists, 422 if conflict, 200 on create.
	body := map[string]any{
		"class":      w.class,
		"vectorizer": "none",
		"properties": []map[string]any{
			{"name": "path", "dataType": []string{"text"}},
			{"name": "chunk_idx", "dataType": []string{"int"}},
			{"name": "text", "dataType": []string{"text"}},
		},
	}
	// Swallow conflict errors — class may already exist.
	if err := w.do(ctx, http.MethodPost, "/v1/schema", body, nil); err != nil {
		if !strings.Contains(err.Error(), "422") && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	w.created = true
	return nil
}

func (w *Weaviate) Upsert(ctx context.Context, chunks []Chunk) error {
	if len(chunks) == 0 {
		return nil
	}
	if w.dims == 0 {
		w.dims = len(chunks[0].Vector)
	}
	if err := w.ensureClass(ctx); err != nil {
		return err
	}
	objects := make([]map[string]any, len(chunks))
	for i, c := range chunks {
		objects[i] = map[string]any{
			"class":  w.class,
			"id":     deterministicUUID(c.ID),
			"vector": c.Vector,
			"properties": map[string]any{
				"path":      c.Path,
				"chunk_idx": c.ChunkIdx,
				"text":      c.Text,
			},
		}
	}
	return w.do(ctx, http.MethodPost, "/v1/batch/objects", map[string]any{"objects": objects}, nil)
}

func (w *Weaviate) RemoveByPath(ctx context.Context, path string) error {
	body := map[string]any{
		"match": map[string]any{
			"class": w.class,
			"where": map[string]any{
				"path":      []string{"path"},
				"operator":  "Equal",
				"valueText": path,
			},
		},
	}
	return w.do(ctx, http.MethodDelete, "/v1/batch/objects", body, nil)
}

func (w *Weaviate) Reset(ctx context.Context) error {
	_ = w.do(ctx, http.MethodDelete, "/v1/schema/"+w.class, nil, nil)
	w.created = false
	return nil
}

func (w *Weaviate) Count(ctx context.Context) (int, error) {
	q := fmt.Sprintf(`{"query":"{ Aggregate { %s { meta { count } } } }"}`, w.class)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.baseURL+"/v1/graphql", strings.NewReader(q))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if w.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+w.apiKey)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return 0, nil
	}
	var parsed struct {
		Data struct {
			Aggregate map[string][]struct {
				Meta struct {
					Count int `json:"count"`
				} `json:"meta"`
			} `json:"Aggregate"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return 0, nil
	}
	entries := parsed.Data.Aggregate[w.class]
	if len(entries) == 0 {
		return 0, nil
	}
	return entries[0].Meta.Count, nil
}

func (w *Weaviate) Search(ctx context.Context, vector []float32, topK int) ([]Result, error) {
	if topK <= 0 {
		topK = DefaultTopK
	}
	// GraphQL nearVector query — returns objects + their properties.
	vecJSON, _ := json.Marshal(vector)
	query := fmt.Sprintf(`{
	  Get {
	    %s (nearVector: {vector: %s}, limit: %d) {
	      path chunk_idx text
	      _additional { distance }
	    }
	  }
	}`, w.class, string(vecJSON), topK)
	body := map[string]any{"query": query}
	var parsed struct {
		Data struct {
			Get map[string][]struct {
				Path       string  `json:"path"`
				ChunkIdx   float64 `json:"chunk_idx"`
				Text       string  `json:"text"`
				Additional struct {
					Distance float64 `json:"distance"`
				} `json:"_additional"`
			} `json:"Get"`
		} `json:"data"`
	}
	if err := w.do(ctx, http.MethodPost, "/v1/graphql", body, &parsed); err != nil {
		return nil, err
	}
	hits := parsed.Data.Get[w.class]
	out := make([]Result, len(hits))
	for i, h := range hits {
		// Weaviate's cosine distance is 1 - cosine_similarity; flip for
		// "higher = more relevant" at the API boundary.
		out[i] = Result{
			Path:     h.Path,
			ChunkIdx: int(h.ChunkIdx),
			Score:    1 - h.Additional.Distance,
			Snippet:  snippet(h.Text, defaultSnippetLen),
		}
	}
	return out, nil
}

func (w *Weaviate) Close() error { return nil }

