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

// Pinecone is a Store backed by a Pinecone serverless / pod-based index.
//
// URL is the index's "host" URL as surfaced by Pinecone (e.g.
// https://my-index-12345.svc.aped-4627-b74a.pinecone.io). The caller supplies
// the API key. Namespace is optional.
type Pinecone struct {
	host      string
	apiKey    string
	namespace string
	dims      int
	client    *http.Client
}

func NewPinecone(host, apiKey, namespace string, dims int) (*Pinecone, error) {
	if host == "" {
		return nil, fmt.Errorf("pinecone: url (index host) is required")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("pinecone: api_key is required")
	}
	return &Pinecone{
		host:      strings.TrimRight(host, "/"),
		apiKey:    apiKey,
		namespace: namespace,
		dims:      dims,
		client:    &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (p *Pinecone) do(ctx context.Context, path string, body any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.host+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Api-Key", p.apiKey)
	req.Header.Set("X-Pinecone-API-Version", "2024-10")
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		got, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("pinecone %s: %s: %s", path, resp.Status, truncate(string(got), 200))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (p *Pinecone) Upsert(ctx context.Context, chunks []Chunk) error {
	if len(chunks) == 0 {
		return nil
	}
	if p.dims == 0 {
		p.dims = len(chunks[0].Vector)
	}
	vectors := make([]map[string]any, len(chunks))
	for i, c := range chunks {
		vectors[i] = map[string]any{
			"id":     c.ID,
			"values": c.Vector,
			"metadata": map[string]any{
				"path":      c.Path,
				"chunk_idx": c.ChunkIdx,
				"text":      c.Text,
			},
		}
	}
	body := map[string]any{"vectors": vectors}
	if p.namespace != "" {
		body["namespace"] = p.namespace
	}
	return p.do(ctx, "/vectors/upsert", body, nil)
}

func (p *Pinecone) RemoveByPath(ctx context.Context, path string) error {
	body := map[string]any{
		"filter": map[string]any{"path": map[string]any{"$eq": path}},
	}
	if p.namespace != "" {
		body["namespace"] = p.namespace
	}
	return p.do(ctx, "/vectors/delete", body, nil)
}

func (p *Pinecone) Reset(ctx context.Context) error {
	body := map[string]any{"deleteAll": true}
	if p.namespace != "" {
		body["namespace"] = p.namespace
	}
	return p.do(ctx, "/vectors/delete", body, nil)
}

func (p *Pinecone) Count(ctx context.Context) (int, error) {
	body := map[string]any{}
	if p.namespace != "" {
		body["filter"] = map[string]any{}
	}
	var parsed struct {
		Namespaces map[string]struct {
			VectorCount int `json:"vectorCount"`
		} `json:"namespaces"`
		TotalVectorCount int `json:"totalVectorCount"`
	}
	if err := p.do(ctx, "/describe_index_stats", body, &parsed); err != nil {
		return 0, nil
	}
	if p.namespace != "" {
		return parsed.Namespaces[p.namespace].VectorCount, nil
	}
	return parsed.TotalVectorCount, nil
}

func (p *Pinecone) Search(ctx context.Context, vector []float32, topK int) ([]Result, error) {
	if topK <= 0 {
		topK = DefaultTopK
	}
	body := map[string]any{
		"vector":          vector,
		"topK":            topK,
		"includeMetadata": true,
	}
	if p.namespace != "" {
		body["namespace"] = p.namespace
	}
	var parsed struct {
		Matches []struct {
			ID       string  `json:"id"`
			Score    float64 `json:"score"`
			Metadata struct {
				Path     string  `json:"path"`
				ChunkIdx float64 `json:"chunk_idx"`
				Text     string  `json:"text"`
			} `json:"metadata"`
		} `json:"matches"`
	}
	if err := p.do(ctx, "/query", body, &parsed); err != nil {
		return nil, err
	}
	out := make([]Result, len(parsed.Matches))
	for i, m := range parsed.Matches {
		out[i] = Result{
			Path:     m.Metadata.Path,
			ChunkIdx: int(m.Metadata.ChunkIdx),
			Score:    m.Score,
			Snippet:  snippet(m.Metadata.Text, defaultSnippetLen),
		}
	}
	return out, nil
}

func (p *Pinecone) GetVectors(ctx context.Context, path string) ([]Chunk, error) {
	return nil, nil
}

func (p *Pinecone) Close() error { return nil }
