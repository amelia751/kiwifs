package vectorstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Milvus is a Store backed by Milvus's HTTP REST API (v2). Using REST (not
// gRPC) means zero extra Go dependencies — the Milvus Go SDK drags in the
// full gRPC toolchain, which we don't need for the small write volume a
// knowledge base produces.
//
// Docs: https://milvus.io/docs/restful_api.md
type Milvus struct {
	baseURL    string
	apiKey     string
	collection string
	dims       int
	client     *http.Client
	created    bool
}

// NewMilvus wires a Milvus client. url defaults to http://localhost:19530,
// collection defaults to "kiwifs". apiKey is sent as Bearer auth — leave
// empty when running without authentication (the default dev setup).
func NewMilvus(url, apiKey, collection string, dims int) (*Milvus, error) {
	if url == "" {
		url = "http://localhost:19530"
	}
	if collection == "" {
		collection = "kiwifs"
	}
	return &Milvus{
		baseURL:    strings.TrimRight(url, "/"),
		apiKey:     apiKey,
		collection: collection,
		dims:       dims,
		client:     &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (m *Milvus) do(ctx context.Context, method, path string, body any, out any) error {
	var buf io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		buf = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, m.baseURL+path, buf)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if m.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+m.apiKey)
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("milvus %s %s: %s: %s", method, path, resp.Status, truncate(string(raw), 200))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// ensureCollection creates the collection on first write once we know the
// vector dimensions. Milvus v2's REST create endpoint is idempotent enough
// — an existing collection with matching schema returns a benign error we
// swallow in do().
func (m *Milvus) ensureCollection(ctx context.Context, dims int) error {
	if m.created {
		return nil
	}
	body := map[string]any{
		"collectionName": m.collection,
		"dimension":      dims,
		"metricType":     "COSINE",
		"primaryField":   "id",
		"vectorField":    "vector",
	}
	// "create" is an alias for a specific dimensional collection; if it
	// already exists with a matching dimension, Milvus returns a friendly
	// error that we tolerate by checking existence first.
	var exists struct {
		Data bool `json:"data"`
	}
	_ = m.do(ctx, http.MethodPost, "/v2/vectordb/collections/has",
		map[string]any{"collectionName": m.collection}, &exists)
	if exists.Data {
		m.created = true
		return nil
	}
	if err := m.do(ctx, http.MethodPost, "/v2/vectordb/collections/create", body, nil); err != nil {
		return err
	}
	m.created = true
	return nil
}

func (m *Milvus) Upsert(ctx context.Context, chunks []Chunk) error {
	if len(chunks) == 0 {
		return nil
	}
	if m.dims == 0 {
		m.dims = len(chunks[0].Vector)
	}
	if err := m.ensureCollection(ctx, m.dims); err != nil {
		return err
	}
	rows := make([]map[string]any, len(chunks))
	for i, c := range chunks {
		rows[i] = map[string]any{
			"id":        c.ID,
			"vector":    c.Vector,
			"path":      c.Path,
			"chunk_idx": c.ChunkIdx,
			"text":      c.Text,
		}
	}
	return m.do(ctx, http.MethodPost, "/v2/vectordb/entities/upsert",
		map[string]any{"collectionName": m.collection, "data": rows}, nil)
}

func (m *Milvus) RemoveByPath(ctx context.Context, path string) error {
	// Milvus REST v2 delete-by-filter: the filter is a boolean expression
	// against scalar fields.
	body := map[string]any{
		"collectionName": m.collection,
		"filter":         fmt.Sprintf(`path == "%s"`, escapeMilvusString(path)),
	}
	return m.do(ctx, http.MethodPost, "/v2/vectordb/entities/delete", body, nil)
}

func (m *Milvus) Reset(ctx context.Context) error {
	_ = m.do(ctx, http.MethodPost, "/v2/vectordb/collections/drop",
		map[string]any{"collectionName": m.collection}, nil)
	m.created = false
	return nil
}

func (m *Milvus) Count(ctx context.Context) (int, error) {
	var parsed struct {
		Data struct {
			RowCount int `json:"rowCount"`
		} `json:"data"`
	}
	err := m.do(ctx, http.MethodPost, "/v2/vectordb/collections/describe",
		map[string]any{"collectionName": m.collection}, &parsed)
	if err != nil {
		return 0, nil
	}
	return parsed.Data.RowCount, nil
}

func (m *Milvus) Search(ctx context.Context, vector []float32, topK int) ([]Result, error) {
	if topK <= 0 {
		topK = DefaultTopK
	}
	body := map[string]any{
		"collectionName": m.collection,
		"data":           []any{vector},
		"limit":          topK,
		"outputFields":   []string{"path", "chunk_idx", "text"},
	}
	var parsed struct {
		Data []struct {
			Distance float64 `json:"distance"`
			ID       string  `json:"id"`
			Path     string  `json:"path"`
			ChunkIdx int     `json:"chunk_idx"`
			Text     string  `json:"text"`
		} `json:"data"`
	}
	if err := m.do(ctx, http.MethodPost, "/v2/vectordb/entities/search", body, &parsed); err != nil {
		return nil, err
	}
	out := make([]Result, len(parsed.Data))
	for i, r := range parsed.Data {
		out[i] = Result{
			Path:     r.Path,
			ChunkIdx: r.ChunkIdx,
			Score:    r.Distance,
			Snippet:  snippet(r.Text, defaultSnippetLen),
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out, nil
}

func (m *Milvus) Close() error { return nil }

func escapeMilvusString(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s)
}
