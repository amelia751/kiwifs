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

// Qdrant is a Store backed by a Qdrant HTTP API. Collection is auto-created
// on first write if missing. Auth uses the `api-key` header.
//
// Docs: https://qdrant.tech/documentation/concepts/
type Qdrant struct {
	baseURL    string
	apiKey     string
	collection string
	dims       int // learned from the first Upsert batch
	client     *http.Client
	created    bool
}

// NewQdrant wires up a Qdrant client. url defaults to http://localhost:6333.
// Collection defaults to "kiwifs".
func NewQdrant(url, apiKey, collection string, dims int) (*Qdrant, error) {
	if url == "" {
		url = "http://localhost:6333"
	}
	if collection == "" {
		collection = "kiwifs"
	}
	return &Qdrant{
		baseURL:    strings.TrimRight(url, "/"),
		apiKey:     apiKey,
		collection: collection,
		dims:       dims,
		client:     &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (q *Qdrant) do(ctx context.Context, method, path string, body any, out any) error {
	var buf io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		buf = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, q.baseURL+path, buf)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if q.apiKey != "" {
		req.Header.Set("api-key", q.apiKey)
	}
	resp, err := q.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 && resp.StatusCode != 404 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("qdrant %s %s: %s: %s", method, path, resp.Status, truncate(string(raw), 200))
	}
	if out != nil && resp.StatusCode/100 == 2 {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// ensureCollection creates the collection if it doesn't exist. Called lazily
// on the first Upsert so we know the vector dimensions.
func (q *Qdrant) ensureCollection(ctx context.Context, dims int) error {
	if q.created {
		return nil
	}
	// PUT is idempotent in Qdrant — creating an existing collection with
	// different parameters is an error, but same params succeed as no-op.
	body := map[string]any{
		"vectors": map[string]any{
			"size":     dims,
			"distance": "Cosine",
		},
	}
	if err := q.do(ctx, http.MethodPut, "/collections/"+q.collection, body, nil); err != nil {
		return err
	}
	q.created = true
	return nil
}

func (q *Qdrant) Upsert(ctx context.Context, chunks []Chunk) error {
	if len(chunks) == 0 {
		return nil
	}
	if q.dims == 0 {
		q.dims = len(chunks[0].Vector)
	}
	if err := q.ensureCollection(ctx, q.dims); err != nil {
		return err
	}
	points := make([]map[string]any, len(chunks))
	for i, c := range chunks {
		points[i] = map[string]any{
			// Qdrant requires uuid or uint — we hash the string ID into the
			// payload and use a UUIDv5-ish deterministic string as the id.
			"id":     deterministicUUID(c.ID),
			"vector": c.Vector,
			"payload": map[string]any{
				"id":        c.ID,
				"path":      c.Path,
				"chunk_idx": c.ChunkIdx,
				"text":      c.Text,
			},
		}
	}
	return q.do(ctx, http.MethodPut, "/collections/"+q.collection+"/points?wait=true",
		map[string]any{"points": points}, nil)
}

func (q *Qdrant) RemoveByPath(ctx context.Context, path string) error {
	body := map[string]any{
		"filter": map[string]any{
			"must": []map[string]any{
				{"key": "path", "match": map[string]any{"value": path}},
			},
		},
	}
	return q.do(ctx, http.MethodPost, "/collections/"+q.collection+"/points/delete?wait=true", body, nil)
}

func (q *Qdrant) Reset(ctx context.Context) error {
	// Delete then re-create on next Upsert. A 404 is fine — the collection
	// may not exist yet. Other errors propagate so callers know the reset
	// didn't actually happen.
	if err := q.do(ctx, http.MethodDelete, "/collections/"+q.collection, nil, nil); err != nil {
		if !strings.Contains(err.Error(), "404") {
			return err
		}
	}
	q.created = false
	return nil
}

func (q *Qdrant) Count(ctx context.Context) (int, error) {
	var parsed struct {
		Result struct {
			Count int `json:"count"`
		} `json:"result"`
	}
	err := q.do(ctx, http.MethodPost, "/collections/"+q.collection+"/points/count",
		map[string]any{"exact": true}, &parsed)
	if err != nil {
		// 404 (collection not created yet) surfaces as an error from do();
		// that's the only case where "treat as empty" is correct. Other
		// failures (network, auth, server error) should propagate so the
		// caller doesn't mistake a broken backend for an empty store.
		if strings.Contains(err.Error(), "404") {
			return 0, nil
		}
		return 0, err
	}
	return parsed.Result.Count, nil
}

func (q *Qdrant) Search(ctx context.Context, vector []float32, topK int) ([]Result, error) {
	if topK <= 0 {
		topK = DefaultTopK
	}
	body := map[string]any{
		"vector":       vector,
		"limit":        topK,
		"with_payload": true,
	}
	var parsed struct {
		Result []struct {
			Score   float64 `json:"score"`
			Payload struct {
				Path     string `json:"path"`
				ChunkIdx int    `json:"chunk_idx"`
				Text     string `json:"text"`
			} `json:"payload"`
		} `json:"result"`
	}
	if err := q.do(ctx, http.MethodPost, "/collections/"+q.collection+"/points/search", body, &parsed); err != nil {
		return nil, err
	}
	out := make([]Result, len(parsed.Result))
	for i, r := range parsed.Result {
		out[i] = Result{
			Path:     r.Payload.Path,
			ChunkIdx: r.Payload.ChunkIdx,
			Score:    r.Score,
			Snippet:  snippet(r.Payload.Text, defaultSnippetLen),
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out, nil
}

func (q *Qdrant) Close() error { return nil }

