package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type RemoteBackend struct {
	base      string
	apiKey    string
	apiPrefix string
	client    *http.Client
}

func NewRemoteBackend(baseURL, apiKey, space string) *RemoteBackend {
	prefix := "/api/kiwi"
	if space != "" && space != "default" {
		prefix = "/api/kiwi/" + space
	}
	return &RemoteBackend{
		base:      strings.TrimRight(baseURL, "/"),
		apiKey:    apiKey,
		apiPrefix: prefix,
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (r *RemoteBackend) do(ctx context.Context, method, path string, body io.Reader, headers ...string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, r.base+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Actor", "mcp")
	if r.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+r.apiKey)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("KiwiFS server at %s is not reachable: %w", r.base, err)
	}
	return resp, nil
}

type httpError struct {
	StatusCode int
	Message    string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("%s (HTTP %d)", e.Message, e.StatusCode)
}

const maxResponseSize = 64 * 1024 * 1024 // 64 MiB

func (r *RemoteBackend) readBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		msg := string(data)
		var errResp struct {
			Message string `json:"message"`
			Error   any    `json:"error"`
		}
		if json.Unmarshal(data, &errResp) == nil {
			if errResp.Message != "" {
				msg = errResp.Message
			} else if s, ok := errResp.Error.(string); ok && s != "" {
				msg = s
			}
		}
		return nil, &httpError{StatusCode: resp.StatusCode, Message: msg}
	}
	return data, nil
}

func (r *RemoteBackend) ReadFile(ctx context.Context, path string) (string, string, error) {
	resp, err := r.do(ctx, http.MethodGet, r.apiPrefix+"/file?path="+url.QueryEscape(path), nil)
	if err != nil {
		return "", "", err
	}
	data, err := r.readBody(resp)
	if err != nil {
		return "", "", err
	}
	etag := strings.Trim(resp.Header.Get("ETag"), `"`)
	return string(data), etag, nil
}

func (r *RemoteBackend) WriteFile(ctx context.Context, path, content, actor, provenance string) (string, error) {
	hdrs := []string{"Content-Type", "text/markdown"}
	if actor != "" {
		hdrs = append(hdrs, "X-Actor", actor)
	}
	if provenance != "" {
		hdrs = append(hdrs, "X-Provenance", provenance)
	}
	resp, err := r.do(ctx, http.MethodPut, r.apiPrefix+"/file?path="+url.QueryEscape(path), strings.NewReader(content), hdrs...)
	if err != nil {
		return "", err
	}
	data, err := r.readBody(resp)
	if err != nil {
		return "", err
	}
	var result struct {
		ETag string `json:"etag"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return "", err
	}
	return result.ETag, nil
}

func (r *RemoteBackend) DeleteFile(ctx context.Context, path, actor string) error {
	var hdrs []string
	if actor != "" {
		hdrs = append(hdrs, "X-Actor", actor)
	}
	resp, err := r.do(ctx, http.MethodDelete, r.apiPrefix+"/file?path="+url.QueryEscape(path), nil, hdrs...)
	if err != nil {
		return err
	}
	_, err = r.readBody(resp)
	return err
}

func (r *RemoteBackend) Tree(ctx context.Context, path string) (json.RawMessage, error) {
	q := r.apiPrefix+"/tree"
	if path != "" {
		q += "?path=" + url.QueryEscape(path)
	}
	resp, err := r.do(ctx, http.MethodGet, q, nil)
	if err != nil {
		return nil, err
	}
	return r.readBody(resp)
}

func (r *RemoteBackend) Search(ctx context.Context, query string, limit, offset int, pathPrefix string) ([]SearchResult, error) {
	q := r.apiPrefix+"/search?q=" + url.QueryEscape(query)
	if limit > 0 {
		q += "&limit=" + strconv.Itoa(limit)
	}
	if offset > 0 {
		q += "&offset=" + strconv.Itoa(offset)
	}
	if pathPrefix != "" {
		q += "&pathPrefix=" + url.QueryEscape(pathPrefix)
	}
	resp, err := r.do(ctx, http.MethodGet, q, nil)
	if err != nil {
		return nil, err
	}
	data, err := r.readBody(resp)
	if err != nil {
		return nil, err
	}
	var result struct {
		Results []SearchResult `json:"results"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	for i := range result.Results {
		result.Results[i].Snippet = stripMarkTags(result.Results[i].Snippet)
	}
	return result.Results, nil
}

func (r *RemoteBackend) SearchSemantic(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	body := fmt.Sprintf(`{"query":%q,"topK":%d}`, query, limit)
	resp, err := r.do(ctx, http.MethodPost, r.apiPrefix+"/search/semantic", strings.NewReader(body), "Content-Type", "application/json")
	if err != nil {
		return nil, err
	}
	data, err := r.readBody(resp)
	if err != nil {
		return nil, err
	}
	var result struct {
		Results []struct {
			Path  string  `json:"path"`
			Chunk string  `json:"chunk"`
			Score float32 `json:"score"`
		} `json:"results"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	out := make([]SearchResult, len(result.Results))
	for i, sr := range result.Results {
		out[i] = SearchResult{Path: sr.Path, Snippet: sr.Chunk, Score: float64(sr.Score)}
	}
	return out, nil
}

func (r *RemoteBackend) QueryMeta(ctx context.Context, filters []string, sort, order string, limit, offset int) ([]MetaResult, error) {
	q := r.apiPrefix+"/meta?"
	params := url.Values{}
	for _, f := range filters {
		params.Add("where", f)
	}
	if sort != "" {
		params.Set("sort", sort)
	}
	if order != "" {
		params.Set("order", order)
	}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	if offset > 0 {
		params.Set("offset", strconv.Itoa(offset))
	}
	resp, err := r.do(ctx, http.MethodGet, q+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	data, err := r.readBody(resp)
	if err != nil {
		return nil, err
	}
	var result struct {
		Results []MetaResult `json:"results"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result.Results, nil
}

func (r *RemoteBackend) Versions(ctx context.Context, path string) ([]Version, error) {
	resp, err := r.do(ctx, http.MethodGet, r.apiPrefix+"/versions?path="+url.QueryEscape(path), nil)
	if err != nil {
		return nil, err
	}
	data, err := r.readBody(resp)
	if err != nil {
		return nil, err
	}
	var result struct {
		Versions []Version `json:"versions"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result.Versions, nil
}

func (r *RemoteBackend) BulkWrite(ctx context.Context, files []BulkFile, actor, provenance string) (map[string]string, error) {
	type reqFile struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	req := struct {
		Files []reqFile `json:"files"`
		Actor string    `json:"actor,omitempty"`
	}{
		Actor: actor,
	}
	for _, f := range files {
		req.Files = append(req.Files, reqFile{Path: f.Path, Content: f.Content})
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal bulk request: %w", err)
	}
	hdrs := []string{"Content-Type", "application/json"}
	if provenance != "" {
		hdrs = append(hdrs, "X-Provenance", provenance)
	}
	resp, err := r.do(ctx, http.MethodPost, r.apiPrefix+"/bulk", strings.NewReader(string(body)), hdrs...)
	if err != nil {
		return nil, err
	}
	data, err := r.readBody(resp)
	if err != nil {
		return nil, err
	}
	var result struct {
		ETags map[string]string `json:"etags"`
	}
	if json.Unmarshal(data, &result) == nil && len(result.ETags) > 0 {
		return result.ETags, nil
	}
	if err := json.Unmarshal(data, &result); err != nil {
		stderr.Printf("bulk write response parse warning: %v", err)
	}
	return map[string]string{}, nil
}

func (r *RemoteBackend) Backlinks(ctx context.Context, path string) ([]Backlink, error) {
	resp, err := r.do(ctx, http.MethodGet, r.apiPrefix+"/backlinks?path="+url.QueryEscape(path), nil)
	if err != nil {
		return nil, err
	}
	data, err := r.readBody(resp)
	if err != nil {
		return nil, err
	}
	var result struct {
		Backlinks []Backlink `json:"backlinks"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result.Backlinks, nil
}

func (r *RemoteBackend) PublicURL() string { return "" }

func (r *RemoteBackend) ResolveWikiLinks(ctx context.Context, content string) string {
	body := fmt.Sprintf(`{"content":%s}`, mustMarshalString(content))
	resp, err := r.do(ctx, http.MethodPost, r.apiPrefix+"/resolve-links", strings.NewReader(body), "Content-Type", "application/json")
	if err != nil {
		return content
	}
	data, err := r.readBody(resp)
	if err != nil {
		return content
	}
	var result struct {
		Content string `json:"content"`
	}
	if json.Unmarshal(data, &result) == nil && result.Content != "" {
		return result.Content
	}
	return content
}

func mustMarshalString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (r *RemoteBackend) Health(ctx context.Context) error {
	resp, err := r.do(ctx, http.MethodGet, "/health", nil)
	if err != nil {
		return err
	}
	_, err = r.readBody(resp)
	return err
}

func (r *RemoteBackend) Close() error { return nil }
