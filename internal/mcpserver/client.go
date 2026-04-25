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

func (r *RemoteBackend) getJSON(ctx context.Context, path string, out any) error {
	resp, err := r.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	data, err := r.readBody(resp)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func (r *RemoteBackend) postJSON(ctx context.Context, path string, body, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := r.do(ctx, http.MethodPost, path, strings.NewReader(string(data)), "Content-Type", "application/json")
	if err != nil {
		return err
	}
	respData, err := r.readBody(resp)
	if err != nil {
		return err
	}
	if out != nil {
		return json.Unmarshal(respData, out)
	}
	return nil
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
	q := r.apiPrefix + "/tree"
	if path != "" {
		q += "?path=" + url.QueryEscape(path)
	}
	var raw json.RawMessage
	if err := r.getJSON(ctx, q, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func (r *RemoteBackend) Search(ctx context.Context, query string, limit, offset int, pathPrefix string) ([]SearchResult, error) {
	q := r.apiPrefix + "/search?q=" + url.QueryEscape(query)
	if limit > 0 {
		q += "&limit=" + strconv.Itoa(limit)
	}
	if offset > 0 {
		q += "&offset=" + strconv.Itoa(offset)
	}
	if pathPrefix != "" {
		q += "&pathPrefix=" + url.QueryEscape(pathPrefix)
	}
	var result struct {
		Results []SearchResult `json:"results"`
	}
	if err := r.getJSON(ctx, q, &result); err != nil {
		return nil, err
	}
	for i := range result.Results {
		result.Results[i].Snippet = stripMarkTags(result.Results[i].Snippet)
	}
	return result.Results, nil
}

func (r *RemoteBackend) SearchSemantic(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	var result struct {
		Results []struct {
			Path  string  `json:"path"`
			Chunk string  `json:"chunk"`
			Score float32 `json:"score"`
		} `json:"results"`
	}
	if err := r.postJSON(ctx, r.apiPrefix+"/search/semantic", map[string]any{"query": query, "topK": limit}, &result); err != nil {
		return nil, err
	}
	out := make([]SearchResult, len(result.Results))
	for i, sr := range result.Results {
		out[i] = SearchResult{Path: sr.Path, Snippet: sr.Chunk, Score: float64(sr.Score)}
	}
	return out, nil
}

func (r *RemoteBackend) QueryMeta(ctx context.Context, filters []string, sort, order string, limit, offset int) ([]MetaResult, error) {
	return r.QueryMetaOr(ctx, filters, nil, sort, order, limit, offset)
}

func (r *RemoteBackend) QueryMetaOr(ctx context.Context, andFilters, orFilters []string, sort, order string, limit, offset int) ([]MetaResult, error) {
	params := url.Values{}
	for _, f := range andFilters {
		params.Add("where", f)
	}
	for _, f := range orFilters {
		params.Add("or", f)
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
	var result struct {
		Results []MetaResult `json:"results"`
	}
	if err := r.getJSON(ctx, r.apiPrefix+"/meta?"+params.Encode(), &result); err != nil {
		return nil, err
	}
	return result.Results, nil
}

func (r *RemoteBackend) ViewRefresh(ctx context.Context, path string) (bool, error) {
	var result struct {
		Status string `json:"status"`
	}
	if err := r.postJSON(ctx, r.apiPrefix+"/view/refresh", map[string]string{"path": path}, &result); err != nil {
		return false, err
	}
	return result.Status == "regenerated", nil
}

func (r *RemoteBackend) QueryDQL(ctx context.Context, dql string, limit, offset int) (*QueryResult, error) {
	q := r.apiPrefix + "/query?q=" + url.QueryEscape(dql)
	if limit > 0 {
		q += "&limit=" + strconv.Itoa(limit)
	}
	if offset > 0 {
		q += "&offset=" + strconv.Itoa(offset)
	}
	var result QueryResult
	if err := r.getJSON(ctx, q, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (r *RemoteBackend) Versions(ctx context.Context, path string) ([]Version, error) {
	var result struct {
		Versions []Version `json:"versions"`
	}
	if err := r.getJSON(ctx, r.apiPrefix+"/versions?path="+url.QueryEscape(path), &result); err != nil {
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
	var result struct {
		Backlinks []Backlink `json:"backlinks"`
	}
	if err := r.getJSON(ctx, r.apiPrefix+"/backlinks?path="+url.QueryEscape(path), &result); err != nil {
		return nil, err
	}
	return result.Backlinks, nil
}

func (r *RemoteBackend) PublicURL() string { return "" }

func (r *RemoteBackend) ResolveWikiLinks(ctx context.Context, content string) string {
	var result struct {
		Content string `json:"content"`
	}
	if err := r.postJSON(ctx, r.apiPrefix+"/resolve-links", map[string]string{"content": content}, &result); err != nil {
		return content
	}
	if result.Content != "" {
		return result.Content
	}
	return content
}

func (r *RemoteBackend) Health(ctx context.Context) error {
	return r.getJSON(ctx, "/health", &json.RawMessage{})
}

func (r *RemoteBackend) Close() error { return nil }
