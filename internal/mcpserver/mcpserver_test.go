package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

func TestReadFile(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	content, etag, err := b.ReadFile(context.Background(), "index.md")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if content == "" {
		t.Fatal("expected non-empty content")
	}
	if etag == "" {
		t.Fatal("expected non-empty etag")
	}
	if got := "# Index"; content[:7] != got {
		t.Fatalf("content prefix = %q, want %q", content[:7], got)
	}
}

func TestReadFileNotFound(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	_, _, err := b.ReadFile(context.Background(), "nonexistent.md")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestWriteFile(t *testing.T) {
	b, tmp := setupTestBackend(t)
	defer b.Close()

	etag, err := b.WriteFile(context.Background(), "test.md", "# Test\n\nHello", "test-agent", "")
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if etag == "" {
		t.Fatal("expected non-empty etag")
	}

	data, err := os.ReadFile(filepath.Join(tmp, "test.md"))
	if err != nil {
		t.Fatalf("read written file: %v", err)
	}
	if string(data) != "# Test\n\nHello" {
		t.Fatalf("written content = %q, want %q", string(data), "# Test\n\nHello")
	}
}

func TestDeleteFile(t *testing.T) {
	b, tmp := setupTestBackend(t)
	defer b.Close()

	err := b.DeleteFile(context.Background(), "index.md", "test-agent")
	if err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}

	if _, err := os.Stat(filepath.Join(tmp, "index.md")); !os.IsNotExist(err) {
		t.Fatal("expected file to be deleted")
	}
}

func TestSearch(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	results, err := b.Search(context.Background(), "authentication", 10, 0, "")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least one search result")
	}
	found := false
	for _, r := range results {
		if r.Path == "concepts/auth.md" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected concepts/auth.md in results, got %v", results)
	}
}

func TestTree(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	text, err := treeText(context.Background(), b, "/", 3, "")
	if err != nil {
		t.Fatalf("treeText: %v", err)
	}
	if text == "" {
		t.Fatal("expected non-empty tree text")
	}
	if !strings.Contains(text, "index.md") {
		t.Fatalf("tree text missing index.md: %s", text)
	}
	if !strings.Contains(text, "concepts/") {
		t.Fatalf("tree text missing concepts/: %s", text)
	}
}

func TestBulkWrite(t *testing.T) {
	b, tmp := setupTestBackend(t)
	defer b.Close()

	files := []BulkFile{
		{Path: "bulk1.md", Content: "# Bulk 1"},
		{Path: "bulk2.md", Content: "# Bulk 2"},
	}
	etags, err := b.BulkWrite(context.Background(), files, "test-agent", "")
	if err != nil {
		t.Fatalf("BulkWrite: %v", err)
	}
	if len(etags) != len(files) {
		t.Fatalf("expected %d etags, got %d", len(files), len(etags))
	}

	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(tmp, f.Path))
		if err != nil {
			t.Fatalf("read %s: %v", f.Path, err)
		}
		if string(data) != f.Content {
			t.Fatalf("%s content = %q, want %q", f.Path, string(data), f.Content)
		}
	}
}

func TestQueryMeta(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	results, err := b.QueryMeta(context.Background(), []string{"$.status=published"}, "", "", 20, 0)
	if err != nil {
		t.Fatalf("QueryMeta: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least one meta result for status=published")
	}
	if results[0].Path != "concepts/auth.md" {
		t.Fatalf("expected concepts/auth.md, got %s", results[0].Path)
	}
}

func TestMCPServerCreation(t *testing.T) {
	tmp := t.TempDir()
	kiwiDir := filepath.Join(tmp, ".kiwi")
	os.MkdirAll(kiwiDir, 0o755)
	os.WriteFile(filepath.Join(kiwiDir, "config.toml"), []byte(`
[search]
engine = "sqlite"
[versioning]
strategy = "none"
`), 0o644)
	os.WriteFile(filepath.Join(tmp, "SCHEMA.md"), []byte("# Schema\n"), 0o644)

	s, backend, err := New(Options{Root: tmp})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer backend.Close()

	if s == nil {
		t.Fatal("expected non-nil MCPServer")
	}
}

func TestBulkWriteToolDeclaresArrayItemsSchema(t *testing.T) {
	s, backend, err := New(Options{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer backend.Close()

	tool := s.GetTool("kiwi_bulk_write")
	if tool == nil {
		t.Fatal("expected kiwi_bulk_write tool")
	}

	files, ok := tool.Tool.InputSchema.Properties["files"].(map[string]any)
	if !ok {
		t.Fatalf("files schema = %#v, want object", tool.Tool.InputSchema.Properties["files"])
	}
	items, ok := files["items"].(map[string]any)
	if !ok {
		t.Fatalf("files schema missing object items: %#v", files)
	}
	if items["type"] != "object" {
		t.Fatalf("files.items.type = %#v, want object", items["type"])
	}
	props, ok := items["properties"].(map[string]any)
	if !ok {
		t.Fatalf("files.items.properties = %#v, want object", items["properties"])
	}
	if _, ok := props["path"]; !ok {
		t.Fatalf("files.items.properties missing path: %#v", props)
	}
	if _, ok := props["content"]; !ok {
		t.Fatalf("files.items.properties missing content: %#v", props)
	}
}

func TestHTTPHandlerHealth(t *testing.T) {
	s, backend, err := New(Options{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer backend.Close()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	newHTTPHandler(s, time.Now()).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("health status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), `"transport":"http"`) {
		t.Fatalf("health body = %q, want transport http", rec.Body.String())
	}
}

func TestToolHandlerRead(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	text := mustCallTool(t, handleRead(b), "kiwi_read", map[string]any{"path": "index.md"})
	if !strings.Contains(text, "# Index") {
		t.Fatalf("expected content to contain '# Index', got: %s", text[:50])
	}
}

func TestToolHandlerReadNotFound(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	handler := handleRead(b)
	req := mcp.CallToolRequest{}
	req.Params.Name = "kiwi_read"
	req.Params.Arguments = map[string]any{"path": "nonexistent.md"}

	result, err := handler(context.Background(), req)
	if err != nil {
		t.Fatalf("handleRead: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected isError=true for missing file")
	}
	text := result.Content[0].(mcp.TextContent).Text
	if !strings.Contains(text, "File not found") {
		t.Fatalf("expected 'File not found' error, got: %s", text)
	}
}

func TestToolHandlerWrite(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	text := mustCallTool(t, handleWrite(b), "kiwi_write", map[string]any{
		"path":    "new-page.md",
		"content": "# New Page\n\nContent here.",
	})
	if !strings.Contains(text, "Written new-page.md") {
		t.Fatalf("expected 'Written' message, got: %s", text)
	}
}

func TestToolHandlerSearch(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	mustCallTool(t, handleSearch(b), "kiwi_search", map[string]any{"query": "knowledge"})
}

func TestToolHandlerDelete(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	text := mustCallTool(t, handleDelete(b), "kiwi_delete", map[string]any{"path": "index.md"})
	if !strings.Contains(text, "Deleted index.md") {
		t.Fatalf("expected 'Deleted' message, got: %s", text)
	}
}

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"http 404", &httpError{StatusCode: 404, Message: "not found"}, true},
		{"http 500", &httpError{StatusCode: 500, Message: "internal error"}, true},
		{"os not exist", os.ErrNotExist, true},
		{"os wrapped not exist", fmt.Errorf("read file: %w", os.ErrNotExist), true},
		{"permission denied", errString("permission denied"), false},
		{"connection refused", errString("connection refused"), false},
	}
	for _, tt := range tests {
		want := tt.want
		if tt.name == "http 500" {
			want = false
		}
		got := isNotFound(tt.err)
		if got != want {
			t.Errorf("isNotFound(%s) = %v, want %v", tt.name, got, want)
		}
	}
}

func TestStripMarkTags(t *testing.T) {
	input := "This is <mark>highlighted</mark> text with <mark>multiple</mark> marks."
	want := "This is highlighted text with multiple marks."
	got := stripMarkTags(input)
	if got != want {
		t.Fatalf("stripMarkTags = %q, want %q", got, want)
	}
}

func TestFormatSize(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{500, "500 B"},
		{1536, "1.5 KB"},
		{2097152, "2.0 MB"},
	}
	for _, tt := range tests {
		got := formatSize(tt.bytes)
		if got != tt.want {
			t.Errorf("formatSize(%d) = %q, want %q", tt.bytes, got, tt.want)
		}
	}
}

func TestRemoteWriteFileSendsHeaders(t *testing.T) {
	var gotActor, gotProvenance, gotContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotActor = r.Header.Get("X-Actor")
		gotProvenance = r.Header.Get("X-Provenance")
		gotContentType = r.Header.Get("Content-Type")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"etag": "abc123"})
	}))
	defer srv.Close()

	rb := NewRemoteBackend(srv.URL, "", "default")
	_, err := rb.WriteFile(context.Background(), "test.md", "# Hello", "my-agent", "run:run-42")
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if gotActor != "my-agent" {
		t.Errorf("X-Actor = %q, want %q", gotActor, "my-agent")
	}
	if gotProvenance != "run:run-42" {
		t.Errorf("X-Provenance = %q, want %q", gotProvenance, "run:run-42")
	}
	if gotContentType != "text/markdown" {
		t.Errorf("Content-Type = %q, want %q", gotContentType, "text/markdown")
	}
}

func TestRemoteDeleteFileSendsActor(t *testing.T) {
	var gotActor string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotActor = r.Header.Get("X-Actor")
		w.WriteHeader(200)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rb := NewRemoteBackend(srv.URL, "", "default")
	err := rb.DeleteFile(context.Background(), "test.md", "my-agent")
	if err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if gotActor != "my-agent" {
		t.Errorf("X-Actor = %q, want %q", gotActor, "my-agent")
	}
}

func TestRemoteBulkWriteSendsProvenance(t *testing.T) {
	var gotProvenance, gotContentType string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotProvenance = r.Header.Get("X-Provenance")
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"etags": map[string]string{"a.md": "e1", "b.md": "e2"},
		})
	}))
	defer srv.Close()

	rb := NewRemoteBackend(srv.URL, "", "default")
	files := []BulkFile{{Path: "a.md", Content: "# A"}, {Path: "b.md", Content: "# B"}}
	etags, err := rb.BulkWrite(context.Background(), files, "agent", "commit:abc")
	if err != nil {
		t.Fatalf("BulkWrite: %v", err)
	}
	if gotProvenance != "commit:abc" {
		t.Errorf("X-Provenance = %q, want %q", gotProvenance, "commit:abc")
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want %q", gotContentType, "application/json")
	}
	if len(gotBody) == 0 {
		t.Fatal("expected non-empty request body")
	}
	if etags["a.md"] != "e1" || etags["b.md"] != "e2" {
		t.Errorf("etags = %v, want a.md=e1 b.md=e2", etags)
	}
}

func TestRemoteSpacePrefixing(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"results":[]}`))
	}))
	defer srv.Close()

	rb := NewRemoteBackend(srv.URL, "", "my-space")
	rb.Search(context.Background(), "test", 10, 0, "")
	if !strings.HasPrefix(gotPath, "/api/kiwi/my-space/search") {
		t.Errorf("path = %q, want prefix /api/kiwi/my-space/search", gotPath)
	}

	rb2 := NewRemoteBackend(srv.URL, "", "default")
	rb2.Search(context.Background(), "test", 10, 0, "")
	if !strings.HasPrefix(gotPath, "/api/kiwi/search") {
		t.Errorf("path = %q, want prefix /api/kiwi/search", gotPath)
	}
}

func TestFormatTreeJSONRecursive(t *testing.T) {
	tree := `{
		"children": [
			{"name": "docs", "path": "docs", "isDir": true, "size": 0, "children": [
				{"name": "api.md", "path": "docs/api.md", "isDir": false, "size": 1024, "children": null},
				{"name": "sub", "path": "docs/sub", "isDir": true, "size": 0, "children": [
					{"name": "deep.md", "path": "docs/sub/deep.md", "isDir": false, "size": 512, "children": null}
				]}
			]},
			{"name": "readme.md", "path": "readme.md", "isDir": false, "size": 256, "children": null}
		]
	}`
	result := formatTreeJSON(json.RawMessage(tree), 5, "")
	if !strings.Contains(result, "docs/") {
		t.Errorf("missing docs/ in result: %s", result)
	}
	if !strings.Contains(result, "  api.md") {
		t.Errorf("missing indented api.md: %s", result)
	}
	if !strings.Contains(result, "    deep.md") {
		t.Errorf("missing doubly-indented deep.md: %s", result)
	}
	if !strings.Contains(result, "readme.md") {
		t.Errorf("missing readme.md: %s", result)
	}
}

func TestSearchEmptyAfterPrefixFilter(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	text := mustCallTool(t, handleSearch(b), "kiwi_search", map[string]any{
		"query":       "knowledge",
		"path_prefix": "nonexistent-prefix/",
	})
	if !strings.Contains(text, "No results found in nonexistent-prefix/") {
		t.Fatalf("expected 'No results found in nonexistent-prefix/', got: %s", text)
	}
}

func TestHttpErrorIsNotFound(t *testing.T) {
	err404 := &httpError{StatusCode: 404, Message: "not found"}
	err500 := &httpError{StatusCode: 500, Message: "server error"}

	if !isNotFound(err404) {
		t.Error("expected 404 to be not-found")
	}
	if isNotFound(err500) {
		t.Error("expected 500 to NOT be not-found")
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestNegativeLimitOffset(t *testing.T) {
	tests := []struct {
		name string
		args map[string]any
	}{
		{"negative limit", map[string]any{"limit": float64(-5)}},
		{"negative offset", map[string]any{"offset": float64(-3)}},
		{"both negative", map[string]any{"limit": float64(-1), "offset": float64(-10)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limit := intArg(tt.args, "limit", 20)
			offset := intArg(tt.args, "offset", 0)
			if limit < 0 {
				t.Errorf("intArg returned negative limit: %d", limit)
			}
			if offset < 0 {
				t.Errorf("intArg returned negative offset: %d", offset)
			}
		})
	}
}

func TestNegativeLimitSearchHandler(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	mustCallTool(t, handleSearch(b), "kiwi_search", map[string]any{
		"query":  "knowledge",
		"limit":  float64(-5),
		"offset": float64(-3),
	})
}

func TestNegativeLimitQueryMetaHandler(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	mustCallTool(t, handleQueryMeta(b), "kiwi_query_meta", map[string]any{
		"filters": []any{"$.status=published"},
		"limit":   float64(-10),
		"offset":  float64(-5),
	})
}

func TestFormatTreeJSONParseFailure(t *testing.T) {
	result := formatTreeJSON(json.RawMessage(`{invalid json`), 3, "")
	if strings.Contains(result, "{invalid") {
		t.Fatalf("formatTreeJSON returned raw JSON on failure: %s", result)
	}
	if !strings.Contains(result, "error parsing tree") {
		t.Fatalf("expected error message, got: %s", result)
	}
}

func TestHandleQueryMetaSortedKeys(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	text := mustCallTool(t, handleQueryMeta(b), "kiwi_query_meta", map[string]any{
		"filters": []any{"$.status=published"},
	})
	statusIdx := strings.Index(text, "  status:")
	tagsIdx := strings.Index(text, "  tags:")
	if statusIdx < 0 || tagsIdx < 0 {
		t.Fatalf("expected both status and tags in output, got: %s", text)
	}
	if statusIdx > tagsIdx {
		t.Fatalf("keys not sorted alphabetically: status at %d, tags at %d", statusIdx, tagsIdx)
	}
}

func TestHandleBulkWriteMapSlice(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	text := mustCallTool(t, handleBulkWrite(b), "kiwi_bulk_write", map[string]any{
		"files": []map[string]any{
			{"path": "mapslice1.md", "content": "# Map Slice 1"},
			{"path": "mapslice2.md", "content": "# Map Slice 2"},
		},
	})
	if !strings.Contains(text, "Written 2 files") {
		t.Fatalf("expected 'Written 2 files', got: %s", text)
	}
}

func TestResourceFileURLDecoding(t *testing.T) {
	b, tmp := setupTestBackend(t)
	defer b.Close()

	os.MkdirAll(filepath.Join(tmp, "my docs"), 0o755)
	os.WriteFile(filepath.Join(tmp, "my docs", "page.md"), []byte("# Spaced Path\n"), 0o644)

	content, _, err := b.ReadFile(context.Background(), "my docs/page.md")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(content, "# Spaced Path") {
		t.Fatalf("unexpected content: %s", content)
	}
}

func TestRemoteBackendErrorPaths(t *testing.T) {
	t.Run("server returns 500", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			w.Write([]byte(`{"message":"internal server error"}`))
		}))
		defer srv.Close()

		rb := NewRemoteBackend(srv.URL, "", "default")
		_, _, err := rb.ReadFile(context.Background(), "test.md")
		if err == nil {
			t.Fatal("expected error for 500 response")
		}
		if !strings.Contains(err.Error(), "internal server error") {
			t.Fatalf("expected error message in response, got: %v", err)
		}
	})

	t.Run("server returns 204", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(204)
		}))
		defer srv.Close()

		rb := NewRemoteBackend(srv.URL, "", "default")
		content, _, err := rb.ReadFile(context.Background(), "test.md")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if content != "" {
			t.Fatalf("expected empty content for 204, got: %q", content)
		}
	})

	t.Run("connection refused", func(t *testing.T) {
		rb := NewRemoteBackend("http://127.0.0.1:1", "", "default")
		_, _, err := rb.ReadFile(context.Background(), "test.md")
		if err == nil {
			t.Fatal("expected error for refused connection")
		}
	})
}

func TestRemoteResolveWikiLinks(t *testing.T) {
	t.Run("returns resolved content", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf("expected POST, got %s", r.Method)
			}
			if !strings.HasSuffix(r.URL.Path, "/resolve-links") {
				t.Errorf("unexpected path: %s", r.URL.Path)
			}
			body, _ := io.ReadAll(r.Body)
			var req struct{ Content string }
			json.Unmarshal(body, &req)
			resolved := strings.ReplaceAll(req.Content, "[[auth]]", "[auth](https://wiki.co/page/concepts/auth.md)")
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"content": resolved})
		}))
		defer srv.Close()

		rb := NewRemoteBackend(srv.URL, "", "default")
		got := rb.ResolveWikiLinks(context.Background(), "See [[auth]] for details.")
		if !strings.Contains(got, "https://wiki.co/page/concepts/auth.md") {
			t.Fatalf("expected resolved link, got: %s", got)
		}
	})

	t.Run("returns original on server error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			w.Write([]byte(`{"message":"internal error"}`))
		}))
		defer srv.Close()

		rb := NewRemoteBackend(srv.URL, "", "default")
		original := "See [[auth]] for details."
		got := rb.ResolveWikiLinks(context.Background(), original)
		if got != original {
			t.Fatalf("expected original content on error, got: %s", got)
		}
	})

	t.Run("returns original on connection failure", func(t *testing.T) {
		rb := NewRemoteBackend("http://127.0.0.1:1", "", "default")
		original := "See [[auth]] for details."
		got := rb.ResolveWikiLinks(context.Background(), original)
		if got != original {
			t.Fatalf("expected original content on connection failure, got: %s", got)
		}
	})
}

func TestMCP_KiwiQuery(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	text := mustCallTool(t, handleQuery(b), "kiwi_query", map[string]any{
		"query":  `TABLE _path`,
		"format": "table",
	})
	if !strings.Contains(text, "|") {
		t.Errorf("expected markdown table, got:\n%s", text)
	}
}

func TestMCP_KiwiViewRefresh(t *testing.T) {
	b, tmp := setupTestBackend(t)
	defer b.Close()

	// Write a computed view file
	viewContent := "---\nkiwi-view: true\nkiwi-query: TABLE _path\n---\n<!-- kiwi:auto -->\n"
	viewPath := filepath.Join(tmp, "views")
	os.MkdirAll(viewPath, 0o755)
	os.WriteFile(filepath.Join(viewPath, "test.md"), []byte(viewContent), 0o644)

	// Re-index so the view file appears in file_meta
	b.init()
	if b.stack != nil && b.stack.Searcher != nil {
		if sq, ok := b.stack.Searcher.(interface {
			Reindex(context.Context) (int, error)
		}); ok {
			sq.Reindex(context.Background())
		}
	}

	mustCallTool(t, handleViewRefresh(b), "kiwi_view_refresh", map[string]any{
		"path": "views/test.md",
	})
}

func TestMCP_KiwiMemoryReport(t *testing.T) {
	b, tmp := setupTestBackend(t)
	defer b.Close()

	epDir := filepath.Join(tmp, "episodes")
	if err := os.MkdirAll(epDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(epDir, "run.md"), []byte(`---
memory_kind: episodic
episode_id: mcp-ep-1
---
# run
`), 0o644); err != nil {
		t.Fatal(err)
	}

	h := handleMemoryReport(b)
	out := mustCallTool(t, h, "kiwi_memory_report", map[string]any{})
	if want := "Unmerged (no merged-from): 1"; !strings.Contains(out, want) {
		t.Fatalf("want %q in:\n%s", want, out)
	}

	if err := os.WriteFile(filepath.Join(tmp, "concepts", "sum.md"), []byte(`---
memory_kind: semantic
merged-from:
  - type: episode
    id: mcp-ep-1
---
# Summary
`), 0o644); err != nil {
		t.Fatal(err)
	}

	out2 := mustCallTool(t, h, "kiwi_memory_report", map[string]any{})
	if want := "Unmerged (no merged-from): 0"; !strings.Contains(out2, want) {
		t.Fatalf("want %q in:\n%s", want, out2)
	}
}
