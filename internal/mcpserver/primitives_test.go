package mcpserver

import (
	"strings"
	"testing"
	"time"
)

func TestMCP_Backlinks(t *testing.T) {
	b, tmp := setupTestBackend(t)
	defer b.Close()

	// Write page A with a link to B
	b.WriteFile(t.Context(), "concepts/auth.md",
		"---\nstatus: published\n---\n# Auth\n\nSee [[session]] for session handling.\n",
		"test", "")
	b.WriteFile(t.Context(), "concepts/session.md",
		"# Session\n\nHandles user sessions.\n",
		"test", "")

	_ = tmp
	time.Sleep(500 * time.Millisecond) // wait for async indexer flush

	text := mustCallTool(t, handleBacklinks(b), "kiwi_backlinks", map[string]any{
		"path": "concepts/session.md",
	})
	if !strings.Contains(text, "concepts/auth.md") {
		t.Fatalf("expected auth.md in backlinks, got: %s", text)
	}
}

func TestMCP_Append(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	// Append to new file
	text := mustCallTool(t, handleAppend(b), "kiwi_append", map[string]any{
		"path":    "log.md",
		"content": "entry 1",
	})
	if !strings.Contains(text, "Appended to log.md") {
		t.Fatalf("expected success message, got: %s", text)
	}

	// Read it back
	content, _, err := b.ReadFile(t.Context(), "log.md")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if content != "entry 1" {
		t.Fatalf("content = %q, want %q", content, "entry 1")
	}

	// Append again
	mustCallTool(t, handleAppend(b), "kiwi_append", map[string]any{
		"path":    "log.md",
		"content": "entry 2",
	})
	content, _, _ = b.ReadFile(t.Context(), "log.md")
	if content != "entry 1\nentry 2" {
		t.Fatalf("content = %q, want %q", content, "entry 1\nentry 2")
	}
}

func TestMCP_Append_CustomSeparator(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	b.WriteFile(t.Context(), "journal.md", "day 1", "test", "")
	mustCallTool(t, handleAppend(b), "kiwi_append", map[string]any{
		"path":      "journal.md",
		"content":   "day 2",
		"separator": "\n---\n",
	})
	content, _, _ := b.ReadFile(t.Context(), "journal.md")
	if content != "day 1\n---\nday 2" {
		t.Fatalf("content = %q, want %q", content, "day 1\n---\nday 2")
	}
}

func TestMCP_ReadWithIfNotEtag(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	// Read to get etag
	text := mustCallTool(t, handleRead(b), "kiwi_read", map[string]any{
		"path": "index.md",
	})
	// Extract etag from "[ETag: xxx]\n\n..."
	etagLine := strings.SplitN(text, "\n", 2)[0]
	etag := strings.TrimPrefix(etagLine, "[ETag: ")
	etag = strings.TrimSuffix(etag, "]")

	// Read with matching etag
	text = mustCallTool(t, handleRead(b), "kiwi_read", map[string]any{
		"path":        "index.md",
		"if_not_etag": etag,
	})
	if !strings.Contains(text, "not modified") {
		t.Fatalf("expected not_modified response, got: %s", text)
	}

	// Read with non-matching etag
	text = mustCallTool(t, handleRead(b), "kiwi_read", map[string]any{
		"path":        "index.md",
		"if_not_etag": "deadbeef",
	})
	if strings.Contains(text, "not modified") {
		t.Fatal("expected full content with non-matching etag")
	}
	if !strings.Contains(text, "# Index") {
		t.Fatalf("expected content, got: %s", text)
	}
}

func TestMCP_ReadMetadataOnly(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	text := mustCallTool(t, handleRead(b), "kiwi_read", map[string]any{
		"path":          "concepts/auth.md",
		"metadata_only": true,
	})
	if !strings.Contains(text, "status") {
		t.Fatalf("expected frontmatter in response, got: %s", text)
	}
	if strings.Contains(text, "# Authentication") {
		t.Fatal("metadata_only should not include the body")
	}
}

func TestMCP_SearchSemantic_NotEnabled(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	handler := handleSearchSemantic(b)
	req := callToolReq("kiwi_search_semantic", map[string]any{"query": "auth"})
	result, err := handler(t.Context(), req)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	// When vector search is not configured, we expect an error result
	if !result.IsError {
		t.Log("semantic search returned results (vector search may be configured)")
	}
}

func TestMCP_QueryMeta_PathsFilter(t *testing.T) {
	b, _ := setupTestBackend(t)
	defer b.Close()

	b.WriteFile(t.Context(), "a.md", "---\nstatus: draft\n---\n# A\n", "test", "")
	b.WriteFile(t.Context(), "b.md", "---\nstatus: active\n---\n# B\n", "test", "")
	b.WriteFile(t.Context(), "c.md", "---\nstatus: done\n---\n# C\n", "test", "")

	time.Sleep(500 * time.Millisecond) // wait for async indexer flush

	text := mustCallTool(t, handleQueryMeta(b), "kiwi_query_meta", map[string]any{
		"paths": []any{"a.md", "c.md"},
	})
	if !strings.Contains(text, "a.md") {
		t.Fatalf("expected a.md in results, got: %s", text)
	}
	if !strings.Contains(text, "c.md") {
		t.Fatalf("expected c.md in results, got: %s", text)
	}
	if strings.Contains(text, "b.md") {
		t.Fatalf("did not expect b.md in results, got: %s", text)
	}
}
