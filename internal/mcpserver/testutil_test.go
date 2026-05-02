package mcpserver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

func mustCallTool(t *testing.T, handler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error), name string, args map[string]any) string {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Name = name
	req.Params.Arguments = args
	result, err := handler(context.Background(), req)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if result.IsError {
		t.Fatalf("%s: unexpected error: %v", name, result.Content)
	}
	if len(result.Content) == 0 {
		return ""
	}
	return result.Content[0].(mcp.TextContent).Text
}

func callToolReq(name string, args map[string]any) mcp.CallToolRequest {
	req := mcp.CallToolRequest{}
	req.Params.Name = name
	req.Params.Arguments = args
	return req
}

func setupTestBackend(t *testing.T) (*LocalBackend, string) {
	t.Helper()
	tmp := t.TempDir()
	kiwiDir := filepath.Join(tmp, ".kiwi")
	os.MkdirAll(kiwiDir, 0o755)
	os.WriteFile(filepath.Join(kiwiDir, "config.toml"), []byte(`
[search]
engine = "sqlite"
[versioning]
strategy = "none"
`), 0o644)
	os.WriteFile(filepath.Join(tmp, "index.md"), []byte("# Index\n\nWelcome to the knowledge base.\n"), 0o644)
	os.MkdirAll(filepath.Join(tmp, "concepts"), 0o755)
	os.WriteFile(filepath.Join(tmp, "concepts", "auth.md"), []byte("---\nstatus: published\ntags:\n  - security\n---\n# Authentication\n\nAuth overview.\n"), 0o644)
	os.WriteFile(filepath.Join(tmp, "SCHEMA.md"), []byte("# Schema\n\nKnowledge base schema.\n"), 0o644)

	b := NewLocalBackend(tmp)
	return b, tmp
}
