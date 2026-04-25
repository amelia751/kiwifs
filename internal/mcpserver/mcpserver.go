package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/dataview"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

var stderr = log.New(os.Stderr, "kiwifs-mcp: ", log.LstdFlags)

type Options struct {
	Remote string
	Root   string
	APIKey string
	Space  string
}

func New(opts Options) (*server.MCPServer, Backend, error) {
	var backend Backend
	if opts.Remote != "" {
		backend = NewRemoteBackend(opts.Remote, opts.APIKey, opts.Space)
	} else {
		backend = NewLocalBackend(opts.Root)
	}

	s := server.NewMCPServer(
		"kiwifs",
		"1.0.0",
		server.WithRecovery(),
		server.WithToolHandlerMiddleware(auditMiddleware),
	)

	registerTools(s, backend, opts)
	registerResources(s, backend, opts)

	return s, backend, nil
}

func auditMiddleware(next server.ToolHandlerFunc) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		start := time.Now()
		result, err := next(ctx, req)
		isErr := err != nil || (result != nil && result.IsError)
		stderr.Printf("tool=%s duration=%s error=%v", req.Params.Name, time.Since(start).Round(time.Millisecond), isErr)
		return result, err
	}
}

func registerTools(s *server.MCPServer, b Backend, opts Options) {
	pathOpts := []mcp.PropertyOption{
		mcp.Required(),
		mcp.Description("Relative path like concepts/auth.md"),
		mcp.MaxLength(500),
		mcp.Pattern(`^[^.][a-zA-Z0-9/_\-. ]+$`),
	}

	s.AddTools(
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_read",
				mcp.WithDescription("Read a markdown file from the knowledge base. Use this to check existing knowledge before writing — e.g. read the coverage strategy before deciding what to test, or read failure patterns to check if a similar failure has been seen before."),
				mcp.WithString("path", pathOpts...),
				mcp.WithBoolean("resolve_links", mcp.Description("When true, resolve [[wiki-links]] to full permalink URLs in the returned content. Default false (raw markdown).")),
				mcp.WithReadOnlyHintAnnotation(true),
				mcp.WithDestructiveHintAnnotation(false),
			),
			Handler: handleRead(b),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_write",
				mcp.WithDescription("Write a markdown file to the knowledge base. Creates the file if it doesn't exist, overwrites if it does. Every write is an atomic git commit — old content is preserved in git history. Use provenance to link this write to the run or process that produced the knowledge."),
				mcp.WithString("path", pathOpts...),
				mcp.WithString("content", mcp.Required(), mcp.Description("Markdown content to write"), mcp.MaxLength(32*1024*1024)),
				mcp.WithString("actor", mcp.Description("Who is writing — defaults to mcp-agent")),
				mcp.WithString("provenance", mcp.Description("Link to source, format type:id, e.g. run:run-249")),
				mcp.WithDestructiveHintAnnotation(true),
				mcp.WithIdempotentHintAnnotation(true),
			),
			Handler: handleWrite(b),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_search",
				mcp.WithDescription("Search the knowledge base using full-text search. Returns ranked results with snippets. Use this to find relevant knowledge — e.g. search for an error message to find similar past failures, or search for a concept to find related pages."),
				mcp.WithString("query", mcp.Required(), mcp.Description("Search query")),
				mcp.WithNumber("limit", mcp.Description("Max results (default 20, max 50)")),
				mcp.WithString("path_prefix", mcp.Description("Filter to a subtree like failures/")),
				mcp.WithNumber("offset", mcp.Description("Offset for pagination (default 0)")),
				mcp.WithReadOnlyHintAnnotation(true),
				mcp.WithDestructiveHintAnnotation(false),
			),
			Handler: handleSearch(b),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_tree",
				mcp.WithDescription("List files and folders in the knowledge base. Use this to understand what knowledge exists before reading or writing. Returns an indented tree with file sizes."),
				mcp.WithString("path", mcp.Description("Subtree root, defaults to root"), mcp.MaxLength(500), mcp.Pattern(`^[^.][a-zA-Z0-9/_\-. ]*$`)),
				mcp.WithNumber("depth", mcp.Description("Tree depth (default 3)")),
				mcp.WithBoolean("include_permalinks", mcp.Description("When true, include permalink URLs next to each file. Default false.")),
				mcp.WithReadOnlyHintAnnotation(true),
				mcp.WithDestructiveHintAnnotation(false),
			),
			Handler: handleTree(b, opts),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_query_meta",
				mcp.WithDescription("Query files by their YAML frontmatter fields. Use this for structured queries like 'find all failure patterns with status=open' or 'find all run records for project X sorted by date'. Filter format: $.field=value (e.g. $.status=published, $.priority=high). Filters can be empty to return all rows."),
				mcp.WithArray("filters", mcp.Description("Filters in format $.field=value (AND-ed). Can be empty to return all rows."), mcp.WithStringItems()),
				mcp.WithArray("or", mcp.Description("OR-group filters in format $.field=value (OR-ed together, AND-ed with filters)"), mcp.WithStringItems()),
				mcp.WithString("sort", mcp.Description("Sort field like $.last-exercised")),
				mcp.WithString("order", mcp.Description("asc or desc")),
				mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
				mcp.WithNumber("offset", mcp.Description("Offset for pagination (default 0)")),
				mcp.WithReadOnlyHintAnnotation(true),
				mcp.WithDestructiveHintAnnotation(false),
			),
			Handler: handleQueryMeta(b),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_query",
				mcp.WithDescription("Run a DQL (Dataview Query Language) query against the knowledge base. Supports TABLE, LIST, COUNT, DISTINCT queries with WHERE filters, SORT, GROUP BY, FLATTEN, and pagination. Examples: 'TABLE name, status FROM \"students/\" WHERE status = \"active\" SORT name ASC', 'COUNT WHERE tags IN (\"math\")', 'DISTINCT status'."),
				mcp.WithString("query", mcp.Required(), mcp.Description("DQL query text")),
				mcp.WithString("format", mcp.Description("Output format: table, list, json (default table)")),
				mcp.WithNumber("limit", mcp.Description("Max results (default 20)")),
				mcp.WithReadOnlyHintAnnotation(true),
				mcp.WithDestructiveHintAnnotation(false),
			),
			Handler: handleQuery(b),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_view_refresh",
				mcp.WithDescription("Force-regenerate a computed view file. A computed view is a markdown file with 'kiwi-view: true' in frontmatter — its body is auto-generated from the DQL query in 'kiwi-query'. Use this to refresh a dashboard or report view."),
				mcp.WithString("path", mcp.Required(), mcp.Description("Path to the computed view file")),
				mcp.WithReadOnlyHintAnnotation(false),
				mcp.WithDestructiveHintAnnotation(false),
				mcp.WithIdempotentHintAnnotation(true),
			),
			Handler: handleViewRefresh(b),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_delete",
				mcp.WithDescription("Delete a file from the knowledge base. The deletion is a git commit — the file's history is preserved and can be restored. Use sparingly; prefer updating content over deleting."),
				mcp.WithString("path", pathOpts...),
				mcp.WithString("actor", mcp.Description("Who is deleting")),
				mcp.WithDestructiveHintAnnotation(true),
			),
			Handler: handleDelete(b),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_bulk_write",
				mcp.WithDescription("Write multiple files in a single atomic git commit. Use this when updating related files together — e.g. writing a run record and updating the coverage strategy in the same operation. Old content is preserved in git history."),
				mcp.WithArray("files", mcp.Required(), mcp.Description("Array of {path, content} objects")),
				mcp.WithString("actor", mcp.Description("Who is writing — defaults to mcp-agent")),
				mcp.WithString("provenance", mcp.Description("Link to source, format type:id")),
				mcp.WithDestructiveHintAnnotation(true),
				mcp.WithIdempotentHintAnnotation(true),
			),
			Handler: handleBulkWrite(b),
		},
	)
}

func registerResources(s *server.MCPServer, b Backend, opts Options) {
	var schemaMu sync.Mutex
	var schemaText string
	var schemaLoaded bool

	s.AddResource(
		mcp.NewResource("kiwi://schema", "Knowledge Base Schema",
			mcp.WithResourceDescription("The knowledge base schema — instructions for how to maintain the wiki (ingest, query, lint patterns)"),
			mcp.WithMIMEType("text/markdown"),
		),
		func(ctx context.Context, _ mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
			schemaMu.Lock()
			if !schemaLoaded {
				schemaText = loadSchema(opts, b, ctx)
				if schemaText != "" {
					schemaLoaded = true
				}
			}
			text := schemaText
			schemaMu.Unlock()

			if text == "" {
				return nil, fmt.Errorf("no SCHEMA.md found")
			}
			return []mcp.ResourceContents{
				mcp.TextResourceContents{
					URI:      "kiwi://schema",
					MIMEType: "text/markdown",
					Text:     text,
				},
			}, nil
		},
	)

	s.AddResourceTemplate(
		mcp.NewResourceTemplate("kiwi://file/{path}", "Knowledge File",
			mcp.WithTemplateDescription("Read any file from the knowledge base by path"),
			mcp.WithTemplateMIMEType("text/markdown"),
		),
		func(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
			path := strings.TrimPrefix(req.Params.URI, "kiwi://file/")
			if decoded, err := url.PathUnescape(path); err == nil {
				path = decoded
			}
			content, _, err := b.ReadFile(ctx, path)
			if err != nil {
				if isNotFound(err) {
					return nil, fmt.Errorf("file not found at %s", path)
				}
				return nil, fmt.Errorf("failed to read %s: %w", path, err)
			}
			return []mcp.ResourceContents{
				mcp.TextResourceContents{
					URI:      req.Params.URI,
					MIMEType: "text/markdown",
					Text:     content,
				},
			}, nil
		},
	)

	s.AddResourceTemplate(
		mcp.NewResourceTemplate("kiwi://tree/{path}", "Knowledge Tree",
			mcp.WithTemplateDescription("List files and folders under a given path"),
			mcp.WithTemplateMIMEType("text/plain"),
		),
		func(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
			path := strings.TrimPrefix(req.Params.URI, "kiwi://tree/")
			if decoded, err := url.PathUnescape(path); err == nil {
				path = decoded
			}
			text, err := treeText(ctx, b, path, 3, "")
			if err != nil {
				return nil, err
			}
			return []mcp.ResourceContents{
				mcp.TextResourceContents{
					URI:      req.Params.URI,
					MIMEType: "text/plain",
					Text:     text,
				},
			}, nil
		},
	)
}

func loadSchema(opts Options, b Backend, ctx context.Context) string {
	if opts.Root != "" {
		data, err := os.ReadFile(filepath.Join(opts.Root, "SCHEMA.md"))
		if err != nil {
			return ""
		}
		return string(data)
	}
	content, _, err := b.ReadFile(ctx, "SCHEMA.md")
	if err != nil {
		return ""
	}
	return content
}

func handleRead(b Backend) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		path, _ := args["path"].(string)
		if path == "" {
			return mcp.NewToolResultError("path is required"), nil
		}
		content, etag, err := b.ReadFile(ctx, path)
		if err != nil {
			if isNotFound(err) {
				return mcp.NewToolResultError(fmt.Sprintf("File not found at %s. Use kiwi_tree to see available files.", path)), nil
			}
			return mcp.NewToolResultError(fmt.Sprintf("Failed to read %s: %v", path, err)), nil
		}
		if resolveLinks, _ := args["resolve_links"].(bool); resolveLinks {
			content = b.ResolveWikiLinks(ctx, content)
		}
		result := content
		if etag != "" {
			result = fmt.Sprintf("[ETag: %s]\n\n%s", etag, content)
		}
		return mcp.NewToolResultText(result), nil
	}
}

func handleWrite(b Backend) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		path, _ := args["path"].(string)
		content, _ := args["content"].(string)
		actor, _ := args["actor"].(string)
		provenance, _ := args["provenance"].(string)
		if path == "" {
			return mcp.NewToolResultError("path is required"), nil
		}
		if content == "" {
			return mcp.NewToolResultError("content is required"), nil
		}
		if actor == "" {
			actor = "mcp-agent"
		}
		etag, err := b.WriteFile(ctx, path, content, actor, provenance)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Failed to write %s: %v", path, err)), nil
		}
		return mcp.NewToolResultText(fmt.Sprintf("Written %s (ETag: %s)", path, etag)), nil
	}
}

func handleSearch(b Backend) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		query, _ := args["query"].(string)
		if query == "" {
			return mcp.NewToolResultError("query is required"), nil
		}
		limit := intArg(args, "limit", 20)
		if limit > 50 {
			limit = 50
		}
		offset := intArg(args, "offset", 0)
		prefix, _ := args["path_prefix"].(string)

		results, err := b.Search(ctx, query, limit+1, offset, prefix)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Search failed: %v", err)), nil
		}
		if len(results) == 0 {
			if prefix != "" {
				return mcp.NewToolResultText(fmt.Sprintf("No results found in %s.", prefix)), nil
			}
			return mcp.NewToolResultText("No results found."), nil
		}

		hasMore := len(results) > limit
		if hasMore {
			results = results[:limit]
		}

		var sb strings.Builder
		for i, r := range results {
			fmt.Fprintf(&sb, "%d. %s (score: %.2f)\n", i+1+offset, r.Path, r.Score)
			if r.Snippet != "" {
				fmt.Fprintf(&sb, "   %s\n", r.Snippet)
			}
			sb.WriteString("\n")
		}
		if hasMore {
			fmt.Fprintf(&sb, "Showing %d-%d. Use offset=%d to see more.\n", offset+1, offset+limit, offset+limit)
		}
		return mcp.NewToolResultText(sb.String()), nil
	}
}

func handleTree(b Backend, opts Options) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		path, _ := args["path"].(string)
		if path == "" {
			path = "/"
		}
		depth := intArg(args, "depth", 3)

		var publicURL string
		if incl, _ := args["include_permalinks"].(bool); incl {
			publicURL = b.PublicURL()
		}

		text, err := treeText(ctx, b, path, depth, publicURL)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Failed to list tree: %v", err)), nil
		}
		if text == "" {
			text = "(empty)"
		}
		return mcp.NewToolResultText(text), nil
	}
}

func treeText(ctx context.Context, b Backend, path string, depth int, publicURL string) (string, error) {
	tree, err := b.Tree(ctx, path)
	if err != nil {
		return "", err
	}
	return formatTreeJSON(tree, depth, publicURL), nil
}

type treeNode struct {
	Name     string     `json:"name"`
	Path     string     `json:"path"`
	IsDir    bool       `json:"isDir"`
	Size     int64      `json:"size"`
	Children []treeNode `json:"children"`
}

func formatTreeJSON(data json.RawMessage, depth int, publicURL string) string {
	var root struct {
		Children []treeNode `json:"children"`
	}
	if err := json.Unmarshal(data, &root); err != nil {
		return fmt.Sprintf("(error parsing tree: %v)", err)
	}
	var sb strings.Builder
	writeTreeNodes(&sb, root.Children, "", depth, publicURL)
	return sb.String()
}

func writeTreeNodes(sb *strings.Builder, nodes []treeNode, prefix string, depth int, publicURL string) {
	for _, n := range nodes {
		if n.IsDir {
			sb.WriteString(prefix + n.Name + "/\n")
			if depth > 0 {
				writeTreeNodes(sb, n.Children, prefix+"  ", depth-1, publicURL)
			}
		} else {
			line := fmt.Sprintf("%s%s (%s)", prefix, n.Name, formatSize(n.Size))
			if publicURL != "" && n.Path != "" {
				line += "  " + config.Permalink(publicURL, n.Path)
			}
			sb.WriteString(line + "\n")
		}
	}
}

func handleQueryMeta(b Backend) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()

		var filters []string
		if raw, ok := args["filters"]; ok {
			switch v := raw.(type) {
			case []any:
				for _, item := range v {
					if s, ok := item.(string); ok {
						filters = append(filters, s)
					}
				}
			case []string:
				filters = v
			}
		}

		var orFilters []string
		if raw, ok := args["or"]; ok {
			switch v := raw.(type) {
			case []any:
				for _, item := range v {
					if s, ok := item.(string); ok {
						orFilters = append(orFilters, s)
					}
				}
			case []string:
				orFilters = v
			}
		}

		sortField, _ := args["sort"].(string)
		order, _ := args["order"].(string)
		limit := intArg(args, "limit", 20)
		offset := intArg(args, "offset", 0)

		results, err := b.QueryMetaOr(ctx, filters, orFilters, sortField, order, limit+1, offset)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Query failed: %v", err)), nil
		}
		if len(results) == 0 {
			return mcp.NewToolResultText("No matching files found."), nil
		}

		hasMore := len(results) > limit
		if hasMore {
			results = results[:limit]
		}

		var sb strings.Builder
		for _, r := range results {
			fmt.Fprintf(&sb, "- %s\n", r.Path)
			if len(r.Frontmatter) > 0 {
				var fm map[string]any
				if json.Unmarshal(r.Frontmatter, &fm) == nil {
					keys := make([]string, 0, len(fm))
					for k := range fm {
						keys = append(keys, k)
					}
					sort.Strings(keys)
					for _, k := range keys {
						fmt.Fprintf(&sb, "  %s: %v\n", k, fm[k])
					}
				}
			}
		}
		if hasMore {
			fmt.Fprintf(&sb, "\nShowing %d-%d. Use offset=%d to see more.\n", offset+1, offset+limit, offset+limit)
		}
		return mcp.NewToolResultText(sb.String()), nil
	}
}

func handleViewRefresh(b Backend) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		path, _ := args["path"].(string)
		if path == "" {
			return mcp.NewToolResultError("path is required"), nil
		}
		changed, err := b.ViewRefresh(ctx, path)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("View refresh failed: %v", err)), nil
		}
		if changed {
			return mcp.NewToolResultText(fmt.Sprintf("Regenerated view %s", path)), nil
		}
		return mcp.NewToolResultText(fmt.Sprintf("View %s is already up to date", path)), nil
	}
}

func handleQuery(b Backend) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		query, _ := args["query"].(string)
		if query == "" {
			return mcp.NewToolResultError("query is required"), nil
		}
		format, _ := args["format"].(string)
		if format == "" {
			format = "table"
		}
		limit := intArg(args, "limit", 20)

		result, err := b.QueryDQL(ctx, query, limit, 0)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Query failed: %v", err)), nil
		}

		dvResult := &dataview.QueryResult{
			Columns: result.Columns,
			Rows:    result.Rows,
			Total:   result.Total,
			HasMore: result.HasMore,
		}
		for _, g := range result.Groups {
			dvResult.Groups = append(dvResult.Groups, dataview.GroupResult{Key: g.Key, Count: g.Count})
		}
		rendered := dataview.Render(dvResult, format)
		return mcp.NewToolResultText(rendered), nil
	}
}

func handleDelete(b Backend) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		path, _ := args["path"].(string)
		actor, _ := args["actor"].(string)
		if path == "" {
			return mcp.NewToolResultError("path is required"), nil
		}
		if actor == "" {
			actor = "mcp-agent"
		}
		if err := b.DeleteFile(ctx, path, actor); err != nil {
			if isNotFound(err) {
				return mcp.NewToolResultError(fmt.Sprintf("File not found at %s. Use kiwi_tree to see available files.", path)), nil
			}
			return mcp.NewToolResultError(fmt.Sprintf("Failed to delete %s: %v", path, err)), nil
		}
		return mcp.NewToolResultText(fmt.Sprintf("Deleted %s", path)), nil
	}
}

func handleBulkWrite(b Backend) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		actor, _ := args["actor"].(string)
		provenance, _ := args["provenance"].(string)
		if actor == "" {
			actor = "mcp-agent"
		}

		var files []BulkFile
		if raw, ok := args["files"]; ok {
			switch v := raw.(type) {
			case []any:
				for _, item := range v {
					if m, ok := item.(map[string]any); ok {
						p, _ := m["path"].(string)
						c, _ := m["content"].(string)
						if p != "" {
							files = append(files, BulkFile{Path: p, Content: c})
						}
					}
				}
			case []map[string]any:
				for _, m := range v {
					p, _ := m["path"].(string)
					c, _ := m["content"].(string)
					if p != "" {
						files = append(files, BulkFile{Path: p, Content: c})
					}
				}
			}
		}
		if len(files) == 0 {
			return mcp.NewToolResultError("files is required — array of {path, content} objects"), nil
		}

		etags, err := b.BulkWrite(ctx, files, actor, provenance)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Bulk write failed: %v", err)), nil
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "Written %d files in a single commit\n", len(files))
		for _, f := range files {
			if etag, ok := etags[f.Path]; ok {
				fmt.Fprintf(&sb, "  %s (ETag: %s)\n", f.Path, etag)
			} else {
				fmt.Fprintf(&sb, "  %s\n", f.Path)
			}
		}
		return mcp.NewToolResultText(sb.String()), nil
	}
}

func intArg(args map[string]any, key string, def int) int {
	v, ok := args[key]
	if !ok {
		return def
	}
	var n int
	switch raw := v.(type) {
	case float64:
		n = int(raw)
	case int:
		n = raw
	case json.Number:
		if i, err := raw.Int64(); err == nil {
			n = int(i)
		} else {
			return def
		}
	default:
		return def
	}
	if n < 0 {
		return def
	}
	return n
}

func isNotFound(err error) bool {
	var he *httpError
	if errors.As(err, &he) {
		return he.StatusCode == 404
	}
	return errors.Is(err, os.ErrNotExist)
}

func Serve(opts Options) error {
	s, backend, err := New(opts)
	if err != nil {
		return err
	}
	defer backend.Close()

	if opts.Remote != "" {
		if err := backend.Health(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "warning: KiwiFS server at %s is not reachable: %v\n", opts.Remote, err)
		}
	}

	return server.ServeStdio(s)
}
