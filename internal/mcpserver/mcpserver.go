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

	"bytes"

	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/dataview"
	"github.com/kiwifs/kiwifs/internal/exporter"
	"github.com/kiwifs/kiwifs/internal/importer"
	"github.com/kiwifs/kiwifs/internal/memory"
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
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_aggregate",
				mcp.WithDescription("Aggregate files by a frontmatter field with optional calculations. Use this for analytics like 'count by status', 'average mastery by grade', or 'sum scores by subject'. Supports count, avg, sum, min, max."),
				mcp.WithString("group_by", mcp.Required(), mcp.Description("Field to group by, e.g. status, grade, subject")),
				mcp.WithString("calculate", mcp.Description("Aggregations: count (default), avg:field, sum:field, min:field, max:field. Comma-separated for multiple.")),
				mcp.WithString("where", mcp.Description("Optional DQL WHERE filter expression")),
				mcp.WithString("path_prefix", mcp.Description("Optional path prefix to scope results, e.g. students/")),
				mcp.WithReadOnlyHintAnnotation(true),
				mcp.WithDestructiveHintAnnotation(false),
			),
			Handler: handleAggregate(b),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_import",
				mcp.WithDescription("Import data from an external source (database, CSV, JSON) into the knowledge base. Each record becomes a markdown file with frontmatter. Supports: postgres, mysql, firestore, sqlite, mongodb, csv, json, jsonl, notion, airtable."),
				mcp.WithString("from", mcp.Required(), mcp.Description(`Source type: "postgres" | "mysql" | "firestore" | "sqlite" | "mongodb" | "csv" | "json" | "jsonl" | "notion" | "airtable"`)),
				mcp.WithString("dsn", mcp.Description("Connection string (postgres, mysql)")),
				mcp.WithString("uri", mcp.Description("Connection URI (mongodb)")),
				mcp.WithString("db", mcp.Description("Database file path (sqlite)")),
				mcp.WithString("file", mcp.Description("File path (csv, json, jsonl)")),
				mcp.WithString("table", mcp.Description("Table name (postgres, mysql, sqlite)")),
				mcp.WithString("collection", mcp.Description("Collection name (firestore, mongodb)")),
				mcp.WithString("database", mcp.Description("Database name (mongodb)")),
				mcp.WithString("database_id", mcp.Description("Database ID (notion)")),
				mcp.WithString("base_id", mcp.Description("Base ID (airtable)")),
				mcp.WithString("table_id", mcp.Description("Table ID (airtable)")),
				mcp.WithString("project", mcp.Description("GCP project ID (firestore)")),
				mcp.WithString("query", mcp.Description("Custom SQL query (overrides table)")),
				mcp.WithArray("columns", mcp.Description("Optional column filter"), mcp.WithStringItems()),
				mcp.WithString("prefix", mcp.Description("Path prefix in kiwifs (default: table/collection name)")),
				mcp.WithNumber("limit", mcp.Description("Max rows to import")),
				mcp.WithBoolean("dry_run", mcp.Description("Preview mode — show what would be imported without writing")),
				mcp.WithDestructiveHintAnnotation(true),
				mcp.WithIdempotentHintAnnotation(true),
			),
			Handler: handleImport(b, opts),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_export",
				mcp.WithDescription("Export knowledge base files to JSONL or CSV format. Streams all files (or a subset) with their frontmatter, content, and link data. Optionally include vector embeddings for ML pipelines."),
				mcp.WithString("format", mcp.Required(), mcp.Description(`Output format: "jsonl" | "csv"`)),
				mcp.WithString("path", mcp.Description("Scope to a subdirectory (e.g. students/)")),
				mcp.WithArray("columns", mcp.Description("Frontmatter fields for CSV mode"), mcp.WithStringItems()),
				mcp.WithBoolean("include_content", mcp.Description("Include full markdown content")),
				mcp.WithBoolean("include_embeddings", mcp.Description("Include vector embeddings for each file's chunks")),
				mcp.WithNumber("limit", mcp.Description("Max files to export")),
				mcp.WithReadOnlyHintAnnotation(true),
				mcp.WithDestructiveHintAnnotation(false),
			),
			Handler: handleExport(b, opts),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_analytics",
				mcp.WithDescription("Get knowledge base analytics: total pages/words, health metrics (stale, orphans, broken links, empty, no frontmatter), link coverage stats, and recently updated pages."),
				mcp.WithString("scope", mcp.Description("Optional path prefix to scope results, e.g. students/")),
				mcp.WithNumber("stale_threshold", mcp.Description("Days to consider a page stale (default 30)")),
				mcp.WithReadOnlyHintAnnotation(true),
				mcp.WithDestructiveHintAnnotation(false),
			),
			Handler: handleAnalytics(b),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_memory_report",
				mcp.WithDescription("Report episodic memory coverage: lists markdown files classified as episodic and whether any page cites them under merged-from (central/semantic consolidation). Use before or after merge jobs to find episodes not yet folded into concept pages."),
				mcp.WithString("episodes_prefix", mcp.Description("Override path prefix for episodic files (default from [memory] episodes_path_prefix or episodes/)")),
				mcp.WithReadOnlyHintAnnotation(true),
				mcp.WithDestructiveHintAnnotation(false),
			),
			Handler: handleMemoryReport(b),
		},
		server.ServerTool{
			Tool: mcp.NewTool("kiwi_health_check",
				mcp.WithDescription("Get health information for a specific page: word count, link count, backlink count, days since update, quality score, and any issues (stale, orphan, broken links)."),
				mcp.WithString("path", mcp.Required(), mcp.Description("Path to the page to check, e.g. students/priya-sharma.md")),
				mcp.WithReadOnlyHintAnnotation(true),
				mcp.WithDestructiveHintAnnotation(false),
			),
			Handler: handleHealthCheck(b),
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

func handleAggregate(b Backend) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		groupBy, _ := args["group_by"].(string)
		if groupBy == "" {
			return mcp.NewToolResultError("group_by is required"), nil
		}
		calc, _ := args["calculate"].(string)
		where, _ := args["where"].(string)
		pathPrefix, _ := args["path_prefix"].(string)

		results, err := b.Aggregate(ctx, groupBy, calc, where, pathPrefix)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Aggregate failed: %v", err)), nil
		}
		if len(results) == 0 {
			return mcp.NewToolResultText("No results."), nil
		}

		var sb strings.Builder
		// Sort keys for deterministic output
		keys := make([]string, 0, len(results))
		for k := range results {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			vals := results[k]
			fmt.Fprintf(&sb, "%s:", k)
			vkeys := make([]string, 0, len(vals))
			for vk := range vals {
				vkeys = append(vkeys, vk)
			}
			sort.Strings(vkeys)
			for _, vk := range vkeys {
				fmt.Fprintf(&sb, " %s=%v", vk, vals[vk])
			}
			sb.WriteString("\n")
		}
		return mcp.NewToolResultText(sb.String()), nil
	}
}

func handleMemoryReport(b Backend) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		prefix, _ := args["episodes_prefix"].(string)

		raw, err := b.MemoryReport(ctx, prefix)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Memory report failed: %v", err)), nil
		}

		var rep memory.Report
		if err := json.Unmarshal(raw, &rep); err != nil {
			return mcp.NewToolResultText(string(raw)), nil
		}

		var sb strings.Builder
		fmt.Fprintf(&sb, "Episodic files:           %d\n", rep.EpisodicCount)
		fmt.Fprintf(&sb, "merged-from references:   %d\n", rep.MergedFromRefs)
		fmt.Fprintf(&sb, "Unmerged (no merged-from): %d\n", len(rep.Unmerged))
		for _, u := range rep.Unmerged {
			fmt.Fprintf(&sb, "  - %s", u.Path)
			if u.EpisodeID != "" {
				fmt.Fprintf(&sb, "  episode_id=%s", u.EpisodeID)
			}
			sb.WriteString("\n")
		}
		for _, w := range rep.Warnings {
			fmt.Fprintf(&sb, "warning: %s\n", w)
		}
		if len(rep.Unmerged) == 0 {
			sb.WriteString("All episodic files are referenced by at least one merged-from list.\n")
		}
		return mcp.NewToolResultText(sb.String()), nil
	}
}

func handleAnalytics(b Backend) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		scope, _ := args["scope"].(string)
		staleThreshold := intArg(args, "stale_threshold", 30)

		raw, err := b.Analytics(ctx, scope, staleThreshold)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Analytics failed: %v", err)), nil
		}

		var data struct {
			TotalPages int `json:"total_pages"`
			TotalWords int `json:"total_words"`
			Health     struct {
				Stale         struct{ Count int } `json:"stale"`
				Orphans       struct{ Count int } `json:"orphans"`
				BrokenLinks   struct{ Count int } `json:"broken_links"`
				Empty         struct{ Count int } `json:"empty"`
				NoFrontmatter struct{ Count int } `json:"no_frontmatter"`
			} `json:"health"`
			Coverage struct {
				PagesWithLinks    int     `json:"pages_with_links"`
				PagesWithoutLinks int     `json:"pages_without_links"`
				AvgLinksPerPage   float64 `json:"avg_links_per_page"`
			} `json:"coverage"`
			TopUpdated []struct {
				Path      string `json:"path"`
				UpdatedAt string `json:"updated_at"`
			} `json:"top_updated"`
		}
		if err := json.Unmarshal(raw, &data); err != nil {
			return mcp.NewToolResultText(string(raw)), nil
		}

		var sb strings.Builder
		fmt.Fprintf(&sb, "Knowledge Base Health\n")
		fmt.Fprintf(&sb, "Total pages:     %d\n", data.TotalPages)
		fmt.Fprintf(&sb, "Total words:     %d\n", data.TotalWords)
		fmt.Fprintf(&sb, "Stale (>%dd):    %d pages\n", staleThreshold, data.Health.Stale.Count)
		fmt.Fprintf(&sb, "Orphans:         %d pages\n", data.Health.Orphans.Count)
		fmt.Fprintf(&sb, "Broken links:    %d\n", data.Health.BrokenLinks.Count)
		fmt.Fprintf(&sb, "Empty pages:     %d\n", data.Health.Empty.Count)
		fmt.Fprintf(&sb, "No frontmatter:  %d\n", data.Health.NoFrontmatter.Count)
		sb.WriteString("\nCoverage\n")
		total := data.Coverage.PagesWithLinks + data.Coverage.PagesWithoutLinks
		pct := 0.0
		if total > 0 {
			pct = float64(data.Coverage.PagesWithLinks) / float64(total) * 100
		}
		fmt.Fprintf(&sb, "Pages with links:    %d (%.1f%%)\n", data.Coverage.PagesWithLinks, pct)
		fmt.Fprintf(&sb, "Avg links/page:      %.1f\n", data.Coverage.AvgLinksPerPage)
		if len(data.TopUpdated) > 0 {
			sb.WriteString("\nRecently Updated\n")
			for _, p := range data.TopUpdated {
				fmt.Fprintf(&sb, "  %s  %s\n", p.Path, p.UpdatedAt)
			}
		}
		return mcp.NewToolResultText(sb.String()), nil
	}
}

func handleHealthCheck(b Backend) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		path, _ := args["path"].(string)
		if path == "" {
			return mcp.NewToolResultError("path is required"), nil
		}

		raw, err := b.HealthCheckPage(ctx, path)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Health check failed: %v", err)), nil
		}

		var data struct {
			Path            string   `json:"path"`
			WordCount       int      `json:"word_count"`
			LinkCount       int      `json:"link_count"`
			BacklinkCount   int      `json:"backlink_count"`
			DaysSinceUpdate float64  `json:"days_since_update"`
			QualityScore    *float64 `json:"quality_score,omitempty"`
			Issues          []string `json:"issues"`
		}
		if err := json.Unmarshal(raw, &data); err != nil {
			return mcp.NewToolResultText(string(raw)), nil
		}

		var sb strings.Builder
		fmt.Fprintf(&sb, "Health: %s\n", data.Path)
		fmt.Fprintf(&sb, "  Word count:       %d\n", data.WordCount)
		fmt.Fprintf(&sb, "  Link count:       %d\n", data.LinkCount)
		fmt.Fprintf(&sb, "  Backlink count:   %d\n", data.BacklinkCount)
		fmt.Fprintf(&sb, "  Days since update: %.1f\n", data.DaysSinceUpdate)
		if data.QualityScore != nil {
			fmt.Fprintf(&sb, "  Quality score:    %.2f\n", *data.QualityScore)
		}
		if len(data.Issues) > 0 {
			sb.WriteString("  Issues:\n")
			for _, issue := range data.Issues {
				fmt.Fprintf(&sb, "    - %s\n", issue)
			}
		} else {
			sb.WriteString("  Issues: none\n")
		}
		return mcp.NewToolResultText(sb.String()), nil
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

func handleImport(b Backend, opts Options) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		from, _ := args["from"].(string)
		if from == "" {
			return mcp.NewToolResultError("from is required"), nil
		}

		lb, ok := b.(*LocalBackend)
		if !ok {
			return mcp.NewToolResultError("import is only supported in local mode"), nil
		}
		if err := lb.init(); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("init: %v", err)), nil
		}

		src, err := buildMCPSource(args, from)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		defer src.Close()

		var columns []string
		if raw, ok := args["columns"]; ok {
			switch v := raw.(type) {
			case []any:
				for _, item := range v {
					if s, ok := item.(string); ok {
						columns = append(columns, s)
					}
				}
			case []string:
				columns = v
			}
		}

		prefix, _ := args["prefix"].(string)
		dryRun, _ := args["dry_run"].(bool)
		limit := intArg(args, "limit", 0)

		importOpts := importer.Options{
			Prefix:  prefix,
			Columns: columns,
			DryRun:  dryRun,
			Limit:   limit,
			Actor:   "mcp-import",
		}

		stats, err := importer.Run(ctx, src, lb.stack.Pipeline, importOpts)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Import failed: %v", err)), nil
		}

		var sb strings.Builder
		if dryRun {
			fmt.Fprintf(&sb, "Dry run: would import %d records\n", stats.Imported)
		} else {
			fmt.Fprintf(&sb, "Imported %d records, skipped %d\n", stats.Imported, stats.Skipped)
		}
		if len(stats.Errors) > 0 {
			fmt.Fprintf(&sb, "Errors (%d):\n", len(stats.Errors))
			for _, e := range stats.Errors {
				fmt.Fprintf(&sb, "  - %s\n", e)
			}
		}
		return mcp.NewToolResultText(sb.String()), nil
	}
}

func buildMCPSource(args map[string]any, from string) (importer.Source, error) {
	str := func(key string) string {
		s, _ := args[key].(string)
		return s
	}

	switch from {
	case "postgres":
		dsn := str("dsn")
		table := str("table")
		query := str("query")
		if dsn == "" {
			return nil, fmt.Errorf("dsn is required for postgres")
		}
		if table == "" && query == "" {
			return nil, fmt.Errorf("table or query is required for postgres")
		}
		return importer.NewPostgres(dsn, table, query, nil)
	case "mysql":
		dsn := str("dsn")
		table := str("table")
		query := str("query")
		if dsn == "" {
			return nil, fmt.Errorf("dsn is required for mysql")
		}
		if table == "" && query == "" {
			return nil, fmt.Errorf("table or query is required for mysql")
		}
		return importer.NewMySQL(dsn, table, query, nil)
	case "firestore":
		project := str("project")
		collection := str("collection")
		if project == "" {
			return nil, fmt.Errorf("project is required for firestore")
		}
		if collection == "" {
			return nil, fmt.Errorf("collection is required for firestore")
		}
		return importer.NewFirestore(project, collection)
	case "sqlite":
		dbPath := str("db")
		table := str("table")
		query := str("query")
		if dbPath == "" {
			return nil, fmt.Errorf("db is required for sqlite")
		}
		if table == "" && query == "" {
			return nil, fmt.Errorf("table or query is required for sqlite")
		}
		return importer.NewSQLiteSource(dbPath, table, query)
	case "mongodb":
		uri := str("uri")
		if uri == "" {
			uri = str("dsn")
		}
		database := str("database")
		collection := str("collection")
		if uri == "" {
			return nil, fmt.Errorf("uri is required for mongodb")
		}
		if database == "" {
			return nil, fmt.Errorf("database is required for mongodb")
		}
		if collection == "" {
			return nil, fmt.Errorf("collection is required for mongodb")
		}
		return importer.NewMongoDB(uri, database, collection)
	case "csv":
		filePath := str("file")
		if filePath == "" {
			return nil, fmt.Errorf("file is required for csv")
		}
		return importer.NewCSV(filePath, true)
	case "json", "jsonl":
		filePath := str("file")
		if filePath == "" {
			return nil, fmt.Errorf("file is required for json/jsonl")
		}
		return importer.NewJSON(filePath)
	case "notion":
		apiKey := os.Getenv("NOTION_API_KEY")
		databaseID := str("database_id")
		if databaseID == "" {
			return nil, fmt.Errorf("database_id is required for notion")
		}
		return importer.NewNotion(apiKey, databaseID)
	case "airtable":
		apiKey := os.Getenv("AIRTABLE_API_KEY")
		baseID := str("base_id")
		tableID := str("table_id")
		if baseID == "" {
			return nil, fmt.Errorf("base_id is required for airtable")
		}
		if tableID == "" {
			return nil, fmt.Errorf("table_id is required for airtable")
		}
		return importer.NewAirtable(apiKey, baseID, tableID)
	default:
		return nil, fmt.Errorf("unknown source type %q", from)
	}
}

func handleExport(b Backend, _ Options) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		format, _ := args["format"].(string)
		if format == "" {
			format = "jsonl"
		}
		if format != "jsonl" && format != "csv" {
			return mcp.NewToolResultError("format must be jsonl or csv"), nil
		}

		lb, ok := b.(*LocalBackend)
		if !ok {
			return mcp.NewToolResultError("export is only supported in local mode"), nil
		}
		if err := lb.init(); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("init: %v", err)), nil
		}

		var columns []string
		if raw, ok := args["columns"]; ok {
			switch v := raw.(type) {
			case []any:
				for _, item := range v {
					if s, ok := item.(string); ok {
						columns = append(columns, s)
					}
				}
			case []string:
				columns = v
			}
		}

		pathPrefix, _ := args["path"].(string)
		includeContent, _ := args["include_content"].(bool)
		includeEmb, _ := args["include_embeddings"].(bool)
		limit := intArg(args, "limit", 0)

		var buf bytes.Buffer
		opts := exporter.Options{
			Format:            format,
			PathPrefix:        pathPrefix,
			Columns:           columns,
			IncludeContent:    includeContent,
			IncludeEmbeddings: includeEmb,
			Output:            &buf,
			Limit:             limit,
		}

		count, err := exporter.Export(ctx, lb.stack.Store, lb.stack.Searcher, lb.stack.Vectors, opts)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Export failed: %v", err)), nil
		}

		var sb strings.Builder
		fmt.Fprintf(&sb, "Exported %d files (%s format)\n\n", count, format)
		sb.Write(buf.Bytes())
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
