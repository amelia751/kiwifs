package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kiwifs/kiwifs/internal/comments"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/markdown"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/vectorstore"
	"github.com/kiwifs/kiwifs/internal/versioning"
	"github.com/labstack/echo/v4"
	"golang.org/x/sync/singleflight"
)

// sseHeartbeat is the interval between ":keep-alive" SSE comments.
// Overridden in tests to keep assertions fast.
var sseHeartbeat = 15 * time.Second

// Handlers holds dependencies for all route handlers.
type Handlers struct {
	store     storage.Storage
	versioner versioning.Versioner
	searcher  search.Searcher
	linker    links.Linker
	hub       *events.Hub
	pipe      *pipeline.Pipeline
	vectors   *vectorstore.Service // nil when vector search is disabled
	comments  *comments.Store
	assets    config.AssetsConfig
	root      string

	// graphCache stores the last-computed /graph response; pipeline
	// invalidation callbacks nil it out on every write. atomic.Pointer is
	// enough here — we never mutate the response in place.
	graphCache atomic.Pointer[graphResponse]
	// graphGroup deduplicates concurrent misses: if ten clients hit /graph
	// during a cache rebuild, only one computes it and the others share
	// the result. Avoids the "cold start thundering herd" that used to
	// drive N parallel full-tree walks.
	graphGroup singleflight.Group
}

// invalidateGraphCache drops the cached /graph response. Wired to the
// pipeline's OnInvalidate hook so any write — REST, bulk, NFS, S3,
// WebDAV, fsnotify — automatically refreshes the cache on the next read.
func (h *Handlers) invalidateGraphCache() {
	h.graphCache.Store(nil)
}

// Health godoc
func (h *Handlers) Health(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

// ─── Tree ────────────────────────────────────────────────────────────────────

type treeEntry struct {
	Path     string       `json:"path"`
	Name     string       `json:"name"`
	IsDir    bool         `json:"isDir"`
	Size     int64        `json:"size,omitempty"`
	Children []*treeEntry `json:"children,omitempty"`
}

// maxTreeDepth caps buildTree recursion so a deeply nested folder — or
// a symlink loop pointing into itself — can't blow up the server with a
// stack overflow. 20 levels is well beyond any realistic knowledge base
// and still stops a pathological tree from crashing the server.
const maxTreeDepth = 20

// Tree returns the directory tree as JSON.
// GET /api/kiwi/tree?path=concepts/
func (h *Handlers) Tree(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		path = "/"
	}
	tree, err := h.buildTree(c.Request().Context(), path, maxTreeDepth)
	if err != nil {
		if os.IsNotExist(err) {
			return echo.NewHTTPError(http.StatusNotFound, "path not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, tree)
}

func (h *Handlers) buildTree(ctx context.Context, path string, depth int) (*treeEntry, error) {
	entries, err := h.store.List(ctx, path)
	if err != nil {
		return nil, err
	}

	cleanPath := strings.Trim(path, "/")
	displayName := filepath.Base(cleanPath)
	if cleanPath == "" {
		displayName = "/"
	}
	root := &treeEntry{
		Path:  cleanPath,
		Name:  displayName,
		IsDir: true,
	}

	for _, e := range entries {
		child := &treeEntry{
			Path:  e.Path,
			Name:  e.Name,
			IsDir: e.IsDir,
			Size:  e.Size,
		}
		if e.IsDir && depth > 0 {
			sub, err := h.buildTree(ctx, e.Path, depth-1)
			if err == nil {
				child.Children = sub.Children
			}
		}
		root.Children = append(root.Children, child)
	}
	return root, nil
}

// ─── File Read ───────────────────────────────────────────────────────────────

// ReadFile returns the raw markdown content of a file.
// GET /api/kiwi/file?path=concepts/authentication.md
//
// Sets ETag and Last-Modified for conditional-GET caching. Honours both
// If-None-Match (ETag) and If-Modified-Since, returning 304 when the
// client's cached copy is still current.
func (h *Handlers) ReadFile(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}

	content, err := h.store.Read(c.Request().Context(), path)
	if err != nil {
		if os.IsNotExist(err) {
			return echo.NewHTTPError(http.StatusNotFound, "file not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	etag := fmt.Sprintf(`"%s"`, pipeline.ETag(content))
	c.Response().Header().Set("ETag", etag)
	c.Response().Header().Set("Cache-Control", "no-cache")

	// Add Last-Modified when we can stat the file. Best-effort: a missing
	// mtime shouldn't fail the read.
	var modTime time.Time
	if info, serr := h.store.Stat(c.Request().Context(), path); serr == nil {
		modTime = info.ModTime.UTC()
		c.Response().Header().Set("Last-Modified", modTime.Format(http.TimeFormat))
	}

	// Conditional GET: If-None-Match (ETag) wins over If-Modified-Since
	// per RFC 7232 §6 — if both are present and ETag matches, we return
	// 304 without consulting the date.
	if match := c.Request().Header.Get("If-None-Match"); match != "" && match == etag {
		return c.NoContent(http.StatusNotModified)
	}
	if !modTime.IsZero() {
		if ims := c.Request().Header.Get("If-Modified-Since"); ims != "" {
			if t, perr := http.ParseTime(ims); perr == nil {
				// Truncate the server-side mtime to whole seconds — HTTP
				// dates have second resolution, so a sub-second drift
				// would otherwise force 200s on unchanged files.
				if !modTime.Truncate(time.Second).After(t) {
					return c.NoContent(http.StatusNotModified)
				}
			}
		}
	}

	return c.Blob(http.StatusOK, detectContentType(path, content), content)
}

// detectContentType picks a Content-Type for ReadFile. Markdown keeps the
// charset-tagged text/markdown it's always served with so existing clients
// round-trip unchanged; everything else falls through to an extension lookup
// and then a content-sniffing fallback, matching nginx's default behaviour.
func detectContentType(path string, content []byte) string {
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".md" || ext == ".markdown" {
		return "text/markdown; charset=utf-8"
	}
	if ct := mime.TypeByExtension(ext); ct != "" {
		return ct
	}
	if ct := http.DetectContentType(content); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

// ─── File Write ──────────────────────────────────────────────────────────────

// WriteFile writes (create or update) a file.
// PUT /api/kiwi/file?path=concepts/authentication.md
// Header: If-Match: "etag" (optional, for optimistic locking)
// Header: X-Actor: agent-name (optional, for git attribution)
// Body: raw markdown string
func (h *Handlers) WriteFile(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}

	// Optimistic locking: parse If-Match and forward to the pipeline so the
	// check happens inside writeMu — checking it here would race against
	// another writer between read and Pipeline.Write.
	ifMatch := strings.Trim(c.Request().Header.Get("If-Match"), `"`)

	// Defence in depth: even if someone disables the global BodyLimit
	// middleware, the per-file PUT can't blow past 32 MB.
	const maxFileBody = 32 << 20
	body, err := io.ReadAll(io.LimitReader(c.Request().Body, maxFileBody+1))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to read body")
	}
	if len(body) > maxFileBody {
		return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "file exceeds 32 MB limit")
	}

	actor := c.Request().Header.Get("X-Actor")
	// Provenance injection runs before the pipeline write so the rewritten
	// frontmatter is the content that gets indexed, committed, and served
	// back on subsequent reads — there's no second write needed to record
	// the entry.
	if provType, provID, ok := pipeline.ParseProvenanceHeader(c.Request().Header.Get("X-Provenance")); ok {
		injected, perr := pipeline.InjectProvenance(body, provType, provID, actor)
		if perr != nil {
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("provenance: %v", perr))
		}
		body = injected
	}

	res, err := h.pipe.WriteWithOpts(c.Request().Context(), path, body, actor, pipeline.WriteOpts{IfMatch: ifMatch})
	if err != nil {
		if errors.Is(err, pipeline.ErrConflict) {
			return echo.NewHTTPError(http.StatusConflict, "file modified since last read — re-fetch and retry")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	c.Response().Header().Set("ETag", fmt.Sprintf(`"%s"`, res.ETag))
	return c.JSON(http.StatusOK, map[string]string{
		"path": res.Path,
		"etag": res.ETag,
	})
}

// ─── Table of Contents ───────────────────────────────────────────────────────

type tocResponse struct {
	Path     string              `json:"path"`
	Headings []markdown.Heading  `json:"headings"`
}

// ToC returns the heading outline of a markdown file. Non-browser clients
// (CLI, agents) get the same document structure the UI's sidebar renders.
// GET /api/kiwi/toc?path=concepts/authentication.md
func (h *Handlers) ToC(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}
	content, err := h.store.Read(c.Request().Context(), path)
	if err != nil {
		if os.IsNotExist(err) {
			return echo.NewHTTPError(http.StatusNotFound, "file not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	headings := markdown.Headings(content)
	if headings == nil {
		headings = []markdown.Heading{}
	}
	return c.JSON(http.StatusOK, tocResponse{Path: path, Headings: headings})
}

// ─── Bulk Write ──────────────────────────────────────────────────────────────

type bulkFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

type bulkRequest struct {
	Files   []bulkFile `json:"files"`
	Actor   string     `json:"actor,omitempty"`
	Message string     `json:"message,omitempty"`
}

type bulkResult struct {
	Path string `json:"path"`
	ETag string `json:"etag"`
}

type bulkResponse struct {
	Count int          `json:"count"`
	Files []bulkResult `json:"files"`
}

// BulkWrite writes multiple files atomically: one git commit covers them all,
// so a multi-file agent run produces one history entry instead of N.
// POST /api/kiwi/bulk
// Body: { "files": [{"path", "content"}, ...], "actor": "...", "message": "..." }
func (h *Handlers) BulkWrite(c echo.Context) error {
	var req bulkRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid JSON body")
	}
	if len(req.Files) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "files is required and must be non-empty")
	}

	actor := req.Actor
	if actor == "" {
		actor = c.Request().Header.Get("X-Actor")
	}

	// Provenance: a single X-Provenance header on the bulk request applies
	// to every file in the batch. Agents that stamp one "run" across many
	// outputs get the right record without repeating themselves per file.
	provType, provID, hasProv := pipeline.ParseProvenanceHeader(c.Request().Header.Get("X-Provenance"))

	files := make([]struct {
		Path    string
		Content []byte
	}, len(req.Files))
	for i, f := range req.Files {
		files[i].Path = f.Path
		content := []byte(f.Content)
		if hasProv {
			injected, perr := pipeline.InjectProvenance(content, provType, provID, actor)
			if perr != nil {
				return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("provenance on %s: %v", f.Path, perr))
			}
			content = injected
		}
		files[i].Content = content
	}
	pipeResults, err := h.pipe.BulkWrite(c.Request().Context(), files, actor, req.Message)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	results := make([]bulkResult, len(pipeResults))
	for i, r := range pipeResults {
		results[i] = bulkResult{Path: r.Path, ETag: r.ETag}
	}
	return c.JSON(http.StatusOK, bulkResponse{Count: len(results), Files: results})
}

// ─── Asset Upload ────────────────────────────────────────────────────────────

// defaultMaxAssetSize caps individual uploads when config omits max_file_size.
// Sits well under the 32 MB body cap so the server can reject oversize files
// before buffering them end-to-end.
const defaultMaxAssetSize = 10 << 20 // 10 MiB

// defaultAllowedAssetTypes is the MIME allowlist when [assets].allowed_types
// is unset — the common image formats plus PDF, which is what the BlockNote
// paste-and-embed flow produces. Anything richer (video, audio, archives)
// is opt-in via explicit config so a default install doesn't silently
// become a generic file host.
var defaultAllowedAssetTypes = []string{
	"image/png",
	"image/jpeg",
	"image/gif",
	"image/webp",
	"image/svg+xml",
	"application/pdf",
}

type uploadResponse struct {
	Path        string `json:"path"`
	Markdown    string `json:"markdown"`
	ContentType string `json:"contentType"`
	Size        int64  `json:"size"`
	ETag        string `json:"etag"`
}

// UploadAsset stores a binary upload as a regular file inside the knowledge
// tree and returns the markdown embed snippet for it. The file flows through
// the same pipeline as markdown writes so it lands in git history and triggers
// the usual SSE broadcast — callers watching /events see a "write" event with
// the asset's path.
//
// POST /api/kiwi/assets?path=<directory>
// Body: multipart/form-data with a "file" field.
func (h *Handlers) UploadAsset(c echo.Context) error {
	dir := strings.TrimSpace(c.QueryParam("path"))
	dir = strings.Trim(dir, "/")
	if strings.Contains(dir, "..") {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid path")
	}

	file, err := c.FormFile("file")
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "file field is required")
	}

	maxSize := h.assetMaxSize()
	if file.Size > maxSize {
		return echo.NewHTTPError(http.StatusRequestEntityTooLarge,
			fmt.Sprintf("file exceeds %d-byte limit", maxSize))
	}

	// filepath.Base strips any traversal segments (..\, ../, C:\…) that a
	// browser upload could smuggle through the filename — browsers usually
	// send a bare basename, but we don't trust them to.
	name := filepath.Base(file.Filename)
	if name == "." || name == "/" || name == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid filename")
	}
	if strings.HasPrefix(name, ".") {
		return echo.NewHTTPError(http.StatusBadRequest, "hidden filenames are not allowed")
	}

	src, err := file.Open()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to open upload")
	}
	defer src.Close()
	// +1 so a file exactly at the limit still fits but one byte over trips
	// the guard below.
	content, err := io.ReadAll(io.LimitReader(src, maxSize+1))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to read upload")
	}
	if int64(len(content)) > maxSize {
		return echo.NewHTTPError(http.StatusRequestEntityTooLarge,
			fmt.Sprintf("file exceeds %d-byte limit", maxSize))
	}

	// Content type: trust the sniffed bytes over the client-declared header.
	// Browsers happily send `application/octet-stream` for formats they
	// don't recognise, and the header is attacker-controlled anyway.
	ct := detectContentType(name, content)
	ct = strings.SplitN(ct, ";", 2)[0]
	ct = strings.TrimSpace(ct)
	if !h.assetAllowed(ct) {
		return echo.NewHTTPError(http.StatusUnsupportedMediaType,
			fmt.Sprintf("content type %q is not in the allowlist", ct))
	}

	fullPath := name
	if dir != "" {
		fullPath = dir + "/" + name
	}
	actor := c.Request().Header.Get("X-Actor")
	res, err := h.pipe.Write(c.Request().Context(), fullPath, content, actor)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, uploadResponse{
		Path:        res.Path,
		Markdown:    assetMarkdown(res.Path, name, ct),
		ContentType: ct,
		Size:        int64(len(content)),
		ETag:        res.ETag,
	})
}

// assetMaxSize resolves the configured max upload size, falling back to the
// package default. Config parse errors fall back too — a bad humanised
// value shouldn't take the whole endpoint down.
func (h *Handlers) assetMaxSize() int64 {
	if h.assets.MaxFileSize == "" {
		return defaultMaxAssetSize
	}
	n, err := parseSize(h.assets.MaxFileSize)
	if err != nil || n <= 0 {
		return defaultMaxAssetSize
	}
	return n
}

// assetAllowed reports whether ct is on the configured allowlist (or the
// default allowlist when none is configured). Comparison is case-insensitive
// because MIME types aren't — `IMAGE/PNG` and `image/png` mean the same thing.
func (h *Handlers) assetAllowed(ct string) bool {
	allowed := h.assets.AllowedTypes
	if len(allowed) == 0 {
		allowed = defaultAllowedAssetTypes
	}
	ct = strings.ToLower(ct)
	for _, a := range allowed {
		if strings.EqualFold(a, ct) {
			return true
		}
	}
	return false
}

// assetMarkdown picks the right embed syntax for the content type: images
// render inline via `![alt](url)`, everything else becomes a plain link so
// the editor can still surface PDFs and other downloads without a broken
// image icon.
func assetMarkdown(path, name, ct string) string {
	alt := strings.TrimSuffix(name, filepath.Ext(name))
	if alt == "" {
		alt = name
	}
	url := "/" + path
	if strings.HasPrefix(ct, "image/") {
		return fmt.Sprintf("![%s](%s)", alt, url)
	}
	return fmt.Sprintf("[%s](%s)", alt, url)
}

// parseSize turns "10MB" / "512KB" / "1GiB" / a raw byte count into an int64.
// Purposefully tiny — we don't need humanize's full parser, and keeping this
// local means config.toml accepts the same shorthand in both decimal (MB/GB)
// and binary (MiB/GiB) variants without a new dependency.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	// Split trailing unit suffix off the numeric head.
	i := 0
	for i < len(s) && (s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	numStr := s[:i]
	unit := strings.ToLower(strings.TrimSpace(s[i:]))
	if numStr == "" {
		return 0, fmt.Errorf("missing number in %q", s)
	}
	n, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q", numStr)
	}
	var mul float64
	switch unit {
	case "", "b":
		mul = 1
	case "k", "kb":
		mul = 1000
	case "kib":
		mul = 1024
	case "m", "mb":
		mul = 1000 * 1000
	case "mib":
		mul = 1024 * 1024
	case "g", "gb":
		mul = 1000 * 1000 * 1000
	case "gib":
		mul = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown unit %q", unit)
	}
	return int64(n * mul), nil
}

// ─── File Delete ─────────────────────────────────────────────────────────────

// DeleteFile deletes a file.
// DELETE /api/kiwi/file?path=concepts/authentication.md
func (h *Handlers) DeleteFile(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}
	if !h.store.Exists(c.Request().Context(), path) {
		return echo.NewHTTPError(http.StatusNotFound, "file not found")
	}

	if err := h.pipe.Delete(c.Request().Context(), path, c.Request().Header.Get("X-Actor")); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, map[string]string{"deleted": path})
}

// ─── Search ──────────────────────────────────────────────────────────────────

type searchResponse struct {
	Query   string          `json:"query"`
	Limit   int             `json:"limit"`
	Offset  int             `json:"offset"`
	Results []search.Result `json:"results"`
}

// Search performs a full-text search across all .md files. Supports
// pagination via ?limit= (default 50, max 200) and ?offset= (default 0).
// GET /api/kiwi/search?q=WebSocket+timeout&limit=20&offset=40
func (h *Handlers) Search(c echo.Context) error {
	q := c.QueryParam("q")
	if q == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "q is required")
	}
	limit := search.NormalizeLimit(parseIntParam(c, "limit", 0))
	offset := search.NormalizeOffset(parseIntParam(c, "offset", 0))
	results, err := h.searcher.Search(c.Request().Context(), q, limit, offset)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if results == nil {
		results = []search.Result{}
	}
	return c.JSON(http.StatusOK, searchResponse{
		Query:   q,
		Limit:   limit,
		Offset:  offset,
		Results: results,
	})
}

// parseIntParam returns the named query param as an int, falling back to
// fallback on missing or malformed values. Kept tiny — errors are ignored
// deliberately because the caller clamps the result anyway.
func parseIntParam(c echo.Context, name string, fallback int) int {
	raw := c.QueryParam(name)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}

// ─── Semantic Search ─────────────────────────────────────────────────────────

type semanticRequest struct {
	Query  string `json:"query"`
	TopK   int    `json:"topK"`
	Offset int    `json:"offset"`
}

type semanticResponse struct {
	Query   string               `json:"query"`
	TopK    int                  `json:"topK"`
	Offset  int                  `json:"offset"`
	Results []vectorstore.Result `json:"results"`
}

// SemanticSearch runs a vector search over the knowledge base. Supports
// pagination via topK + offset — for vector stores the natural "page size"
// is the top-K returned by the ANN index; offset then skips the first N
// of those before returning the requested page.
// POST /api/kiwi/search/semantic
// Body: {"query": "...", "topK": 10, "offset": 0}
// 503 when vector search isn't configured (config.search.vector.enabled = false).
func (h *Handlers) SemanticSearch(c echo.Context) error {
	if h.vectors == nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "semantic search is not enabled")
	}
	// Accept the body both via POST JSON and via ?q= for quick curl testing.
	req := semanticRequest{}
	if c.Request().Method == http.MethodPost {
		_ = c.Bind(&req)
	}
	if req.Query == "" {
		req.Query = c.QueryParam("q")
	}
	if req.TopK == 0 {
		req.TopK = parseIntParam(c, "topK", 0)
	}
	if req.Offset == 0 {
		req.Offset = parseIntParam(c, "offset", 0)
	}
	if req.Query == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "query is required")
	}
	topK := req.TopK
	if topK <= 0 {
		topK = vectorstore.DefaultTopK
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	// Over-fetch by `offset` so we can slice client-side — ANN backends
	// don't natively support offset, but the hit rate drops quickly past
	// the first page so the extra cost is small.
	results, err := h.vectors.Search(c.Request().Context(), req.Query, topK+offset)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if offset >= len(results) {
		results = nil
	} else {
		results = results[offset:]
	}
	if len(results) > topK {
		results = results[:topK]
	}
	if results == nil {
		results = []vectorstore.Result{}
	}
	return c.JSON(http.StatusOK, semanticResponse{
		Query:   req.Query,
		TopK:    topK,
		Offset:  offset,
		Results: results,
	})
}

// ─── Metadata Query ──────────────────────────────────────────────────────────

// metaQuerier is the narrow interface handlers.Meta uses to talk to the
// searcher. Keeping it here (not in the search package) documents the one
// call the API layer actually needs and avoids widening the Searcher
// interface for engines that don't support structured metadata (grep).
type metaQuerier interface {
	QueryMeta(ctx context.Context, filters []search.MetaFilter, sort, order string, limit, offset int) ([]search.MetaResult, error)
}

type metaResponse struct {
	Count   int                  `json:"count"`
	Limit   int                  `json:"limit"`
	Offset  int                  `json:"offset"`
	Results []search.MetaResult  `json:"results"`
}

// Meta runs a structured query against the file_meta index. Useful for
// questions the FTS index can't answer efficiently ("show every published
// page with priority=high, sorted by last-exercised desc").
//
// GET /api/kiwi/meta
//
// Query params:
//
//	where=$.status=published          — repeat for AND
//	where=$.priority!=low
//	where=$.derived-from[*].id=run-249 — array predicate via json_each
//	sort=$.last-exercised              — any $. path
//	order=desc                         — asc (default) | desc
//	limit=20                           — clamped to [1, 200]
//	offset=40
//
// Returns 501 Not Implemented when the active search backend (grep) has no
// metadata index — clients can fall back to walking /tree instead.
func (h *Handlers) Meta(c echo.Context) error {
	mq, ok := h.searcher.(metaQuerier)
	if !ok {
		return echo.NewHTTPError(http.StatusNotImplemented, "metadata index requires sqlite search backend")
	}

	// Accept multiple ?where= clauses. Each is "<field><op><value>".
	// We split on the FIRST operator match so values containing "=", "<",
	// etc. don't mis-parse.
	var filters []search.MetaFilter
	for _, raw := range c.QueryParams()["where"] {
		f, err := parseMetaWhere(raw)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		filters = append(filters, f)
	}

	sort := c.QueryParam("sort")
	order := c.QueryParam("order")
	limit := parseIntParam(c, "limit", 0)
	offset := parseIntParam(c, "offset", 0)

	results, err := mq.QueryMeta(c.Request().Context(), filters, sort, order, limit, offset)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	return c.JSON(http.StatusOK, metaResponse{
		Count:   len(results),
		Limit:   search.NormalizeLimit(limit),
		Offset:  search.NormalizeOffset(offset),
		Results: results,
	})
}

// parseMetaWhere splits a "<field><op><value>" expression into a MetaFilter.
// Operators are scanned in order of decreasing length so "!=" and ">=" beat
// their single-char prefixes, and the literal keywords "LIKE" / "NOT LIKE"
// are detected via a separate " like " / " not like " split.
func parseMetaWhere(expr string) (search.MetaFilter, error) {
	// Longest-first so ">=" isn't parsed as ">".
	for _, op := range []string{"!=", "<=", ">=", "<>", "=", "<", ">"} {
		if i := strings.Index(expr, op); i > 0 {
			return search.MetaFilter{
				Field: strings.TrimSpace(expr[:i]),
				Op:    op,
				Value: strings.TrimSpace(expr[i+len(op):]),
			}, nil
		}
	}
	// Case-insensitive " like "/" not like " — spaces required so we don't
	// confuse them with field names containing the substring.
	lower := strings.ToLower(expr)
	if i := strings.Index(lower, " not like "); i > 0 {
		return search.MetaFilter{
			Field: strings.TrimSpace(expr[:i]),
			Op:    "NOT LIKE",
			Value: strings.TrimSpace(expr[i+len(" not like "):]),
		}, nil
	}
	if i := strings.Index(lower, " like "); i > 0 {
		return search.MetaFilter{
			Field: strings.TrimSpace(expr[:i]),
			Op:    "LIKE",
			Value: strings.TrimSpace(expr[i+len(" like "):]),
		}, nil
	}
	return search.MetaFilter{}, fmt.Errorf("invalid where clause %q — expected <field><op><value>", expr)
}

// ─── Versions ────────────────────────────────────────────────────────────────

type versionsResponse struct {
	Path     string               `json:"path"`
	Versions []versioning.Version `json:"versions"`
}

// Versions returns the version history of a file.
// GET /api/kiwi/versions?path=concepts/authentication.md
func (h *Handlers) Versions(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}
	versions, err := h.versioner.Log(c.Request().Context(), path)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if versions == nil {
		versions = []versioning.Version{}
	}
	return c.JSON(http.StatusOK, versionsResponse{Path: path, Versions: versions})
}

// Version returns file content at a specific version.
// GET /api/kiwi/version?path=concepts/authentication.md&version=abc123
func (h *Handlers) Version(c echo.Context) error {
	path := c.QueryParam("path")
	hash := c.QueryParam("version")
	if path == "" || hash == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path and version are required")
	}
	content, err := h.versioner.Show(c.Request().Context(), path, hash)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "version not found")
	}
	return c.Blob(http.StatusOK, "text/markdown; charset=utf-8", content)
}

// Diff returns a unified diff between two versions.
// GET /api/kiwi/diff?path=foo.md&from=abc123&to=def456
func (h *Handlers) Diff(c echo.Context) error {
	path := c.QueryParam("path")
	from := c.QueryParam("from")
	to := c.QueryParam("to")
	if path == "" || from == "" || to == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path, from, and to are required")
	}
	diff, err := h.versioner.Diff(c.Request().Context(), path, from, to)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.String(http.StatusOK, diff)
}

// ─── Blame ───────────────────────────────────────────────────────────────────

type blameResponse struct {
	Path  string                  `json:"path"`
	Lines []versioning.BlameLine  `json:"lines"`
}

// Blame returns per-line git blame for a file.
// GET /api/kiwi/blame?path=concepts/authentication.md
func (h *Handlers) Blame(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}
	lines, err := h.versioner.Blame(c.Request().Context(), path)
	if err != nil {
		if errors.Is(err, versioning.ErrBlameUnsupported) {
			return echo.NewHTTPError(http.StatusNotImplemented, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if lines == nil {
		lines = []versioning.BlameLine{}
	}
	return c.JSON(http.StatusOK, blameResponse{Path: path, Lines: lines})
}

// ─── SSE Events ──────────────────────────────────────────────────────────────

// Events opens a Server-Sent Events stream. Each knowledge change (write,
// delete, bulk) is pushed as an SSE record with a proper `event:` field so
// clients can wire `eventSource.addEventListener('write', ...)` straight
// through rather than dispatching on a JSON field.
// GET /api/kiwi/events
func (h *Handlers) Events(c echo.Context) error {
	ch, err := h.hub.Subscribe()
	if err != nil {
		// Hub at capacity — reject before sending any SSE headers so the
		// client sees a clean 503 instead of a half-open event stream.
		return echo.NewHTTPError(http.StatusServiceUnavailable, err.Error())
	}
	defer h.hub.Unsubscribe(ch)

	c.Response().Header().Set("Content-Type", "text/event-stream")
	c.Response().Header().Set("Cache-Control", "no-cache")
	c.Response().Header().Set("Connection", "keep-alive")
	c.Response().WriteHeader(http.StatusOK)
	c.Response().Flush()

	// Proxies (nginx, ALB, CloudFront) close idle TCP connections after ~60s.
	// Emit an SSE comment every 15s so the stream keeps flowing even when no
	// knowledge changes occur. The leading ':' makes it a comment — clients
	// silently ignore it.
	ticker := time.NewTicker(sseHeartbeat)
	defer ticker.Stop()

	ctx := c.Request().Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := fmt.Fprint(c.Response(), ":keep-alive\n\n"); err != nil {
				return nil
			}
			c.Response().Flush()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			op := msg.Op
			if op == "" {
				op = "message"
			}
			if _, err := fmt.Fprintf(c.Response(), "event: %s\ndata: %s\n\n", op, msg.Data); err != nil {
				return nil
			}
			c.Response().Flush()
		}
	}
}

// ─── Backlinks ───────────────────────────────────────────────────────────────

type backlinksResponse struct {
	Path      string        `json:"path"`
	Backlinks []links.Entry `json:"backlinks"`
}

// Backlinks returns the pages that reference this file via [[wiki-link]] syntax.
// Only available when the search engine supports links (sqlite).
// GET /api/kiwi/backlinks?path=concepts/authentication.md
func (h *Handlers) Backlinks(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}
	if h.linker == nil {
		return c.JSON(http.StatusOK, backlinksResponse{Path: path, Backlinks: []links.Entry{}})
	}
	entries, err := h.linker.Backlinks(c.Request().Context(), path)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if entries == nil {
		entries = []links.Entry{}
	}
	return c.JSON(http.StatusOK, backlinksResponse{Path: path, Backlinks: entries})
}

// ─── Graph ───────────────────────────────────────────────────────────────────

type graphNode struct {
	Path string `json:"path"`
}

type graphResponse struct {
	Nodes []graphNode  `json:"nodes"`
	Edges []links.Edge `json:"edges"`
}

// Graph returns the full wiki-link graph: every markdown file as a node plus
// every [[target]] edge as a raw (source, target) pair. Targets are returned
// unresolved so the client can apply the same fuzzy path rules used for
// in-page link rendering (exact path / stem / basename / prefix).
// GET /api/kiwi/graph
//
// Cached: the response is cached at the handler level and invalidated
// event-driven via the pipeline's OnInvalidate hook. A singleflight group
// collapses concurrent misses so a 5k-file tree walk doesn't fan out into
// N parallel walks when many clients refresh at once.
func (h *Handlers) Graph(c echo.Context) error {
	if cached := h.graphCache.Load(); cached != nil {
		return c.JSON(http.StatusOK, cached)
	}
	// singleflight.Do fans every concurrent caller through one compute —
	// subsequent hits while a rebuild is in flight share its result.
	v, err, _ := h.graphGroup.Do("graph", func() (any, error) {
		if cached := h.graphCache.Load(); cached != nil {
			return cached, nil
		}
		// singleflight collapses N callers into one compute, so honouring
			// any single caller's ctx would let one early-cancel kill the
			// shared work everyone else is waiting on. Use Background here;
			// the compute is bounded by the index size, not request lifetime.
			resp, err := h.computeGraph(context.Background())
		if err != nil {
			return nil, err
		}
		h.graphCache.Store(resp)
		return resp, nil
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, v)
}

// computeGraph does the full-tree walk + edges fetch. Split out so Graph
// can share one compute across concurrent callers via singleflight.
func (h *Handlers) computeGraph(ctx context.Context) (*graphResponse, error) {
	nodes := []graphNode{}
	// Walk via the storage abstraction so non-local backends (future
	// S3-backed or network-FS storage) produce identical results. The
	// previous filepath.Walk implementation bypassed the abstraction and
	// would break the moment a different storage backend was wired in.
	walkErr := storage.Walk(ctx, h.store, "/", func(e storage.Entry) error {
		nodes = append(nodes, graphNode{Path: e.Path})
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Path < nodes[j].Path })

	edges := []links.Edge{}
	if h.linker != nil {
		e, err := h.linker.AllEdges(ctx)
		if err != nil {
			return nil, err
		}
		if e != nil {
			edges = e
		}
	}
	return &graphResponse{Nodes: nodes, Edges: edges}, nil
}

// ─── Templates ───────────────────────────────────────────────────────────────

type templateEntry struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type templatesResponse struct {
	Templates []templateEntry `json:"templates"`
}

// ListTemplates enumerates .md files under <root>/.kiwi/templates/.
// GET /api/kiwi/templates
//
// Deliberately bypasses storage.Storage: the storage abstraction treats
// `.kiwi/` as infrastructure (hidden from List/Walk), so a call through
// it would always return empty. Templates live inside that hidden tree
// on purpose — they're configuration, not knowledge — so a direct
// os.ReadDir here is the right call. If a future storage backend needs
// this path (e.g. network-FS storage), extend Storage with a ReadInfra
// method rather than lifting templates into the user-visible tree.
func (h *Handlers) ListTemplates(c echo.Context) error {
	dir := filepath.Join(h.root, ".kiwi", "templates")
	out := []templateEntry{}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return c.JSON(http.StatusOK, templatesResponse{Templates: out})
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".md") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".md")
		out = append(out, templateEntry{Name: name, Path: filepath.Join(".kiwi/templates", e.Name())})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return c.JSON(http.StatusOK, templatesResponse{Templates: out})
}

type templateBody struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

// ReadTemplate returns the raw content of a template by name (no .md suffix).
// GET /api/kiwi/template?name=run-report
//
// Also bypasses storage.Storage — same rationale as ListTemplates.
func (h *Handlers) ReadTemplate(c echo.Context) error {
	name := c.QueryParam("name")
	if name == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "name is required")
	}
	// Reject traversal — templates live in a flat dir.
	if strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid template name")
	}
	p := filepath.Join(h.root, ".kiwi", "templates", name+".md")
	content, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return echo.NewHTTPError(http.StatusNotFound, "template not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, templateBody{Name: name, Content: string(content)})
}

// ─── Comments ────────────────────────────────────────────────────────────────

type commentsResponse struct {
	Path     string             `json:"path"`
	Comments []comments.Comment `json:"comments"`
}

type commentBody struct {
	Anchor comments.Anchor `json:"anchor"`
	Body   string          `json:"body"`
	Author string          `json:"author,omitempty"`
}

// ListComments returns all inline annotations for a markdown file.
// GET /api/kiwi/comments?path=concepts/authentication.md
func (h *Handlers) ListComments(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}
	list, err := h.comments.List(path)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, commentsResponse{Path: path, Comments: list})
}

// AddComment appends a new inline annotation and git-commits the JSON file
// so comment history stays part of the repo audit trail.
// POST /api/kiwi/comments?path=<path>
// Body: { "anchor": {"quote", "prefix", "suffix", "offset"}, "body": "...", "author": "..." }
func (h *Handlers) AddComment(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}
	var body commentBody
	if err := c.Bind(&body); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid JSON body")
	}
	actor := body.Author
	if actor == "" {
		actor = c.Request().Header.Get("X-Actor")
	}
	if actor == "" {
		actor = pipeline.DefaultActor
	}
	record, err := h.comments.Add(path, comments.Comment{
		Anchor: body.Anchor,
		Body:   body.Body,
		Author: actor,
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	jsonPath := h.comments.FilePath(path)
	if cerr := h.versioner.Commit(c.Request().Context(), jsonPath, actor, fmt.Sprintf("comment: %s — %s", path, shortID(record.ID))); cerr != nil {
		log.Printf("handlers: commit comment %s: %v", path, cerr)
	}
	if h.hub != nil {
		h.hub.Broadcast(events.Event{Op: "comment.add", Path: path, Actor: actor})
	}
	return c.JSON(http.StatusOK, record)
}

// DeleteComment removes an annotation by id.
// DELETE /api/kiwi/comments/:id?path=<path>
func (h *Handlers) DeleteComment(c echo.Context) error {
	id := c.Param("id")
	path := c.QueryParam("path")
	if path == "" || id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path and id are required")
	}
	actor := c.Request().Header.Get("X-Actor")
	if actor == "" {
		actor = pipeline.DefaultActor
	}
	if err := h.comments.Delete(path, id); err != nil {
		if os.IsNotExist(err) {
			return echo.NewHTTPError(http.StatusNotFound, "comment not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	jsonPath := h.comments.FilePath(path)
	if cerr := h.versioner.Commit(c.Request().Context(), jsonPath, actor, fmt.Sprintf("comment-delete: %s — %s", path, shortID(id))); cerr != nil {
		log.Printf("handlers: commit comment-delete %s: %v", path, cerr)
	}
	if h.hub != nil {
		h.hub.Broadcast(events.Event{Op: "comment.delete", Path: path, Actor: actor})
	}
	return c.JSON(http.StatusOK, map[string]string{"deleted": id, "path": path})
}

// PATCH /api/kiwi/comments/:id?path=<path>
func (h *Handlers) ResolveComment(c echo.Context) error {
	id := c.Param("id")
	path := c.QueryParam("path")
	if path == "" || id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path and id are required")
	}
	actor := c.Request().Header.Get("X-Actor")
	if actor == "" {
		actor = pipeline.DefaultActor
	}

	var body struct {
		Resolved bool `json:"resolved"`
	}
	if err := c.Bind(&body); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid body")
	}

	updated, err := h.comments.Resolve(path, id, body.Resolved)
	if err != nil {
		if os.IsNotExist(err) {
			return echo.NewHTTPError(http.StatusNotFound, "comment not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	jsonPath := h.comments.FilePath(path)
	verb := "resolve"
	if !body.Resolved {
		verb = "unresolve"
	}
	if cerr := h.versioner.Commit(c.Request().Context(), jsonPath, actor, fmt.Sprintf("comment-%s: %s — %s", verb, path, shortID(id))); cerr != nil {
		log.Printf("handlers: commit comment-%s %s: %v", verb, path, cerr)
	}
	if h.hub != nil {
		h.hub.Broadcast(events.Event{Op: "comment.resolve", Path: path, Actor: actor})
	}
	return c.JSON(http.StatusOK, updated)
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// ─── Theme ──────────────────────────────────────────────────────────────────

// GetTheme returns the current theme from .kiwi/theme.json.
// GET /api/kiwi/theme
func (h *Handlers) GetTheme(c echo.Context) error {
	p := filepath.Join(h.root, ".kiwi", "theme.json")
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return c.JSON(http.StatusOK, map[string]any{})
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	var theme map[string]any
	if err := json.Unmarshal(data, &theme); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "invalid theme.json")
	}
	return c.JSON(http.StatusOK, theme)
}

// PutTheme saves theme overrides to .kiwi/theme.json.
// PUT /api/kiwi/theme
func (h *Handlers) PutTheme(c echo.Context) error {
	const maxBody = 64 << 10
	body, err := io.ReadAll(io.LimitReader(c.Request().Body, maxBody+1))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to read body")
	}
	if len(body) > maxBody {
		return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "theme JSON exceeds 64 KB")
	}
	var theme map[string]any
	if err := json.Unmarshal(body, &theme); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid JSON")
	}
	formatted, err := json.MarshalIndent(theme, "", "  ")
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	dir := filepath.Join(h.root, ".kiwi")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	p := filepath.Join(dir, "theme.json")
	if err := os.WriteFile(p, formatted, 0o644); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	actor := c.Request().Header.Get("X-Actor")
	if actor == "" {
		actor = pipeline.DefaultActor
	}
	if cerr := h.versioner.Commit(c.Request().Context(), ".kiwi/theme.json", actor, "theme: update"); cerr != nil {
		log.Printf("handlers: commit theme: %v", cerr)
	}
	return c.JSON(http.StatusOK, theme)
}

