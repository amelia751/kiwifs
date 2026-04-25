package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/labstack/echo/v4"
)

func (h *Handlers) Tree(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		path = "/"
	}
	st, err := storage.BuildTree(c.Request().Context(), h.store, path, maxTreeDepth)
	if err != nil {
		if os.IsNotExist(err) {
			return echo.NewHTTPError(http.StatusNotFound, "path not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	tree := toTreeEntry(st)
	h.addPermalinks(tree)
	return c.JSON(http.StatusOK, tree)
}

func toTreeEntry(st *storage.TreeEntry) *treeEntry {
	if st == nil {
		return nil
	}
	e := &treeEntry{
		Path:  st.Path,
		Name:  st.Name,
		IsDir: st.IsDir,
		Size:  st.Size,
	}
	for _, c := range st.Children {
		e.Children = append(e.Children, toTreeEntry(c))
	}
	return e
}

func (h *Handlers) addPermalinks(entry *treeEntry) {
	if entry == nil {
		return
	}
	if !entry.IsDir && entry.Path != "" {
		entry.Permalink = config.Permalink(h.publicURL, entry.Path)
	}
	for _, child := range entry.Children {
		h.addPermalinks(child)
	}
}

func (h *Handlers) ReadFile(c echo.Context) error {
	path, err := requirePath(c)
	if err != nil {
		return err
	}

	if h.viewReg != nil && h.viewReg.IsStale(path) {
		_, _ = h.viewReg.RegenerateIfStale(c.Request().Context(), path)
	}

	content, err := readFileOr404(c.Request().Context(), h.store, path)
	if err != nil {
		return err
	}

	etag := fmt.Sprintf(`"%s"`, pipeline.ETag(content))
	c.Response().Header().Set("ETag", etag)
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".md" || ext == ".markdown" {
		c.Response().Header().Set("Cache-Control", "no-cache")
	} else {
		c.Response().Header().Set("Cache-Control", "public, max-age=3600, must-revalidate")
		c.Response().Header().Set("Vary", "Authorization, Cookie")
	}

	var modTime time.Time
	if info, serr := h.store.Stat(c.Request().Context(), path); serr == nil {
		modTime = info.ModTime.UTC()
		c.Response().Header().Set("Last-Modified", modTime.Format(http.TimeFormat))
	}

	if match := c.Request().Header.Get("If-None-Match"); match != "" && match == etag {
		return c.NoContent(http.StatusNotModified)
	}
	if !modTime.IsZero() {
		if ims := c.Request().Header.Get("If-Modified-Since"); ims != "" {
			if t, perr := http.ParseTime(ims); perr == nil {
				if !modTime.Truncate(time.Second).After(t) {
					return c.NoContent(http.StatusNotModified)
				}
			}
		}
	}

	if pl := config.Permalink(h.publicURL, path); pl != "" {
		c.Response().Header().Set("X-Permalink", pl)
	}

	if c.QueryParam("resolve_links") == "true" && h.publicURL != "" && h.linkResolver != nil {
		content = []byte(h.linkResolver.Resolve(c.Request().Context(), string(content), h.publicURL))
	}

	return c.Blob(http.StatusOK, detectContentType(path, content), content)
}

type resolveLinksRequest struct {
	Content string `json:"content"`
}

func (h *Handlers) ResolveLinks(c echo.Context) error {
	var req resolveLinksRequest
	if err := bindJSON(c, &req); err != nil {
		return err
	}
	if h.publicURL == "" {
		return c.JSON(http.StatusOK, map[string]string{"content": req.Content})
	}
	resolved := req.Content
	if h.linkResolver != nil {
		resolved = h.linkResolver.Resolve(c.Request().Context(), req.Content, h.publicURL)
	}
	return c.JSON(http.StatusOK, map[string]string{"content": resolved})
}

func (h *Handlers) WriteFile(c echo.Context) error {
	path, err := requirePath(c)
	if err != nil {
		return err
	}

	ifMatch := strings.Trim(c.Request().Header.Get("If-Match"), `"`)

	const maxFileBody = 32 << 20
	body, err := io.ReadAll(io.LimitReader(c.Request().Body, maxFileBody+1))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to read body")
	}
	if len(body) > maxFileBody {
		return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "file exceeds 32 MB limit")
	}

	actor := c.Request().Header.Get("X-Actor")
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

func (h *Handlers) BulkWrite(c echo.Context) error {
	var req bulkRequest
	if err := bindJSON(c, &req); err != nil {
		return err
	}
	if len(req.Files) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "files is required and must be non-empty")
	}

	actor := req.Actor
	if actor == "" {
		actor = c.Request().Header.Get("X-Actor")
	}

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

const defaultMaxAssetSize = 10 << 20 // 10 MiB

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
	content, err := io.ReadAll(io.LimitReader(src, maxSize+1))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to read upload")
	}
	if int64(len(content)) > maxSize {
		return echo.NewHTTPError(http.StatusRequestEntityTooLarge,
			fmt.Sprintf("file exceeds %d-byte limit", maxSize))
	}

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

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
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

func (h *Handlers) DeleteFile(c echo.Context) error {
	path, err := requirePath(c)
	if err != nil {
		return err
	}
	if !h.store.Exists(c.Request().Context(), path) {
		return echo.NewHTTPError(http.StatusNotFound, "file not found")
	}

	if err := h.pipe.Delete(c.Request().Context(), path, c.Request().Header.Get("X-Actor")); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, map[string]string{"deleted": path})
}
