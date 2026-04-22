package api

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/versioning"
	"github.com/labstack/echo/v4"
)

// Handlers holds dependencies for all route handlers.
type Handlers struct {
	store     storage.Storage
	versioner versioning.Versioner
	searcher  search.Searcher
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

// Tree returns the directory tree as JSON.
// GET /api/kiwi/tree?path=concepts/
func (h *Handlers) Tree(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		path = "/"
	}
	tree, err := h.buildTree(path)
	if err != nil {
		if os.IsNotExist(err) {
			return echo.NewHTTPError(http.StatusNotFound, "path not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, tree)
}

func (h *Handlers) buildTree(path string) (*treeEntry, error) {
	entries, err := h.store.List(path)
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
		if e.IsDir {
			sub, err := h.buildTree(e.Path)
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
func (h *Handlers) ReadFile(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}

	content, err := h.store.Read(path)
	if err != nil {
		if os.IsNotExist(err) {
			return echo.NewHTTPError(http.StatusNotFound, "file not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	etag := contentETag(content)
	c.Response().Header().Set("ETag", fmt.Sprintf(`"%s"`, etag))
	c.Response().Header().Set("Cache-Control", "no-cache")
	return c.Blob(http.StatusOK, "text/markdown; charset=utf-8", content)
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

	// Optimistic locking: check ETag if provided.
	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch != "" {
		existing, err := h.store.Read(path)
		if err == nil {
			currentETag := fmt.Sprintf(`"%s"`, contentETag(existing))
			if ifMatch != currentETag {
				return echo.NewHTTPError(http.StatusConflict, "file modified since last read — re-fetch and retry")
			}
		}
		// If file doesn't exist yet, ignore the If-Match header.
	}

	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to read body")
	}

	if err := h.store.Write(path, body); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	actor := c.Request().Header.Get("X-Actor")
	if actor == "" {
		actor = "kiwifs"
	}
	msg := fmt.Sprintf("%s: %s", actor, path)
	_ = h.versioner.Commit(path, actor, msg)

	etag := contentETag(body)
	c.Response().Header().Set("ETag", fmt.Sprintf(`"%s"`, etag))
	return c.JSON(http.StatusOK, map[string]string{
		"path": path,
		"etag": etag,
	})
}

// ─── File Delete ─────────────────────────────────────────────────────────────

// DeleteFile deletes a file.
// DELETE /api/kiwi/file?path=concepts/authentication.md
func (h *Handlers) DeleteFile(c echo.Context) error {
	path := c.QueryParam("path")
	if path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}
	if !h.store.Exists(path) {
		return echo.NewHTTPError(http.StatusNotFound, "file not found")
	}

	actor := c.Request().Header.Get("X-Actor")
	if actor == "" {
		actor = "kiwifs"
	}

	if err := h.store.Delete(path); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	_ = h.versioner.CommitDelete(path, actor)
	return c.JSON(http.StatusOK, map[string]string{"deleted": path})
}

// ─── Search ──────────────────────────────────────────────────────────────────

type searchResponse struct {
	Query   string          `json:"query"`
	Results []search.Result `json:"results"`
}

// Search performs a full-text search across all .md files.
// GET /api/kiwi/search?q=WebSocket+timeout
func (h *Handlers) Search(c echo.Context) error {
	q := c.QueryParam("q")
	if q == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "q is required")
	}
	results, err := h.searcher.Search(q)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if results == nil {
		results = []search.Result{}
	}
	return c.JSON(http.StatusOK, searchResponse{Query: q, Results: results})
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
	versions, err := h.versioner.Log(path)
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
	content, err := h.versioner.Show(path, hash)
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
	diff, err := h.versioner.Diff(path, from, to)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.String(http.StatusOK, diff)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func contentETag(content []byte) string {
	h := sha256.Sum256(content)
	return fmt.Sprintf("%x", h[:8])
}
