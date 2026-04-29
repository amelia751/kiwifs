package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kiwifs/kiwifs/internal/comments"
	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/janitor"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/labstack/echo/v4"
)

type templateEntry struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type templatesResponse struct {
	Templates []templateEntry `json:"templates"`
}

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

func (h *Handlers) ReadTemplate(c echo.Context) error {
	name := c.QueryParam("name")
	if name == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "name is required")
	}
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

type commentsResponse struct {
	Path     string             `json:"path"`
	Comments []comments.Comment `json:"comments"`
}

type commentBody struct {
	Anchor comments.Anchor `json:"anchor"`
	Body   string          `json:"body"`
	Author string          `json:"author,omitempty"`
}

func (h *Handlers) ListComments(c echo.Context) error {
	path, err := requirePath(c)
	if err != nil {
		return err
	}
	list, err := h.comments.List(path)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, commentsResponse{Path: path, Comments: list})
}

func (h *Handlers) AddComment(c echo.Context) error {
	path, err := requirePath(c)
	if err != nil {
		return err
	}
	var body commentBody
	if err := bindJSON(c, &body); err != nil {
		return err
	}
	actor := sanitizeActor(body.Author)
	if actor == "anonymous" {
		actor = sanitizeActor(c.Request().Header.Get("X-Actor"))
	}
	if actor == "anonymous" {
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

func (h *Handlers) DeleteComment(c echo.Context) error {
	id := c.Param("id")
	path := c.QueryParam("path")
	if path == "" || id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path and id are required")
	}
	actor := sanitizeActor(c.Request().Header.Get("X-Actor"))
	if actor == "anonymous" {
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

func (h *Handlers) ResolveComment(c echo.Context) error {
	id := c.Param("id")
	path := c.QueryParam("path")
	if path == "" || id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path and id are required")
	}
	actor := sanitizeActor(c.Request().Header.Get("X-Actor"))
	if actor == "anonymous" {
		actor = pipeline.DefaultActor
	}

	var body struct {
		Resolved bool `json:"resolved"`
	}
	if err := bindJSON(c, &body); err != nil {
		return err
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

func (h *Handlers) UIConfig(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		"themeLocked": h.ui.ThemeLocked,
	})
}

func (h *Handlers) Janitor(c echo.Context) error {
	defaultStale := h.janitorStaleDays
	if defaultStale <= 0 {
		defaultStale = janitor.DefaultStaleDays
	}
	staleDays := parseIntParam(c, "staleDays", defaultStale)
	fresh := c.QueryParam("fresh") == "1" || c.QueryParam("fresh") == "true"

	if !fresh && h.janitorSched != nil && staleDays == defaultStale {
		if cached := h.janitorSched.LastResult(); cached != nil {
			if ls := h.janitorSched.LastScan(); !ls.IsZero() {
				c.Response().Header().Set("X-Kiwi-Janitor-LastScan", ls.UTC().Format(time.RFC3339))
			}
			return c.JSON(http.StatusOK, cached)
		}
	}

	scanner := janitor.New(h.root, h.store, h.searcher, staleDays)
	result, err := scanner.Scan(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, result)
}

func (h *Handlers) PutTheme(c echo.Context) error {
	if h.ui.ThemeLocked {
		return echo.NewHTTPError(http.StatusForbidden, "theme editing is locked by admin")
	}
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
	actor := sanitizeActor(c.Request().Header.Get("X-Actor"))
	if actor == "anonymous" {
		actor = pipeline.DefaultActor
	}
	if cerr := h.versioner.Commit(c.Request().Context(), ".kiwi/theme.json", actor, "theme: update"); cerr != nil {
		log.Printf("handlers: commit theme: %v", cerr)
	}
	return c.JSON(http.StatusOK, theme)
}
