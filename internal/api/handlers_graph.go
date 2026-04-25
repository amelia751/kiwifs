package api

import (
	"context"
	"net/http"
	"sort"

	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/markdown"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/labstack/echo/v4"
)

type tocResponse struct {
	Path     string             `json:"path"`
	Headings []markdown.Heading `json:"headings"`
}

func (h *Handlers) ToC(c echo.Context) error {
	path, err := requirePath(c)
	if err != nil {
		return err
	}
	content, err := readFileOr404(c.Request().Context(), h.store, path)
	if err != nil {
		return err
	}
	headings := markdown.Headings(content)
	if headings == nil {
		headings = []markdown.Heading{}
	}
	return c.JSON(http.StatusOK, tocResponse{Path: path, Headings: headings})
}

type backlinksResponse struct {
	Path      string        `json:"path"`
	Backlinks []links.Entry `json:"backlinks"`
}

func (h *Handlers) Backlinks(c echo.Context) error {
	path, err := requirePath(c)
	if err != nil {
		return err
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

type graphNode struct {
	Path string   `json:"path"`
	Tags []string `json:"tags,omitempty"`
}

type graphResponse struct {
	Nodes []graphNode  `json:"nodes"`
	Edges []links.Edge `json:"edges"`
}

func (h *Handlers) Graph(c echo.Context) error {
	if cached := h.graphCache.Load(); cached != nil {
		return c.JSON(http.StatusOK, cached)
	}
	v, err, _ := h.graphGroup.Do("graph", func() (any, error) {
		if cached := h.graphCache.Load(); cached != nil {
			return cached, nil
		}
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

func (h *Handlers) computeGraph(ctx context.Context) (*graphResponse, error) {
	nodes := []graphNode{}
	walkErr := storage.Walk(ctx, h.store, "/", func(e storage.Entry) error {
		node := graphNode{Path: e.Path}
		if raw, err := h.store.Read(ctx, e.Path); err == nil {
			node.Tags = extractFrontmatterTags(raw)
		}
		nodes = append(nodes, node)
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

func extractFrontmatterTags(raw []byte) []string {
	fm, err := markdown.Frontmatter(raw)
	if err != nil || fm == nil {
		return nil
	}
	val, ok := fm["tags"]
	if !ok {
		val, ok = fm["labels"]
	}
	if !ok {
		return nil
	}
	switch v := val.(type) {
	case []any:
		tags := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && s != "" {
				tags = append(tags, s)
			}
		}
		return tags
	case string:
		if v != "" {
			return []string{v}
		}
	}
	return nil
}
