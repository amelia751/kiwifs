package api

import (
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kiwifs/kiwifs/internal/comments"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/dataview"
	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/janitor"
	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/rbac"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/vectorstore"
	"github.com/kiwifs/kiwifs/internal/versioning"
	"github.com/labstack/echo/v4"
	"golang.org/x/sync/singleflight"
)

var sseHeartbeat = 15 * time.Second

type Handlers struct {
	store        storage.Storage
	versioner    versioning.Versioner
	searcher     search.Searcher
	linker       links.Linker
	hub          *events.Hub
	pipe         *pipeline.Pipeline
	vectors      *vectorstore.Service
	dv           *dataview.Executor
	viewReg      *dataview.Registry
	comments     *comments.Store
	shares       *rbac.ShareStore
	assets       config.AssetsConfig
	ui           config.UIConfig
	root         string
	publicURL    string
	linkResolver *links.Resolver

	janitorSched     *janitor.Scheduler
	janitorStaleDays int

	memoryEpisodesPrefix string

	graphCache atomic.Pointer[graphResponse]
	graphGroup singleflight.Group
}

func (h *Handlers) invalidateGraphCache() {
	h.graphCache.Store(nil)
}

type treeEntry struct {
	Path      string       `json:"path"`
	Name      string       `json:"name"`
	IsDir     bool         `json:"isDir"`
	Size      int64        `json:"size,omitempty"`
	Permalink string       `json:"permalink,omitempty"`
	Children  []*treeEntry `json:"children,omitempty"`
}

const maxTreeDepth = 20

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
