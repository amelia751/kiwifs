package api

import (
	"context"
	"net/http"
	"os"
	"strings"

	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/labstack/echo/v4"
)

func bindJSON(c echo.Context, v any) error {
	if err := c.Bind(v); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid JSON body")
	}
	return nil
}

func requirePath(c echo.Context) (string, error) {
	path := c.QueryParam("path")
	if path == "" {
		return "", echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}
	return path, nil
}

// sanitizeActor strips control characters (newlines, null bytes, tabs) and
// clamps the actor string to a safe length for use in git env vars and
// frontmatter. Returns "anonymous" if the result is empty.
func sanitizeActor(raw string) string {
	s := strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f { // control characters
			return -1
		}
		return r
	}, raw)
	if len(s) > 256 {
		s = s[:256]
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return "anonymous"
	}
	return s
}

func readFileOr404(ctx context.Context, store storage.Storage, path string) ([]byte, error) {
	content, err := store.Read(ctx, path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, echo.NewHTTPError(http.StatusNotFound, "file not found")
		}
		return nil, echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return content, nil
}

func buildSearchEntries(results []search.Result, publicURL string) []searchResultEntry {
	entries := make([]searchResultEntry, len(results))
	for i, r := range results {
		entries[i] = searchResultEntry{
			Path:      r.Path,
			Matches:   r.Matches,
			Score:     r.Score,
			Snippet:   r.Snippet,
			Permalink: config.Permalink(publicURL, r.Path),
		}
	}
	return entries
}
