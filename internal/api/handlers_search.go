package api

import (
	"context"
	"net/http"
	"time"

	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/vectorstore"
	"github.com/labstack/echo/v4"
)

type searchResultEntry struct {
	Path      string         `json:"path"`
	Matches   []search.Match `json:"matches"`
	Score     float64        `json:"score,omitempty"`
	Snippet   string         `json:"snippet,omitempty"`
	Permalink string         `json:"permalink,omitempty"`
}

type searchResponse struct {
	Query   string              `json:"query"`
	Limit   int                 `json:"limit"`
	Offset  int                 `json:"offset"`
	Results []searchResultEntry `json:"results"`
}

func (h *Handlers) Search(c echo.Context) error {
	q := c.QueryParam("q")
	if q == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "q is required")
	}
	limit := search.NormalizeLimit(parseIntParam(c, "limit", 0))
	offset := search.NormalizeOffset(parseIntParam(c, "offset", 0))
	boost := c.QueryParam("boost")
	var (
		results []search.Result
		err     error
	)
	if ts, ok := h.searcher.(search.TrustSearcher); ok && boost != "none" && boost != "off" {
		results, err = ts.SearchBoosted(c.Request().Context(), q, limit, offset, "")
	} else {
		results, err = h.searcher.Search(c.Request().Context(), q, limit, offset, "")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if results == nil {
		results = []search.Result{}
	}
	if ma := c.QueryParam("modifiedAfter"); ma != "" {
		cutoff, perr := time.Parse(time.RFC3339, ma)
		if perr != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid modifiedAfter: expected RFC3339 date")
		}
		{
			if df, ok := h.searcher.(search.DateFilterer); ok {
				paths := make([]string, len(results))
				for i, r := range results {
					paths[i] = r.Path
				}
				kept, ferr := df.FilterByDate(c.Request().Context(), paths, cutoff)
				if ferr == nil {
					keptSet := make(map[string]bool, len(kept))
					for _, p := range kept {
						keptSet[p] = true
					}
					filtered := results[:0]
					for _, r := range results {
						if keptSet[r.Path] {
							filtered = append(filtered, r)
						}
					}
					results = filtered
				} else {
					filtered := results[:0]
					for _, r := range results {
						info, serr := h.store.Stat(c.Request().Context(), r.Path)
						if serr == nil && info.ModTime.After(cutoff) {
							filtered = append(filtered, r)
						}
					}
					results = filtered
				}
			} else {
				filtered := results[:0]
				for _, r := range results {
					info, serr := h.store.Stat(c.Request().Context(), r.Path)
					if serr == nil && info.ModTime.After(cutoff) {
						filtered = append(filtered, r)
					}
				}
				results = filtered
			}
		}
	}
	return c.JSON(http.StatusOK, searchResponse{
		Query:   q,
		Limit:   limit,
		Offset:  offset,
		Results: buildSearchEntries(results, h.publicURL),
	})
}

func (h *Handlers) VerifiedSearch(c echo.Context) error {
	q := c.QueryParam("q")
	if q == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "q is required")
	}
	limit := search.NormalizeLimit(parseIntParam(c, "limit", 0))
	offset := search.NormalizeOffset(parseIntParam(c, "offset", 0))

	ts, ok := h.searcher.(search.TrustSearcher)
	if !ok {
		return echo.NewHTTPError(http.StatusNotImplemented, "verified search requires sqlite search backend")
	}
	results, err := ts.SearchVerified(c.Request().Context(), q, limit, offset, "")
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
		Results: buildSearchEntries(results, h.publicURL),
	})
}

type staleResponse struct {
	StaleDays int                 `json:"staleDays"`
	Count     int                 `json:"count"`
	Results   []search.MetaResult `json:"results"`
}

func (h *Handlers) StalePages(c echo.Context) error {
	sd, ok := h.searcher.(search.StaleDetector)
	if !ok {
		return echo.NewHTTPError(http.StatusNotImplemented, "stale detection requires sqlite search backend")
	}
	days := parseIntParam(c, "days", 30)
	if days <= 0 {
		days = 30
	}
	results, err := sd.StalePages(c.Request().Context(), days)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if results == nil {
		results = []search.MetaResult{}
	}
	return c.JSON(http.StatusOK, staleResponse{
		StaleDays: days,
		Count:     len(results),
		Results:   results,
	})
}

type contradictionsResponse struct {
	Path  string   `json:"path"`
	Paths []string `json:"contradictions"`
}

func (h *Handlers) Contradictions(c echo.Context) error {
	cd, ok := h.searcher.(search.ContradictionDetector)
	if !ok {
		return echo.NewHTTPError(http.StatusNotImplemented, "contradiction detection requires sqlite search backend")
	}
	path, err := requirePath(c)
	if err != nil {
		return err
	}
	paths, err := cd.FindContradictions(c.Request().Context(), path)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if paths == nil {
		paths = []string{}
	}
	return c.JSON(http.StatusOK, contradictionsResponse{
		Path:  path,
		Paths: paths,
	})
}

type semanticRequest struct {
	Query         string `json:"query"`
	TopK          int    `json:"topK"`
	Offset        int    `json:"offset"`
	ModifiedAfter string `json:"modifiedAfter,omitempty"`
}

type semanticResponse struct {
	Query   string               `json:"query"`
	TopK    int                  `json:"topK"`
	Offset  int                  `json:"offset"`
	Results []vectorstore.Result `json:"results"`
}

func (h *Handlers) SemanticSearch(c echo.Context) error {
	if h.vectors == nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "semantic search is not enabled")
	}
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
	if ma := req.ModifiedAfter; ma != "" {
		cutoff, perr := time.Parse(time.RFC3339, ma)
		if perr != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid modifiedAfter: expected RFC3339 date")
		}
		filtered := results[:0]
		for _, r := range results {
			info, serr := h.store.Stat(c.Request().Context(), r.Path)
			if serr == nil && info.ModTime.After(cutoff) {
				filtered = append(filtered, r)
			}
		}
		results = filtered
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

type metaQuerier interface {
	QueryMeta(ctx context.Context, filters []search.MetaFilter, sort, order string, limit, offset int) ([]search.MetaResult, error)
	QueryMetaOr(ctx context.Context, andFilters, orFilters []search.MetaFilter, sort, order string, limit, offset int) ([]search.MetaResult, error)
}

type metaResultEntry struct {
	Path        string         `json:"path"`
	Frontmatter map[string]any `json:"frontmatter"`
	Permalink   string         `json:"permalink,omitempty"`
}

type metaResponse struct {
	Count   int               `json:"count"`
	Limit   int               `json:"limit"`
	Offset  int               `json:"offset"`
	Results []metaResultEntry `json:"results"`
}

func (h *Handlers) Meta(c echo.Context) error {
	mq, ok := h.searcher.(metaQuerier)
	if !ok {
		return echo.NewHTTPError(http.StatusNotImplemented, "metadata index requires sqlite search backend")
	}

	var andFilters []search.MetaFilter
	for _, raw := range c.QueryParams()["where"] {
		f, err := search.ParseMetaFilter(raw)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		andFilters = append(andFilters, f)
	}

	var orFilters []search.MetaFilter
	for _, raw := range c.QueryParams()["or"] {
		f, err := search.ParseMetaFilter(raw)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		orFilters = append(orFilters, f)
	}

	sortField := c.QueryParam("sort")
	order := c.QueryParam("order")
	limit := parseIntParam(c, "limit", 0)
	offset := parseIntParam(c, "offset", 0)

	results, err := mq.QueryMetaOr(c.Request().Context(), andFilters, orFilters, sortField, order, limit, offset)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	entries := make([]metaResultEntry, len(results))
	for i, r := range results {
		entries[i] = metaResultEntry{
			Path:        r.Path,
			Frontmatter: r.Frontmatter,
			Permalink:   config.Permalink(h.publicURL, r.Path),
		}
	}
	return c.JSON(http.StatusOK, metaResponse{
		Count:   len(entries),
		Limit:   search.NormalizeLimit(limit),
		Offset:  search.NormalizeOffset(offset),
		Results: entries,
	})
}

