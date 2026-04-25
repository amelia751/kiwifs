package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/kiwifs/kiwifs/internal/dataview"
	"github.com/labstack/echo/v4"
)

type queryResponse struct {
	Columns []string               `json:"columns"`
	Rows    []map[string]any       `json:"rows"`
	Total   int                    `json:"total"`
	HasMore bool                   `json:"has_more"`
	Groups  []dataview.GroupResult `json:"groups,omitempty"`
}

func (h *Handlers) Query(c echo.Context) error {
	if h.dv == nil {
		return echo.NewHTTPError(http.StatusNotImplemented, "dataview requires sqlite search backend")
	}
	q := c.QueryParam("q")
	if q == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "q is required")
	}
	limit := parseIntParam(c, "limit", 0)
	offset := parseIntParam(c, "offset", 0)
	format := c.QueryParam("format")
	if format == "" {
		format = "json"
	}

	result, err := h.dv.Query(c.Request().Context(), q, limit, offset)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	if format == "table" || format == "list" || format == "count" || format == "distinct" {
		rendered := dataview.Render(result, format)
		return c.String(http.StatusOK, rendered)
	}

	return c.JSON(http.StatusOK, queryResponse{
		Columns: result.Columns,
		Rows:    result.Rows,
		Total:   result.Total,
		HasMore: result.HasMore,
		Groups:  result.Groups,
	})
}

type aggregateGroup struct {
	Key   string `json:"key"`
	Count int    `json:"count,omitempty"`
}

type aggregateResponse struct {
	Groups []aggregateGroup `json:"groups"`
}

func (h *Handlers) QueryAggregate(c echo.Context) error {
	if h.dv == nil {
		return echo.NewHTTPError(http.StatusNotImplemented, "dataview requires sqlite search backend")
	}
	groupBy := c.QueryParam("group_by")
	if groupBy == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "group_by is required")
	}

	if !dataview.ValidFieldName(groupBy) {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid group_by field")
	}

	var dql strings.Builder
	dql.WriteString("TABLE " + groupBy)

	wheres := c.QueryParams()["where"]
	for _, w := range wheres {
		if _, err := dataview.ParseExpr(w); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest,
				fmt.Sprintf("invalid where expression: %v", err))
		}
	}
	if len(wheres) > 0 {
		dql.WriteString(" WHERE ")
		dql.WriteString(strings.Join(wheres, " AND "))
	}

	dql.WriteString(" GROUP BY " + groupBy)

	result, err := h.dv.Query(c.Request().Context(), dql.String(), 0, 0)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	groups := make([]aggregateGroup, 0, len(result.Groups))
	for _, g := range result.Groups {
		groups = append(groups, aggregateGroup{
			Key:   g.Key,
			Count: g.Count,
		})
	}

	return c.JSON(http.StatusOK, aggregateResponse{Groups: groups})
}

type viewRefreshRequest struct {
	Path string `json:"path"`
}

func (h *Handlers) ViewRefresh(c echo.Context) error {
	if h.dv == nil {
		return echo.NewHTTPError(http.StatusNotImplemented, "dataview requires sqlite search backend")
	}
	var req viewRefreshRequest
	if err := bindJSON(c, &req); err != nil {
		return err
	}
	if req.Path == "" {
		req.Path = c.QueryParam("path")
	}
	if req.Path == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path is required")
	}

	changed, err := dataview.RegenerateView(c.Request().Context(), h.store, h.dv, req.Path)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	status := "unchanged"
	if changed {
		status = "regenerated"
	}
	return c.JSON(http.StatusOK, map[string]string{
		"path":   req.Path,
		"status": status,
	})
}
