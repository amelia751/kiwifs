package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/kiwifs/kiwifs/internal/dataview"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/tracing"
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
	tracing.Record(c.Request().Context(), tracing.Event{Kind: tracing.KindDQL, Query: q, HitCount: result.Total})

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

// aggregateResponse is keyed by group value, each containing calculated aggregates.
type aggregateResponse struct {
	Groups map[string]map[string]any `json:"groups"`
}

// parseCalcSpecs parses comma-separated calc values like "count,avg:mastery,max:score".
// Returns a list of {func, field} pairs. "count" has no field.
type calcSpec struct {
	fn    string // count, avg, sum, min, max
	field string // empty for count
}

var validAggFuncs = map[string]bool{
	"count": true, "avg": true, "sum": true, "min": true, "max": true,
}

func parseCalcSpecs(raw string) ([]calcSpec, error) {
	if raw == "" {
		return []calcSpec{{fn: "count"}}, nil
	}
	parts := strings.Split(raw, ",")
	specs := make([]calcSpec, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if p == "count" {
			specs = append(specs, calcSpec{fn: "count"})
			continue
		}
		fn, field, ok := strings.Cut(p, ":")
		if !ok || field == "" {
			return nil, fmt.Errorf("invalid calc %q: expected func:field (e.g. avg:mastery)", p)
		}
		if !validAggFuncs[fn] {
			return nil, fmt.Errorf("unsupported aggregate function %q (supported: count, avg, sum, min, max)", fn)
		}
		if !dataview.ValidFieldName(field) {
			return nil, fmt.Errorf("invalid field name in calc: %q", field)
		}
		specs = append(specs, calcSpec{fn: fn, field: field})
	}
	if len(specs) == 0 {
		return []calcSpec{{fn: "count"}}, nil
	}
	return specs, nil
}

func (cs calcSpec) label() string {
	if cs.field == "" {
		return cs.fn
	}
	return cs.fn + ":" + cs.field
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

	calcs, err := parseCalcSpecs(c.QueryParam("calc"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	wheres := c.QueryParams()["where"]
	for _, w := range wheres {
		if _, err := dataview.ParseExpr(w); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest,
				fmt.Sprintf("invalid where expression: %v", err))
		}
	}

	pathPrefix := c.QueryParam("path_prefix")

	sq, ok := h.searcher.(*search.SQLite)
	if !ok {
		return echo.NewHTTPError(http.StatusNotImplemented, "aggregate requires sqlite search backend")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("SELECT json_extract(frontmatter, '$.%s') AS grp", groupBy))
	for _, cs := range calcs {
		switch cs.fn {
		case "count":
			sb.WriteString(", COUNT(*) AS agg_count")
		case "avg":
			sb.WriteString(fmt.Sprintf(", AVG(json_extract(frontmatter, '$.%s')) AS `agg_%s`", cs.field, cs.label()))
		case "sum":
			sb.WriteString(fmt.Sprintf(", SUM(json_extract(frontmatter, '$.%s')) AS `agg_%s`", cs.field, cs.label()))
		case "min":
			sb.WriteString(fmt.Sprintf(", MIN(json_extract(frontmatter, '$.%s')) AS `agg_%s`", cs.field, cs.label()))
		case "max":
			sb.WriteString(fmt.Sprintf(", MAX(json_extract(frontmatter, '$.%s')) AS `agg_%s`", cs.field, cs.label()))
		}
	}
	sb.WriteString(" FROM file_meta")

	var conditions []string
	var args []any

	if pathPrefix != "" {
		conditions = append(conditions, "path LIKE ? || '%'")
		args = append(args, pathPrefix)
	}
	for _, w := range wheres {
		conditions = append(conditions, w)
	}

	if len(conditions) > 0 {
		sb.WriteString(" WHERE " + strings.Join(conditions, " AND "))
	}
	sb.WriteString(fmt.Sprintf(" GROUP BY json_extract(frontmatter, '$.%s')", groupBy))

	rows, err := sq.ReadDB().QueryContext(c.Request().Context(), sb.String(), args...)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	defer rows.Close()

	groups := make(map[string]map[string]any)
	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
		key := fmt.Sprint(vals[0])
		if key == "<nil>" {
			key = "(none)"
		}
		bucket := make(map[string]any)
		for i, cs := range calcs {
			val := vals[i+1]
			switch v := val.(type) {
			case int64:
				bucket[cs.label()] = v
			case float64:
				bucket[cs.label()] = v
			default:
				bucket[cs.label()] = v
			}
		}
		groups[key] = bucket
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
