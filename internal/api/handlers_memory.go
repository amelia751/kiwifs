package api

import (
	"net/http"

	"github.com/kiwifs/kiwifs/internal/memory"
	"github.com/labstack/echo/v4"
)

// MemoryReport returns episodic vs merged-from coverage for consolidation pipelines.
// Query param episodes_prefix overrides [memory] episodes_path_prefix from config.
func (h *Handlers) MemoryReport(c echo.Context) error {
	ctx := c.Request().Context()
	prefix := c.QueryParam("episodes_prefix")
	if prefix == "" {
		prefix = h.memoryEpisodesPrefix
	}
	opt := memory.Options{EpisodesPathPrefix: prefix}
	rep, err := memory.Scan(ctx, h.store, opt)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, rep)
}
