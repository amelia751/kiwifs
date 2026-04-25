package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
)

var (
	startedAt    = time.Now()
	buildVersion = "dev"
)

func SetBuildVersion(v string) {
	if v == "" {
		return
	}
	buildVersion = v
}

func (h *Handlers) Health(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handlers) Healthz(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		"status":  "ok",
		"uptime":  time.Since(startedAt).String(),
		"version": buildVersion,
	})
}

func (h *Handlers) Readyz(c echo.Context) error {
	if h.store == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]string{"status": "no-store"})
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), 500*time.Millisecond)
	defer cancel()
	if _, err := h.store.Stat(ctx, ""); err != nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]string{
			"status": "storage-unreachable",
			"error":  err.Error(),
		})
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "ready"})
}

func (h *Handlers) Metrics(c echo.Context) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# HELP kiwi_build_info Static build metadata.\n")
	fmt.Fprintf(&b, "# TYPE kiwi_build_info gauge\n")
	fmt.Fprintf(&b, "kiwi_build_info{version=%q} 1\n", buildVersion)

	fmt.Fprintf(&b, "# HELP kiwi_uptime_seconds Seconds since server start.\n")
	fmt.Fprintf(&b, "# TYPE kiwi_uptime_seconds gauge\n")
	fmt.Fprintf(&b, "kiwi_uptime_seconds %.0f\n", time.Since(startedAt).Seconds())

	if h.hub != nil {
		fmt.Fprintf(&b, "# HELP kiwi_sse_subscribers Current SSE subscriber count.\n")
		fmt.Fprintf(&b, "# TYPE kiwi_sse_subscribers gauge\n")
		fmt.Fprintf(&b, "kiwi_sse_subscribers %d\n", h.hub.Count())
	}
	if h.janitorSched != nil {
		if r := h.janitorSched.LastResult(); r != nil {
			fmt.Fprintf(&b, "# HELP kiwi_janitor_issues Total janitor issues at last scan.\n")
			fmt.Fprintf(&b, "# TYPE kiwi_janitor_issues gauge\n")
			fmt.Fprintf(&b, "kiwi_janitor_issues %d\n", len(r.Issues))
		}
	}
	return c.Blob(http.StatusOK, "text/plain; version=0.0.4; charset=utf-8", []byte(b.String()))
}
