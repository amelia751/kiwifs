package api

import (
	"errors"
	"net/http"

	"github.com/kiwifs/kiwifs/internal/versioning"
	"github.com/labstack/echo/v4"
)

type versionsResponse struct {
	Path     string               `json:"path"`
	Versions []versioning.Version `json:"versions"`
}

func (h *Handlers) Versions(c echo.Context) error {
	path, err := requirePath(c)
	if err != nil {
		return err
	}
	versions, err := h.versioner.Log(c.Request().Context(), path)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if versions == nil {
		versions = []versioning.Version{}
	}
	return c.JSON(http.StatusOK, versionsResponse{Path: path, Versions: versions})
}

func (h *Handlers) Version(c echo.Context) error {
	path := c.QueryParam("path")
	hash := c.QueryParam("version")
	if path == "" || hash == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path and version are required")
	}
	content, err := h.versioner.Show(c.Request().Context(), path, hash)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "version not found")
	}
	return c.Blob(http.StatusOK, "text/markdown; charset=utf-8", content)
}

func (h *Handlers) Diff(c echo.Context) error {
	path := c.QueryParam("path")
	from := c.QueryParam("from")
	to := c.QueryParam("to")
	if path == "" || from == "" || to == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "path, from, and to are required")
	}
	diff, err := h.versioner.Diff(c.Request().Context(), path, from, to)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.String(http.StatusOK, diff)
}

type blameResponse struct {
	Path  string                 `json:"path"`
	Lines []versioning.BlameLine `json:"lines"`
}

func (h *Handlers) Blame(c echo.Context) error {
	path, err := requirePath(c)
	if err != nil {
		return err
	}
	lines, err := h.versioner.Blame(c.Request().Context(), path)
	if err != nil {
		if errors.Is(err, versioning.ErrBlameUnsupported) {
			return echo.NewHTTPError(http.StatusNotImplemented, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if lines == nil {
		lines = []versioning.BlameLine{}
	}
	return c.JSON(http.StatusOK, blameResponse{Path: path, Lines: lines})
}
