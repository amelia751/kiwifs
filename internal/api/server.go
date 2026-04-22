package api

import (
	"net/http"

	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/versioning"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// Server is the KiwiFS HTTP server.
type Server struct {
	cfg      *config.Config
	store    storage.Storage
	versioner versioning.Versioner
	searcher search.Searcher
	echo     *echo.Echo
}

// NewServer creates and configures the server.
func NewServer(
	cfg *config.Config,
	store storage.Storage,
	versioner versioning.Versioner,
	searcher search.Searcher,
) *Server {
	s := &Server{
		cfg:      cfg,
		store:    store,
		versioner: versioner,
		searcher: searcher,
		echo:     echo.New(),
	}
	s.echo.HideBanner = true
	s.echo.HidePort = true
	s.setupMiddleware()
	s.setupRoutes()
	return s
}

func (s *Server) setupMiddleware() {
	s.echo.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodOptions},
		AllowHeaders: []string{"Content-Type", "Authorization", "If-Match", "X-Actor"},
		ExposeHeaders: []string{"ETag", "Last-Modified"},
	}))
	s.echo.Use(middleware.Recover())

	if s.cfg.Auth.Type == "apikey" && s.cfg.Auth.APIKey != "" {
		s.echo.Use(apiKeyMiddleware(s.cfg.Auth.APIKey))
	}
}

func (s *Server) setupRoutes() {
	h := &Handlers{
		store:    s.store,
		versioner: s.versioner,
		searcher: s.searcher,
	}

	s.echo.GET("/health", h.Health)

	api := s.echo.Group("/api/kiwi")
	api.GET("/tree", h.Tree)
	api.GET("/file", h.ReadFile)
	api.PUT("/file", h.WriteFile)
	api.DELETE("/file", h.DeleteFile)
	api.GET("/search", h.Search)
	api.GET("/versions", h.Versions)
	api.GET("/version", h.Version)
	api.GET("/diff", h.Diff)
}

func (s *Server) Start(addr string) error {
	return s.echo.Start(addr)
}

func apiKeyMiddleware(key string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			auth := c.Request().Header.Get("Authorization")
			if auth != "Bearer "+key {
				return echo.NewHTTPError(http.StatusUnauthorized, "invalid API key")
			}
			return next(c)
		}
	}
}
