package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/kiwifs/kiwifs/internal/comments"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/dataview"
	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/janitor"
	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/rbac"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/vectorstore"
	"github.com/kiwifs/kiwifs/internal/webui"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type Server struct {
	cfg          *config.Config
	pipe         *pipeline.Pipeline
	vectors      *vectorstore.Service
	comments     *comments.Store
	shares       *rbac.ShareStore
	linkResolver *links.Resolver
	echo         *echo.Echo

	janitorSched  *janitor.Scheduler
	janitorCancel context.CancelFunc

	auth atomic.Pointer[liveAuth]
}

type liveAuth struct {
	typ     string
	global  string
	keys    []config.APIKeyEntry
	oidcMW  echo.MiddlewareFunc
	oidcIss string
}

func (s *Server) SetJanitorScheduler(sched *janitor.Scheduler) {
	s.janitorSched = sched
}

func NewServer(
	cfg *config.Config,
	pipe *pipeline.Pipeline,
	vectors *vectorstore.Service,
	cstore *comments.Store,
	shares *rbac.ShareStore,
	lr *links.Resolver,
) *Server {
	s := &Server{
		cfg:          cfg,
		pipe:         pipe,
		vectors:      vectors,
		comments:     cstore,
		shares:       shares,
		linkResolver: lr,
		echo:         echo.New(),
	}
	s.echo.HideBanner = true
	s.echo.HidePort = true
	s.echo.HTTPErrorHandler = sanitizingErrorHandler
	s.setupMiddleware()
	s.setupRoutes()
	return s
}

func sanitizingErrorHandler(err error, c echo.Context) {
	if c.Response().Committed {
		return
	}
	code := http.StatusInternalServerError
	var public any = "internal server error"
	if he, ok := err.(*echo.HTTPError); ok {
		code = he.Code
		if code < 500 {
			public = he.Message
		} else {
			log.Printf("api 5xx %s %s: %v", c.Request().Method, c.Request().URL.Path, he.Message)
		}
	} else {
		log.Printf("api error %s %s: %v", c.Request().Method, c.Request().URL.Path, err)
	}
	if c.Request().Method == http.MethodHead {
		_ = c.NoContent(code)
		return
	}
	_ = c.JSON(code, map[string]any{"error": public})
}

func (s *Server) Hub() *events.Hub { return s.pipe.Hub }

func (s *Server) setupMiddleware() {
	s.echo.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "${time_rfc3339} ${method} ${uri} ${status} ${latency_human} ${bytes_in}b in ${bytes_out}b out\n",
		Skipper: func(c echo.Context) bool {
			p := c.Path()
			return p == "/health" || p == "/healthz" || p == "/readyz" || p == "/metrics"
		},
	}))
	s.echo.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOriginFunc: s.corsOriginAllowed,
		AllowMethods:    []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodOptions},
		AllowHeaders:    []string{"Content-Type", "Authorization", "If-Match", "X-Actor", "X-Provenance"},
		ExposeHeaders:   []string{"ETag", "Last-Modified", "X-Permalink"},
	}))
	s.echo.Use(middleware.Recover())
	s.echo.Use(middleware.BodyLimit("110M"))
	s.echo.Use(middleware.RateLimiterWithConfig(middleware.RateLimiterConfig{
		Skipper: func(c echo.Context) bool {
			p := c.Path()
			return p == "/health" || p == "/healthz" || p == "/readyz" || p == "/metrics"
		},
		Store: middleware.NewRateLimiterMemoryStoreWithConfig(middleware.RateLimiterMemoryStoreConfig{
			Rate:      100,
			Burst:     200,
			ExpiresIn: 3 * time.Minute,
		}),
		IdentifierExtractor: func(c echo.Context) (string, error) {
			return c.RealIP(), nil
		},
		DenyHandler: func(c echo.Context, _ string, _ error) error {
			return echo.NewHTTPError(http.StatusTooManyRequests, "rate limit exceeded")
		},
	}))
}

func (s *Server) authMiddleware() echo.MiddlewareFunc {
	s.installAuth(&s.cfg.Auth)
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			la := s.auth.Load()
			if la == nil {
				return next(c)
			}
			switch la.typ {
			case "apikey":
				if la.global == "" {
					return next(c)
				}
				return apiKeyHandler(la.global)(next)(c)
			case "perspace":
				if len(la.keys) == 0 {
					return next(c)
				}
				return perSpaceKeyHandler(la.keys)(next)(c)
			case "oidc":
				if la.oidcMW == nil {
					return next(c)
				}
				return la.oidcMW(next)(c)
			}
			return next(c)
		}
	}
}

func (s *Server) ReloadAuth(cfg *config.AuthConfig) {
	s.installAuth(cfg)
	log.Printf("auth: reloaded (type=%s)", cfg.Type)
}

func (s *Server) installAuth(cfg *config.AuthConfig) {
	next := &liveAuth{typ: cfg.Type, global: cfg.APIKey, keys: cfg.APIKeys, oidcIss: cfg.OIDC.Issuer}
	if cur := s.auth.Load(); cur != nil && cur.oidcIss == next.oidcIss && cur.oidcMW != nil {
		next.oidcMW = cur.oidcMW
	} else if cfg.Type == "oidc" && cfg.OIDC.Issuer != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		p, err := oidc.NewProvider(ctx, cfg.OIDC.Issuer)
		if err != nil {
			log.Printf("warning: OIDC provider setup failed (%v) — auth disabled", err)
		} else {
			verifier := p.Verifier(&oidc.Config{ClientID: cfg.OIDC.ClientID})
			next.oidcMW = oidcMiddleware(verifier)
		}
	}
	s.auth.Store(next)
}

func (s *Server) setupRoutes() {
	// Build the dataview executor, auto-indexer, and view registry if the
	// search backend is SQLite.
	var dvExec *dataview.Executor
	var viewReg *dataview.Registry
	if sq, ok := s.pipe.Searcher.(*search.SQLite); ok {
		readDB := sq.ReadDB()
		writeDB := sq.WriteDB()
		dvExec = dataview.NewExecutor(readDB)
		ai := dataview.NewAutoIndexer(writeDB, readDB, s.cfg.Dataview.MaxAutoIndexes)
		dvExec.SetAutoIndexer(ai)

		timeout, _ := time.ParseDuration(s.cfg.Dataview.QueryTimeout)
		if timeout == 0 {
			timeout = 5 * time.Second
		}
		maxRows := s.cfg.Dataview.MaxScanRows
		if maxRows == 0 {
			maxRows = 10000
		}
		dvExec.SetLimits(maxRows, timeout)

		viewReg = dataview.NewRegistry(dvExec, s.pipe.Store)
		_ = viewReg.Scan(context.Background())
	}

	h := &Handlers{
		store:            s.pipe.Store,
		versioner:        s.pipe.Versioner,
		searcher:         s.pipe.Searcher,
		linker:           s.pipe.Linker,
		hub:              s.pipe.Hub,
		pipe:             s.pipe,
		vectors:          s.vectors,
		dv:               dvExec,
		viewReg:          viewReg,
		comments:         s.comments,
		shares:           s.shares,
		assets:           s.cfg.Assets,
		ui:               s.cfg.UI,
		root:             s.pipe.Store.AbsPath(""),
		janitorSched:         s.janitorSched,
		janitorStaleDays:     s.cfg.Janitor.StaleDays,
		memoryEpisodesPrefix: s.cfg.Memory.EpisodesPathPrefix,
		publicURL:            s.cfg.ResolvedPublicURL(),
		linkResolver:         s.linkResolver,
	}
	prev := s.pipe.OnInvalidate
	s.pipe.OnInvalidate = func() {
		if prev != nil {
			prev()
		}
		h.invalidateGraphCache()
	}

	if viewReg != nil {
		s.pipe.OnPathChange = func(path string) {
			viewReg.OnWrite(path)
		}
	}

	s.echo.GET("/health", h.Health)
	s.echo.GET("/healthz", h.Healthz)
	s.echo.GET("/readyz", h.Readyz)
	s.echo.GET("/metrics", h.Metrics)

	api := s.echo.Group("/api/kiwi")
	if mw := s.authMiddleware(); mw != nil {
		api.Use(mw)
	}
	api.GET("/changes", h.Changes)
	api.GET("/tree", h.Tree)
	api.GET("/file", h.ReadFile)
	api.GET("/readlink", h.Readlink)
	api.PUT("/file", h.WriteFile)
	api.DELETE("/file", h.DeleteFile)
	api.POST("/rename", h.RenameFile)
	api.POST("/rename-dir", h.RenameDir)
	api.POST("/bulk", h.BulkWrite)
	api.POST("/file/append", h.AppendFile)
	api.POST("/assets", h.UploadAsset)
	api.POST("/resolve-links", h.ResolveLinks)
	api.GET("/search", h.Search)
	api.GET("/search/verified", h.VerifiedSearch)
	api.POST("/search/semantic", h.SemanticSearch)
	api.GET("/search/semantic", h.SemanticSearch)
	api.GET("/meta", h.Meta)
	api.GET("/stale", h.StalePages)
	api.GET("/contradictions", h.Contradictions)
	api.GET("/versions", h.Versions)
	api.GET("/version", h.Version)
	api.GET("/diff", h.Diff)
	api.GET("/blame", h.Blame)
	api.GET("/events", h.Events)
	api.GET("/backlinks", h.Backlinks)
	api.GET("/graph", h.Graph)
	api.GET("/toc", h.ToC)
	api.GET("/templates", h.ListTemplates)
	api.GET("/template", h.ReadTemplate)
	api.GET("/comments", h.ListComments)
	api.POST("/comments", h.AddComment)
	api.DELETE("/comments/:id", h.DeleteComment)
	api.PATCH("/comments/:id", h.ResolveComment)
	api.GET("/theme", h.GetTheme)
	api.PUT("/theme", h.PutTheme)
	api.GET("/ui-config", h.UIConfig)
	api.GET("/janitor", h.Janitor)
	api.GET("/memory/report", h.MemoryReport)
	api.GET("/query", h.Query)
	api.GET("/query/aggregate", h.QueryAggregate)
	api.POST("/view/refresh", h.ViewRefresh)
	api.POST("/import", h.Import)
	api.GET("/export", h.Export)
	api.GET("/analytics", h.Analytics)
	api.GET("/health-check", h.HealthCheck)

	api.POST("/share", h.CreateShareLink)
	api.GET("/share", h.ListShareLinks)
	api.DELETE("/share/:id", h.RevokeShareLink)

	s.echo.GET("/api/kiwi/public/:token", h.PublicPage)
	s.echo.GET("/api/kiwi/public/file", h.PublicFile)
	s.echo.GET("/api/kiwi/public/tree", h.PublicTree)

	s.echo.GET("/raw/*", h.ServeRawFile)

	uiHandler := webui.Handler()
	s.echo.GET("/", uiHandler)
	s.echo.GET("/*", uiHandler)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.echo.ServeHTTP(w, r)
}

func (s *Server) Start(addr string) error {
	if s.janitorSched != nil {
		ctx, cancel := context.WithCancel(context.Background())
		s.janitorCancel = cancel
		s.janitorSched.Start(ctx)
	}
	return s.echo.Start(addr)
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.janitorCancel != nil {
		s.janitorCancel()
	}
	if s.janitorSched != nil {
		s.janitorSched.Stop()
	}
	return s.echo.Shutdown(ctx)
}

func (s *Server) corsOriginAllowed(origin string) (bool, error) {
	if isLoopbackOrigin(origin) {
		return true, nil
	}
	if s.cfg.Auth.Type == "" || s.cfg.Auth.Type == "none" {
		return false, nil
	}
	if len(s.cfg.Server.CORSOrigins) > 0 {
		for _, allowed := range s.cfg.Server.CORSOrigins {
			if origin == allowed {
				return true, nil
			}
		}
		return false, nil
	}
	return true, nil
}

func isLoopbackOrigin(origin string) bool {
	for _, p := range []string{
		"http://localhost", "https://localhost",
		"http://127.0.0.1", "https://127.0.0.1",
		"http://[::1]", "https://[::1]",
	} {
		if origin == p || strings.HasPrefix(origin, p+":") {
			return true
		}
	}
	return false
}

func apiKeyMiddleware(key string) echo.MiddlewareFunc {
	return apiKeyHandler(key)
}

func apiKeyHandler(key string) echo.MiddlewareFunc {
	expected := []byte("Bearer " + key)
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			got := []byte(c.Request().Header.Get("Authorization"))
			if subtle.ConstantTimeCompare(got, expected) != 1 {
				return echo.NewHTTPError(http.StatusUnauthorized, "invalid API key")
			}
			return next(c)
		}
	}
}

func perSpaceKeyMiddleware(keys []config.APIKeyEntry) echo.MiddlewareFunc {
	return perSpaceKeyHandler(keys)
}

func perSpaceKeyHandler(keys []config.APIKeyEntry) echo.MiddlewareFunc {
	type entry struct {
		hash  [32]byte
		space string
		actor string
	}
	km := make(map[[32]byte]entry, len(keys))
	for _, k := range keys {
		h := sha256.Sum256([]byte(k.Key))
		km[h] = entry{hash: h, space: k.Space, actor: k.Actor}
	}
	inScope := func(space, path string) bool {
		if space == "" || path == "" {
			return true
		}
		return path == space || strings.HasPrefix(path, space+"/")
	}
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			auth := c.Request().Header.Get("Authorization")
			raw, ok := strings.CutPrefix(auth, "Bearer ")
			if !ok || raw == "" {
				return echo.NewHTTPError(http.StatusUnauthorized, "missing bearer token")
			}
			incoming := sha256.Sum256([]byte(raw))
			e, ok := km[incoming]
			if !ok || subtle.ConstantTimeCompare(incoming[:], e.hash[:]) != 1 {
				return echo.NewHTTPError(http.StatusUnauthorized, "invalid API key")
			}
			if e.space != "" {
				if !inScope(e.space, c.QueryParam("path")) {
					return echo.NewHTTPError(http.StatusForbidden, "path outside key scope")
				}
				if c.Request().Method == http.MethodPost && strings.HasSuffix(c.Path(), "/bulk") {
					body, err := io.ReadAll(c.Request().Body)
					if err != nil {
						return echo.NewHTTPError(http.StatusBadRequest, "failed to read body")
					}
					c.Request().Body = io.NopCloser(bytes.NewReader(body))
					var parsed struct {
						Files []struct {
							Path string `json:"path"`
						} `json:"files"`
					}
					if err := json.Unmarshal(body, &parsed); err == nil {
						for _, f := range parsed.Files {
							if !inScope(e.space, f.Path) {
								return echo.NewHTTPError(http.StatusForbidden, "bulk path outside key scope")
							}
						}
					}
				}
			}
			c.Request().Header.Set("X-Actor", e.actor)
			if e.space != "" {
				c.Request().Header.Set("X-Space", e.space)
			}
			return next(c)
		}
	}
}

func oidcMiddleware(verifier *oidc.IDTokenVerifier) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			auth := c.Request().Header.Get("Authorization")
			raw, ok := strings.CutPrefix(auth, "Bearer ")
			if !ok || raw == "" {
				return echo.NewHTTPError(http.StatusUnauthorized, "missing bearer token")
			}
			token, err := verifier.Verify(c.Request().Context(), raw)
			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, "invalid token")
			}
			var claims struct {
				Email string `json:"email"`
				Sub   string `json:"sub"`
			}
			if err := token.Claims(&claims); err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, "invalid claims")
			}
			actor := claims.Email
			if actor == "" {
				actor = claims.Sub
			}
			c.Request().Header.Set("X-Actor", actor)
			return next(c)
		}
	}
}
