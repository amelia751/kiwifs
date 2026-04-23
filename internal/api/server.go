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
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/kiwifs/kiwifs/internal/comments"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/vectorstore"
	"github.com/kiwifs/kiwifs/internal/webui"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// Server is the KiwiFS HTTP server. Dependencies that handlers need for
// read operations (store, versioner, searcher, linker, hub) all live on the
// Pipeline — storing them again here would be duplication that drifts when
// the bootstrap wiring changes.
type Server struct {
	cfg      *config.Config
	pipe     *pipeline.Pipeline
	vectors  *vectorstore.Service // nil when vector search is disabled
	comments *comments.Store
	echo     *echo.Echo
}

// NewServer creates and configures the server. The pipeline carries every
// shared dependency (store, versioner, searcher, linker, hub) — callers
// don't need to pass them separately.
func NewServer(
	cfg *config.Config,
	pipe *pipeline.Pipeline,
	vectors *vectorstore.Service,
	cstore *comments.Store,
) *Server {
	s := &Server{
		cfg:      cfg,
		pipe:     pipe,
		vectors:  vectors,
		comments: cstore,
		echo:     echo.New(),
	}
	s.echo.HideBanner = true
	s.echo.HidePort = true
	s.echo.HTTPErrorHandler = sanitizingErrorHandler
	s.setupMiddleware()
	s.setupRoutes()
	return s
}

// sanitizingErrorHandler keeps internal error details out of HTTP responses.
// Storage errors carry absolute paths, git errors carry shell output, and
// SQLite errors quote SQL fragments — none of which clients should see.
// 4xx messages are always client-relevant ("path is required", "invalid
// JSON body") so they pass through unchanged; 5xx and uncategorised
// errors are replaced with a generic message and logged with the real
// cause for operators.
func sanitizingErrorHandler(err error, c echo.Context) {
	if c.Response().Committed {
		return
	}
	code := http.StatusInternalServerError
	var public any = "internal server error"
	if he, ok := err.(*echo.HTTPError); ok {
		code = he.Code
		if code < 500 {
			// Client errors are safe to return verbatim — they describe
			// what the caller did wrong, not what the server contains.
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

// Hub returns the shared SSE hub so alt-protocol servers can broadcast
// through the same channel REST clients subscribe to.
func (s *Server) Hub() *events.Hub { return s.pipe.Hub }

func (s *Server) setupMiddleware() {
	// Request logging goes first so it captures the full lifecycle
	// (including panics caught by Recover). The Skipper omits /health to
	// avoid log noise from LB probes hitting every 10-30s — standard
	// practice for Kubernetes/ALB/ECS health checks.
	s.echo.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "${time_rfc3339} ${method} ${uri} ${status} ${latency_human} ${bytes_in}b in ${bytes_out}b out\n",
		Skipper: func(c echo.Context) bool {
			return c.Path() == "/health"
		},
	}))
	// CORS: when auth=none we restrict to localhost so a random webpage on
	// the internet can't make full CRUD calls to a developer's server bound
	// to 0.0.0.0. With any auth backend configured the API is gated by a
	// bearer/cookie anyway, so a permissive wildcard is acceptable there
	// (matching how most authenticated SaaS APIs ship). The dynamic check
	// runs once per OPTIONS preflight so the perf cost is negligible.
	s.echo.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOriginFunc: s.corsOriginAllowed,
		AllowMethods:    []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodOptions},
		AllowHeaders:    []string{"Content-Type", "Authorization", "If-Match", "X-Actor", "X-Provenance"},
		ExposeHeaders:   []string{"ETag", "Last-Modified"},
	}))
	s.echo.Use(middleware.Recover())
	// Cap request bodies so a single malicious PUT / bulk write can't OOM
	// the server. 32MB is large enough for realistic knowledge files
	// (markdown) plus bulk batches of them, but well below available RAM.
	s.echo.Use(middleware.BodyLimit("32M"))
	// Per-client rate limit. Even with auth=none the BodyLimit alone
	// can't stop a client from flooding writes — each one triggers a
	// git commit + full-text + vector index — so cap the sustained
	// rate per IP. The Skipper keeps /health unlimited so LB probes
	// can't ever trip the limiter (a shared NAT could otherwise push
	// the health endpoint into 429 and make the pod look unhealthy).
	s.echo.Use(middleware.RateLimiterWithConfig(middleware.RateLimiterConfig{
		Skipper: func(c echo.Context) bool {
			return c.Path() == "/health"
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

// authMiddleware builds the configured authentication middleware or
// returns nil when auth is off. Callers apply it to /api/kiwi only —
// /health must stay reachable to LB probes (a token-gated health check
// defeats the point of a health check), and the SPA catch-all serves
// the static UI bundle which is the login entrypoint rather than a
// protected resource.
func (s *Server) authMiddleware() echo.MiddlewareFunc {
	switch s.cfg.Auth.Type {
	case "apikey":
		if s.cfg.Auth.APIKey != "" {
			return apiKeyMiddleware(s.cfg.Auth.APIKey)
		}
	case "perspace":
		if len(s.cfg.Auth.APIKeys) > 0 {
			return perSpaceKeyMiddleware(s.cfg.Auth.APIKeys)
		}
	case "oidc":
		if s.cfg.Auth.OIDC.Issuer != "" {
			p, err := oidc.NewProvider(context.Background(), s.cfg.Auth.OIDC.Issuer)
			if err != nil {
				log.Printf("warning: OIDC provider setup failed (%v) — auth disabled", err)
				return nil
			}
			verifier := p.Verifier(&oidc.Config{ClientID: s.cfg.Auth.OIDC.ClientID})
			return oidcMiddleware(verifier)
		}
	}
	return nil
}

func (s *Server) setupRoutes() {
	h := &Handlers{
		store:     s.pipe.Store,
		versioner: s.pipe.Versioner,
		searcher:  s.pipe.Searcher,
		linker:    s.pipe.Linker,
		hub:       s.pipe.Hub,
		pipe:      s.pipe,
		vectors:   s.vectors,
		comments:  s.comments,
		assets:    s.cfg.Assets,
		root:      s.pipe.Store.AbsPath(""),
	}
	// Chain cache invalidation onto the pipeline's fan-out so any write —
	// REST, WebDAV, NFS, S3, fsnotify — drops the /graph cache. Chained
	// rather than overwritten so if another layer sets OnInvalidate first,
	// both hooks run.
	prev := s.pipe.OnInvalidate
	s.pipe.OnInvalidate = func() {
		if prev != nil {
			prev()
		}
		h.invalidateGraphCache()
	}

	// /health stays outside any auth middleware so LB probes can reach it.
	s.echo.GET("/health", h.Health)

	api := s.echo.Group("/api/kiwi")
	if mw := s.authMiddleware(); mw != nil {
		api.Use(mw)
	}
	api.GET("/tree", h.Tree)
	api.GET("/file", h.ReadFile)
	api.PUT("/file", h.WriteFile)
	api.DELETE("/file", h.DeleteFile)
	api.POST("/bulk", h.BulkWrite)
	api.POST("/assets", h.UploadAsset)
	api.GET("/search", h.Search)
	api.POST("/search/semantic", h.SemanticSearch)
	api.GET("/search/semantic", h.SemanticSearch)
	api.GET("/meta", h.Meta)
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

	// Embedded UI — must be last so it acts as a catch-all SPA fallback.
	// /api/* and /health are matched above this.
	uiHandler := webui.Handler()
	s.echo.GET("/", uiHandler)
	s.echo.GET("/*", uiHandler)
}

// ServeHTTP lets the server act as an http.Handler, used by the multi-space
// manager to forward requests into a space's fully-configured Echo instance.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.echo.ServeHTTP(w, r)
}

func (s *Server) Start(addr string) error {
	return s.echo.Start(addr)
}

// Shutdown closes the HTTP server gracefully, waiting for in-flight
// requests to finish (bounded by the caller's ctx deadline).
func (s *Server) Shutdown(ctx context.Context) error {
	return s.echo.Shutdown(ctx)
}

// corsOriginAllowed decides whether a CORS Origin should be echoed back as
// allowed. Loopback origins always pass — that's the dev-server case the
// catch-all wildcard used to cover. Beyond that, anonymous (auth=none)
// installs reject everything else, while authenticated installs default to
// permissive because the bearer/cookie is the real gate.
func (s *Server) corsOriginAllowed(origin string) (bool, error) {
	if isLoopbackOrigin(origin) {
		return true, nil
	}
	if s.cfg.Auth.Type == "" || s.cfg.Auth.Type == "none" {
		return false, nil
	}
	return true, nil
}

// isLoopbackOrigin matches http(s)://localhost or 127.0.0.1 (any port).
// Plain prefix matching is enough — Origin headers are normalised by the
// browser to "<scheme>://<host>[:port]", with no path.
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
	expected := []byte("Bearer " + key)
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// subtle.ConstantTimeCompare avoids a timing side-channel that
			// a plain `auth != "Bearer "+key` leaks: byte-wise string
			// comparison short-circuits at the first mismatch, so an
			// attacker can iteratively measure response time to recover
			// the key one character at a time.
			got := []byte(c.Request().Header.Get("Authorization"))
			if subtle.ConstantTimeCompare(got, expected) != 1 {
				return echo.NewHTTPError(http.StatusUnauthorized, "invalid API key")
			}
			return next(c)
		}
	}
}

func perSpaceKeyMiddleware(keys []config.APIKeyEntry) echo.MiddlewareFunc {
	type entry struct {
		hash  [32]byte
		space string
		actor string
	}
	// Hash-then-compare: keys are indexed by their SHA-256 hash so the
	// map lookup happens on fixed-length values, and the verification
	// step is a ConstantTimeCompare on the hash. A plain map[string] keyed
	// by the raw token leaks "is this prefix valid" through the lookup
	// time it takes Go to walk the bucket — apiKeyMiddleware avoided this
	// by comparing constant-time, but the per-space variant skipped that
	// step until now.
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
			// Even on hash hit, do a constant-time compare on the full
			// digest so a future change that loosens the lookup (say to
			// O(N) iteration) doesn't silently reintroduce a timing
			// channel via the verification step.
			if !ok || subtle.ConstantTimeCompare(incoming[:], e.hash[:]) != 1 {
				return echo.NewHTTPError(http.StatusUnauthorized, "invalid API key")
			}
			if e.space != "" {
				// Scope check #1: reject cross-space access when the
				// path arrives in the query string (most endpoints use
				// ?path=...).
				if !inScope(e.space, c.QueryParam("path")) {
					return echo.NewHTTPError(http.StatusForbidden, "path outside key scope")
				}
				// Scope check #2: bulk writes carry per-file paths in
				// the JSON body, not as query params. The previous
				// implementation only checked ?path=, so a per-space
				// key could POST /api/kiwi/bulk with files targeting
				// any space — trivial cross-tenant write access. Peek
				// the body, validate every path, then splice the body
				// back so the handler still reads a full stream.
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
