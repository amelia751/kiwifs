package spaces

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/kiwifs/kiwifs/internal/api"
	"github.com/kiwifs/kiwifs/internal/bootstrap"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/labstack/echo/v4"
)

// Manager manages multiple independent knowledge spaces.
// Each space has its own storage, versioner, searcher, and pipeline.
type Manager struct {
	spaces map[string]*Space
	// order preserves registration order so resolveSpace can pick a
	// deterministic default (the first space registered) instead of
	// whichever Go map iteration happened to return.
	order []string
	mu    sync.RWMutex
}

// Space represents a single knowledge space with its own backend.
// Server handles REST dispatch; Stack is exposed so the alt-protocol
// servers (S3 buckets, future NFS sub-mounts) can pull the per-space
// Store and Pipeline without going through bootstrap.Build a second
// time. nil Stack signals "we don't own teardown for this space" — set
// for the default space registered via RegisterServer with externally
// managed lifetime.
type Space struct {
	Name   string
	Root   string
	Server *api.Server
	Stack  *bootstrap.Stack

	// ownStack is true when the manager built the stack itself
	// (AddSpace) and should Close() it on shutdown. RegisterStack
	// callers retain ownership of the lifetime — typical for the
	// default space that already has its own defer chain in serve.go.
	ownStack bool
}

// NewManager creates a new space manager.
func NewManager() *Manager {
	return &Manager{
		spaces: make(map[string]*Space),
	}
}

// AddSpace registers a new space. The whole dependency stack is built by
// bootstrap.Build so adding a space picks up the same error-handling policy
// (git → Noop fallback, CoW MaxVersions, vector reindex) as the default
// space wired up by the serve command.
func (m *Manager) AddSpace(name, root string, cfg *config.Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.spaces[name]; exists {
		return fmt.Errorf("space %q already exists", name)
	}

	stack, err := bootstrap.Build(name, root, cfg)
	if err != nil {
		return err
	}

	m.spaces[name] = &Space{
		Name:     name,
		Root:     root,
		Server:   stack.Server,
		Stack:    stack,
		ownStack: true,
	}
	m.order = append(m.order, name)
	return nil
}

// RegisterStack registers a pre-built bootstrap.Stack under a space
// name. Used by serve.go for the default space, where the stack was
// already wired up in single-space style — registering it here lets
// alt-protocol servers (S3) reach the same Store and Pipeline as the
// REST handler. Stack lifetime stays with the original owner; the
// manager doesn't Close it on its own teardown.
func (m *Manager) RegisterStack(name, root string, stack *bootstrap.Stack) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.spaces[name]; exists {
		return fmt.Errorf("space %q already exists", name)
	}
	m.spaces[name] = &Space{Name: name, Root: root, Server: stack.Server, Stack: stack}
	m.order = append(m.order, name)
	return nil
}

// RegisterServer registers a pre-built api.Server only — used by tests
// that exercise routing without the full dependency graph.
func (m *Manager) RegisterServer(name, root string, server *api.Server) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.spaces[name]; exists {
		return fmt.Errorf("space %q already exists", name)
	}
	m.spaces[name] = &Space{Name: name, Root: root, Server: server}
	m.order = append(m.order, name)
	return nil
}

// GetSpace retrieves a space by name.
func (m *Manager) GetSpace(name string) (*Space, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	space, ok := m.spaces[name]
	return space, ok
}

// Close tears down every space's stack. Returns the first error but keeps
// going so one broken space doesn't leak the others' sqlite/vector handles.
//
// Skips spaces registered via RegisterStack (Stack lifetime owned by the
// caller — typically the serve command's defer chain) and spaces
// registered via RegisterServer (no stack at all).
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for _, name := range m.order {
		sp, ok := m.spaces[name]
		if !ok || sp.Stack == nil || !sp.ownStack {
			continue
		}
		if err := sp.Stack.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ListSpaces returns all registered space names in registration order.
func (m *Manager) ListSpaces() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, len(m.order))
	copy(out, m.order)
	return out
}

// Handler returns an HTTP handler that routes multi-space requests.
// Requests to /api/kiwi/{space}/... are rewritten to /api/kiwi/... and
// forwarded to the resolved space's fully-configured api.Server. Requests
// without a space segment go to the first registered (default) space.
func (m *Manager) Handler() http.Handler {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})

	e.GET("/api/spaces", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]interface{}{
			"spaces": m.ListSpaces(),
		})
	})

	// Catch-all: resolve space, rewrite path, forward to the space's server.
	e.Any("/*", func(c echo.Context) error {
		space := m.resolveSpace(c.Request())
		if space == nil {
			return echo.NewHTTPError(http.StatusNotFound, "no space configured")
		}
		space.Server.ServeHTTP(c.Response(), c.Request())
		return nil
	})

	return e
}

// resolveSpace picks the target space from the URL. If the path matches
// /api/kiwi/{space}/... and {space} is a registered name, the request URL
// is rewritten to strip the space segment so the downstream api.Server
// sees normal /api/kiwi/... routes. Unmatched paths fall through to the
// first registered (default) space.
func (m *Manager) resolveSpace(r *http.Request) *Space {
	path := r.URL.Path
	const prefix = "/api/kiwi/"
	if strings.HasPrefix(path, prefix) {
		rest := strings.TrimPrefix(path, prefix)
		if idx := strings.IndexByte(rest, '/'); idx > 0 {
			candidate := rest[:idx]
			if space, ok := m.GetSpace(candidate); ok {
				r.URL.Path = prefix + rest[idx+1:]
				return space
			}
		} else if rest != "" {
			if space, ok := m.GetSpace(rest); ok {
				r.URL.Path = prefix
				return space
			}
		}
	}
	// Default: first registered space. Using m.order (not a bare map
	// iteration) keeps the default deterministic — otherwise every
	// restart could route "/api/kiwi/..." to a different space.
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, name := range m.order {
		if sp, ok := m.spaces[name]; ok {
			return sp
		}
	}
	return nil
}
