// Package webui serves the embedded React UI at /.
//
// The go:embed directive has to live at or above ui/dist in the file tree,
// so the actual embed lives in the main package (see ui_assets.go at the
// module root). This package accepts an fs.FS from main via SetAssets() and
// exposes an Echo handler.
package webui

import (
	"io/fs"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
)

var assets fs.FS

// SetAssets wires up the embedded UI filesystem. It must be called once at
// startup, before the API server starts. If never called, Handler() returns
// a "UI not built" page so at least the user sees a helpful error.
func SetAssets(f fs.FS) {
	assets = f
}

// Handler returns an Echo handler that serves the UI. Any path that isn't a
// real asset falls back to index.html so client-side routing keeps working.
// /api/* and /health are carved out by Echo before this handler runs.
func Handler() echo.HandlerFunc {
	if assets == nil {
		return notBuiltHandler("no UI assets registered (SetAssets was not called)")
	}
	if !hasIndex(assets) {
		return notBuiltHandler("ui/dist/index.html not found — run `cd ui && npm run build`")
	}

	fileServer := http.FileServer(http.FS(assets))
	indexBytes, _ := fs.ReadFile(assets, "index.html")

	return func(c echo.Context) error {
		req := c.Request()
		path := strings.TrimPrefix(req.URL.Path, "/")

		if path != "" && exists(assets, path) {
			fileServer.ServeHTTP(c.Response(), req)
			return nil
		}

		c.Response().Header().Set("Content-Type", "text/html; charset=utf-8")
		c.Response().Header().Set("Cache-Control", "no-cache")
		c.Response().WriteHeader(http.StatusOK)
		_, err := c.Response().Write(indexBytes)
		return err
	}
}

func hasIndex(f fs.FS) bool {
	_, err := fs.Stat(f, "index.html")
	return err == nil
}

func exists(f fs.FS, path string) bool {
	if path == "" {
		return false
	}
	info, err := fs.Stat(f, path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func notBuiltHandler(reason string) echo.HandlerFunc {
	body := `<!doctype html>
<html><head><meta charset="utf-8"><title>KiwiFS — UI not built</title>
<style>body{font-family:ui-sans-serif,system-ui;max-width:640px;margin:60px auto;padding:0 24px;color:#222}
code{background:#f3f4f6;padding:2px 6px;border-radius:4px}
.reason{color:#666;font-family:ui-monospace,monospace;font-size:13px;margin-top:8px}</style>
</head><body>
<h1>KiwiFS</h1>
<p>The web UI isn't bundled into this binary yet.</p>
<p>From the repo root:</p>
<pre><code>cd ui &amp;&amp; npm install &amp;&amp; npm run build
cd .. &amp;&amp; go build -o kiwifs .</code></pre>
<p class="reason">Reason: ` + htmlEscape(reason) + `</p>
<p>The REST API is still available under <code>/api/kiwi/*</code>.</p>
</body></html>`
	return func(c echo.Context) error {
		c.Response().Header().Set("Content-Type", "text/html; charset=utf-8")
		return c.String(http.StatusOK, body)
	}
}

func htmlEscape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\"", "&quot;")
	return r.Replace(s)
}
