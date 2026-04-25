package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/markdown"
)

// TestMetaEndpoint covers the happy path: write a markdown file with
// frontmatter, then GET /api/kiwi/meta with a matching where clause.
func TestMetaEndpoint(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	body := "---\nstatus: published\npriority: high\n---\n# Hi\n"
	mustPutFile(t, s, "doc.md", body)

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/meta?where=$.status=published", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /meta: %d %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Count   int `json:"count"`
		Results []struct {
			Path        string                 `json:"path"`
			Frontmatter map[string]interface{} `json:"frontmatter"`
		} `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v (body=%s)", err, rec.Body.String())
	}
	if out.Count != 1 || out.Results[0].Path != "doc.md" {
		t.Fatalf("want 1 result doc.md, got %+v", out)
	}
	if out.Results[0].Frontmatter["priority"] != "high" {
		t.Fatalf("frontmatter missing priority: %+v", out.Results[0].Frontmatter)
	}
}

// TestWriteFileWithProvenance puts a file with X-Provenance and verifies
// (a) the returned file has `derived-from` in its frontmatter and (b) the
// /meta endpoint can find it by run id.
func TestWriteFileWithProvenance(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	// Put a brand-new file (no existing frontmatter) with the header.
	req := httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path=runs/run-249.md", strings.NewReader("# Run 249\n"))
	req.Header.Set("X-Provenance", "run:run-249")
	req.Header.Set("X-Actor", "agent:exec_abc")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body.String())
	}

	// Read it back — frontmatter should now contain a derived-from entry.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=runs/run-249.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET: %d %s", rec.Code, rec.Body.String())
	}
	got := rec.Body.String()
	if !strings.Contains(got, "derived-from:") {
		t.Fatalf("derived-from missing from body:\n%s", got)
	}
	if !strings.Contains(got, "id: run-249") {
		t.Fatalf("run id missing from frontmatter:\n%s", got)
	}
	if !strings.Contains(got, "actor: agent:exec_abc") {
		t.Fatalf("actor missing from frontmatter:\n%s", got)
	}

	// Query /meta with the array predicate — this is the provenance lookup
	// downstream clients care about.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/meta?where=$.derived-from[*].id=run-249", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /meta: %d %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Count   int `json:"count"`
		Results []struct {
			Path string `json:"path"`
		} `json:"results"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v (%s)", err, rec.Body.String())
	}
	if out.Count != 1 || out.Results[0].Path != "runs/run-249.md" {
		t.Fatalf("want one result runs/run-249.md, got %+v", out)
	}
}

// TestBulkWriteWithProvenance verifies the bulk path also injects
// provenance into every file in the batch.
func TestBulkWriteWithProvenance(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	body := `{"files":[{"path":"a.md","content":"# A\n"},{"path":"b.md","content":"# B\n"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/bulk", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Provenance", "run:run-777")
	req.Header.Set("X-Actor", "agent:batch")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /bulk: %d %s", rec.Code, rec.Body.String())
	}

	// Both files should match the run-777 query.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/meta?where=$.derived-from[*].id=run-777", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /meta: %d %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Count != 2 {
		t.Fatalf("want 2 results, got %d (%s)", out.Count, rec.Body.String())
	}
}

// TestMetaEndpointGrepReturns501 — the grep backend doesn't implement
// QueryMeta, so /meta must respond 501 Not Implemented rather than returning
// stale empty results or crashing.
func TestMetaEndpointGrepReturns501(t *testing.T) {
	s := buildTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/meta?where=$.status=published", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 with grep searcher, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestWriteFileRejectsOversizeBody(t *testing.T) {
	s := buildTestServer(t)
	// 33MB body — above the 32MB cap.
	body := bytes.Repeat([]byte("x"), 33<<20)
	req := httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path=big.md", bytes.NewReader(body))
	req.Header.Set("Content-Type", "text/markdown")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge && rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 413/400 for oversize body, got %d", rec.Code)
	}
}

// TestWriteFileIfMatchConflictUnderRace pounds the same path with N
// concurrent writers all sending the same stale If-Match. Without the
// in-pipeline ETag check, every writer's TOCTOU passes and they all return
// 200; with the fix exactly one wins and the rest get 409.
func TestWriteFileIfMatchConflictUnderRace(t *testing.T) {
	s := buildTestServer(t)

	// Seed the file so callers have a real ETag to send.
	req := httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path=race.md", strings.NewReader("v0\n"))
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("seed PUT: %d %s", rec.Code, rec.Body.String())
	}
	etag := rec.Header().Get("ETag")

	const writers = 16
	results := make(chan int, writers)
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		i := i
		go func() {
			<-start
			body := strings.NewReader("v" + string(rune('A'+i)) + "\n")
			req := httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path=race.md", body)
			req.Header.Set("If-Match", etag)
			rec := httptest.NewRecorder()
			s.echo.ServeHTTP(rec, req)
			results <- rec.Code
		}()
	}
	close(start)

	wins, conflicts, other := 0, 0, 0
	for i := 0; i < writers; i++ {
		switch <-results {
		case http.StatusOK:
			wins++
		case http.StatusConflict:
			conflicts++
		default:
			other++
		}
	}
	if wins != 1 {
		t.Fatalf("expected exactly 1 winner, got %d wins, %d conflicts, %d other", wins, conflicts, other)
	}
	if conflicts != writers-1 {
		t.Fatalf("expected %d 409 conflicts, got %d (other=%d)", writers-1, conflicts, other)
	}
}

// TestPerSpaceKeyMiddlewareValidates exercises the auth=perspace path:
// a valid bearer must reach the handler, an invalid one must 401, and
// the middleware must stamp X-Actor/X-Space onto the request.
func TestPerSpaceKeyMiddlewareValidates(t *testing.T) {
	dir, pipe, cstore := buildTestPipeline(t)
	cfg := &config.Config{}
	cfg.Storage.Root = dir
	cfg.Auth.Type = "perspace"
	cfg.Auth.APIKeys = []config.APIKeyEntry{
		{Key: "secret-team-a", Space: "team-a", Actor: "alice"},
		{Key: "secret-team-b", Space: "team-b", Actor: "bob"},
	}
	s := NewServer(cfg, pipe, nil, cstore, nil, nil)

	cases := []struct {
		name string
		auth string
		path string
		want int
	}{
		{"valid key, in-scope path", "Bearer secret-team-a", "team-a/note.md", http.StatusOK},
		{"valid key, out-of-scope path", "Bearer secret-team-a", "team-b/note.md", http.StatusForbidden},
		{"invalid key", "Bearer wrong", "team-a/note.md", http.StatusUnauthorized},
		{"missing bearer", "", "team-a/note.md", http.StatusUnauthorized},
		{"prefix-match attempt", "Bearer secret-team-a-extra", "team-a/note.md", http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.NewReader("# hi\n")
			req := httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path="+tc.path, body)
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			rec := httptest.NewRecorder()
			s.echo.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("got %d, want %d (%s)", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

// TestCORSAuthNoneRejectsRemoteOrigin guards the Fix 5 contract: with
// auth=none, only loopback origins get a CORS allow header. Any random
// remote origin must NOT get echoed back, so a webpage on the open
// internet can't poke a developer's server bound to 0.0.0.0.
func TestCORSAuthNoneRejectsRemoteOrigin(t *testing.T) {
	s := buildTestServer(t) // auth=none by default in buildTestServer

	cases := []struct {
		origin string
		want   string // expected Access-Control-Allow-Origin (empty = absent)
	}{
		{"http://localhost:5173", "http://localhost:5173"},
		{"http://127.0.0.1:8080", "http://127.0.0.1:8080"},
		{"https://evil.example.com", ""},
		{"http://attacker.test", ""},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodOptions, "/api/kiwi/file?path=x.md", nil)
		req.Header.Set("Origin", tc.origin)
		req.Header.Set("Access-Control-Request-Method", "GET")
		rec := httptest.NewRecorder()
		s.echo.ServeHTTP(rec, req)
		got := rec.Header().Get("Access-Control-Allow-Origin")
		if got != tc.want {
			t.Fatalf("origin=%q: ACAO=%q, want %q", tc.origin, got, tc.want)
		}
	}
}

// TestErrorHandlerSanitizes5xxButPreserves4xx checks the Fix 6 contract:
// internal errors (5xx) come back as a generic message, while 4xx user
// errors (path is required, invalid JSON) keep their helpful text.
func TestErrorHandlerSanitizes5xxButPreserves4xx(t *testing.T) {
	s := buildTestServer(t)

	t.Run("4xx message preserved", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file", nil) // missing ?path=
		rec := httptest.NewRecorder()
		s.echo.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("want 400, got %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "path is required") {
			t.Fatalf("4xx body should keep helpful message, got: %s", rec.Body.String())
		}
	})

	t.Run("5xx message scrubbed", func(t *testing.T) {
		// /diff with a bogus version handed straight to git produces an
		// internal error whose message would otherwise quote the local
		// git command + repo path.
		req := httptest.NewRequest(http.MethodGet, "/api/kiwi/diff?path=note.md&from=BOGUS&to=ALSO", nil)
		rec := httptest.NewRecorder()
		s.echo.ServeHTTP(rec, req)
		if rec.Code < 500 {
			t.Skipf("noop versioner returned %d, not 5xx — sanitization path not exercised here", rec.Code)
		}
		body := rec.Body.String()
		if strings.Contains(body, "/private") || strings.Contains(body, "/tmp") || strings.Contains(body, t.TempDir()[:5]) {
			t.Fatalf("5xx body leaked filesystem path: %s", body)
		}
		if !strings.Contains(body, "internal server error") {
			t.Fatalf("5xx body should be generic, got: %s", body)
		}
	})
}

func TestHealthEndpoint(t *testing.T) {
	s := buildTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/health returned %d", rec.Code)
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	s := buildTestServer(t)
	// PUT
	req := httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path=note.md", strings.NewReader("# hi\n"))
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body.String())
	}
	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatalf("missing ETag header")
	}
	// GET
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=note.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET: %d", rec.Code)
	}
	if rec.Body.String() != "# hi\n" {
		t.Fatalf("body mismatch: %q", rec.Body.String())
	}
	if rec.Header().Get("ETag") != etag {
		t.Fatalf("ETag mismatch after roundtrip")
	}
}

// SSE end-to-end: a live HTTP server streams responses, unlike
// httptest.ResponseRecorder. Assert the `event:` line precedes `data:`.
func TestSSEEmitsEventField(t *testing.T) {
	s := buildTestServer(t)
	ts := httptest.NewServer(s.echo)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/kiwi/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET events: %v", err)
	}
	defer resp.Body.Close()

	// Give the handler a moment to subscribe, then broadcast.
	time.Sleep(50 * time.Millisecond)
	s.Hub().Broadcast(events.Event{Op: "write", Path: "x.md"})

	reader := bufio.NewReader(resp.Body)
	var sawEvent, sawData bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.HasPrefix(line, "event: write") {
			sawEvent = true
		}
		if strings.HasPrefix(line, "data: ") {
			sawData = true
		}
		if sawEvent && sawData {
			break
		}
	}
	if !sawEvent {
		t.Fatalf("expected `event: write` line in SSE stream")
	}
	if !sawData {
		t.Fatalf("expected `data: ` line in SSE stream")
	}
}

// The SSE stream must emit periodic ":keep-alive" comments so proxies
// (nginx, ALB, CloudFront) don't close idle connections at ~60s.
func TestSSEHeartbeat(t *testing.T) {
	// Speed up the heartbeat so the test doesn't block for 15s.
	orig := sseHeartbeat
	sseHeartbeat = 100 * time.Millisecond
	defer func() { sseHeartbeat = orig }()

	s := buildTestServer(t)
	ts := httptest.NewServer(s.echo)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/kiwi/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET events: %v", err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	sawKeepAlive := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.HasPrefix(line, ":keep-alive") {
			sawKeepAlive = true
			break
		}
	}
	if !sawKeepAlive {
		t.Fatalf("expected `:keep-alive` comment in SSE stream")
	}
}

func TestToCEndpoint(t *testing.T) {
	s := buildTestServer(t)

	body := "# Title\n\ncontent\n\n## Section A\n\n### Sub A1\n"
	mustPutFile(t, s, "doc.md", body)

	req := httptest.NewRequest(http.MethodGet, "/api/kiwi/toc?path=doc.md", nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /toc: %d %s", rec.Code, rec.Body.String())
	}
	var got struct {
		Path     string             `json:"path"`
		Headings []markdown.Heading `json:"headings"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Path != "doc.md" {
		t.Fatalf("path: %q", got.Path)
	}
	if len(got.Headings) != 3 {
		t.Fatalf("want 3 headings, got %d", len(got.Headings))
	}
	want := []markdown.Heading{
		{Level: 1, Text: "Title", Slug: "title"},
		{Level: 2, Text: "Section A", Slug: "section-a"},
		{Level: 3, Text: "Sub A1", Slug: "sub-a1"},
	}
	for i, h := range want {
		if got.Headings[i] != h {
			t.Fatalf("heading %d: got %+v want %+v", i, got.Headings[i], h)
		}
	}

	// 404 on missing path
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/toc?path=nonexistent.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing path: expected 404, got %d", rec.Code)
	}

	// 400 on missing query param
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/toc", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("no path: expected 400, got %d", rec.Code)
	}
}

func TestUploadAssetPNG(t *testing.T) {
	s := buildTestServer(t)
	body, ct := buildMultipart(t, "file", "diagram.png", pngMagic)

	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/assets?path=concepts/", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /assets: %d %s", rec.Code, rec.Body.String())
	}

	var out struct {
		Path        string `json:"path"`
		Markdown    string `json:"markdown"`
		ContentType string `json:"contentType"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Path != "concepts/diagram.png" {
		t.Fatalf("path: %q", out.Path)
	}
	if out.ContentType != "image/png" {
		t.Fatalf("contentType: %q", out.ContentType)
	}
	if !strings.Contains(out.Markdown, "![diagram]") {
		t.Fatalf("markdown missing image syntax: %q", out.Markdown)
	}

	// GET it back — Content-Type header should reflect the sniffed type,
	// not text/markdown.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=concepts/diagram.png", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET asset: %d", rec.Code)
	}
	got := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(got, "image/png") {
		t.Fatalf("content-type: %q", got)
	}
	if !bytes.Equal(rec.Body.Bytes(), pngMagic) {
		t.Fatalf("body mismatch — upload didn't round-trip cleanly")
	}
}

func TestUploadAssetRejectsDisallowedType(t *testing.T) {
	s := buildTestServer(t)
	// A plain ELF-ish magic so http.DetectContentType returns
	// application/octet-stream, which isn't on the default allowlist.
	body, ct := buildMultipart(t, "file", "evil.bin", []byte{0x7f, 'E', 'L', 'F', 0, 0, 0, 0})
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/assets", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("want 415, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestUploadAssetSanitisesFilename(t *testing.T) {
	s := buildTestServer(t)
	// Browser should never send a path like this, but we defend in depth:
	// filepath.Base strips the traversal prefix so the file lands as the
	// basename inside the target directory.
	body, ct := buildMultipart(t, "file", "../../etc/passwd.png", pngMagic)
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/assets?path=safe/", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /assets: %d %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Path != "safe/passwd.png" {
		t.Fatalf("want safe/passwd.png, got %q", out.Path)
	}
}

func TestUploadAssetRejectsOversize(t *testing.T) {
	s := buildTestServerWithAssets(t, config.AssetsConfig{MaxFileSize: "512"})
	// 2048 bytes — 4× the 512-byte cap above.
	large := make([]byte, 2048)
	copy(large, pngMagic)
	body, ct := buildMultipart(t, "file", "big.png", large)

	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/assets", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestReadFileLastModified(t *testing.T) {
	s := buildTestServer(t)

	req := httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path=note.md", strings.NewReader("first\n"))
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT v1: %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=note.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET v1: %d", rec.Code)
	}
	lm1 := rec.Header().Get("Last-Modified")
	if lm1 == "" {
		t.Fatalf("expected Last-Modified header")
	}
	if _, err := http.ParseTime(lm1); err != nil {
		t.Fatalf("Last-Modified not parseable: %q: %v", lm1, err)
	}
	etag1 := rec.Header().Get("ETag")

	// If-None-Match with matching etag → 304.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=note.md", nil)
	req.Header.Set("If-None-Match", etag1)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("If-None-Match: want 304, got %d", rec.Code)
	}

	// If-Modified-Since with the same timestamp → 304.
	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=note.md", nil)
	req.Header.Set("If-Modified-Since", lm1)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("If-Modified-Since: want 304, got %d", rec.Code)
	}

	// Modify the file; Last-Modified must change (or at least not regress).
	// File mtimes have ~1s granularity on many filesystems, so sleep a
	// beat to guarantee a distinct timestamp.
	time.Sleep(1100 * time.Millisecond)
	req = httptest.NewRequest(http.MethodPut, "/api/kiwi/file?path=note.md", strings.NewReader("second line\n"))
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT v2: %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=note.md", nil)
	rec = httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET v2: %d", rec.Code)
	}
	lm2 := rec.Header().Get("Last-Modified")
	if lm2 == lm1 {
		t.Fatalf("Last-Modified did not change after update: %q", lm2)
	}
	t1, _ := http.ParseTime(lm1)
	t2, _ := http.ParseTime(lm2)
	if !t2.After(t1) {
		t.Fatalf("Last-Modified regressed: %q → %q", lm1, lm2)
	}
}

// TestGraphCachingAndInvalidation covers the two guarantees of the graph
// cache: a second request must hit the cached pointer (not recompute), and
// a subsequent write must invalidate so the next /graph sees fresh data.
func TestGraphCachingAndInvalidation(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	mustPutFile(t, s, "a.md", "# a\n")

	first := httptest.NewRecorder()
	s.echo.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/api/kiwi/graph", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first /graph: %d", first.Code)
	}
	// The cache must be populated after the first read.
	h := s.echo.Routes()
	_ = h
	// Grab a handle to the handlers via a second call while the cache is
	// hot — we can't inspect the atomic pointer directly from this test,
	// but we verify the behaviour: two identical reads return identical
	// bodies, then a write changes the body.
	second := httptest.NewRecorder()
	s.echo.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/api/kiwi/graph", nil))
	if second.Code != http.StatusOK {
		t.Fatalf("second /graph: %d", second.Code)
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("cache not hit: first=%q second=%q", first.Body.String(), second.Body.String())
	}

	// Write a new file — OnInvalidate fires, cache drops, next /graph
	// includes b.md.
	mustPutFile(t, s, "b.md", "# b\n")

	third := httptest.NewRecorder()
	s.echo.ServeHTTP(third, httptest.NewRequest(http.MethodGet, "/api/kiwi/graph", nil))
	if third.Code != http.StatusOK {
		t.Fatalf("third /graph: %d", third.Code)
	}
	if !strings.Contains(third.Body.String(), "b.md") {
		t.Fatalf("invalidation missed — b.md absent: %s", third.Body.String())
	}
	if third.Body.String() == second.Body.String() {
		t.Fatalf("invalidation didn't refresh response")
	}
}

func TestResolveLinksEndpoint(t *testing.T) {
	s := buildTestServerWithPublicURL(t, "https://wiki.co")

	mustPutFile(t, s, "concepts/auth.md", "# Auth\n")

	t.Run("resolves wiki links", func(t *testing.T) {
		body := `{"content":"See [[auth]] for details."}`
		req := httptest.NewRequest(http.MethodPost, "/api/kiwi/resolve-links", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s.echo.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("POST resolve-links: %d %s", rec.Code, rec.Body.String())
		}
		var out struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !strings.Contains(out.Content, "https://wiki.co/page/concepts/auth.md") {
			t.Fatalf("expected resolved link, got: %s", out.Content)
		}
	})

	t.Run("returns unchanged when public_url empty", func(t *testing.T) {
		s2 := buildTestServerWithPublicURL(t, "")
		body := `{"content":"See [[auth]] for details."}`
		req := httptest.NewRequest(http.MethodPost, "/api/kiwi/resolve-links", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s2.echo.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("POST: %d", rec.Code)
		}
		var out struct {
			Content string `json:"content"`
		}
		json.Unmarshal(rec.Body.Bytes(), &out)
		if out.Content != "See [[auth]] for details." {
			t.Fatalf("expected unchanged content, got: %s", out.Content)
		}
	})
}

func TestReadFileResolveLinks(t *testing.T) {
	s := buildTestServerWithPublicURL(t, "https://wiki.co")

	mustPutFile(t, s, "concepts/auth.md", "# Auth\n")

	content := "See [[auth]] for details.\n"
	mustPutFile(t, s, "readme.md", content)

	t.Run("resolve_links=true rewrites links", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=readme.md&resolve_links=true", nil)
		rec := httptest.NewRecorder()
		s.echo.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET: %d", rec.Code)
		}
		body := rec.Body.String()
		if !strings.Contains(body, "https://wiki.co/page/concepts/auth.md") {
			t.Fatalf("expected resolved link in body, got: %s", body)
		}
		if strings.Contains(body, "[[auth]]") {
			t.Fatalf("wiki link not replaced: %s", body)
		}
	})

	t.Run("without resolve_links keeps wiki links", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/kiwi/file?path=readme.md", nil)
		rec := httptest.NewRecorder()
		s.echo.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET: %d", rec.Code)
		}
		body := rec.Body.String()
		if !strings.Contains(body, "[[auth]]") {
			t.Fatalf("expected raw wiki link, got: %s", body)
		}
	})
}

func TestHandler_Query(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	for _, f := range []struct {
		path, body string
	}{
		{"alpha.md", "---\nname: Alpha\nstatus: active\n---\n# Alpha\n"},
		{"beta.md", "---\nname: Beta\nstatus: draft\n---\n# Beta\n"},
		{"gamma.md", "---\nname: Gamma\nstatus: active\n---\n# Gamma\n"},
	} {
		mustPutFile(t, s, f.path, f.body)
	}

	req := httptest.NewRequest(http.MethodGet, `/api/kiwi/query?q=TABLE+name+WHERE+status+%3D+"active"&format=json`, nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /query: %d %s", rec.Code, rec.Body.String())
	}

	var resp queryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(resp.Rows), resp.Rows)
	}
}

func TestHandler_Query_BadDQL(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	req := httptest.NewRequest(http.MethodGet, `/api/kiwi/query?q=FROBNICATE+stuff`, nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestHandler_QueryAggregate(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	for _, f := range []struct {
		path, body string
	}{
		{"a.md", "---\nstatus: active\n---\n"},
		{"b.md", "---\nstatus: active\n---\n"},
		{"c.md", "---\nstatus: draft\n---\n"},
	} {
		mustPutFile(t, s, f.path, f.body)
	}

	req := httptest.NewRequest(http.MethodGet, `/api/kiwi/query/aggregate?group_by=status`, nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /query/aggregate: %d %s", rec.Code, rec.Body.String())
	}

	var resp aggregateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Groups) < 2 {
		t.Fatalf("want at least 2 groups, got %d: %v", len(resp.Groups), resp.Groups)
	}

	total := 0
	for _, g := range resp.Groups {
		total += g.Count
	}
	if total != 3 {
		t.Fatalf("total count = %d, want 3", total)
	}
}

func TestHandler_QueryAggregate_InvalidField(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	req := httptest.NewRequest(http.MethodGet, `/api/kiwi/query/aggregate?group_by=status;+DROP`, nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for injection attempt, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestHandler_QueryAggregate_UnionBypass(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	req := httptest.NewRequest(http.MethodGet,
		`/api/kiwi/query/aggregate?group_by=status&where=1%3D1+UNION+SELECT+*+FROM+sqlite_master`, nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for UNION injection, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestHandler_ViewRefresh(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	// Seed some data files
	for _, f := range []struct {
		path, body string
	}{
		{"students/a.md", "---\nname: Alpha\nstatus: active\n---\n"},
		{"students/b.md", "---\nname: Beta\nstatus: draft\n---\n"},
	} {
		mustPutFile(t, s, f.path, f.body)
	}

	// Create a computed view file
	viewBody := "---\nkiwi-view: true\nkiwi-query: TABLE name FROM \"students/\"\n---\n<!-- kiwi:auto -->\n"
	mustPutFile(t, s, "views/test.md", viewBody)

	// Refresh the view
	refreshBody := `{"path":"views/test.md"}`
	req := httptest.NewRequest(http.MethodPost, "/api/kiwi/view/refresh", strings.NewReader(refreshBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /view/refresh: %d %s", rec.Code, rec.Body.String())
	}

	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["path"] != "views/test.md" {
		t.Errorf("path = %q, want views/test.md", resp["path"])
	}
}

func TestHandler_Query_FormatTable(t *testing.T) {
	s, _ := buildSQLiteTestServer(t)

	for _, f := range []struct {
		path, body string
	}{
		{"a.md", "---\nname: Alpha\nstatus: active\n---\n"},
		{"b.md", "---\nname: Beta\nstatus: draft\n---\n"},
	} {
		mustPutFile(t, s, f.path, f.body)
	}

	req := httptest.NewRequest(http.MethodGet, `/api/kiwi/query?q=TABLE+name&format=table`, nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /query format=table: %d %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	if !strings.Contains(body, "| ") {
		t.Errorf("expected markdown table with '| ', got:\n%s", body)
	}
	if !strings.Contains(body, "Alpha") {
		t.Errorf("expected Alpha in table output, got:\n%s", body)
	}
}
