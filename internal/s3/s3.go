// Package s3 exposes a S3-compatible HTTP API on top of the KiwiFS
// pipeline. The wire format and request routing come from
// github.com/johannesboyne/gofakes3 (battle-tested by rclone, awscli,
// boto3, MinIO clients); the per-request behaviour comes from the
// adapter in this package, which routes writes through the pipeline so
// versioning / search / link / vector / SSE all fan out exactly as they
// would for a REST PUT.
//
// The previous hand-rolled implementation only spoke ListObjectsV1
// without delimiter or pagination — `aws s3 ls --recursive`,
// `aws s3 sync`, and boto3's `list_objects_v2` all failed silently or
// returned partial results. Swapping to the library gets V2 + delimiter
// + continuation tokens for free.
package s3

import (
	"crypto/subtle"
	"fmt"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/storage"
)

// Server is the HTTP-facing S3 endpoint. It composes a gofakes3 server
// (XML framing, request routing, signature parsing) with the local
// adapter (which knows how to read/write through the pipeline) and a
// thin Bearer-token auth layer.
type Server struct {
	handler http.Handler
	apiKey  string
}

// New wires the gofakes3 server to a Backend serving the default
// single-space bucket "knowledge". apiKey, if non-empty, gates every
// request behind a Bearer token — matches the REST API's auth=none /
// auth=apikey distinction.
//
// Use NewMultiSpace when --space entries are configured so each space
// is addressable as its own bucket.
func New(_ string, pipe *pipeline.Pipeline, store storage.Storage, apiKey string) *Server {
	return NewMultiSpace(map[string]SpaceBackend{
		defaultBucket: {Store: store, Pipe: pipe},
	}, []string{defaultBucket}, apiKey)
}

// NewMultiSpace wires the gofakes3 server with one bucket per space.
// `order` controls the order ListBuckets returns names — it should match
// the registration order from spaces.Manager so the bucket list is
// deterministic across restarts.
func NewMultiSpace(buckets map[string]SpaceBackend, order []string, apiKey string) *Server {
	be := newAdapter(buckets, order)
	g := gofakes3.New(be,
		// AutoBucket spares clients an explicit CreateBucket on first
		// PUT — they treat the single "knowledge" bucket as already
		// existing, which is what they observe via ListBuckets anyway.
		gofakes3.WithAutoBucket(true),
		// We never enforce time skew; an agent on a sandboxed VM may
		// have a wonky clock and we don't care for read/write
		// correctness.
		gofakes3.WithTimeSkewLimit(0),
	)
	return &Server{handler: g.Server(), apiKey: apiKey}
}

// Handler returns the http.Handler suitable for mounting at /s3 (or
// wherever the serve command exposes it). When apiKey is set, every
// request must carry `Authorization: Bearer <apiKey>` — gofakes3
// otherwise speaks unauthenticated S3, which would expose the entire
// knowledge tree to anyone who can reach the port.
func (s *Server) Handler() http.Handler {
	if s.apiKey == "" {
		return s.handler
	}
	expected := []byte("Bearer " + s.apiKey)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, expected) != 1 {
			// gofakes3's response framing is XML; replicate so a denied
			// client sees the same error shape it would for any other
			// failure rather than a free-form text body that breaks
			// SDK parsing.
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>invalid or missing API key</Message></Error>`)
			return
		}
		s.handler.ServeHTTP(w, r)
	})
}

// weakETag derives a cheap tag from size+mtime. Suitable for HEAD and
// LIST responses where the full content hash is too expensive to compute
// on every call; GET responses still carry a real MD5 of the body.
func weakETag(size int64, modTime time.Time) string {
	return fmt.Sprintf("%d-%d", size, modTime.UTC().UnixNano())
}

// contentTypeFor picks a reasonable Content-Type based on file extension
// (so .md files render as Markdown in S3 clients that respect the
// header) and falls back to http.DetectContentType on the body when the
// extension isn't recognised.
func contentTypeFor(key string, body []byte) string {
	ext := strings.ToLower(filepath.Ext(key))
	if ext == ".md" || ext == ".markdown" {
		return "text/markdown; charset=utf-8"
	}
	if ct := mime.TypeByExtension(ext); ct != "" {
		return ct
	}
	if len(body) > 0 {
		return http.DetectContentType(body)
	}
	return "application/octet-stream"
}
