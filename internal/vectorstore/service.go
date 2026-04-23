package vectorstore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/kiwifs/kiwifs/internal/embed"
	"github.com/kiwifs/kiwifs/internal/storage"
)

// Service is the high-level semantic-search entry point: it chunks incoming
// text, embeds each chunk with the configured Embedder, writes the vectors
// into the Store, and answers queries by embedding the query and asking the
// Store for the nearest chunks.
//
// API handlers and the fsnotify watcher call Enqueue/EnqueueDelete on the
// hot request path; the embedding call is slow enough (network hop) that
// indexing is pushed onto a background worker pool.
type Service struct {
	root     string
	source   storage.Storage // reindex source; keeps the vectorstore layer storage-agnostic
	embedder embed.Embedder
	store    Store

	chunkSize    int
	chunkOverlap int
	workerCount  int

	queue  chan job
	wg     sync.WaitGroup
	stopCh chan struct{}

	// skip is the "this upsert was rolled back before the worker got to it"
	// set. BulkWrite records a path here when it rolls back; the worker
	// LoadAndDelete's before embedding so stale content never reaches the
	// store. AfterFunc prunes entries so an unprocessed skip doesn't linger
	// forever (e.g. if the path is never touched again).
	skip sync.Map // map[string]struct{}
}

// defaultWorkerCount is the number of parallel embedders. Most providers
// tolerate 5 concurrent connections from one client without rate limiting
// (OpenAI, Cohere, Ollama, Vertex all sit above that); picking 5 gets most
// of the parallelism benefit without threatening the provider's defaults.
const defaultWorkerCount = 5

// skipTTL bounds how long a BulkWrite rollback's skip marker is kept. The
// worker normally consumes the queue within a second or two; a full minute
// is generous for extremely slow embedders and still short enough to keep
// the sync.Map bounded under sustained rollbacks.
const skipTTL = 60 * time.Second

type job struct {
	kind    string // "upsert" | "delete"
	path    string
	content []byte
}

// Options tunes the Service. Zero values fall back to sane defaults.
type Options struct {
	ChunkSize    int
	ChunkOverlap int
	QueueSize    int
	// WorkerCount is the number of parallel embed+upsert goroutines. 0
	// falls back to defaultWorkerCount; embedding providers are network-
	// bound and easily handle 5× concurrency, which turns a serial 500ms
	// per chunk into a ~100ms tail for a burst of writes.
	WorkerCount int
}

// ErrDisabled is returned by endpoints that need a Service when none was
// configured. Callers translate this into a 503 at the HTTP boundary.
var ErrDisabled = errors.New("semantic search is not enabled")

// NewService wires an Embedder to a Store. Both dependencies must be
// non-nil; callers check config.Enabled before constructing. source is
// used only by Reindex to walk the knowledge base — passing nil keeps the
// indexer active but disables reindexing (useful for tests with inline
// Enqueue calls).
func NewService(root string, source storage.Storage, embedder embed.Embedder, store Store, opts Options) *Service {
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 1500
	}
	if opts.ChunkOverlap < 0 {
		opts.ChunkOverlap = 150
	}
	if opts.ChunkOverlap >= opts.ChunkSize {
		opts.ChunkOverlap = opts.ChunkSize / 10
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = 256
	}
	if opts.WorkerCount <= 0 {
		opts.WorkerCount = defaultWorkerCount
	}
	s := &Service{
		root:         root,
		source:       source,
		embedder:     embedder,
		store:        store,
		chunkSize:    opts.ChunkSize,
		chunkOverlap: opts.ChunkOverlap,
		workerCount:  opts.WorkerCount,
		queue:        make(chan job, opts.QueueSize),
		stopCh:       make(chan struct{}),
	}
	for i := 0; i < s.workerCount; i++ {
		s.wg.Add(1)
		go s.worker()
	}
	return s
}

// Close flushes pending work and releases resources. After the worker
// goroutine exits we drain whatever was still sitting in the queue so a
// shutdown that races with an in-flight Enqueue doesn't silently drop an
// embedding job — a dropped upsert would leave the vector index stale
// until the next reindex.
func (s *Service) Close() error {
	select {
	case <-s.stopCh:
		// already closed
	default:
		close(s.stopCh)
	}
	s.wg.Wait()
	for {
		select {
		case j := <-s.queue:
			s.run(j)
		default:
			return s.store.Close()
		}
	}
}

// Enqueue schedules an async upsert for (path, content). Safe to call from
// hot request paths — drops onto a buffered channel and returns immediately.
// When the queue is full, the new job is dropped with a log line; reindex
// recovers anything lost this way.
func (s *Service) Enqueue(path string, content []byte) {
	if s == nil {
		return
	}
	s.submit(job{kind: "upsert", path: path, content: append([]byte(nil), content...)})
}

// EnqueueDelete schedules an async removal for path.
func (s *Service) EnqueueDelete(path string) {
	if s == nil {
		return
	}
	s.submit(job{kind: "delete", path: path})
}

func (s *Service) submit(j job) {
	select {
	case s.queue <- j:
	default:
		log.Printf("vectorstore: queue full, dropping %s %s", j.kind, j.path)
	}
}

func (s *Service) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stopCh:
			return
		case j := <-s.queue:
			s.run(j)
		}
	}
}

func (s *Service) run(j job) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	switch j.kind {
	case "upsert":
		// BulkWrite rollback path: a failed bulk commit restores the
		// on-disk content but can't rescind an already-queued embed job.
		// The skip set lets the rollback say "don't embed this" after the
		// fact so the vector index isn't seeded from the aborted version.
		if _, skipped := s.skip.LoadAndDelete(j.path); skipped {
			return
		}
		if err := s.Index(ctx, j.path, j.content); err != nil {
			log.Printf("vectorstore: index %s: %v", j.path, err)
		}
	case "delete":
		if err := s.store.RemoveByPath(ctx, j.path); err != nil {
			log.Printf("vectorstore: remove %s: %v", j.path, err)
		}
	}
}

// SkipPath tells the worker pool to discard any upsert job currently queued
// for path. Used by the pipeline's BulkWrite rollback: by the time the
// commit fails and rollback runs, the upsert may already be in the queue
// with the soon-to-be-aborted content, so a plain "restore on disk" isn't
// enough — the vector index would otherwise embed the bad version.
// A TTL'd auto-clear keeps the set bounded if the worker never processes
// the matching job (e.g. Close races, or the path is deleted instead).
func (s *Service) SkipPath(path string) {
	if s == nil {
		return
	}
	s.skip.Store(path, struct{}{})
	time.AfterFunc(skipTTL, func() { s.skip.Delete(path) })
}

// Index embeds content and upserts its chunks. Safe to call concurrently;
// the embedding call may block on the network. Empty files clear any stale
// rows and return nil.
func (s *Service) Index(ctx context.Context, path string, content []byte) error {
	if !storage.IsKnowledgeFile(path) {
		return nil
	}
	parts := chunk(string(content), s.chunkSize, s.chunkOverlap)
	if len(parts) == 0 {
		return s.store.RemoveByPath(ctx, path)
	}
	vectors, err := s.embedder.Embed(ctx, parts)
	if err != nil {
		return fmt.Errorf("embed: %w", err)
	}
	if len(vectors) != len(parts) {
		return fmt.Errorf("embedder returned %d vectors for %d inputs", len(vectors), len(parts))
	}
	chunks := make([]Chunk, len(parts))
	for i, p := range parts {
		chunks[i] = Chunk{
			ID:       fmt.Sprintf("%s#%d", path, i),
			Path:     path,
			ChunkIdx: i,
			Text:     p,
			Vector:   vectors[i],
		}
	}
	// Remove first, then upsert: if the file shrank, any higher chunk IDs
	// left behind from the previous version are cleared out.
	if err := s.store.RemoveByPath(ctx, path); err != nil {
		return fmt.Errorf("remove stale: %w", err)
	}
	return s.store.Upsert(ctx, chunks)
}

// Search embeds the query and runs a top-k nearest-neighbour search.
func (s *Service) Search(ctx context.Context, query string, topK int) ([]Result, error) {
	if strings.TrimSpace(query) == "" {
		return nil, nil
	}
	if topK <= 0 {
		topK = DefaultTopK
	}
	vecs, err := s.embedder.Embed(ctx, []string{query})
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	if len(vecs) != 1 {
		return nil, fmt.Errorf("embedder returned %d vectors for 1 input", len(vecs))
	}
	return s.store.Search(ctx, vecs[0], topK)
}

// Reindex walks the knowledge root and re-embeds every indexable file.
// Returns the number of files processed. Walks via the Storage abstraction
// so non-local backends (future S3-backed or network-FS storage) don't
// need their own reindex code path.
func (s *Service) Reindex(ctx context.Context) (int, error) {
	if s.source == nil {
		return 0, fmt.Errorf("reindex source storage is not configured")
	}
	if err := s.store.Reset(ctx); err != nil {
		return 0, fmt.Errorf("reset store: %w", err)
	}
	count := 0
	walkErr := storage.Walk(ctx, s.source, "/", func(e storage.Entry) error {
		content, err := s.source.Read(ctx, e.Path)
		if err != nil {
			return nil
		}
		if err := s.Index(ctx, e.Path, content); err != nil {
			log.Printf("vectorstore: index %s: %v", e.Path, err)
			return nil
		}
		count++
		return nil
	})
	if walkErr != nil {
		return count, walkErr
	}
	return count, nil
}

// Count reports the number of vectors currently in the store.
func (s *Service) Count(ctx context.Context) (int, error) {
	return s.store.Count(ctx)
}

