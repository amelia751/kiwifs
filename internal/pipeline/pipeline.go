// Package pipeline is the single write funnel used by every access protocol.
//
// Writes arriving through REST, WebDAV, NFS, or S3 all end here, so versioning,
// search indexing, link extraction, and SSE broadcast happen exactly once per
// change — regardless of which protocol delivered it.
package pipeline

import (
	"context"
	"crypto/sha1"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/vectorstore"
	"github.com/kiwifs/kiwifs/internal/versioning"
)

// inflightWindow is how long a path stays in the inflight set after a
// pipeline-originated write. It must exceed the watcher debounce (500ms)
// with headroom to cover OS event latency — 2s is comfortably safe.
const inflightWindow = 2 * time.Second

// DefaultActor is stamped into git commits when no caller-supplied actor is
// provided. Protocols that always know their actor (WebDAV → "webdav",
// S3 → "s3", watcher → "fswatch") pass their own; the REST API falls
// through to this when X-Actor is absent.
const DefaultActor = "kiwifs"

// Pipeline bundles the side effects that must run on every write or delete.
// Dependencies are held by interface so protocols can construct one cheaply
// and share it — the struct is immutable after construction.
type Pipeline struct {
	Store     storage.Storage
	Versioner versioning.Versioner
	Searcher  search.Searcher
	Linker    links.Linker            // may be nil (grep search has no link index)
	Hub       *events.Hub             // may be nil (tests / protocols that don't broadcast)
	Vectors   *vectorstore.Service    // may be nil when vector search is disabled

	// OnInvalidate, when set, fires on every write and delete. The API
	// layer wires it to cache-invalidation callbacks (graph endpoint, etc.)
	// so the pipeline doesn't need to know which layers cache what — it
	// just announces "something changed".
	OnInvalidate func()

	// writeMu serialises the whole Store.Write → Versioner.Commit sequence
	// across concurrent Write / BulkWrite / Delete / Observe* callers.
	// Without it, a REST-origin Write could race with an fsnotify-origin
	// Observe and the two goroutines' `git add` calls could stage each
	// other's files into the wrong commit — the per-versioner mutex only
	// covers add+commit, not the "my files vs. their files" boundary.
	writeMu sync.Mutex

	// inflight tracks paths recently written via Write/BulkWrite/Delete so
	// the fsnotify watcher can tell its own echo events apart from real
	// out-of-band edits. Without this, every REST write triggers a
	// watcher Observe that re-embeds the same content — doubling the
	// vector-embedding API cost per change.
	inflight sync.Map // map[string]time.Time
}

// Result is returned from Write so callers can set ETag headers, log, etc.
type Result struct {
	Path string
	ETag string
}

// ErrConflict is returned by Write when an If-Match precondition no longer
// holds at commit time. Mapped to HTTP 409 by the REST handler.
var ErrConflict = fmt.Errorf("file modified since last read")

// WriteOpts carries the optional knobs that don't fit Write's hot signature.
// Today only IfMatch is set; new fields go here so callers don't churn.
type WriteOpts struct {
	// IfMatch, if non-empty, must match the current file's ETag. The check
	// runs inside writeMu so two concurrent writers with the same stale tag
	// can't both pass. Empty value disables the check (create-or-overwrite
	// semantics for callers that don't use optimistic locking).
	IfMatch string
}

// preDeleter is an optional hook some versioners expose so they can snapshot
// a file before the storage layer removes it. Satisfied by the CoW
// versioner; git doesn't need it (git rm captures the deletion).
type preDeleter interface {
	PreDeleteSnapshot(path string) error
}

func coalesce(actor string) string {
	if actor == "" {
		return DefaultActor
	}
	return actor
}

// New builds a pipeline. Pass nil for linker, hub, or vectors when those
// aren't wired. vectors is nil when [search.vector] is disabled.
func New(
	store storage.Storage,
	versioner versioning.Versioner,
	searcher search.Searcher,
	linker links.Linker,
	hub *events.Hub,
	vectors *vectorstore.Service,
) *Pipeline {
	return &Pipeline{
		Store:     store,
		Versioner: versioner,
		Searcher:  searcher,
		Linker:    linker,
		Hub:       hub,
		Vectors:   vectors,
	}
}

// metaIndexer is the optional interface index-backed searchers implement to
// keep a structured frontmatter table alongside the FTS index. Grep search
// doesn't satisfy it, which is why the pipeline type-asserts at call time.
type metaIndexer interface {
	IndexMeta(ctx context.Context, path string, content []byte) error
	RemoveMeta(ctx context.Context, path string) error
}

// allRemover is the optional interface index-backed searchers implement to
// drop every table entry for a path in a single atomic transaction. Running
// the three deletes (docs/links/file_meta) as independent statements risks
// a crash between them leaving the indices drifted against the storage
// layer; RemoveAll folds them into one tx.
type allRemover interface {
	RemoveAll(ctx context.Context, path string) error
}

// indexFile pushes content into every index (search, links, vectors, meta).
// Errors are logged, not returned — side-effect failures must not block
// the write that triggered them. Caller must hold writeMu.
func (p *Pipeline) indexFile(ctx context.Context, path string, content []byte) {
	if err := p.Searcher.Index(ctx, path, content); err != nil {
		log.Printf("pipeline: searcher.Index(%s): %v", path, err)
	}
	if p.Linker != nil {
		if err := p.Linker.IndexLinks(ctx, path, links.Extract(content)); err != nil {
			log.Printf("pipeline: linker.IndexLinks(%s): %v", path, err)
		}
	}
	if meta, ok := p.Searcher.(metaIndexer); ok {
		if err := meta.IndexMeta(ctx, path, content); err != nil {
			log.Printf("pipeline: searcher.IndexMeta(%s): %v", path, err)
		}
	}
	p.Vectors.Enqueue(path, content)
}

// deindexFile removes a path from every index. Caller must hold writeMu.
// Prefers the searcher's RemoveAll when available so docs/links/file_meta
// all drop in one transaction; falls back to the three-call path for grep
// search, which has no RemoveAll equivalent.
func (p *Pipeline) deindexFile(ctx context.Context, path string) {
	if ra, ok := p.Searcher.(allRemover); ok {
		if err := ra.RemoveAll(ctx, path); err != nil {
			log.Printf("pipeline: searcher.RemoveAll(%s): %v", path, err)
		}
	} else {
		if err := p.Searcher.Remove(ctx, path); err != nil {
			log.Printf("pipeline: searcher.Remove(%s): %v", path, err)
		}
		if p.Linker != nil {
			if err := p.Linker.RemoveLinks(ctx, path); err != nil {
				log.Printf("pipeline: linker.RemoveLinks(%s): %v", path, err)
			}
		}
		if meta, ok := p.Searcher.(metaIndexer); ok {
			if err := meta.RemoveMeta(ctx, path); err != nil {
				log.Printf("pipeline: searcher.RemoveMeta(%s): %v", path, err)
			}
		}
	}
	p.Vectors.EnqueueDelete(path)
}

// broadcast sends an event to all SSE subscribers if the hub is wired, and
// fires the cache-invalidation callback (if any) so layers caching derived
// views — like the graph endpoint — can drop their entries on any write.
func (p *Pipeline) broadcast(ev events.Event) {
	if p.Hub != nil {
		p.Hub.Broadcast(ev)
	}
	if p.OnInvalidate != nil {
		p.OnInvalidate()
	}
}

// markInflight records that we just touched `path` so the fsnotify-driven
// Observe path can skip the echo event. The entry auto-expires after
// inflightWindow to avoid unbounded map growth.
func (p *Pipeline) markInflight(path string) {
	p.inflight.Store(path, time.Now())
	time.AfterFunc(inflightWindow, func() { p.inflight.Delete(path) })
}

// isInflight reports whether `path` was written via the pipeline within the
// inflight window. Returns true for echo events the watcher should skip.
func (p *Pipeline) isInflight(path string) bool {
	v, ok := p.inflight.Load(path)
	if !ok {
		return false
	}
	t, _ := v.(time.Time)
	if time.Since(t) > inflightWindow {
		p.inflight.Delete(path)
		return false
	}
	return true
}

// tryPreDelete gives CoW-style versioners a chance to snapshot the file
// before it's removed. No-op for versioners that don't implement it.
func (p *Pipeline) tryPreDelete(path string) {
	if pd, ok := p.Versioner.(preDeleter); ok {
		if err := pd.PreDeleteSnapshot(path); err != nil {
			log.Printf("pipeline: preDeleteSnapshot(%s): %v", path, err)
		}
	}
}

// Write persists a single file and fans out to versioner, searcher, linker,
// and the SSE hub. Returns the new ETag so HTTP handlers can echo it.
//
// ctx is checked at entry and again just before each blocking step so a
// caller-cancelled request (HTTP client disconnect, server shutdown) bows
// out before doing further work. ctx is not yet plumbed into Store /
// Versioner / Searcher — those are Phase 2-4.
//
// Side-effect errors (versioner / searcher / linker) are logged and then
// intentionally non-fatal: a failed commit must not make the write itself
// look failed to the caller, and the watcher/reindex paths will recover on
// next iteration. Silent swallowing hides real problems (a broken git
// install can drop an entire audit trail) — so log everything.
func (p *Pipeline) Write(ctx context.Context, path string, content []byte, actor string) (Result, error) {
	return p.WriteWithOpts(ctx, path, content, actor, WriteOpts{})
}

// WriteWithOpts is Write plus optional preconditions (currently just
// If-Match). The ETag check runs inside writeMu so it's atomic with the
// store write — without that the check is TOCTOU and two stale-ETag
// writers can both win.
func (p *Pipeline) WriteWithOpts(ctx context.Context, path string, content []byte, actor string, opts WriteOpts) (Result, error) {
	if path == "" {
		return Result{}, fmt.Errorf("path is required")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	actor = coalesce(actor)
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	// Re-check after acquiring the lock so a caller cancelled while
	// queueing behind another writer doesn't perform a now-pointless write.
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if opts.IfMatch != "" {
		if current, err := p.Store.Read(ctx, path); err == nil {
			if ETag(current) != opts.IfMatch {
				return Result{}, ErrConflict
			}
		}
		// Missing file with If-Match set: treat as create-allowed (matches
		// the previous handler-side behaviour and RFC 7232 §3.1, which
		// applies If-Match only when the resource exists).
	}
	// Mark before the disk write so the fsnotify event fires while the
	// entry is already visible — otherwise a fast watcher could observe
	// before we record it.
	p.markInflight(path)
	if err := p.Store.Write(ctx, path, content); err != nil {
		return Result{}, err
	}
	if err := p.Versioner.Commit(ctx, path, actor, fmt.Sprintf("%s: %s", actor, path)); err != nil {
		log.Printf("pipeline: versioner.Commit(%s): %v", path, err)
	}
	p.indexFile(ctx, path, content)
	etag := ETag(content)
	p.broadcast(events.Event{Op: "write", Path: path, Actor: actor, ETag: etag})
	return Result{Path: path, ETag: etag}, nil
}

// Observe runs every pipeline side effect *except* the storage write, for
// callers that already put the file on disk themselves — the fsnotify
// watcher is the obvious example: when an agent writes directly to a
// mounted root, the file is already on disk by the time we detect it.
//
// This keeps the watcher behind the same funnel as REST/NFS/WebDAV/S3
// (versioning, search, links, vectors, SSE) without doubling up the disk
// write that would otherwise re-fire fsnotify in an echo loop.
func (p *Pipeline) Observe(ctx context.Context, path string, content []byte, actor string) Result {
	// Echo suppression: a Write we just did will re-trigger fsnotify. Skip
	// — the real write already ran every side effect.
	if p.isInflight(path) {
		return Result{Path: path, ETag: ETag(content)}
	}
	if err := ctx.Err(); err != nil {
		return Result{Path: path, ETag: ETag(content)}
	}
	actor = coalesce(actor)
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if err := p.Versioner.Commit(ctx, path, actor, fmt.Sprintf("%s: %s", actor, path)); err != nil {
		log.Printf("pipeline: versioner.Commit(%s): %v", path, err)
	}
	p.indexFile(ctx, path, content)
	etag := ETag(content)
	p.broadcast(events.Event{Op: "write", Path: path, Actor: actor, ETag: etag})
	return Result{Path: path, ETag: etag}
}

// ObserveDelete is the sibling of Observe for out-of-band deletions: the
// file is already gone, we just need every index to catch up.
func (p *Pipeline) ObserveDelete(ctx context.Context, path, actor string) {
	if p.isInflight(path) {
		return
	}
	if err := ctx.Err(); err != nil {
		return
	}
	actor = coalesce(actor)
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.tryPreDelete(path)
	if err := p.Versioner.CommitDelete(ctx, path, actor); err != nil {
		log.Printf("pipeline: versioner.CommitDelete(%s): %v", path, err)
	}
	p.deindexFile(ctx, path)
	p.broadcast(events.Event{Op: "delete", Path: path, Actor: actor})
}

// BulkWrite persists many files under a single git commit. Used for atomic
// multi-file agent runs so the history shows one entry per logical
// operation.
//
// Atomicity: before writing any file, we capture the current on-disk state
// of every target so we can roll back if a later file fails partway. The
// rollback isn't perfect — between our write and our roll-back another
// process could observe the intermediate state — but it keeps the
// on-disk/indexed picture consistent at rest, which is the guarantee
// callers actually depend on.
func (p *Pipeline) BulkWrite(ctx context.Context, files []struct {
	Path    string
	Content []byte
}, actor, message string) ([]Result, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("files is required and must be non-empty")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	actor = coalesce(actor)
	for i, f := range files {
		if f.Path == "" {
			return nil, fmt.Errorf("files[%d].path is required", i)
		}
	}
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type preImage struct {
		path    string
		content []byte
		existed bool
	}
	preimages := make([]preImage, 0, len(files))
	for _, f := range files {
		pre := preImage{path: f.Path}
		if p.Store.Exists(ctx, f.Path) {
			if c, err := p.Store.Read(ctx, f.Path); err == nil {
				pre.content = c
				pre.existed = true
			}
		}
		preimages = append(preimages, pre)
	}

	rollback := func(upTo int) {
		for i := upTo - 1; i >= 0; i-- {
			pre := preimages[i]
			// Dequeue any already-enqueued vector upsert for this path.
			// The embed worker runs async, so a rollback that only
			// restored storage + FTS would still let stale content land
			// in the vector index seconds later; SkipPath short-circuits
			// the worker before the embed fires.
			if p.Vectors != nil {
				p.Vectors.SkipPath(pre.path)
			}
			if pre.existed {
				_ = p.Store.Write(ctx, pre.path, pre.content)
				_ = p.Searcher.Index(ctx, pre.path, pre.content)
				if p.Linker != nil {
					_ = p.Linker.IndexLinks(ctx, pre.path, links.Extract(pre.content))
				}
				// Re-enqueue the good content so semantic search catches
				// up with the rolled-back-to state — we just invalidated
				// the queue entry above, so this is the only write that
				// will actually embed.
				p.Vectors.Enqueue(pre.path, pre.content)
			} else {
				_ = p.Store.Delete(ctx, pre.path)
				_ = p.Searcher.Remove(ctx, pre.path)
				if p.Linker != nil {
					_ = p.Linker.RemoveLinks(ctx, pre.path)
				}
				p.Vectors.EnqueueDelete(pre.path)
			}
		}
	}

	paths := make([]string, 0, len(files))
	out := make([]Result, 0, len(files))
	for i, f := range files {
		p.markInflight(f.Path)
		if err := p.Store.Write(ctx, f.Path, f.Content); err != nil {
			rollback(i)
			return nil, fmt.Errorf("write %s: %w", f.Path, err)
		}
		p.indexFile(ctx, f.Path, f.Content)
		paths = append(paths, f.Path)
		out = append(out, Result{Path: f.Path, ETag: ETag(f.Content)})
	}
	if message == "" {
		message = fmt.Sprintf("%s: bulk write — %d files", actor, len(paths))
	}
	if err := p.Versioner.BulkCommit(ctx, paths, actor, message); err != nil {
		rollback(len(files))
		return nil, fmt.Errorf("commit: %w", err)
	}
	p.broadcast(events.Event{Op: "bulk", Paths: paths, Actor: actor})
	return out, nil
}

// Delete removes a file and fans out to versioner, searcher, linker, and
// the SSE hub. Missing files are reported via the storage error unchanged.
func (p *Pipeline) Delete(ctx context.Context, path, actor string) error {
	if path == "" {
		return fmt.Errorf("path is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	actor = coalesce(actor)
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	p.markInflight(path)
	p.tryPreDelete(path)
	if err := p.Store.Delete(ctx, path); err != nil {
		return err
	}
	if err := p.Versioner.CommitDelete(ctx, path, actor); err != nil {
		log.Printf("pipeline: versioner.CommitDelete(%s): %v", path, err)
	}
	p.deindexFile(ctx, path)
	p.broadcast(events.Event{Op: "delete", Path: path, Actor: actor})
	return nil
}

// ETag returns the git blob hash of the content — the same value that
// `git hash-object` would emit. Using the real blob hash (not a sha256
// prefix) means the ETag is also a usable handle into the object store
// when git versioning is active, so clients can fetch a historical blob
// by its ETag without a separate lookup.
//
// Security note: SHA-1 is cryptographically broken for collision resistance
// (SHAttered, 2017), but ETags are a cache-correctness mechanism — they
// detect accidental content changes, not adversarial attacks. HTTP RFC 7232
// has no requirement for collision resistance in ETags. We intentionally
// use SHA-1 here because (a) it matches git's object model so the ETag IS
// the blob ID, and (b) switching to SHA-256 would break this identity and
// require a parallel hash. Git itself still defaults to SHA-1 (the SHA-256
// transition is opt-in). If the git backend migrates to SHA-256, this
// function should follow.
//
// Format: sha1("blob <size>\0<content>"), hex-encoded.
func ETag(content []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(content))
	h.Write(content)
	return fmt.Sprintf("%x", h.Sum(nil))
}
