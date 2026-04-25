// Package pipeline is the single write funnel for all protocols (REST, WebDAV,
// NFS, S3) — versioning, indexing, and SSE happen exactly once per change.
package pipeline

import (
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
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

	// Root is the absolute storage root. WriteStream spools oversized
	// uploads into `.kiwi-stream-*` tempfiles under this directory so
	// the final atomic rename stays on the same filesystem. Empty when
	// the pipeline is wired around an in-memory store (tests).
	Root string

	// OnInvalidate, when set, fires on every write and delete. The API
	// layer wires it to cache-invalidation callbacks (graph endpoint, etc.)
	// so the pipeline doesn't need to know which layers cache what — it
	// just announces "something changed".
	OnInvalidate func()

	// OnPathChange, when set, fires on every write/delete with the
	// affected path. Used by the view registry to mark overlapping
	// computed views as stale.
	OnPathChange func(path string)

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
	//
	// The value is an inflightEntry rather than a bare timestamp so the
	// watcher can dedup by content hash as well as by a time window: if
	// fsnotify fires late (after the window), Observe still drops the
	// echo when the content hash matches what we just wrote.
	inflight sync.Map // map[string]inflightEntry

	// uncommittedLog is the path to .kiwi/state/uncommitted.log. When a
	// versioner commit fails after a successful storage write, the path is
	// appended here so it can be retried on the next successful commit or
	// at process startup. Empty disables the feature (tests).
	uncommittedLog string
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

// New builds a pipeline. Pass nil for optional dependencies (linker, hub, vectors).
func New(
	store storage.Storage,
	versioner versioning.Versioner,
	searcher search.Searcher,
	linker links.Linker,
	hub *events.Hub,
	vectors *vectorstore.Service,
	root string,
) *Pipeline {
	var ulog string
	if root != "" {
		ulog = filepath.Join(root, ".kiwi", "state", "uncommitted.log")
	}
	return &Pipeline{
		Store:          store,
		Versioner:      versioner,
		Searcher:       searcher,
		Linker:         linker,
		Hub:            hub,
		Vectors:        vectors,
		Root:           root,
		uncommittedLog: ulog,
	}
}

// metaIndexer is the optional interface index-backed searchers implement to
// keep a structured frontmatter table alongside the FTS index. Grep search
// doesn't satisfy it, which is why the pipeline type-asserts at call time.
type metaIndexer interface {
	IndexMeta(ctx context.Context, path string, content []byte) error
	RemoveMeta(ctx context.Context, path string) error
}

// allRemover wraps docs/links/file_meta deletes in one tx to avoid index drift.
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
	if p.Vectors != nil {
		p.Vectors.Enqueue(path, content)
	}
}

// deindexFile removes a path from every index. Caller must hold writeMu.
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
	if p.Vectors != nil {
		p.Vectors.EnqueueDelete(path)
	}
}

func (p *Pipeline) commitAndTrack(ctx context.Context, path, actor string) {
	if err := p.Versioner.Commit(ctx, path, actor, fmt.Sprintf("%s: %s", actor, path)); err != nil {
		log.Printf("pipeline: versioner.Commit(%s): %v", path, err)
		p.trackUncommitted(path)
	}
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
	if p.OnPathChange != nil {
		if ev.Path != "" {
			p.OnPathChange(ev.Path)
		}
		for _, pa := range ev.Paths {
			p.OnPathChange(pa)
		}
	}
}

// inflightEntry tracks recent writes for fsnotify echo suppression.
type inflightEntry struct {
	at   time.Time
	etag string
}

// markInflight records a recent write so Observe can suppress the fsnotify echo.
func (p *Pipeline) markInflight(path string) {
	p.markInflightEtag(path, "")
}

// markInflightEtag is like markInflight but remembers the content etag so
// late watcher echoes for the same content are dropped even after the
// 2-second time window expires.
func (p *Pipeline) markInflightEtag(path, etag string) {
	p.inflight.Store(path, inflightEntry{at: time.Now(), etag: etag})
	time.AfterFunc(inflightWindow, func() {
		// Don't wipe the entry if it was refreshed in the meantime — only
		// clear stale ones.
		if v, ok := p.inflight.Load(path); ok {
			if ent, ok := v.(inflightEntry); ok && time.Since(ent.at) >= inflightWindow {
				// Keep the etag around for longer to catch delayed echoes,
				// but collapse the timestamp so future writes overwrite.
				p.inflight.Store(path, inflightEntry{at: time.Time{}, etag: ent.etag})
				time.AfterFunc(10*time.Second, func() { p.inflight.Delete(path) })
			}
		}
	})
}

// isInflight reports whether `path` was written via the pipeline within the
// inflight window. Returns true for echo events the watcher should skip.
func (p *Pipeline) isInflight(path string) bool {
	ent, ok := p.loadInflight(path)
	if !ok {
		return false
	}
	return !ent.at.IsZero() && time.Since(ent.at) <= inflightWindow
}

// isInflightEtag reports whether we already wrote this exact content to
// this path recently. The watcher uses it to drop noisy echoes that the
// simple time-based isInflight would miss (e.g. slow fsnotify batch).
func (p *Pipeline) isInflightEtag(path, etag string) bool {
	if etag == "" {
		return false
	}
	ent, ok := p.loadInflight(path)
	if !ok {
		return false
	}
	return ent.etag == etag
}

func (p *Pipeline) loadInflight(path string) (inflightEntry, bool) {
	v, ok := p.inflight.Load(path)
	if !ok {
		return inflightEntry{}, false
	}
	ent, ok := v.(inflightEntry)
	return ent, ok
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
// and the SSE hub. Side-effect errors (versioner/searcher/linker) are logged
// but non-fatal — the watcher/reindex paths recover on next iteration.
func (p *Pipeline) Write(ctx context.Context, path string, content []byte, actor string) (Result, error) {
	return p.WriteWithOpts(ctx, path, content, actor, WriteOpts{})
}

// StreamInMemoryThreshold: uploads above this spool to a temp file via
// WriteStream instead of buffering in RAM.
const StreamInMemoryThreshold = 16 * 1024 * 1024

// WriteStream persists a large payload by spooling to a temp file. Small
// payloads (< StreamInMemoryThreshold) fall back to Write.
func (p *Pipeline) WriteStream(ctx context.Context, path string, body io.Reader, sizeHint int64, actor string) (Result, error) {
	if path == "" {
		return Result{}, fmt.Errorf("path is required")
	}
	if body == nil {
		return Result{}, fmt.Errorf("body is required")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	// Small payload + caller told us the size: avoid all the tempfile
	// dance and keep the behavior identical to Write().
	if sizeHint >= 0 && sizeHint <= StreamInMemoryThreshold {
		buf := make([]byte, 0, sizeHint)
		bb := bytes.NewBuffer(buf)
		if _, err := io.Copy(bb, io.LimitReader(body, StreamInMemoryThreshold+1)); err != nil {
			return Result{}, fmt.Errorf("read body: %w", err)
		}
		return p.Write(ctx, path, bb.Bytes(), actor)
	}

	// Spool to a temp file so peak memory stays bounded. The temp lives
	// next to the storage root so the final rename is same-filesystem
	// (cheap) and visible to backup/FUSE clients as a transient `.kiwi-
	// stream-*` file rather than something in /tmp that could cross a
	// filesystem boundary.
	spoolDir := p.Root
	if spoolDir == "" {
		spoolDir = os.TempDir()
	}
	if err := os.MkdirAll(spoolDir, 0o755); err != nil {
		return Result{}, fmt.Errorf("spool dir: %w", err)
	}
	tmp, err := os.CreateTemp(spoolDir, ".kiwi-stream-*")
	if err != nil {
		return Result{}, fmt.Errorf("spool temp file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() {
		if tmpName != "" {
			os.Remove(tmpName)
		}
	}
	defer cleanup()
	n, err := io.Copy(tmp, body)
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return Result{}, fmt.Errorf("spool write: %w", err)
	}

	// Re-open and read back only if this is a knowledge file small
	// enough to index; large binaries get a streaming in-place write
	// that skips the FTS + vector fan-out entirely.
	knowledgeIndex := storage.IsKnowledgeFile(path) && n <= StreamInMemoryThreshold
	var content []byte
	if knowledgeIndex {
		content, err = os.ReadFile(tmpName)
		if err != nil {
			return Result{}, fmt.Errorf("read spool: %w", err)
		}
	}

	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	p.markInflight(path)
	if content != nil {
		if err := p.Store.Write(ctx, path, content); err != nil {
			return Result{}, err
		}
	} else {
		// Atomically move the spooled file into the store. This keeps
		// peak RAM at whatever io.Copy used — effectively 32 KB.
		abs := p.Store.AbsPath(path)
		if abs == "" {
			return Result{}, fmt.Errorf("storage %T does not expose AbsPath; cannot stream", p.Store)
		}
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			return Result{}, fmt.Errorf("mkdir: %w", err)
		}
		if err := os.Rename(tmpName, abs); err != nil {
			return Result{}, fmt.Errorf("stream rename into store: %w", err)
		}
		tmpName = "" // handed off to the store
	}
	actor = coalesce(actor)
	p.commitAndTrack(ctx, path, actor)
	if knowledgeIndex {
		p.indexFile(ctx, path, content)
	}
	var etag string
	if content != nil {
		etag = ETag(content)
	} else {
		// For large streamed files we don't have the blob in RAM to
		// compute the git blob hash; emit a size+mtime weak ETag so the
		// response isn't empty. Full-fidelity ETag will be computed on
		// the next read (which hashes from disk).
		etag = fmt.Sprintf("W/\"%d\"", n)
	}
	p.broadcast(events.Event{Op: "write", Path: path, Actor: actor, ETag: etag})
	return Result{Path: path, ETag: etag}, nil
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
	// before we record it. We stamp the etag so a delayed fsnotify batch
	// that arrives after inflightWindow still dedups by content.
	p.markInflightEtag(path, ETag(content))
	if err := p.Store.Write(ctx, path, content); err != nil {
		return Result{}, err
	}
	p.commitAndTrack(ctx, path, actor)
	p.indexFile(ctx, path, content)
	etag := ETag(content)
	p.broadcast(events.Event{Op: "write", Path: path, Actor: actor, ETag: etag})
	return Result{Path: path, ETag: etag}, nil
}

// Observe runs pipeline side effects (versioning, search, links, SSE) without
// writing to disk — used by the fsnotify watcher when the file already exists.
func (p *Pipeline) Observe(ctx context.Context, path string, content []byte, actor string) Result {
	etag := ETag(content)
	// Echo suppression, two layers:
	//   1. Time-based: we just wrote this path within inflightWindow.
	//   2. Content-based: we just wrote this exact content — even if the
	//      window expired, re-running indexing would only waste CPU and
	//      emit a duplicate commit.
	if p.isInflight(path) || p.isInflightEtag(path, etag) {
		return Result{Path: path, ETag: etag}
	}
	if err := ctx.Err(); err != nil {
		return Result{Path: path, ETag: etag}
	}
	actor = coalesce(actor)
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.commitAndTrack(ctx, path, actor)
	p.indexFile(ctx, path, content)
	// Stamp the etag so a second fsnotify batch for the same content
	// is deduped (some editors re-touch mtime without changing bytes).
	p.markInflightEtag(path, etag)
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

// BulkWrite persists many files under a single git commit. On partial
// failure it rolls back to the pre-write state (best-effort, not ACID).
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
		// If the versioner has a staging area (git), clear any paths
		// we've already `git add`-ed in this batch so a subsequent
		// write can't accidentally include them. Without this, a
		// failed bulk commit leaves the index dirty and the next
		// unrelated REST write commits half of the rolled-back batch
		// under the wrong message.
		if un, ok := p.Versioner.(versioning.Unstager); ok && upTo > 0 {
			staged := make([]string, 0, upTo)
			for i := 0; i < upTo; i++ {
				staged = append(staged, preimages[i].path)
			}
			if err := un.Unstage(ctx, staged); err != nil {
				log.Printf("pipeline: versioner.Unstage(%v): %v", staged, err)
			}
		}
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
				if p.Vectors != nil {
					p.Vectors.Enqueue(pre.path, pre.content)
				}
			} else {
				_ = p.Store.Delete(ctx, pre.path)
				_ = p.Searcher.Remove(ctx, pre.path)
				if p.Linker != nil {
					_ = p.Linker.RemoveLinks(ctx, pre.path)
				}
				if p.Vectors != nil {
					p.Vectors.EnqueueDelete(pre.path)
				}
			}
		}
	}

	paths := make([]string, 0, len(files))
	out := make([]Result, 0, len(files))
	for i, f := range files {
		p.markInflightEtag(f.Path, ETag(f.Content))
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
		p.broadcast(events.Event{Op: "rollback", Paths: paths, Actor: actor})
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

// CommitOnly stages and commits a path under writeMu without indexing.
// Used for .kiwi/ metadata files (config.toml, templates) that should be
// version-tracked but don't belong in the search or vector index.
func (p *Pipeline) CommitOnly(ctx context.Context, path, actor, message string) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if err := p.Versioner.Commit(ctx, path, actor, message); err != nil {
		log.Printf("pipeline: versioner.Commit(%s): %v", path, err)
	}
}

// ETag returns the git blob hash — sha1("blob <size>\0<content>") — so the
// ETag doubles as a git object ID when versioning is active.
func ETag(content []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(content))
	h.Write(content)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// trackUncommitted appends a path to the uncommitted log so it can be
// retried on the next successful commit. Caller must hold writeMu.
func (p *Pipeline) trackUncommitted(path string) {
	if p.uncommittedLog == "" {
		return
	}
	f, err := os.OpenFile(p.uncommittedLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("pipeline: trackUncommitted: open: %v", err)
		return
	}
	defer f.Close()
	fmt.Fprintln(f, path)
}

// DrainUncommitted reads the uncommitted log and attempts to recommit
// each path. Successfully committed paths are removed from the log.
// Call at process startup or after a successful commit.
func (p *Pipeline) DrainUncommitted(ctx context.Context) {
	if p.uncommittedLog == "" {
		return
	}
	data, err := os.ReadFile(p.uncommittedLog)
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 || (len(lines) == 1 && lines[0] == "") {
		return
	}

	seen := make(map[string]bool)
	var remaining []string
	for _, path := range lines {
		path = strings.TrimSpace(path)
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		if err := p.Versioner.Commit(ctx, path, DefaultActor, "recommit: "+path); err != nil {
			log.Printf("pipeline: recommit(%s): %v", path, err)
			remaining = append(remaining, path)
		} else {
			log.Printf("pipeline: recommitted %s", path)
		}
	}

	if len(remaining) == 0 {
		os.Remove(p.uncommittedLog)
	} else {
		os.WriteFile(p.uncommittedLog, []byte(strings.Join(remaining, "\n")+"\n"), 0644)
	}
}
