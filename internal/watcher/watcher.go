package watcher

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/storage"
)

const defaultDebounce = 500 * time.Millisecond

// Watcher watches the knowledge root for out-of-band .md file changes
// (agent writes that bypass the HTTP API) and re-plays them through the
// shared pipeline so versioning, search, and SSE broadcast all happen in
// one place. Routing through pipeline (rather than hand-calling each
// side-effect) means future pipeline additions — audit log, webhooks,
// comment hooks — automatically cover fsnotify-origin writes too.
//
// Content reads go through storage.Storage rather than os.ReadFile so the
// watcher respects the storage layer's view of the tree (hidden-file filter,
// future non-local backends, test fakes). The fsnotify subscription itself
// still takes raw paths because the kernel API needs them — there's no
// storage-abstracted equivalent for that.
type Watcher struct {
	root     string
	store    storage.Storage // reads content through the same abstraction every other module uses
	pipe     *pipeline.Pipeline
	fsw      *fsnotify.Watcher
	debounce time.Duration
	actor    string

	mu      sync.Mutex
	pending map[string]struct{}
	timer   *time.Timer

	stopCh chan struct{}
	doneCh chan struct{}
}

// New creates a watcher rooted at root. It does not start watching until
// Start is called. Every detected change is fanned out through pipe so
// versioning/search/vector/SSE stay consistent with REST-origin writes.
// store is used to read file content when a change is detected; passing
// nil falls back to direct os.ReadFile so tests can opt out of the
// abstraction if they need to.
func New(root string, store storage.Storage, pipe *pipeline.Pipeline) (*Watcher, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	w := &Watcher{
		root:     abs,
		store:    store,
		pipe:     pipe,
		fsw:      fsw,
		debounce: defaultDebounce,
		actor:    "fswatch",
		pending:  make(map[string]struct{}),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	if err := w.addTree(abs); err != nil {
		fsw.Close()
		return nil, err
	}
	return w, nil
}

// Start begins processing filesystem events in a background goroutine.
func (w *Watcher) Start() {
	go w.run()
}

// Close stops the watcher, flushing any pending work.
func (w *Watcher) Close() error {
	select {
	case <-w.stopCh:
		// already closed
	default:
		close(w.stopCh)
	}
	<-w.doneCh

	w.mu.Lock()
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	w.mu.Unlock()

	w.flush()
	return w.fsw.Close()
}

func skipDirName(name string) bool {
	return strings.HasPrefix(name, ".") || name == "node_modules"
}

func (w *Watcher) addTree(dir string) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		if path != dir && skipDirName(d.Name()) {
			return filepath.SkipDir
		}
		if addErr := w.fsw.Add(path); addErr != nil {
			log.Printf("watcher: add %s: %v", path, addErr)
		}
		return nil
	})
}

func (w *Watcher) run() {
	defer close(w.doneCh)
	for {
		select {
		case <-w.stopCh:
			return
		case err, ok := <-w.fsw.Errors:
			if !ok {
				return
			}
			log.Printf("watcher: %v", err)
		case ev, ok := <-w.fsw.Events:
			if !ok {
				return
			}
			w.handle(ev)
		}
	}
}

func (w *Watcher) handle(ev fsnotify.Event) {
	// A new directory appearing under a watched dir means we need to
	// subscribe to it so we see events for files created inside.
	if ev.Op&fsnotify.Create != 0 {
		if fi, err := os.Stat(ev.Name); err == nil && fi.IsDir() {
			if !skipDirName(filepath.Base(ev.Name)) {
				if addErr := w.addTree(ev.Name); addErr != nil {
					log.Printf("watcher: add %s: %v", ev.Name, addErr)
				}
				// Scan the new directory for files that were created before
				// the watch was established — this closes the race where
				// mkdir + write happen in quick succession.
				w.scanDir(ev.Name)
			}
			return
		}
	}
	if !w.relevantFile(ev.Name) {
		return
	}
	if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) == 0 {
		return
	}
	w.schedule(ev.Name)
}

// scanDir walks a directory and schedules any .md files it finds. This
// handles the race where a new directory is created and files are written
// to it before the fsnotify watch is established.
func (w *Watcher) scanDir(dir string) {
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if path != dir && skipDirName(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if w.relevantFile(path) {
			w.schedule(path)
		}
		return nil
	})
}

func (w *Watcher) relevantFile(path string) bool {
	if !strings.HasSuffix(path, ".md") {
		return false
	}
	rel, err := filepath.Rel(w.root, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return false
	}
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		if skipDirName(part) {
			return false
		}
	}
	return true
}

func (w *Watcher) schedule(absPath string) {
	w.mu.Lock()
	w.pending[absPath] = struct{}{}
	if w.timer == nil {
		w.timer = time.AfterFunc(w.debounce, w.flush)
	} else {
		w.timer.Reset(w.debounce)
	}
	w.mu.Unlock()
}

func (w *Watcher) flush() {
	w.mu.Lock()
	pending := w.pending
	w.pending = make(map[string]struct{})
	w.timer = nil
	w.mu.Unlock()

	if len(pending) == 0 {
		return
	}

	for absPath := range pending {
		rel, err := filepath.Rel(w.root, absPath)
		if err != nil {
			continue
		}
		rel = filepath.ToSlash(rel)
		content, err := w.readContent(rel, absPath)
		if err != nil {
			if os.IsNotExist(err) {
				w.handleDelete(rel)
				continue
			}
			log.Printf("watcher: read %s: %v", rel, err)
			continue
		}
		w.handleWrite(rel, content)
	}
}

// readContent reads a changed file through the storage abstraction, falling
// back to a direct os.ReadFile when no storage was wired (tests). Routing
// through storage keeps the watcher's view of the tree consistent with
// everything else — same hidden-file filter, same path-resolution rules,
// transparent for non-local backends that may eventually exist.
func (w *Watcher) readContent(rel, absPath string) ([]byte, error) {
	if w.store != nil {
		return w.store.Read(context.Background(), rel)
	}
	return os.ReadFile(absPath)
}

func (w *Watcher) handleWrite(rel string, content []byte) {
	// Observe (not Write) — the file is already on disk, so we want every
	// pipeline side-effect *except* storage.Write, which would otherwise
	// trigger another fsnotify event and echo-loop.
	w.pipe.Observe(context.Background(), rel, content, w.actor)
}

func (w *Watcher) handleDelete(rel string) {
	w.pipe.ObserveDelete(context.Background(), rel, w.actor)
}
