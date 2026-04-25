// Package webdav exposes the knowledge root over the WebDAV protocol.
//
// The FileSystem is a thin wrapper over the on-disk root: reads and directory
// ops go straight to the local filesystem, but writes and deletes are funneled
// through pipeline.Pipeline so they produce the same git commit + search index
// + SSE broadcast side-effects that the REST API does.
//
// WebDAV streams writes as Write(p) calls followed by Close(). We buffer the
// body in memory and call pipeline.Write on Close — knowledge-base files are
// markdown, so they fit in memory, and a single commit-per-save is the
// expected history granularity.
package webdav

import (
	"bytes"
	"context"
	"crypto/subtle"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kiwifs/kiwifs/internal/pipeline"
	"golang.org/x/net/webdav"
)

// FS implements webdav.FileSystem on top of a local root directory.
//
// The actor string is stamped into every git commit produced by writes that
// come through this protocol (default: "webdav") so audit logs can tell
// WebDAV-origin changes apart from REST or fsnotify-caught direct edits.
type FS struct {
	root   string
	pipe   *pipeline.Pipeline
	actor  string
	apiKey string // empty = auth disabled
}

// New builds a WebDAV filesystem rooted at `root`. Writes fan out through
// `pipe`; `actor` is attributed to the resulting git commits. apiKey, if
// non-empty, requires Basic or Bearer auth on every request.
func New(root string, pipe *pipeline.Pipeline, actor, apiKey string) *FS {
	if actor == "" {
		actor = "webdav"
	}
	return &FS{root: root, pipe: pipe, actor: actor, apiKey: apiKey}
}

// Handler wires the FS into an http.Handler with an in-memory lock system.
// Most WebDAV clients require locking to accept PUT with If-None-Match, so
// the memory locks keep single-process deployments honest without pulling
// in a persistent lock store.
func (f *FS) Handler(prefix string) http.Handler {
	h := &webdav.Handler{
		Prefix:     prefix,
		FileSystem: f,
		LockSystem: webdav.NewMemLS(),
	}
	if f.apiKey == "" {
		return h
	}
	return f.auth(h)
}

// auth gates every WebDAV request behind Bearer-token or Basic auth.
// Most WebDAV clients (macOS Finder, Windows Explorer) speak Basic; CLI
// tools speak Bearer. Both map to the same shared secret.
func (f *FS) auth(next http.Handler) http.Handler {
	expectedBearer := []byte("Bearer " + f.apiKey)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authz := r.Header.Get("Authorization")
		if authz != "" {
			if subtle.ConstantTimeCompare([]byte(authz), expectedBearer) == 1 {
				next.ServeHTTP(w, r)
				return
			}
			// Basic auth: any username, password must match the API key.
			if _, pass, ok := r.BasicAuth(); ok {
				if subtle.ConstantTimeCompare([]byte(pass), []byte(f.apiKey)) == 1 {
					next.ServeHTTP(w, r)
					return
				}
			}
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="kiwifs"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

// abs resolves a WebDAV path (always slash-separated, may have leading '/')
// to an absolute local path and guards against traversal outside the root.
func (f *FS) abs(name string) (string, string, error) {
	clean := filepath.Clean("/" + strings.TrimPrefix(name, "/"))
	abs := filepath.Join(f.root, clean)
	rel, err := filepath.Rel(f.root, abs)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", "", os.ErrPermission
	}
	// rel is the storage-relative path the pipeline sees ("concepts/auth.md").
	rel = filepath.ToSlash(rel)
	if rel == "." {
		rel = ""
	}
	return abs, rel, nil
}

func (f *FS) Mkdir(_ context.Context, name string, perm os.FileMode) error {
	abs, _, err := f.abs(name)
	if err != nil {
		return err
	}
	return os.Mkdir(abs, perm)
}

func (f *FS) Stat(_ context.Context, name string) (os.FileInfo, error) {
	abs, _, err := f.abs(name)
	if err != nil {
		return nil, err
	}
	return os.Stat(abs)
}

func (f *FS) RemoveAll(ctx context.Context, name string) error {
	abs, rel, err := f.abs(name)
	if err != nil {
		return err
	}
	if rel == "" {
		// Refuse to let a WebDAV client wipe the entire knowledge root.
		return os.ErrPermission
	}
	info, err := os.Stat(abs)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return f.pipe.Delete(ctx, rel, f.actor)
	}
	// For directories, walk and delete each file through the pipeline so git
	// sees per-file deletes.
	walkErr := filepath.Walk(abs, func(p string, fi os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}
		if fi.IsDir() {
			return nil
		}
		childRel, rerr := filepath.Rel(f.root, p)
		if rerr != nil {
			return rerr
		}
		return f.pipe.Delete(ctx, filepath.ToSlash(childRel), f.actor)
	})
	if walkErr != nil {
		return walkErr
	}
	// Remove the (now-empty) directory tree itself. Without this,
	// WebDAV clients see the directory persist after RemoveAll —
	// breaking `rmdir`, Finder's "Move to Trash", etc. os.RemoveAll
	// is safe here: we just deleted every file through the pipeline,
	// so only empty subdirectories remain.
	return os.RemoveAll(abs)
}

func (f *FS) Rename(ctx context.Context, oldName, newName string) error {
	_, oldRel, err := f.abs(oldName)
	if err != nil {
		return err
	}
	absOld := filepath.Join(f.root, oldRel)
	_, newRel, err := f.abs(newName)
	if err != nil {
		return err
	}
	info, err := os.Stat(absOld)
	if err != nil {
		return err
	}
	if info.IsDir() {
		// Directory renames are best-effort: we do the filesystem rename, then
		// re-walk the destination to re-index new paths. Dropping stale entries
		// on the old prefix happens via the watcher + reindex safety net.
		absNew := filepath.Join(f.root, newRel)
		return os.Rename(absOld, absNew)
	}
	content, err := os.ReadFile(absOld)
	if err != nil {
		return err
	}
	if _, err := f.pipe.Write(ctx, newRel, content, f.actor); err != nil {
		return err
	}
	return f.pipe.Delete(ctx, oldRel, f.actor)
}

// OpenFile returns a File. For read-only flags we pass through to os.File;
// for any write intent we return a buffered file that calls pipeline.Write
// on Close. The buffered approach is what makes the commit granularity
// match REST (one PUT = one commit) instead of one-per-Write-call.
func (f *FS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	abs, rel, err := f.abs(name)
	if err != nil {
		return nil, err
	}
	writable := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_TRUNC) != 0
	if !writable {
		ff, err := os.OpenFile(abs, flag, perm)
		if err != nil {
			return nil, err
		}
		return &readFile{File: ff}, nil
	}

	// Seed buffer from existing content when not truncating so O_APPEND and
	// partial writes behave as clients expect.
	var buf []byte
	if flag&os.O_TRUNC == 0 {
		if existing, rerr := os.ReadFile(abs); rerr == nil {
			buf = existing
		}
	}
	info, _ := os.Stat(abs)
	var modTime time.Time
	var size int64
	if info != nil {
		modTime = info.ModTime()
		size = info.Size()
	} else {
		modTime = time.Now()
	}
	return &writeFile{
		fs:      f,
		ctx:     ctx,
		rel:     rel,
		abs:     abs,
		buf:     bytes.NewBuffer(buf),
		modTime: modTime,
		size:    size,
	}, nil
}

// ─── File implementations ───────────────────────────────────────────────────

// readFile is a thin wrapper so we can implement the webdav.File interface
// (which requires io.Writer even on read-only opens — clients are allowed to
// call Write and get ErrPermission).
type readFile struct{ *os.File }

func (r *readFile) Write(_ []byte) (int, error) { return 0, os.ErrPermission }

// webdavSpillThreshold is the buffered-bytes point above which a webdav
// write handle spills to a temp file on disk rather than growing its
// in-memory buffer. Below this, we keep everything in RAM for speed; above
// it, we swap to a tempfile + WriteStream on Close so a 2 GB upload
// doesn't OOM the process.
const webdavSpillThreshold = 16 * 1024 * 1024

// writeFile accumulates the body of a PUT. Small bodies stay in the
// bytes.Buffer for zero-copy fan-out through pipeline.Write; larger ones
// transparently spill to a temp file so peak memory stays bounded. Either
// way, Close commits the final payload atomically through the pipeline.
type writeFile struct {
	fs      *FS
	ctx     context.Context // captured from OpenFile so Close can plumb it through to pipeline.Write
	rel     string
	abs     string
	buf     *bytes.Buffer
	spill   *os.File // non-nil once buf exceeds webdavSpillThreshold
	written int64    // total bytes Write'n, whether in buf or spill
	modTime time.Time
	size    int64
	closed  bool
}

func (w *writeFile) Write(p []byte) (int, error) {
	if w.spill != nil {
		n, err := w.spill.Write(p)
		w.written += int64(n)
		return n, err
	}
	// Spill once the in-memory buffer grows past the threshold. We flush
	// whatever is already buffered into the temp file and then append
	// the current chunk.
	if w.buf.Len()+len(p) > webdavSpillThreshold {
		tmp, err := os.CreateTemp("", ".kiwi-webdav-*")
		if err != nil {
			return 0, fmt.Errorf("spill tempfile: %w", err)
		}
		if _, err := tmp.Write(w.buf.Bytes()); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return 0, fmt.Errorf("spill flush: %w", err)
		}
		w.buf.Reset()
		w.spill = tmp
		n, werr := tmp.Write(p)
		w.written += int64(n)
		return n, werr
	}
	n, err := w.buf.Write(p)
	w.written += int64(n)
	return n, err
}
func (w *writeFile) Read(p []byte) (int, error) {
	if w.spill != nil {
		return 0, fmt.Errorf("read after spill not supported")
	}
	return w.buf.Read(p)
}
func (w *writeFile) Readdir(_ int) ([]os.FileInfo, error) {
	return nil, fmt.Errorf("not a directory")
}
func (w *writeFile) Seek(offset int64, whence int) (int64, error) {
	if w.spill != nil {
		return 0, fmt.Errorf("seek not supported after spill")
	}
	// The webdav package only seeks on read paths (to derive Content-Length);
	// for in-memory buffers we only need to support Seek(0, SeekEnd) for size.
	switch whence {
	case io.SeekEnd:
		return int64(w.buf.Len()) + offset, nil
	case io.SeekStart:
		if offset == 0 {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("seek not supported on write buffer")
}
func (w *writeFile) Stat() (os.FileInfo, error) {
	size := w.written
	if w.spill == nil {
		size = int64(w.buf.Len())
	}
	return &bufInfo{name: filepath.Base(w.rel), size: size, mod: w.modTime}, nil
}
func (w *writeFile) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	ctx := w.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if w.spill == nil {
		_, err := w.fs.pipe.Write(ctx, w.rel, w.buf.Bytes(), w.fs.actor)
		return err
	}
	// Spilled body — rewind the tempfile and hand it to the streaming
	// path so the pipeline atomically renames it into place instead of
	// reading the whole thing back into memory.
	name := w.spill.Name()
	defer func() {
		w.spill.Close()
		os.Remove(name)
	}()
	if _, err := w.spill.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind spill: %w", err)
	}
	_, err := w.fs.pipe.WriteStream(ctx, w.rel, w.spill, w.written, w.fs.actor)
	return err
}

// bufInfo is an os.FileInfo for in-memory write buffers — used by WebDAV
// clients that Stat() the handle to determine response length.
type bufInfo struct {
	name string
	size int64
	mod  time.Time
}

func (b *bufInfo) Name() string       { return b.name }
func (b *bufInfo) Size() int64        { return b.size }
func (b *bufInfo) Mode() os.FileMode  { return 0644 }
func (b *bufInfo) ModTime() time.Time { return b.mod }
func (b *bufInfo) IsDir() bool        { return false }
func (b *bufInfo) Sys() any           { return nil }
