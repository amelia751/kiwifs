package nfs

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/willscott/go-nfs"
)

// stableAuthHandler combines NullAuth semantics (accept all mounts) with
// deterministic handle generation that survives restarts.
type stableAuthHandler struct {
	fs      *kiwiFS
	handles *stableHandles
}

func (h *stableAuthHandler) Mount(ctx context.Context, conn net.Conn, req nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	return nfs.MountStatusOk, h.fs, []nfs.AuthFlavor{nfs.AuthFlavorNull}
}

func (h *stableAuthHandler) Change(f billy.Filesystem) billy.Change {
	if c, ok := f.(billy.Change); ok {
		return c
	}
	return nil
}

func (h *stableAuthHandler) FSStat(ctx context.Context, f billy.Filesystem, s *nfs.FSStat) error {
	return nil
}

func (h *stableAuthHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	return h.handles.ToHandle(f, path)
}

func (h *stableAuthHandler) FromHandle(b []byte) (billy.Filesystem, []string, error) {
	return h.handles.FromHandle(b)
}

func (h *stableAuthHandler) InvalidateHandle(f billy.Filesystem, b []byte) error {
	return h.handles.InvalidateHandle(f, b)
}

func (h *stableAuthHandler) HandleLimit() int {
	return h.handles.HandleLimit()
}

// Server wraps a userspace NFS server that exposes the knowledge folder
// via NFSv3. All writes flow through the KiwiFS pipeline, ensuring they
// are versioned, indexed, and broadcast via SSE.
type Server struct {
	root     string
	pipeline *pipeline.Pipeline
	handler  nfs.Handler
}

// New creates a new NFS server instance.
// root: the knowledge directory to expose
// pipe: the write pipeline (for versioning, indexing, SSE)
// allow: CIDR allowlist of mountable sources. Empty slice means "localhost
// only" — NFSv3 has no real authentication so the only meaningful defence
// is a network-level allowlist. Passing nil falls back to the same safe
// default so callers can't accidentally expose a world-open NFS port.
func New(root string, pipe *pipeline.Pipeline, allow []*net.IPNet) (*Server, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root: %w", err)
	}

	// Wrap the root directory with a write-intercepting filesystem.
	// Reads go directly to disk. Writes go through the pipeline.
	fs := &kiwiFS{root: absRoot, pipe: pipe}

	// Create NFS handler with stable handles. Unlike go-nfs's default
	// CachingHandler (random UUIDs, lost on restart), stableHandles
	// derives handles from SHA-256(namespaceUUID + path), so they
	// survive server restarts and eliminate ESTALE for cached clients.
	// This is the NFS-Ganesha HandleMap pattern.
	handles := newStableHandles(absRoot)
	handler := nfs.Handler(&stableAuthHandler{fs: fs, handles: handles})
	if len(allow) == 0 {
		allow = DefaultAllow()
	}
	handler = &allowHandler{inner: handler, allow: allow}

	return &Server{
		root:     absRoot,
		pipeline: pipe,
		handler:  handler,
	}, nil
}

// DefaultAllow is the localhost-only CIDR set used when no --nfs-allow is
// passed. It covers IPv4 loopback (127.0.0.0/8) and IPv6 loopback (::1/128).
func DefaultAllow() []*net.IPNet {
	_, v4, _ := net.ParseCIDR("127.0.0.0/8")
	_, v6, _ := net.ParseCIDR("::1/128")
	return []*net.IPNet{v4, v6}
}

// ParseAllow parses a comma-separated list of CIDRs into []*net.IPNet.
// Bare IPs are accepted (implicit /32 or /128). Empty input returns nil
// so callers can distinguish "no flag" from "explicit empty set".
func ParseAllow(spec string) ([]*net.IPNet, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	var out []*net.IPNet
	for _, raw := range strings.Split(spec, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if !strings.Contains(raw, "/") {
			ip := net.ParseIP(raw)
			if ip == nil {
				return nil, fmt.Errorf("invalid IP %q", raw)
			}
			if ip.To4() != nil {
				raw = raw + "/32"
			} else {
				raw = raw + "/128"
			}
		}
		_, cidr, err := net.ParseCIDR(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q: %w", raw, err)
		}
		out = append(out, cidr)
	}
	return out, nil
}

// allowHandler wraps an nfs.Handler with a per-connection IP allowlist.
// Every other method delegates transparently; only Mount short-circuits
// with MountStatusErrAcces when the client isn't in the allowlist.
type allowHandler struct {
	inner nfs.Handler
	allow []*net.IPNet
}

func (h *allowHandler) Mount(ctx context.Context, conn net.Conn, req nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	if !ipAllowed(conn.RemoteAddr(), h.allow) {
		log.Printf("nfs: mount denied from %s (not in allowlist)", conn.RemoteAddr())
		return nfs.MountStatusErrAcces, nil, nil
	}
	return h.inner.Mount(ctx, conn, req)
}

func (h *allowHandler) Change(fs billy.Filesystem) billy.Change {
	return h.inner.Change(fs)
}

func (h *allowHandler) FSStat(ctx context.Context, fs billy.Filesystem, s *nfs.FSStat) error {
	return h.inner.FSStat(ctx, fs, s)
}

func (h *allowHandler) ToHandle(fs billy.Filesystem, s []string) []byte {
	return h.inner.ToHandle(fs, s)
}

func (h *allowHandler) FromHandle(b []byte) (billy.Filesystem, []string, error) {
	return h.inner.FromHandle(b)
}

func (h *allowHandler) InvalidateHandle(fs billy.Filesystem, b []byte) error {
	return h.inner.InvalidateHandle(fs, b)
}

func (h *allowHandler) HandleLimit() int {
	return h.inner.HandleLimit()
}

func ipAllowed(addr net.Addr, allow []*net.IPNet) bool {
	if len(allow) == 0 {
		return false
	}
	var ip net.IP
	switch a := addr.(type) {
	case *net.TCPAddr:
		ip = a.IP
	case *net.UDPAddr:
		ip = a.IP
	default:
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			return false
		}
		ip = net.ParseIP(host)
	}
	if ip == nil {
		return false
	}
	for _, c := range allow {
		if c.Contains(ip) {
			return true
		}
	}
	return false
}

// Handler returns the NFS protocol handler for serving.
func (s *Server) Handler() nfs.Handler {
	return s.handler
}

// Process-local advisory locking. NOT distributed — multiple KiwiFS
// processes cannot coordinate locks.
var (
	fileLocks   = make(map[string]string) // path → holder ID
	fileLocksMu sync.Mutex
)

var (
	openFiles      = make(map[string]int)
	openFilesMu    sync.Mutex
	pendingUnlinks   = make(map[string]bool)
	pendingUnlinksMu sync.Mutex
	hiddenPaths      = make(map[string]string)
	hiddenPathsMu    sync.Mutex
)

// kiwiFS implements the billy.Filesystem interface, routing writes
// through the KiwiFS pipeline.
type kiwiFS struct {
	root string
	pipe *pipeline.Pipeline
}

// The billy.Filesystem interface expects certain methods. We implement a
// minimal subset focused on read/write operations.

func (fs *kiwiFS) Create(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
}

func (fs *kiwiFS) Open(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *kiwiFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	fullPath, err := fs.safePath(filename)
	if err != nil {
		return nil, err
	}

	// For writes, we intercept and route through the pipeline
	if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		openFilesMu.Lock()
		openFiles[filename]++
		openFilesMu.Unlock()
		return &kiwiFile{
			path:     filename,
			fullPath: fullPath,
			fs:       fs,
			flag:     flag,
		}, nil
	}

	// For reads, check if file was unlinked and hidden
	actualPath := fullPath
	hiddenPathsMu.Lock()
	if hp, ok := hiddenPaths[filename]; ok {
		actualPath = hp
	}
	hiddenPathsMu.Unlock()

	f, err := os.OpenFile(actualPath, flag, perm)
	if err != nil {
		return nil, err
	}

	openFilesMu.Lock()
	openFiles[filename]++
	openFilesMu.Unlock()

	return &kiwiFile{
		path:     filename,
		fullPath: fullPath,
		fs:       fs,
		osFile:   f,
		flag:     flag,
	}, nil
}

func (fs *kiwiFS) Stat(filename string) (os.FileInfo, error) {
	fullPath, err := fs.safePath(filename)
	if err != nil {
		return nil, err
	}
	return os.Stat(fullPath)
}

func (fs *kiwiFS) ReadDir(path string) ([]os.FileInfo, error) {
	fullPath, err := fs.safePath(path)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	infos := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		// Skip any leading-dot entry (.git, .kiwi, .versions, …) to stay in
		// sync with the storage layer's hidden() filter — otherwise NFS
		// clients see directories the REST API deliberately hides.
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		info, err := entry.Info()
		if err == nil {
			infos = append(infos, info)
		}
	}
	return infos, nil
}

func (fs *kiwiFS) MkdirAll(filename string, perm os.FileMode) error {
	fullPath, err := fs.safePath(filename)
	if err != nil {
		return err
	}
	return os.MkdirAll(fullPath, perm)
}

func (fs *kiwiFS) Remove(filename string) error {
	fullPath, err := fs.safePath(filename)
	if err != nil {
		return err
	}
	info, serr := os.Stat(fullPath)
	if serr != nil {
		return serr
	}
	if info.IsDir() {
		return os.Remove(fullPath)
	}

	openFilesMu.Lock()
	count := openFiles[filename]
	openFilesMu.Unlock()

	if count > 0 {
		pendingUnlinksMu.Lock()
		pendingUnlinks[filename] = true
		pendingUnlinksMu.Unlock()

		hidden := filepath.Join(filepath.Dir(fullPath), ".kiwi-unlinked-"+filepath.Base(fullPath))
		if err := os.Rename(fullPath, hidden); err == nil {
			hiddenPathsMu.Lock()
			hiddenPaths[filename] = hidden
			hiddenPathsMu.Unlock()
		}
		fs.pipe.DeindexFile(context.Background(), filename)
		return nil
	}

	return fs.pipe.Delete(context.Background(), filename, "nfs")
}

func (fs *kiwiFS) Rename(oldpath, newpath string) error {
	absOld, err := fs.safePath(oldpath)
	if err != nil {
		return err
	}
	absNew, err := fs.safePath(newpath)
	if err != nil {
		return err
	}

	info, err := os.Stat(absOld)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		_, rerr := fs.pipe.Rename(context.Background(), oldpath, newpath, "nfs")
		return rerr
	}

	// os.Rename follows POSIX rename(2): if absNew is an empty dir it is
	// replaced; if non-empty, ENOTEMPTY. Files under a replaced destination
	// are not de-indexed — accepted gap for rename-onto-existing-dir.
	if err := os.Rename(absOld, absNew); err != nil {
		return err
	}

	ctx := context.Background()
	var allPaths []string
	filepath.WalkDir(absNew, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			log.Printf("nfs: dir rename walk error: %v", err)
			return nil
		}
		if d.IsDir() {
			return nil
		}
		relToNew, relErr := filepath.Rel(absNew, path)
		if relErr != nil {
			log.Printf("nfs: dir rename rel(%s, %s): %v", absNew, path, relErr)
			return nil
		}
		newRel := filepath.ToSlash(filepath.Join(newpath, relToNew))
		oldRel := filepath.ToSlash(filepath.Join(oldpath, relToNew))
		content, rerr := os.ReadFile(path)
		if rerr != nil {
			log.Printf("nfs: dir rename read(%s): %v", path, rerr)
			return nil
		}
		fs.pipe.IndexFile(ctx, newRel, content)
		fs.pipe.DeindexFile(ctx, oldRel)
		allPaths = append(allPaths, newRel, oldRel)
		return nil
	})

	if len(allPaths) > 0 {
		msg := fmt.Sprintf("nfs: rename dir %s → %s", oldpath, newpath)
		fs.pipe.BulkCommitOnly(ctx, allPaths, "nfs", msg)
	}
	return nil
}

func (fs *kiwiFS) safePath(filename string) (string, error) {
	return storage.GuardPath(fs.root, filepath.ToSlash(filename))
}

// kiwiFile wraps file operations, routing writes through the pipeline.
type kiwiFile struct {
	path         string  // relative path (e.g., "runs/run-249.md")
	fullPath     string  // absolute path on disk
	fs           *kiwiFS // parent filesystem
	osFile       *os.File
	flag         int
	buffer       []byte // write buffer (accumulated until Close)
	lockID       string // unique ID for advisory locking
	lastSyncETag string // ETag at last Sync, to skip redundant Close flush
}

func (f *kiwiFile) Read(p []byte) (int, error) {
	if f.osFile != nil {
		return f.osFile.Read(p)
	}
	// If opened for write-only, reading is not allowed
	return 0, fmt.Errorf("file not opened for reading")
}

func (f *kiwiFile) Write(p []byte) (int, error) {
	f.buffer = append(f.buffer, p...)
	f.lastSyncETag = ""
	return len(p), nil
}

func (f *kiwiFile) Close() error {
	var writeErr error
	if len(f.buffer) > 0 && pipeline.ETag(f.buffer) != f.lastSyncETag {
		if _, err := f.fs.pipe.Write(context.Background(), f.path, f.buffer, "nfs"); err != nil {
			writeErr = fmt.Errorf("pipeline write: %w", err)
		}
	}

	f.Unlock()

	if f.osFile != nil {
		if err := f.osFile.Close(); err != nil && writeErr == nil {
			writeErr = err
		}
	}

	openFilesMu.Lock()
	openFiles[f.path]--
	lastClose := openFiles[f.path] <= 0
	if lastClose {
		delete(openFiles, f.path)
	}
	openFilesMu.Unlock()

	if lastClose {
		pendingUnlinksMu.Lock()
		shouldDelete := pendingUnlinks[f.path]
		if shouldDelete {
			delete(pendingUnlinks, f.path)
		}
		pendingUnlinksMu.Unlock()

		if shouldDelete {
			hiddenPathsMu.Lock()
			hp := hiddenPaths[f.path]
			delete(hiddenPaths, f.path)
			hiddenPathsMu.Unlock()
			if hp != "" {
				os.Remove(hp)
			}
			f.fs.pipe.DeferredDelete(context.Background(), f.path, "nfs")
		}
	}

	return writeErr
}

func (f *kiwiFile) Seek(offset int64, whence int) (int64, error) {
	if f.osFile != nil {
		return f.osFile.Seek(offset, whence)
	}
	return 0, fmt.Errorf("seek not supported on write-only file")
}

func (f *kiwiFile) ReadAt(p []byte, off int64) (int, error) {
	if f.osFile != nil {
		return f.osFile.ReadAt(p, off)
	}
	return 0, fmt.Errorf("file not opened for reading")
}

// maxFileSize is the hard limit for in-memory file buffers. Prevents OOM
// from writes at absurdly large offsets (e.g. offset near INT64_MAX).
const maxFileSize = 64 * 1024 * 1024 // 64 MB

func (f *kiwiFile) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	need := off + int64(len(p))
	if need > maxFileSize {
		return 0, fmt.Errorf("write would exceed %d byte limit (offset=%d, len=%d)", maxFileSize, off, len(p))
	}

	if f.buffer == nil && f.osFile != nil {
		existing, err := io.ReadAll(f.osFile)
		if err != nil {
			return 0, fmt.Errorf("read existing content: %w", err)
		}
		f.buffer = existing
		f.osFile.Close()
		f.osFile = nil
	}
	if f.buffer == nil {
		f.buffer = []byte{}
	}

	if int64(len(f.buffer)) < need {
		grown := make([]byte, need)
		copy(grown, f.buffer)
		f.buffer = grown
	}
	copy(f.buffer[off:], p)
	f.lastSyncETag = ""
	return len(p), nil
}

func (f *kiwiFile) Name() string {
	return f.path
}

func (f *kiwiFile) Truncate(size int64) error {
	if size == 0 {
		f.buffer = nil
		f.lastSyncETag = ""
		return nil
	}
	if size < 0 {
		return fmt.Errorf("negative truncate size")
	}
	if size > maxFileSize {
		return fmt.Errorf("truncate size %d exceeds %d byte limit", size, maxFileSize)
	}

	if f.buffer == nil && f.osFile != nil {
		existing, err := io.ReadAll(f.osFile)
		if err != nil {
			return fmt.Errorf("read for truncate: %w", err)
		}
		f.buffer = existing
		f.osFile.Close()
		f.osFile = nil
	}
	if f.buffer == nil {
		f.buffer = []byte{}
	}

	if int64(len(f.buffer)) > size {
		f.buffer = f.buffer[:size]
	} else if int64(len(f.buffer)) < size {
		f.buffer = append(f.buffer, make([]byte, size-int64(len(f.buffer)))...)
	}
	f.lastSyncETag = ""
	return nil
}

func (f *kiwiFile) Sync() error {
	if len(f.buffer) == 0 {
		return nil
	}
	_, err := f.fs.pipe.Write(context.Background(), f.path, f.buffer, "nfs")
	if err == nil {
		f.lastSyncETag = pipeline.ETag(f.buffer)
	}
	return err
}

func (f *kiwiFile) Lock() error {
	fileLocksMu.Lock()
	defer fileLocksMu.Unlock()

	if holder, held := fileLocks[f.path]; held && holder != f.lockID {
		return fmt.Errorf("file is locked by another handle")
	}
	if f.lockID == "" {
		f.lockID = fmt.Sprintf("%p-%d", f, time.Now().UnixNano())
	}
	fileLocks[f.path] = f.lockID
	return nil
}

func (f *kiwiFile) Unlock() error {
	fileLocksMu.Lock()
	defer fileLocksMu.Unlock()

	if holder, held := fileLocks[f.path]; held && holder == f.lockID {
		delete(fileLocks, f.path)
	}
	return nil
}

// Ensure kiwiFile implements the billy.File interface
var _ billy.File = (*kiwiFile)(nil)

// Utility methods required by billy.Filesystem interface

func (fs *kiwiFS) Join(elem ...string) string {
	return filepath.Join(elem...)
}

func (fs *kiwiFS) TempFile(dir, prefix string) (billy.File, error) {
	fullPath, err := fs.safePath(dir)
	if err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(fullPath, prefix)
	if err != nil {
		return nil, err
	}
	relPath, _ := filepath.Rel(fs.root, f.Name())
	return &kiwiFile{
		path:     relPath,
		fullPath: f.Name(),
		fs:       fs,
		osFile:   f,
	}, nil
}

func (fs *kiwiFS) Readlink(link string) (string, error) {
	fullPath, err := fs.safePath(link)
	if err != nil {
		return "", err
	}
	return os.Readlink(fullPath)
}

func (fs *kiwiFS) Symlink(target, link string) error {
	return fs.pipe.CreateSymlink(context.Background(), link, target, "nfs")
}

func (fs *kiwiFS) Lstat(filename string) (os.FileInfo, error) {
	fullPath, err := fs.safePath(filename)
	if err != nil {
		return nil, err
	}
	return os.Lstat(fullPath)
}

func (fs *kiwiFS) Chroot(path string) (billy.Filesystem, error) {
	newRoot, err := fs.safePath(path)
	if err != nil {
		return nil, err
	}
	return &kiwiFS{root: newRoot, pipe: fs.pipe}, nil
}

func (fs *kiwiFS) Root() string {
	return fs.root
}

// Ensure kiwiFS implements the billy.Filesystem interface
var _ billy.Filesystem = (*kiwiFS)(nil)
