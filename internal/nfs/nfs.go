package nfs

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
)

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

	// Create NFS handler using go-nfs helpers, then wrap it with the
	// IP-allowlist check. go-nfs is NFSv3-only — its native auth story
	// is weak (NullAuthHandler accepts everyone), so reachability is the
	// control surface that actually matters.
	handler := nfshelper.NewNullAuthHandler(fs)
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
		return &kiwiFile{
			path:     filename,
			fullPath: fullPath,
			fs:       fs,
			flag:     flag,
		}, nil
	}

	// For reads, just open the file directly
	f, err := os.OpenFile(fullPath, flag, perm)
	if err != nil {
		return nil, err
	}
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
	if _, err := fs.safePath(filename); err != nil {
		return err
	}
	// Route deletes through pipeline for versioning + indexing.
	// NFS callbacks don't carry a request context (the willscott/go-nfs
	// Handler interface predates context propagation), so the pipeline gets
	// context.Background() until/unless we ship our own forked Handler.
	if strings.HasSuffix(filename, ".md") {
		return fs.pipe.Delete(context.Background(), filename, "nfs")
	}
	// Non-markdown files: direct delete
	fullPath, err := fs.safePath(filename)
	if err != nil {
		return err
	}
	return os.Remove(fullPath)
}

func (fs *kiwiFS) Rename(oldpath, newpath string) error {
	fullOld, err := fs.safePath(oldpath)
	if err != nil {
		return err
	}
	fullNew, err := fs.safePath(newpath)
	if err != nil {
		return err
	}

	// Every rename — markdown or not — goes through the pipeline so git
	// sees a coherent "delete old + write new" pair and the search/link
	// indices get updated. A bare os.Rename would leave the search
	// engine claiming the file still lives at its old path and skip the
	// git commit entirely.
	content, readErr := os.ReadFile(fullOld)
	if readErr != nil {
		return readErr
	}
	if _, werr := fs.pipe.Write(context.Background(), newpath, content, "nfs"); werr != nil {
		return fmt.Errorf("pipeline write %s: %w", newpath, werr)
	}
	if derr := fs.pipe.Delete(context.Background(), oldpath, "nfs"); derr != nil {
		return fmt.Errorf("pipeline delete %s: %w", oldpath, derr)
	}
	_ = fullNew
	return nil
}

// safePath joins filename onto fs.root and rejects any result that escapes
// the root. Using filepath.Rel + a ".." prefix check is the only correct
// way — a plain strings.HasPrefix(full, root) is trivially bypassable
// (e.g. "/data/knowledge-evil" passes when root is "/data/knowledge").
func (fs *kiwiFS) safePath(filename string) (string, error) {
	clean := filepath.Clean("/" + filepath.ToSlash(filename))
	full := filepath.Join(fs.root, clean)
	rel, err := filepath.Rel(fs.root, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal denied: %s", filename)
	}
	return full, nil
}

// kiwiFile wraps file operations, routing writes through the pipeline.
type kiwiFile struct {
	path     string       // relative path (e.g., "runs/run-249.md")
	fullPath string       // absolute path on disk
	fs       *kiwiFS      // parent filesystem
	osFile   *os.File     // underlying OS file (for reads)
	flag     int          // open flags
	buffer   []byte       // write buffer (accumulated until Close)
}

func (f *kiwiFile) Read(p []byte) (int, error) {
	if f.osFile != nil {
		return f.osFile.Read(p)
	}
	// If opened for write-only, reading is not allowed
	return 0, fmt.Errorf("file not opened for reading")
}

func (f *kiwiFile) Write(p []byte) (int, error) {
	// Accumulate writes in memory
	f.buffer = append(f.buffer, p...)
	return len(p), nil
}

func (f *kiwiFile) Close() error {
	// Every write — markdown or not — goes through the pipeline so
	// non-markdown uploads get a git commit, SSE broadcast, and trust-
	// layer row like any other file. The search layer already skips
	// indexing for non-knowledge paths, so a PDF write is correctly
	// versioned but not full-text indexed.
	if len(f.buffer) > 0 {
		if _, err := f.fs.pipe.Write(context.Background(), f.path, f.buffer, "nfs"); err != nil {
			return fmt.Errorf("pipeline write: %w", err)
		}
	}

	if f.osFile != nil {
		return f.osFile.Close()
	}
	return nil
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

func (f *kiwiFile) WriteAt(p []byte, off int64) (int, error) {
	// For simplicity, we don't support random writes via NFS yet.
	// Most agent use cases are append-only or full overwrites.
	return 0, fmt.Errorf("random writes not supported; use sequential writes")
}

func (f *kiwiFile) Name() string {
	return f.path
}

func (f *kiwiFile) Truncate(size int64) error {
	// Reset buffer for truncate to 0
	if size == 0 {
		f.buffer = nil
		return nil
	}
	return fmt.Errorf("truncate to non-zero size not supported")
}

func (f *kiwiFile) Sync() error {
	// Writes are already flushed on Close through the pipeline
	return nil
}

func (f *kiwiFile) Lock() error {
	// File locking not implemented (optimistic locking via ETags in REST API)
	return nil
}

func (f *kiwiFile) Unlock() error {
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
	fullLink, err := fs.safePath(link)
	if err != nil {
		return err
	}
	return os.Symlink(target, fullLink)
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
