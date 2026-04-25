package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// cacheTTL bounds the lifetime of a cached directory listing or file body.
// Production FUSE clients (gcsfuse, rclone mount) default to 30–60s — short
// enough that other writers' changes appear on the next `ls` / re-read but
// long enough to amortize metadata RTTs across a shell session.
const cacheTTL = 30 * time.Second

// apiURL builds a kiwifs REST URL for the given endpoint ("/api/kiwi/file",
// "/api/kiwi/tree", …) with path safely URL-encoded. Raw interpolation
// breaks on any filename containing &, #, =, ?, spaces, or + — all of
// which are legal inside a markdown filename.
func (c *Client) apiURL(endpoint, p string) string {
	q := url.Values{}
	q.Set("path", p)
	return c.remote + endpoint + "?" + q.Encode()
}

// Client wraps a FUSE filesystem that mounts a remote KiwiFS server.
// Reads are served from local cache with background sync.
// Writes block until the remote API confirms the commit.
type Client struct {
	remote string // remote KiwiFS server URL (e.g., "http://localhost:3333")
	client *http.Client

	// auth, when set, is injected into every outbound request. See
	// NewClientWithAuth / ClientAuth for the accepted shapes.
	auth *ClientAuth

	// space selects a named knowledge space on multi-space deployments.
	// Sent as X-Kiwi-Space; leave empty for the default space.
	space string

	cacheMu sync.RWMutex
	dirs    map[string]*dirCacheEntry  // keyed by dir path
	files   map[string]*fileCacheEntry // keyed by file path
}

type dirCacheEntry struct {
	entries []fuse.DirEntry
	stamp   time.Time
}

type fileCacheEntry struct {
	data  []byte
	stamp time.Time
}

// ClientAuth is a tagged union for the authentication styles kiwifs
// supports. Exactly one of APIKey / Bearer / Basic should be non-zero.
// Zero-value means "no auth header".
type ClientAuth struct {
	// APIKey is sent verbatim as `X-API-Key: <value>` — matches the
	// server's `auth.api_key` / per-space API key middleware.
	APIKey string

	// Bearer is sent as `Authorization: Bearer <value>` — use with the
	// OIDC flow or any JWT-issuing proxy in front of kiwifs.
	Bearer string

	// BasicUser/BasicPass, when both set, emit an HTTP Basic header.
	// Useful for Caddy / nginx basic-auth wrappers.
	BasicUser string
	BasicPass string
}

func (a *ClientAuth) empty() bool {
	if a == nil {
		return true
	}
	return a.APIKey == "" && a.Bearer == "" && (a.BasicUser == "" || a.BasicPass == "")
}

// NewClient creates a new FUSE client with no authentication. For protected
// servers prefer NewClientWithAuth.
func NewClient(remote string) *Client {
	return &Client{
		remote: strings.TrimSuffix(remote, "/"),
		client: &http.Client{Timeout: 30 * time.Second},
		dirs:   make(map[string]*dirCacheEntry),
		files:  make(map[string]*fileCacheEntry),
	}
}

// NewClientWithAuth constructs a client that attaches auth and an optional
// space selector to every outbound request. Pass a nil or empty auth to
// disable authentication.
func NewClientWithAuth(remote string, auth *ClientAuth, space string) *Client {
	c := NewClient(remote)
	if !auth.empty() {
		c.auth = auth
	}
	c.space = space
	return c
}

// do is the authenticated request helper. Every FUSE codepath MUST route
// through here so auth and space selection are never forgotten.
func (c *Client) do(req *http.Request) (*http.Response, error) {
	if c.auth != nil {
		if c.auth.APIKey != "" {
			req.Header.Set("X-API-Key", c.auth.APIKey)
		}
		if c.auth.Bearer != "" {
			req.Header.Set("Authorization", "Bearer "+c.auth.Bearer)
		}
		if c.auth.BasicUser != "" && c.auth.BasicPass != "" {
			req.SetBasicAuth(c.auth.BasicUser, c.auth.BasicPass)
		}
	}
	if c.space != "" {
		req.Header.Set("X-Kiwi-Space", c.space)
	}
	return c.client.Do(req)
}

// get is a tiny GET helper that routes through do() so auth is always
// attached. Prefer this over client.Get in new code.
func (c *Client) get(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	return c.do(req)
}

// Mount mounts the remote KiwiFS at the given mountpoint.
func (c *Client) Mount(mountpoint string) error {
	root := &kiwiNode{
		client: c,
		path:   "",
	}

	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Name:   "kiwifs",
			FsName: "kiwifs",
			Debug:  false,
		},
	})
	if err != nil {
		return fmt.Errorf("mount failed: %w", err)
	}

	fmt.Printf("KiwiFS mounted at %s (remote: %s)\n", mountpoint, c.remote)
	fmt.Println("Press Ctrl+C to unmount")

	// Wait until unmount
	server.Wait()
	return nil
}

// cachedDir returns a cached listing when fresh, else nil.
func (c *Client) cachedDir(path string) []fuse.DirEntry {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()
	if e, ok := c.dirs[path]; ok && time.Since(e.stamp) < cacheTTL {
		return e.entries
	}
	return nil
}

func (c *Client) storeDir(path string, entries []fuse.DirEntry) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.dirs[path] = &dirCacheEntry{entries: entries, stamp: time.Now()}
}

func (c *Client) cachedFile(path string) []byte {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()
	if e, ok := c.files[path]; ok && time.Since(e.stamp) < cacheTTL {
		return e.data
	}
	return nil
}

func (c *Client) storeFile(path string, data []byte) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.files[path] = &fileCacheEntry{data: data, stamp: time.Now()}
}

// invalidate drops cached copies of a path and its parent directory listing.
// Called on every local Write / Delete / Mkdir so the next read reflects
// our change without waiting for the TTL.
func (c *Client) invalidate(path string) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	delete(c.files, path)
	parent := ""
	if idx := strings.LastIndexByte(path, '/'); idx > 0 {
		parent = path[:idx]
	}
	delete(c.dirs, parent)
}

// kiwiNode represents a file or directory in the FUSE filesystem.
type kiwiNode struct {
	fs.Inode
	client *Client
	path   string
}

// Ensure kiwiNode implements the necessary interfaces
var _ fs.NodeGetattrer = (*kiwiNode)(nil)
var _ fs.NodeReaddirer = (*kiwiNode)(nil)
var _ fs.NodeLookuper = (*kiwiNode)(nil)
var _ fs.NodeOpener = (*kiwiNode)(nil)
var _ fs.NodeCreater = (*kiwiNode)(nil)
var _ fs.NodeUnlinker = (*kiwiNode)(nil)
var _ fs.NodeMkdirer = (*kiwiNode)(nil)
var _ fs.NodeRmdirer = (*kiwiNode)(nil)

func httpErrno(status int) syscall.Errno {
	switch {
	case status == http.StatusNotFound:
		return syscall.ENOENT
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return syscall.EACCES
	default:
		return syscall.EIO
	}
}

func childPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return filepath.Join(parent, name)
}

// statFile issues a GET against the file endpoint and returns (size, found).
// KiwiFS doesn't implement HEAD on /api/kiwi/file, so the only portable way
// to get a content length is a cached GET. Uses the file cache so a
// subsequent Read is a no-op on the network.
func (n *kiwiNode) statFile() (int64, bool, syscall.Errno) {
	if cached := n.client.cachedFile(n.path); cached != nil {
		return int64(len(cached)), true, 0
	}
	resp, err := n.client.get(n.client.apiURL("/api/kiwi/file", n.path))
	if err != nil {
		return 0, false, syscall.EIO
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, false, 0
	}
	if resp.StatusCode != http.StatusOK {
		return 0, false, httpErrno(resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false, syscall.EIO
	}
	n.client.storeFile(n.path, data)
	return int64(len(data)), true, 0
}

// Getattr retrieves file attributes.
func (n *kiwiNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// If this is the root node
	if n.path == "" {
		out.Mode = 0755 | syscall.S_IFDIR
		out.Size = 4096
		out.Mtime = uint64(time.Now().Unix())
		out.Atime = out.Mtime
		out.Ctime = out.Mtime
		return 0
	}

	size, found, errno := n.statFile()
	if errno != 0 {
		return errno
	}
	if !found {
		// File lookup failed — try treating it as a directory. A successful
		// /tree response is the server's way of saying "this exists and is
		// a dir".
		if _, derr := n.listDir(); derr == 0 {
			out.Mode = 0755 | syscall.S_IFDIR
			out.Size = 4096
			out.Mtime = uint64(time.Now().Unix())
			out.Atime = out.Mtime
			out.Ctime = out.Mtime
			return 0
		}
		return syscall.ENOENT
	}

	out.Mode = 0644 | syscall.S_IFREG
	out.Size = uint64(size)
	out.Mtime = uint64(time.Now().Unix())
	out.Atime = out.Mtime
	out.Ctime = out.Mtime
	return 0
}

// treeResponse mirrors the shape of /api/kiwi/tree. Children live under a
// nested array, *not* at the top level — the previous implementation
// decoded the response as a bare slice and so every `ls` silently returned
// zero entries.
type treeResponse struct {
	Path     string         `json:"path"`
	Name     string         `json:"name"`
	IsDir    bool           `json:"isDir"`
	Children []treeResponse `json:"children"`
}

// listDir fetches a directory listing (cached) and returns the entry list
// the FUSE layer consumes. Returns a 0 errno on success.
func (n *kiwiNode) listDir() ([]fuse.DirEntry, syscall.Errno) {
	if cached := n.client.cachedDir(n.path); cached != nil {
		return cached, 0
	}
	resp, err := n.client.get(n.client.apiURL("/api/kiwi/tree", n.path))
	if err != nil {
		return nil, syscall.EIO
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, httpErrno(resp.StatusCode)
	}
	var tree treeResponse
	if err := json.NewDecoder(resp.Body).Decode(&tree); err != nil {
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(tree.Children))
	for _, child := range tree.Children {
		mode := uint32(syscall.S_IFREG)
		if child.IsDir {
			mode = syscall.S_IFDIR
		}
		entries = append(entries, fuse.DirEntry{Name: child.Name, Mode: mode})
	}
	n.client.storeDir(n.path, entries)
	return entries, 0
}

// Readdir lists directory contents.
func (n *kiwiNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, errno := n.listDir()
	if errno != 0 {
		return nil, errno
	}
	return fs.NewListDirStream(entries), 0
}

// Lookup finds a child node by name.
func (n *kiwiNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	cp := childPath(n.path, name)

	child := &kiwiNode{
		client: n.client,
		path:   cp,
	}

	size, found, errno := child.statFile()
	if errno != 0 {
		return nil, errno
	}
	if found {
		out.Mode = 0644 | syscall.S_IFREG
		out.Size = uint64(size)
		stable := fs.StableAttr{Mode: syscall.S_IFREG}
		return n.NewInode(ctx, child, stable), 0
	}
	// Not a file — maybe a directory.
	if _, derr := child.listDir(); derr == 0 {
		out.Mode = 0755 | syscall.S_IFDIR
		stable := fs.StableAttr{Mode: syscall.S_IFDIR}
		return n.NewInode(ctx, child, stable), 0
	}
	return nil, syscall.ENOENT
}

// Open opens a file for reading or writing.
func (n *kiwiNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return &kiwiFile{
		node:   n,
		client: n.client,
	}, 0, 0
}

// Create creates a new file.
func (n *kiwiNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	cp := childPath(n.path, name)

	child := &kiwiNode{
		client: n.client,
		path:   cp,
	}

	fh := &kiwiFile{
		node:   child,
		client: n.client,
	}

	out.Mode = 0644 | syscall.S_IFREG
	stable := fs.StableAttr{Mode: syscall.S_IFREG}
	return n.NewInode(ctx, child, stable), fh, 0, 0
}

// Unlink deletes a file.
func (n *kiwiNode) Unlink(ctx context.Context, name string) syscall.Errno {
	cp := childPath(n.path, name)

	req, _ := http.NewRequest("DELETE", n.client.apiURL("/api/kiwi/file", cp), nil)
	resp, err := n.client.do(req)
	if err != nil {
		return syscall.EIO
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return httpErrno(resp.StatusCode)
	}
	n.client.invalidate(cp)
	return 0
}

// Mkdir creates a directory on the server. KiwiFS has no explicit "make
// directory" endpoint — directories exist only as path prefixes of files
// — so we write a hidden placeholder (".keep") inside the new directory.
// This matches git's usual convention for preserving empty dirs and makes
// the server-side state consistent with what local FUSE operations expect.
func (n *kiwiNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	cp := childPath(n.path, name)

	placeholder := filepath.Join(cp, ".keep")
	req, _ := http.NewRequest("PUT", n.client.apiURL("/api/kiwi/file", placeholder), bytes.NewReader(nil))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Actor", "fuse")
	resp, err := n.client.do(req)
	if err != nil {
		return nil, syscall.EIO
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, httpErrno(resp.StatusCode)
	}
	n.client.invalidate(placeholder)

	child := &kiwiNode{client: n.client, path: cp}
	out.Mode = 0755 | syscall.S_IFDIR
	stable := fs.StableAttr{Mode: syscall.S_IFDIR}
	return n.NewInode(ctx, child, stable), 0
}

// Rmdir removes a directory.
func (n *kiwiNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	// For now, treat as unlink (the server will handle directory deletion)
	return n.Unlink(ctx, name)
}

// kiwiFile represents an open file handle.
type kiwiFile struct {
	node   *kiwiNode
	client *Client
	data   []byte // cached data for reads/writes
	dirty  bool   // whether Write touched the buffer and we must PUT on Flush
}

// Ensure kiwiFile implements the necessary interfaces
var _ fs.FileReader = (*kiwiFile)(nil)
var _ fs.FileWriter = (*kiwiFile)(nil)
var _ fs.FileFlusher = (*kiwiFile)(nil)

// Read reads from the file.
func (f *kiwiFile) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// If we haven't cached the data yet, try the shared TTL cache before
	// hitting the network.
	if f.data == nil {
		if cached := f.client.cachedFile(f.node.path); cached != nil {
			f.data = cached
		}
	}
	if f.data == nil {
		resp, err := f.client.get(f.client.apiURL("/api/kiwi/file", f.node.path))
		if err != nil {
			return nil, syscall.EIO
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return nil, syscall.EACCES
		}
		if resp.StatusCode != http.StatusOK {
			return nil, syscall.ENOENT
		}

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, syscall.EIO
		}
		f.data = data
		f.client.storeFile(f.node.path, data)
	}

	// Read from cached data
	if off >= int64(len(f.data)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	end := off + int64(len(dest))
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}

	return fuse.ReadResultData(f.data[off:end]), 0
}

// Write writes to the file (accumulates in memory).
func (f *kiwiFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	// Ensure buffer is large enough
	need := off + int64(len(data))
	if int64(len(f.data)) < need {
		newData := make([]byte, need)
		copy(newData, f.data)
		f.data = newData
	}

	// Write to buffer
	copy(f.data[off:], data)
	f.dirty = true
	return uint32(len(data)), 0
}

// Flush writes the buffered data to the remote server.
func (f *kiwiFile) Flush(ctx context.Context) syscall.Errno {
	if !f.dirty {
		return 0
	}

	req, _ := http.NewRequest("PUT", f.client.apiURL("/api/kiwi/file", f.node.path), bytes.NewReader(f.data))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Actor", "fuse")

	resp, err := f.client.do(req)
	if err != nil {
		return syscall.EIO
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return httpErrno(resp.StatusCode)
	}

	// Our write invalidates sibling caches — drop them so the next read
	// fetches the server's new truth rather than whatever used to sit here.
	f.client.invalidate(f.node.path)
	f.client.storeFile(f.node.path, append([]byte(nil), f.data...))
	f.dirty = false
	return 0
}
