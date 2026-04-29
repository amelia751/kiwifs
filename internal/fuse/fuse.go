//go:build !windows

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
	data      []byte
	stamp     time.Time
	isSymlink bool
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

func (c *Client) cachedFile(path string) *fileCacheEntry {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()
	if e, ok := c.files[path]; ok && time.Since(e.stamp) < cacheTTL {
		return e
	}
	return nil
}

func (c *Client) storeFile(path string, data []byte, isSymlink bool) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.files[path] = &fileCacheEntry{data: data, stamp: time.Now(), isSymlink: isSymlink}
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
var _ fs.NodeRenamer = (*kiwiNode)(nil)
var _ fs.NodeSetattrer = (*kiwiNode)(nil)
var _ fs.NodeSymlinker = (*kiwiNode)(nil)
var _ fs.NodeReadlinker = (*kiwiNode)(nil)

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

// fileStat holds metadata returned by statFile.
type fileStat struct {
	size      int64
	modTime   time.Time
	found     bool
	isSymlink bool
}

// statFile issues a GET against the file endpoint and returns file metadata.
// KiwiFS doesn't implement HEAD on /api/kiwi/file, so the only portable way
// to get a content length is a cached GET. Uses the file cache so a
// subsequent Read is a no-op on the network.
func (n *kiwiNode) statFile() (fileStat, syscall.Errno) {
	if cached := n.client.cachedFile(n.path); cached != nil {
		return fileStat{size: int64(len(cached.data)), found: true, isSymlink: cached.isSymlink}, 0
	}
	resp, err := n.client.get(n.client.apiURL("/api/kiwi/file", n.path))
	if err != nil {
		return fileStat{}, syscall.EIO
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fileStat{}, 0
	}
	if resp.StatusCode != http.StatusOK {
		return fileStat{}, httpErrno(resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fileStat{}, syscall.EIO
	}
	var modTime time.Time
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		modTime, _ = http.ParseTime(lm)
	}
	isSymlink := resp.Header.Get("X-File-Type") == "symlink"
	n.client.storeFile(n.path, data, isSymlink)
	return fileStat{size: int64(len(data)), modTime: modTime, found: true, isSymlink: isSymlink}, 0
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

	st, errno := n.statFile()
	if errno != 0 {
		return errno
	}
	if !st.found {
		// File lookup failed — try treating it as a directory. A successful
		// /tree response is the server's way of saying "this exists and is
		// a dir".
		if _, derr := n.listDir(); derr == 0 {
			out.Mode = 0755 | syscall.S_IFDIR
			out.Size = 4096
			now := time.Now()
			out.Mtime = uint64(now.Unix())
			out.Mtimensec = uint32(now.Nanosecond())
			out.Atime = out.Mtime
			out.Atimensec = out.Mtimensec
			out.Ctime = out.Mtime
			out.Ctimensec = out.Mtimensec
			return 0
		}
		return syscall.ENOENT
	}

	if st.isSymlink {
		out.Mode = 0777 | syscall.S_IFLNK
	} else {
		out.Mode = 0644 | syscall.S_IFREG
	}
	out.Size = uint64(st.size)
	modTime := st.modTime
	if modTime.IsZero() {
		modTime = time.Now()
	}
	out.Mtime = uint64(modTime.Unix())
	out.Mtimensec = uint32(modTime.Nanosecond())
	out.Atime = out.Mtime
	out.Atimensec = out.Mtimensec
	out.Ctime = out.Mtime
	out.Ctimensec = out.Mtimensec
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

	st, errno := child.statFile()
	if errno != 0 {
		return nil, errno
	}
	if st.found {
		if st.isSymlink {
			out.Mode = 0777 | syscall.S_IFLNK
			out.Size = uint64(st.size)
			stable := fs.StableAttr{Mode: syscall.S_IFLNK}
			return n.NewInode(ctx, child, stable), 0
		}
		out.Mode = 0644 | syscall.S_IFREG
		out.Size = uint64(st.size)
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
	fh := &kiwiFile{
		node:   n,
		client: n.client,
	}
	if flags&syscall.O_TRUNC != 0 {
		fh.data = []byte{}
		fh.dirty = true
	}
	if flags&syscall.O_APPEND != 0 {
		fh.append = true
	}
	return fh, 0, 0
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

// Rename moves a file from one path to another via the atomic rename endpoint.
func (n *kiwiNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	oldPath := childPath(n.path, name)
	newPath := childPath(newParent.(*kiwiNode).path, newName)

	// Check whether this is a directory — use the bulk rename-dir endpoint.
	if n.client.cachedFile(oldPath) == nil {
		resp, err := n.client.get(n.client.apiURL("/api/kiwi/file", oldPath))
		if err != nil {
			return syscall.EIO
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			child := &kiwiNode{client: n.client, path: oldPath}
			if _, derr := child.listDir(); derr == 0 {
				bodyBytes, _ := json.Marshal(map[string]string{"from": oldPath, "to": newPath})
				dreq, _ := http.NewRequest("POST", n.client.remote+"/api/kiwi/rename-dir", bytes.NewReader(bodyBytes))
				dreq.Header.Set("Content-Type", "application/json")
				dreq.Header.Set("X-Actor", "fuse")
				dresp, derr2 := n.client.do(dreq)
				if derr2 != nil {
					return syscall.EIO
				}
				defer dresp.Body.Close()
				if dresp.StatusCode != http.StatusOK {
					return httpErrno(dresp.StatusCode)
				}
				n.client.invalidate(oldPath)
				n.client.invalidate(newPath)
				return 0
			}
			return syscall.ENOENT
		}
		if resp.StatusCode != http.StatusOK {
			return httpErrno(resp.StatusCode)
		}
		io.Copy(io.Discard, resp.Body)
	}

	bodyBytes, _ := json.Marshal(map[string]string{"from": oldPath, "to": newPath})
	req, _ := http.NewRequest("POST", n.client.remote+"/api/kiwi/rename", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Actor", "fuse")
	resp, err := n.client.do(req)
	if err != nil {
		return syscall.EIO
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return httpErrno(resp.StatusCode)
	}

	n.client.invalidate(oldPath)
	n.client.invalidate(newPath)
	return 0
}

// Setattr handles truncate, chmod, and timestamp changes.
func (n *kiwiNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if in.Valid&fuse.FATTR_SIZE != 0 {
		if in.Size == 0 {
			req, _ := http.NewRequest("PUT", n.client.apiURL("/api/kiwi/file", n.path), bytes.NewReader([]byte{}))
			req.Header.Set("Content-Type", "application/octet-stream")
			req.Header.Set("X-Actor", "fuse")
			resp, err := n.client.do(req)
			if err != nil {
				return syscall.EIO
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return httpErrno(resp.StatusCode)
			}
			n.client.invalidate(n.path)
			n.client.storeFile(n.path, []byte{}, false)
		} else if f != nil {
			kf := f.(*kiwiFile)
			if kf.data == nil {
				if cached := n.client.cachedFile(n.path); cached != nil {
					kf.data = append([]byte(nil), cached.data...)
				} else {
					resp, err := n.client.get(n.client.apiURL("/api/kiwi/file", n.path))
					if err != nil {
						return syscall.EIO
					}
					data, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					kf.data = data
				}
			}
			if int64(len(kf.data)) > int64(in.Size) {
				kf.data = kf.data[:in.Size]
			} else if int64(len(kf.data)) < int64(in.Size) {
				grown := make([]byte, in.Size)
				copy(grown, kf.data)
				kf.data = grown
			}
			kf.dirty = true
		} else {
			var content []byte
			if cached := n.client.cachedFile(n.path); cached != nil {
				content = append([]byte(nil), cached.data...)
			} else {
				resp, err := n.client.get(n.client.apiURL("/api/kiwi/file", n.path))
				if err != nil {
					return syscall.EIO
				}
				data, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				content = data
			}
			if int64(len(content)) > int64(in.Size) {
				content = content[:in.Size]
			} else if int64(len(content)) < int64(in.Size) {
				grown := make([]byte, in.Size)
				copy(grown, content)
				content = grown
			}
			req, _ := http.NewRequest("PUT", n.client.apiURL("/api/kiwi/file", n.path), bytes.NewReader(content))
			req.Header.Set("Content-Type", "application/octet-stream")
			req.Header.Set("X-Actor", "fuse")
			resp, err := n.client.do(req)
			if err != nil {
				return syscall.EIO
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return httpErrno(resp.StatusCode)
			}
			n.client.invalidate(n.path)
			n.client.storeFile(n.path, content, false)
		}
	}
	return n.Getattr(ctx, f, out)
}

// Symlink creates a symlink by writing the target path as file content with
// a special content-type header so the server stores it as a real symlink.
func (n *kiwiNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	cp := childPath(n.path, name)
	content := []byte(target)

	req, _ := http.NewRequest("PUT", n.client.apiURL("/api/kiwi/file", cp), bytes.NewReader(content))
	req.Header.Set("Content-Type", "application/x-symlink")
	req.Header.Set("X-Actor", "fuse")
	resp, err := n.client.do(req)
	if err != nil {
		return nil, syscall.EIO
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, httpErrno(resp.StatusCode)
	}

	n.client.invalidate(cp)

	child := &kiwiNode{client: n.client, path: cp}
	out.Mode = 0777 | syscall.S_IFLNK
	stable := fs.StableAttr{Mode: syscall.S_IFLNK}
	return n.NewInode(ctx, child, stable), 0
}

// Readlink reads the symlink target path from the server's readlink endpoint.
func (n *kiwiNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	resp, err := n.client.get(n.client.apiURL("/api/kiwi/readlink", n.path))
	if err != nil {
		return nil, syscall.EIO
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, httpErrno(resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, syscall.EIO
	}
	return data, 0
}

// kiwiFile represents an open file handle.
type kiwiFile struct {
	node   *kiwiNode
	client *Client
	data   []byte // cached data for reads/writes
	dirty  bool   // whether Write touched the buffer and we must PUT on Flush
	append bool   // when true, Write ignores off and appends
}

// Ensure kiwiFile implements the necessary interfaces
var _ fs.FileReader = (*kiwiFile)(nil)
var _ fs.FileWriter = (*kiwiFile)(nil)
var _ fs.FileFlusher = (*kiwiFile)(nil)
var _ fs.FileFsyncer = (*kiwiFile)(nil)

// Read reads from the file.
func (f *kiwiFile) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// If we haven't cached the data yet, try the shared TTL cache before
	// hitting the network.
	if f.data == nil {
		if cached := f.client.cachedFile(f.node.path); cached != nil {
			f.data = cached.data
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
		f.client.storeFile(f.node.path, data, false)
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

// fuseMaxFileSize limits in-memory file buffers to prevent OOM from
// writes at absurd offsets or unbounded appends.
const fuseMaxFileSize = 64 * 1024 * 1024 // 64 MB

// Write writes to the file (accumulates in memory).
func (f *kiwiFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if f.data == nil && !f.dirty {
		if cached := f.client.cachedFile(f.node.path); cached != nil {
			f.data = append([]byte(nil), cached.data...)
		} else if f.append {
			resp, err := f.client.get(f.client.apiURL("/api/kiwi/file", f.node.path))
			if err == nil {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					f.data = body
					f.client.storeFile(f.node.path, body, false)
				}
			}
		}
	}
	if f.append {
		off = int64(len(f.data))
	}
	if off < 0 {
		return 0, syscall.EFBIG
	}
	need := off + int64(len(data))
	if need > fuseMaxFileSize || need < 0 {
		return 0, syscall.EFBIG
	}
	if int64(len(f.data)) < need {
		newData := make([]byte, need)
		copy(newData, f.data)
		f.data = newData
	}
	copy(f.data[off:], data)
	f.dirty = true
	return uint32(len(data)), 0
}

// flushToServer PUTs the in-memory buffer to the remote server.
func (f *kiwiFile) flushToServer() syscall.Errno {
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

	f.client.invalidate(f.node.path)
	f.client.storeFile(f.node.path, append([]byte(nil), f.data...), false)
	f.dirty = false
	return 0
}

// Flush writes the buffered data to the remote server.
func (f *kiwiFile) Flush(ctx context.Context) syscall.Errno {
	return f.flushToServer()
}

// Fsync flushes data to the server without closing the file.
func (f *kiwiFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return f.flushToServer()
}
