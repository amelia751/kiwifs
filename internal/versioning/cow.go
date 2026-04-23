package versioning

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pmezard/go-difflib/difflib"
)

// DefaultCowMaxVersions is the ceiling applied when config.MaxVersions is
// zero. Matches the spec's example (max_versions = 100) so unconfigured
// deployments don't grow .versions/ without bound.
const DefaultCowMaxVersions = 100

// ErrBlameUnsupported is returned by versioners that don't expose per-line
// attribution (cow, noop). API handlers translate it to HTTP 501.
var ErrBlameUnsupported = errors.New("blame is not supported for this versioning strategy")

// Cow is a copy-on-write versioner. It snapshots file content into
// .versions/<path>/<nanotimestamp>.md before each overwrite and before each
// delete, and prunes to MaxVersions after each write so the .versions/ tree
// doesn't grow unbounded.
type Cow struct {
	root string
	// MaxVersions caps snapshots per file. Zero ≡ DefaultCowMaxVersions; a
	// negative value disables pruning (not recommended).
	MaxVersions int

	// writeMu serialises snapshot/prune across concurrent callers. Unlike
	// the git versioner, CoW has no per-file lock — without this mutex two
	// goroutines writing the same path can interleave snapshot() and
	// prune(), dropping a snapshot created by the other goroutine or
	// reading a half-written file. The pipeline already serialises writes,
	// but direct library callers (Cow used standalone) are unprotected
	// without the mutex here.
	writeMu sync.Mutex
}

func NewCow(root string) (*Cow, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	return &Cow{root: abs, MaxVersions: DefaultCowMaxVersions}, nil
}

func (c *Cow) versionsDir(path string) string {
	return filepath.Join(c.root, ".versions", filepath.FromSlash(path))
}

// snapshot copies the current on-disk content of path into .versions/ and
// prunes old snapshots. A missing source file is a no-op — the caller is
// either writing a brand-new file or snapshotting after-the-fact a delete
// whose source has already been removed.
func (c *Cow) snapshot(path string) error {
	src := filepath.Join(c.root, filepath.FromSlash(path))
	data, err := os.ReadFile(src)
	if err != nil {
		return nil
	}
	return c.writeSnapshot(path, data)
}

// writeSnapshot is the shared tail of snapshot() and the pre-delete path:
// it drops `data` into versions/ with a new timestamp name and prunes.
func (c *Cow) writeSnapshot(path string, data []byte) error {
	dir := c.versionsDir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	hash := fmt.Sprintf("%019d", time.Now().UTC().UnixNano())
	dst := filepath.Join(dir, hash+".md")
	if err := os.WriteFile(dst, data, 0644); err != nil {
		return err
	}
	return c.prune(path)
}

// prune deletes the oldest snapshots so at most MaxVersions remain. Runs
// after every write + every pre-delete snapshot. A negative MaxVersions
// disables pruning entirely.
func (c *Cow) prune(path string) error {
	limit := c.MaxVersions
	if limit == 0 {
		limit = DefaultCowMaxVersions
	}
	if limit < 0 {
		return nil
	}
	dir := c.versionsDir(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
			names = append(names, e.Name())
		}
	}
	if len(names) <= limit {
		return nil
	}
	// Timestamp filenames sort lexicographically = chronologically; oldest
	// first. Drop the head slice so the newest `limit` remain.
	sort.Strings(names)
	excess := len(names) - limit
	for i := 0; i < excess; i++ {
		_ = os.Remove(filepath.Join(dir, names[i]))
	}
	return nil
}

func (c *Cow) Commit(ctx context.Context, path, _, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.snapshot(path)
}

// CommitDelete preserves the last version before a delete finishes. The
// pipeline's PreDeleteSnapshot hook (the REST/NFS/WebDAV/S3 write path)
// captures state before storage.Delete runs, so for those callers this
// method has nothing new to snapshot — the last Commit() already did.
//
// But agent-origin deletes don't go through the pipeline's Delete path:
// the agent removes the file directly, then fsnotify calls
// pipeline.ObserveDelete, which calls here. By that point the file is
// gone — but if Commit was called shortly before (the usual case), the
// .versions/ directory already holds a snapshot that matches the
// deleted content, so we're fine. This method stays defensive: it
// re-calls snapshot() so any last-moment on-disk content still reachable
// gets captured. snapshot() is a no-op when the source is missing.
func (c *Cow) CommitDelete(ctx context.Context, path, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.snapshot(path)
}

// PreDeleteSnapshot captures current on-disk contents before the caller
// deletes a file. The pipeline invokes this so CoW keeps the last version
// reachable via Log/Show even after the working-tree file is gone.
func (c *Cow) PreDeleteSnapshot(path string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.snapshot(path)
}

func (c *Cow) BulkCommit(ctx context.Context, paths []string, _, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	for _, p := range paths {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.snapshot(p); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cow) Log(_ context.Context, path string) ([]Version, error) {
	dir := c.versionsDir(path)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var hashes []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
			hashes = append(hashes, strings.TrimSuffix(e.Name(), ".md"))
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(hashes)))

	versions := make([]Version, 0, len(hashes))
	for _, h := range hashes {
		var ts int64
		fmt.Sscanf(h, "%d", &ts)
		date := time.Unix(0, ts).UTC().Format(time.RFC3339)
		versions = append(versions, Version{
			Hash:    h,
			Date:    date,
			Author:  "",
			Message: "snapshot",
		})
	}
	return versions, nil
}

func (c *Cow) Show(_ context.Context, path, hash string) ([]byte, error) {
	p := filepath.Join(c.versionsDir(path), hash+".md")
	return os.ReadFile(p)
}

func (c *Cow) Diff(ctx context.Context, path, fromHash, toHash string) (string, error) {
	fromData, err := c.Show(ctx, path, fromHash)
	if err != nil {
		return "", fmt.Errorf("from version: %w", err)
	}
	toData, err := c.Show(ctx, path, toHash)
	if err != nil {
		return "", fmt.Errorf("to version: %w", err)
	}
	ud := difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(fromData)),
		B:        difflib.SplitLines(string(toData)),
		FromFile: fromHash,
		ToFile:   toHash,
		Context:  3,
	}
	return difflib.GetUnifiedDiffString(ud)
}

// Blame is not meaningful under CoW — snapshots are whole-file, not
// per-line. Return ErrBlameUnsupported so the API layer can 501 rather
// than silently returning an empty result.
func (c *Cow) Blame(_ context.Context, _ string) ([]BlameLine, error) {
	return nil, ErrBlameUnsupported
}
