package versioning

import "context"

// Version is a snapshot of a file at a point in time.
type Version struct {
	Hash    string `json:"hash"`
	Date    string `json:"date"`
	Author  string `json:"author"`
	Message string `json:"message"`
}

// BlameLine is one line of per-line blame output.
type BlameLine struct {
	Line   int    `json:"line"`
	Author string `json:"author"`
	Date   string `json:"date"`
	Hash   string `json:"hash"`
	Text   string `json:"text"`
}

// Versioner manages file history.
//
// Every method takes a context.Context as its first parameter. The git
// backend forwards it to exec.CommandContext so request cancellation
// (HTTP client disconnect, server shutdown) propagates to subprocesses.
// CoW and Noop check it at entry and bow out early; their work is
// in-process and bounded so deeper plumbing isn't useful.
type Versioner interface {
	// Commit records the current state of a file.
	Commit(ctx context.Context, path, actor, message string) error
	// CommitDelete records a deletion.
	CommitDelete(ctx context.Context, path, actor string) error
	// BulkCommit stages multiple paths and records them in a single commit.
	// Used for atomic multi-file writes so agent runs produce one commit per
	// logical operation rather than one per file.
	BulkCommit(ctx context.Context, paths []string, actor, message string) error
	// Log returns the history for a file, newest first.
	Log(ctx context.Context, path string) ([]Version, error)
	// Show returns file content at a given version hash.
	Show(ctx context.Context, path, hash string) ([]byte, error)
	// Diff returns a unified diff between two versions.
	Diff(ctx context.Context, path, fromHash, toHash string) (string, error)
	// Blame returns per-line attribution for a file.
	Blame(ctx context.Context, path string) ([]BlameLine, error)
}

// Unstager is the optional interface versioners implement when they have
// a staging area separate from the working tree — currently only Git.
// Pipeline calls Unstage on bulk-write rollback so a failed `git commit`
// doesn't leave partially-added files in the index, which would otherwise
// leak into the next unrelated commit and make `git log --follow` lie
// about which files belonged to which logical operation.
type Unstager interface {
	Unstage(ctx context.Context, paths []string) error
}

// Noop is a versioner that does nothing.
type Noop struct{}

func NewNoop() *Noop { return &Noop{} }

func (n *Noop) Commit(_ context.Context, _, _, _ string) error              { return nil }
func (n *Noop) CommitDelete(_ context.Context, _, _ string) error           { return nil }
func (n *Noop) BulkCommit(_ context.Context, _ []string, _, _ string) error { return nil }
func (n *Noop) Log(_ context.Context, _ string) ([]Version, error)          { return nil, nil }
func (n *Noop) Show(_ context.Context, _, _ string) ([]byte, error)         { return nil, nil }
func (n *Noop) Diff(_ context.Context, _, _, _ string) (string, error)      { return "", nil }
func (n *Noop) Blame(_ context.Context, _ string) ([]BlameLine, error) {
	return nil, ErrBlameUnsupported
}
