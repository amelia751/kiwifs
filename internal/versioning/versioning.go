package versioning

// Version is a snapshot of a file at a point in time.
type Version struct {
	Hash    string `json:"hash"`
	Date    string `json:"date"`
	Author  string `json:"author"`
	Message string `json:"message"`
}

// Versioner manages file history.
type Versioner interface {
	// Commit records the current state of a file.
	Commit(path, actor, message string) error
	// CommitDelete records a deletion.
	CommitDelete(path, actor string) error
	// Log returns the history for a file, newest first.
	Log(path string) ([]Version, error)
	// Show returns file content at a given version hash.
	Show(path, hash string) ([]byte, error)
	// Diff returns a unified diff between two versions.
	Diff(path, fromHash, toHash string) (string, error)
}

// Noop is a versioner that does nothing.
type Noop struct{}

func NewNoop() *Noop           { return &Noop{} }
func (n *Noop) Commit(_, _, _ string) error   { return nil }
func (n *Noop) CommitDelete(_, _ string) error { return nil }
func (n *Noop) Log(_ string) ([]Version, error) { return nil, nil }
func (n *Noop) Show(_, _ string) ([]byte, error) { return nil, nil }
func (n *Noop) Diff(_, _, _ string) (string, error) { return "", nil }
