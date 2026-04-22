package search

// Match is a single line match within a file.
type Match struct {
	Line int    `json:"line"`
	Text string `json:"text"`
}

// Result is all matches within a single file.
type Result struct {
	Path    string  `json:"path"`
	Matches []Match `json:"matches"`
}

// Searcher searches across all knowledge files.
type Searcher interface {
	Search(query string) ([]Result, error)
}
