package search

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// Grep implements Searcher using a pure-Go line scanner.
// Zero external deps — works everywhere, fast for up to ~5000 files.
type Grep struct {
	root string
}

func NewGrep(root string) *Grep {
	return &Grep{root: root}
}

func (g *Grep) Search(query string) ([]Result, error) {
	if query == "" {
		return nil, nil
	}
	lower := strings.ToLower(query)

	var results []Result

	err := filepath.WalkDir(g.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		name := d.Name()
		// Skip internal dirs.
		if d.IsDir() && (strings.HasPrefix(name, ".") || name == ".git" || name == ".kiwi") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(name, ".md") {
			return nil
		}

		matches, err := searchFile(path, lower)
		if err != nil || len(matches) == 0 {
			return nil
		}

		rel, err := filepath.Rel(g.root, path)
		if err != nil {
			return nil
		}
		results = append(results, Result{Path: rel, Matches: matches})
		return nil
	})

	return results, err
}

func searchFile(path, lowerQuery string) ([]Match, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var matches []Match
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if strings.Contains(strings.ToLower(line), lowerQuery) {
			matches = append(matches, Match{
				Line: lineNum,
				Text: strings.TrimSpace(line),
			})
		}
	}
	return matches, scanner.Err()
}

