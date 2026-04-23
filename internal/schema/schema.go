// Package schema parses the per-knowledge-base SCHEMA.md file and exposes
// a lint engine that audits the folder against it.
//
// SCHEMA.md is intentionally markdown-first — the file doubles as
// documentation for humans and agents and as machine-readable configuration
// for the linter. We lift a few conventions out of the text:
//
//   - Top-level sections whose name starts with a capital letter are
//     treated as "expected folders" when the section heading matches a
//     directory name under the root.
//   - `[[wiki links]]` anywhere in SCHEMA.md are treated as "expected
//     pages" — if they resolve to missing files, the linter reports an
//     orphan reference.
//
// Everything else in SCHEMA.md is documentation. Callers who need a
// stricter schema can extend Parse() to pull structured rules from a YAML
// frontmatter block.
package schema

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/markdown"
)

// Schema is the machine-readable extraction from SCHEMA.md.
type Schema struct {
	// Path is the absolute path of the SCHEMA.md that produced this.
	Path string
	// Frontmatter contains any YAML metadata at the top of SCHEMA.md.
	Frontmatter map[string]any
	// Sections are the top-level headings in SCHEMA.md.
	Sections []string
	// ExpectedPages are the wiki-link targets named inside SCHEMA.md,
	// normalised against the knowledge root.
	ExpectedPages []string
	// Source is the raw file content — useful for error messages.
	Source []byte
}

// Issue is a single lint finding.
type Issue struct {
	Kind    string `json:"kind"`    // "orphan", "broken-link", "empty-file", "missing-schema"
	Path    string `json:"path"`    // the file the issue is about (may be empty for repo-level issues)
	Target  string `json:"target,omitempty"` // for broken-link: the unresolved [[target]]
	Message string `json:"message"`
}

// Result is the full lint outcome.
type Result struct {
	Schema *Schema `json:"schema,omitempty"`
	Issues []Issue `json:"issues"`
}

// Load reads SCHEMA.md from root. A missing file returns (nil, os.ErrNotExist)
// so callers can distinguish "no schema configured" from "schema corrupt".
func Load(root string) (*Schema, error) {
	p := filepath.Join(root, "SCHEMA.md")
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	return Parse(p, data)
}

// Parse turns a SCHEMA.md body into a structured Schema.
func Parse(path string, data []byte) (*Schema, error) {
	parsed, err := markdown.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	s := &Schema{
		Path:        path,
		Source:      data,
		Frontmatter: parsed.Frontmatter,
	}
	for _, h := range parsed.Headings {
		if h.Level == 2 || h.Level == 1 {
			s.Sections = append(s.Sections, h.Text)
		}
	}
	for _, t := range links.Unique(links.Extract(data)) {
		s.ExpectedPages = append(s.ExpectedPages, t)
	}
	sort.Strings(s.ExpectedPages)
	return s, nil
}

// Lint walks root and checks it against a SCHEMA.md (if present), flagging
// orphan references, broken [[wiki links]] in content, and any pages that
// are empty on disk. A missing SCHEMA.md produces a single informational
// issue and lint continues on the file tree.
func Lint(root string) (*Result, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	res := &Result{}

	sc, err := Load(abs)
	if err != nil {
		if os.IsNotExist(err) {
			res.Issues = append(res.Issues, Issue{
				Kind:    "missing-schema",
				Message: "no SCHEMA.md in the knowledge root — run `kiwifs init` to create one",
			})
		} else {
			return nil, fmt.Errorf("load schema: %w", err)
		}
	} else {
		res.Schema = sc
	}

	// Collect every markdown file on disk, keyed by lowercase path for
	// case-insensitive resolution against wiki-link targets.
	existing := map[string]string{} // lc-path → original path
	var files []string
	walkErr := filepath.WalkDir(abs, func(p string, d os.DirEntry, werr error) error {
		if werr != nil {
			return nil
		}
		name := d.Name()
		if d.IsDir() {
			if strings.HasPrefix(name, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(name), ".md") {
			return nil
		}
		rel, rerr := filepath.Rel(abs, p)
		if rerr != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		files = append(files, rel)
		for _, form := range links.TargetForms(rel) {
			existing[strings.ToLower(form)] = rel
		}
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	sort.Strings(files)

	// 1) Orphan references in SCHEMA.md.
	if sc != nil {
		for _, target := range sc.ExpectedPages {
			if _, ok := existing[strings.ToLower(target)]; !ok {
				res.Issues = append(res.Issues, Issue{
					Kind:    "orphan",
					Path:    filepath.ToSlash(sc.Path),
					Target:  target,
					Message: fmt.Sprintf("SCHEMA.md references [[%s]] but no matching file exists", target),
				})
			}
		}
	}

	// 2) Broken wiki links inside every page + empty files.
	for _, rel := range files {
		full := filepath.Join(abs, rel)
		data, err := os.ReadFile(full)
		if err != nil {
			continue
		}
		if len(strings.TrimSpace(string(data))) == 0 {
			res.Issues = append(res.Issues, Issue{
				Kind:    "empty-file",
				Path:    rel,
				Message: "file is empty",
			})
		}
		for _, t := range links.Unique(links.Extract(data)) {
			if _, ok := existing[strings.ToLower(t)]; ok {
				continue
			}
			res.Issues = append(res.Issues, Issue{
				Kind:    "broken-link",
				Path:    rel,
				Target:  t,
				Message: fmt.Sprintf("[[%s]] doesn't resolve to any file", t),
			})
		}
	}

	return res, nil
}

// Summary renders a compact human-readable lint report. Exit codes in the
// CLI are driven by len(result.Issues).
func (r *Result) Summary() string {
	if r == nil {
		return "no lint result"
	}
	var b strings.Builder
	if len(r.Issues) == 0 {
		b.WriteString("kiwifs lint: clean — no issues\n")
		return b.String()
	}
	fmt.Fprintf(&b, "kiwifs lint: %d issue(s)\n", len(r.Issues))
	// Group by kind for readability.
	sort.Slice(r.Issues, func(i, j int) bool {
		if r.Issues[i].Kind != r.Issues[j].Kind {
			return r.Issues[i].Kind < r.Issues[j].Kind
		}
		return r.Issues[i].Path < r.Issues[j].Path
	})
	for _, is := range r.Issues {
		if is.Path == "" {
			fmt.Fprintf(&b, "  %-12s %s\n", is.Kind, is.Message)
		} else {
			fmt.Fprintf(&b, "  %-12s %s — %s\n", is.Kind, is.Path, is.Message)
		}
	}
	return b.String()
}
