// Package markdown parses knowledge-base markdown files on the backend.
//
// Two outputs callers care about:
//   - Frontmatter (YAML, at the top of the file) — surfaced to the tree /
//     page API so the UI can show metadata badges without shipping a
//     parser itself.
//   - Heading AST — used by the /toc endpoint and by the lint engine to
//     spot empty sections and broken anchor links.
//
// Built on goldmark + goldmark-meta so we stay on a battle-tested parser
// and share configuration with the frontend wherever it uses the same
// library.
package markdown

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/yuin/goldmark"
	meta "github.com/yuin/goldmark-meta"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
)

// Heading is one entry in the table of contents.
type Heading struct {
	Level int    `json:"level"` // 1..6
	Text  string `json:"text"`
	Slug  string `json:"slug"` // slugified, suitable for anchor links
}

// Parsed captures everything we extract from a markdown file in one pass.
type Parsed struct {
	Frontmatter map[string]any `json:"frontmatter,omitempty"`
	Headings    []Heading      `json:"headings,omitempty"`
}

// Parse returns frontmatter + heading outline for content. A malformed
// frontmatter block returns it as empty but still yields the heading list
// — we'd rather render a page with partial metadata than fail open.
func Parse(content []byte) (*Parsed, error) {
	md := goldmark.New(goldmark.WithExtensions(meta.Meta))
	ctx := parser.NewContext()
	doc := md.Parser().Parse(text.NewReader(content), parser.WithContext(ctx))

	out := &Parsed{Frontmatter: meta.Get(ctx)}

	ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		h, ok := n.(*ast.Heading)
		if !ok {
			return ast.WalkContinue, nil
		}
		var buf bytes.Buffer
		for c := h.FirstChild(); c != nil; c = c.NextSibling() {
			extractText(&buf, c, content)
		}
		txt := strings.TrimSpace(buf.String())
		if txt == "" {
			return ast.WalkContinue, nil
		}
		out.Headings = append(out.Headings, Heading{
			Level: h.Level,
			Text:  txt,
			Slug:  Slugify(txt),
		})
		return ast.WalkContinue, nil
	})

	return out, nil
}

// Frontmatter is a lightweight helper when callers only want the metadata
// block — avoids walking the full AST.
func Frontmatter(content []byte) (map[string]any, error) {
	md := goldmark.New(goldmark.WithExtensions(meta.Meta))
	ctx := parser.NewContext()
	md.Parser().Parse(text.NewReader(content), parser.WithContext(ctx))
	m := meta.Get(ctx)
	if m == nil {
		return map[string]any{}, nil
	}
	return m, nil
}

// Headings is the TOC-only accessor, used by /api/kiwi/toc.
func Headings(content []byte) []Heading {
	p, err := Parse(content)
	if err != nil {
		return nil
	}
	return p.Headings
}

// Slugify turns a heading into a URL-safe anchor (GitHub-style): lowercase,
// ASCII letters + digits + hyphens; whitespace to hyphens; everything else
// dropped.
func Slugify(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	b.Grow(len(s))
	prevHyphen := false
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevHyphen = false
		case r == ' ' || r == '-' || r == '_' || r == '\t':
			if !prevHyphen && b.Len() > 0 {
				b.WriteByte('-')
				prevHyphen = true
			}
		}
	}
	out := b.String()
	return strings.TrimRight(out, "-")
}

// DebugString is a compact human-readable dump used in tests / CLI output.
func (p *Parsed) DebugString() string {
	var b strings.Builder
	if len(p.Frontmatter) > 0 {
		b.WriteString("frontmatter:\n")
		for k, v := range p.Frontmatter {
			fmt.Fprintf(&b, "  %s: %v\n", k, v)
		}
	}
	for _, h := range p.Headings {
		fmt.Fprintf(&b, "%s%s (#%s)\n", strings.Repeat("  ", h.Level-1), h.Text, h.Slug)
	}
	return b.String()
}

func extractText(buf *bytes.Buffer, n ast.Node, source []byte) {
	switch v := n.(type) {
	case *ast.Text:
		buf.Write(v.Segment.Value(source))
	default:
		for c := n.FirstChild(); c != nil; c = c.NextSibling() {
			extractText(buf, c, source)
		}
	}
}
