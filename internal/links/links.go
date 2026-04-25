// Package links models the [[wiki-link]] graph across the knowledge base.
//
// A "source" is the page that contains the [[...]] syntax; a "target" is the
// raw string inside the brackets (before any |label). The resolver is
// intentionally fuzzy: [[auth]] matches concepts/authentication.md.
//
// We store targets in their raw form and fan out at query time: when the user
// asks for backlinks of concepts/authentication.md we query for any of
// {concepts/authentication.md, concepts/authentication, authentication.md,
// authentication}. That keeps indexing simple (one pass) while still
// supporting Obsidian-style shorthand.
package links

import (
	"context"
	"net/url"
	"regexp"
	"strings"
)

// Entry is a single backlink row: one source page that links to the target.
type Entry struct {
	Path  string `json:"path"`
	Count int    `json:"count"`
}

// Edge is a raw (source, target) pair as it appears in the wiki-link index.
// Target is the string inside [[...]] — unresolved — so callers can apply
// their own path-resolution rules (exact/stem/prefix).
type Edge struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

// Linker manages the reverse index of wiki links. Engines that don't support
// this (grep) return a nil Linker; API handlers check for nil and return a
// 503-equivalent JSON response.
//
// Every method takes context.Context as the first parameter so SQL-backed
// implementations can forward cancellation to the database driver.
type Linker interface {
	// IndexLinks replaces all links emitted by `source`. Call on every write.
	IndexLinks(ctx context.Context, source string, targets []string) error
	// RemoveLinks drops all link rows for `source`. Call on delete.
	RemoveLinks(ctx context.Context, source string) error
	// Backlinks returns all sources that reference `target` in any of the
	// common fuzzy forms (see package docs).
	Backlinks(ctx context.Context, target string) ([]Entry, error)
	// AllEdges returns every (source, target) pair currently indexed. Used by
	// the graph view so clients can build the full link map in one round trip.
	AllEdges(ctx context.Context) ([]Edge, error)
}

// wikiLinkRe matches [[target]] or [[target|label]]. Target may contain any
// character except ] and |. We deliberately keep this simple — wiki links
// inside fenced code blocks or inline code are still captured, which is
// usually what authors want (code-block [[x]] is quite rare in practice).
var wikiLinkRe = regexp.MustCompile(`\[\[([^\]|]+)(?:\|[^\]]+)?\]\]`)

// Extract pulls [[target]] entries out of a markdown body. Targets are
// returned verbatim (trimmed of surrounding whitespace) in order of
// appearance, with duplicates preserved so callers can derive a weight if
// they want one. Most callers should de-dupe with Unique().
func Extract(content []byte) []string {
	if len(content) == 0 {
		return nil
	}
	matches := wikiLinkRe.FindAllSubmatch(content, -1)
	if len(matches) == 0 {
		return nil
	}
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		t := strings.TrimSpace(string(m[1]))
		if t == "" {
			continue
		}
		out = append(out, t)
	}
	return out
}

// Unique de-dupes a slice of targets case-insensitively while preserving order.
func Unique(targets []string) []string {
	seen := make(map[string]struct{}, len(targets))
	out := make([]string, 0, len(targets))
	for _, t := range targets {
		k := strings.ToLower(t)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, t)
	}
	return out
}

// wikiLinkFullRe captures the full match including optional label for replacement.
var wikiLinkFullRe = regexp.MustCompile(`\[\[([^\]|]+)(?:\|([^\]]+))?\]\]`)

// ResolveWikiLinksToMarkdown rewrites [[target|label]] wiki links in content
// to standard markdown links using permalinks: [label](publicURL/page/path).
// The resolver function maps a raw wiki-link target to its resolved file path,
// returning "" if no match is found. Unresolved links are left as-is.
func ResolveWikiLinksToMarkdown(content, publicURL string, resolver func(target string) string) string {
	if publicURL == "" {
		return content
	}
	return wikiLinkFullRe.ReplaceAllStringFunc(content, func(match string) string {
		sub := wikiLinkFullRe.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		target := strings.TrimSpace(sub[1])
		label := target
		if len(sub) >= 3 && sub[2] != "" {
			label = strings.TrimSpace(sub[2])
		}
		resolved := resolver(target)
		if resolved == "" {
			return match
		}
		segments := strings.Split(resolved, "/")
		for i, s := range segments {
			segments[i] = url.PathEscape(s)
		}
		encodedPath := strings.Join(segments, "/")
		return "[" + label + "](" + publicURL + "/page/" + encodedPath + ")"
	})
}

// TargetForms expands a file path into every syntactic form that could
// appear inside [[...]] to refer to it. The result is suitable for a
// `target IN (…)` query.
//
//	concepts/authentication.md → [
//	  concepts/authentication.md
//	  concepts/authentication
//	  authentication.md
//	  authentication
//	]
func TargetForms(path string) []string {
	p := strings.TrimPrefix(path, "/")
	if p == "" {
		return nil
	}
	forms := []string{p}
	stemPath := strings.TrimSuffix(p, ".md")
	if stemPath != p {
		forms = append(forms, stemPath)
	}
	base := p
	if i := strings.LastIndex(p, "/"); i >= 0 {
		base = p[i+1:]
	}
	if base != p {
		forms = append(forms, base)
		stem := strings.TrimSuffix(base, ".md")
		if stem != base {
			forms = append(forms, stem)
		}
	}
	return forms
}
