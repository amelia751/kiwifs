// Provenance helpers inject X-Provenance header data into YAML frontmatter's
// derived-from list so disk, git, and file_meta stay consistent.
package pipeline

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/kiwifs/kiwifs/internal/markdown"
	"github.com/kiwifs/kiwifs/internal/yamlutil"
	"gopkg.in/yaml.v3"
)

// ProvenanceEntry is the shape written into the `derived-from` list. The
// fields match the SCHEMA.md contract the knowledge template ships
// with, so structured queries like `?where=$.derived-from[*].id=run-249`
// stay portable.
type ProvenanceEntry struct {
	Type  string `yaml:"type"`
	ID    string `yaml:"id"`
	Date  string `yaml:"date"`
	Actor string `yaml:"actor,omitempty"`
	Note  string `yaml:"note,omitempty"`
}

// ParseProvenanceHeader splits an X-Provenance value ("type:id") into its
// parts. Returns ok=false when the header is empty or malformed so callers
// can skip injection cleanly instead of surfacing a 400 for every request.
func ParseProvenanceHeader(h string) (provType, provID string, ok bool) {
	h = strings.TrimSpace(h)
	if h == "" {
		return "", "", false
	}
	t, id, found := strings.Cut(h, ":")
	t = strings.TrimSpace(t)
	id = strings.TrimSpace(id)
	if !found || t == "" || id == "" {
		return "", "", false
	}
	return t, id, true
}

// InjectProvenance adds an entry to the content's `derived-from` list and
// returns the rewritten bytes. A file without frontmatter gets a minimal
// block prepended; a file whose `derived-from` is absent gets a new list;
// an existing list is appended to. The function preserves the body of the
// markdown byte-for-byte so round-tripping doesn't churn unrelated content.
//
// Empty provType or provID is a no-op — callers pass the raw header value
// and we don't want a missing header to touch the file at all.
func InjectProvenance(content []byte, provType, provID, actor string) ([]byte, error) {
	if provType == "" || provID == "" {
		return content, nil
	}

	fmBytes, body, err := markdown.SplitFrontmatter(content)
	if err != nil {
		return nil, err
	}

	// Decode into a yaml.Node rather than a generic map so we round-trip
	// comments, key order, and scalar styles when the frontmatter already
	// has them. A raw map[string]any would lose all of that.
	var root yaml.Node
	if len(fmBytes) > 0 {
		if err := yaml.Unmarshal(fmBytes, &root); err != nil {
			return nil, fmt.Errorf("parse frontmatter: %w", err)
		}
	}
	mapping := yamlutil.EnsureMappingDocument(&root)

	entry := ProvenanceEntry{
		Type:  provType,
		ID:    provID,
		Date:  time.Now().UTC().Format(time.RFC3339),
		Actor: actor,
	}
	var entryNode yaml.Node
	if err := entryNode.Encode(entry); err != nil {
		return nil, fmt.Errorf("encode provenance entry: %w", err)
	}

	yamlutil.AppendToListKey(mapping, "derived-from", &entryNode)

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&root); err != nil {
		return nil, fmt.Errorf("serialise frontmatter: %w", err)
	}
	_ = enc.Close()

	var out bytes.Buffer
	out.Grow(len(content) + 128)
	out.WriteString("---\n")
	out.Write(bytes.TrimRight(buf.Bytes(), "\n"))
	out.WriteString("\n---\n")
	out.Write(body)
	return out.Bytes(), nil
}
