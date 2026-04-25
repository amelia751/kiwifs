// Provenance helpers inject X-Provenance header data into YAML frontmatter's
// derived-from list so disk, git, and file_meta stay consistent.
package pipeline

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/kiwifs/kiwifs/internal/markdown"
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
	mapping := ensureMappingDocument(&root)

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

	appendToDerivedFrom(mapping, &entryNode)

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

// ensureMappingDocument returns the yaml.Node representing the mapping at
// the root of the document, creating the structure if the input was empty.
// goldmark-meta and yaml.Unmarshal both parse a frontmatter block into a
// DocumentNode wrapping a MappingNode, but an empty input leaves the
// DocumentNode with no children — that case needs a synthesised mapping.
func ensureMappingDocument(root *yaml.Node) *yaml.Node {
	if root.Kind == 0 {
		// Input was empty — build a fresh document holding an empty mapping.
		root.Kind = yaml.DocumentNode
	}
	if root.Kind != yaml.DocumentNode {
		// Unusual: a mapping given as the top level without a document
		// wrapper. Wrap it so the encoder emits the expected "key: value"
		// block form instead of embedded flow syntax.
		wrapped := *root
		root.Kind = yaml.DocumentNode
		root.Content = []*yaml.Node{&wrapped}
	}
	if len(root.Content) == 0 {
		mapping := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		root.Content = append(root.Content, mapping)
		return mapping
	}
	return root.Content[0]
}

// appendToDerivedFrom ensures mapping has a `derived-from` sequence and
// appends entry to it. If `derived-from` exists as a scalar (which we'd
// treat as schema violation but don't want to crash on) we coerce it into
// a single-element list first.
func appendToDerivedFrom(mapping, entry *yaml.Node) {
	// Find existing key.
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		k := mapping.Content[i]
		if k.Value == "derived-from" {
			v := mapping.Content[i+1]
			if v.Kind == yaml.SequenceNode {
				v.Content = append(v.Content, entry)
				return
			}
			// Coerce a non-sequence value to a list, preserving whatever
			// was there as the first element.
			seq := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
			clone := *v
			seq.Content = []*yaml.Node{&clone, entry}
			mapping.Content[i+1] = seq
			return
		}
	}
	// No key yet — append one.
	key := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "derived-from"}
	seq := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Content: []*yaml.Node{entry}}
	mapping.Content = append(mapping.Content, key, seq)
}

