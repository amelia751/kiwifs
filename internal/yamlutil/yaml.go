// Package yamlutil provides small helpers for manipulating YAML frontmatter
// nodes. They are extracted from internal/pipeline/provenance and
// internal/memory/merge so both packages share a single implementation.
package yamlutil

import "gopkg.in/yaml.v3"

// EnsureMappingDocument returns the yaml.Node representing the mapping at
// the root of the document, creating the structure if the input was empty.
//
// goldmark-meta and yaml.Unmarshal both parse a frontmatter block into a
// DocumentNode wrapping a MappingNode, but an empty input leaves the
// DocumentNode with no children — that case needs a synthesised mapping.
// A bare mapping passed in without a document wrapper is wrapped so the
// encoder emits the expected "key: value" block form instead of embedded
// flow syntax.
func EnsureMappingDocument(root *yaml.Node) *yaml.Node {
	if root.Kind == 0 {
		// Input was empty — build a fresh document holding an empty mapping.
		root.Kind = yaml.DocumentNode
	}
	if root.Kind != yaml.DocumentNode {
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

// AppendToListKey ensures mapping has a sequence value under keyName and
// appends entry to it. If the existing value at keyName is a non-sequence
// (which we'd treat as a schema violation but don't want to crash on) it is
// coerced into a single-element list first, with the original value as the
// initial element.
func AppendToListKey(mapping *yaml.Node, keyName string, entry *yaml.Node) {
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		k := mapping.Content[i]
		if k.Value == keyName {
			v := mapping.Content[i+1]
			if v.Kind == yaml.SequenceNode {
				v.Content = append(v.Content, entry)
				return
			}
			seq := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
			clone := *v
			seq.Content = []*yaml.Node{&clone, entry}
			mapping.Content[i+1] = seq
			return
		}
	}
	key := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: keyName}
	seq := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Content: []*yaml.Node{entry}}
	mapping.Content = append(mapping.Content, key, seq)
}
