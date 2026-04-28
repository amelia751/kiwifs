package yamlutil

import (
	"bytes"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func encodeRoot(t *testing.T, root *yaml.Node) string {
	t.Helper()
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(root); err != nil {
		t.Fatalf("encode root: %v", err)
	}
	_ = enc.Close()
	return buf.String()
}

func TestEnsureMappingDocument_Empty(t *testing.T) {
	var root yaml.Node
	mapping := EnsureMappingDocument(&root)
	if mapping.Kind != yaml.MappingNode {
		t.Fatalf("expected mapping kind, got %v", mapping.Kind)
	}
	if root.Kind != yaml.DocumentNode {
		t.Fatalf("expected root to become DocumentNode, got %v", root.Kind)
	}
}

func TestEnsureMappingDocument_AlreadyDocument(t *testing.T) {
	var root yaml.Node
	if err := yaml.Unmarshal([]byte("foo: bar\n"), &root); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	mapping := EnsureMappingDocument(&root)
	if mapping.Kind != yaml.MappingNode {
		t.Fatalf("expected mapping kind, got %v", mapping.Kind)
	}
	if got := encodeRoot(t, &root); !strings.Contains(got, "foo: bar") {
		t.Fatalf("expected existing key preserved, got:\n%s", got)
	}
}

func TestEnsureMappingDocument_BareMappingGetsWrapped(t *testing.T) {
	root := &yaml.Node{
		Kind: yaml.MappingNode, Tag: "!!map",
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "k"},
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "v"},
		},
	}
	mapping := EnsureMappingDocument(root)
	if mapping.Kind != yaml.MappingNode {
		t.Fatalf("expected mapping kind, got %v", mapping.Kind)
	}
	if root.Kind != yaml.DocumentNode {
		t.Fatalf("expected root to be DocumentNode after wrapping, got %v", root.Kind)
	}
	if got := encodeRoot(t, root); !strings.Contains(got, "k: v") {
		t.Fatalf("expected wrapped mapping to encode as block form, got:\n%s", got)
	}
}

func TestAppendToListKey_NewKey(t *testing.T) {
	mapping := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	entry := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "first"}
	AppendToListKey(mapping, "items", entry)

	if len(mapping.Content) != 2 {
		t.Fatalf("expected key/value pair, got %d entries", len(mapping.Content))
	}
	if mapping.Content[0].Value != "items" {
		t.Fatalf("expected key 'items', got %q", mapping.Content[0].Value)
	}
	seq := mapping.Content[1]
	if seq.Kind != yaml.SequenceNode || len(seq.Content) != 1 || seq.Content[0].Value != "first" {
		t.Fatalf("expected one-element sequence with 'first', got %+v", seq)
	}
}

func TestAppendToListKey_ExistingSequence(t *testing.T) {
	mapping := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	first := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "a"}
	second := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "b"}
	AppendToListKey(mapping, "items", first)
	AppendToListKey(mapping, "items", second)

	if len(mapping.Content) != 2 {
		t.Fatalf("expected single key/value pair after second append, got %d entries", len(mapping.Content))
	}
	seq := mapping.Content[1]
	if seq.Kind != yaml.SequenceNode || len(seq.Content) != 2 {
		t.Fatalf("expected two-element sequence, got %+v", seq)
	}
	if seq.Content[0].Value != "a" || seq.Content[1].Value != "b" {
		t.Fatalf("expected sequence [a b], got %v / %v", seq.Content[0].Value, seq.Content[1].Value)
	}
}

func TestAppendToListKey_CoercesNonSequenceValue(t *testing.T) {
	// Schema-violating frontmatter where the key already holds a scalar.
	// AppendToListKey should coerce it into a sequence and preserve the
	// original value as the first entry.
	mapping := &yaml.Node{
		Kind: yaml.MappingNode, Tag: "!!map",
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "items"},
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "lone"},
		},
	}
	entry := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "added"}
	AppendToListKey(mapping, "items", entry)

	seq := mapping.Content[1]
	if seq.Kind != yaml.SequenceNode {
		t.Fatalf("expected coerced sequence, got %v", seq.Kind)
	}
	if len(seq.Content) != 2 || seq.Content[0].Value != "lone" || seq.Content[1].Value != "added" {
		t.Fatalf("expected [lone added], got %+v", seq.Content)
	}
}
