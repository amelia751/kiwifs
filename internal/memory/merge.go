package memory

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/kiwifs/kiwifs/internal/markdown"
	"gopkg.in/yaml.v3"
)

// MergedFromEntry is the shape written into the merged-from list on semantic
// (or intermediate consolidation) pages. It records which episodic (or other)
// units were incorporated into this page. Distinct from derived-from, which
// records write-time X-Provenance lineage.
type MergedFromEntry struct {
	Type  string `yaml:"type"`
	ID    string `yaml:"id"`
	Path  string `yaml:"path,omitempty"`
	Date  string `yaml:"date,omitempty"`
	Actor string `yaml:"actor,omitempty"`
	Note  string `yaml:"note,omitempty"`
}

// InjectMergedFrom appends entries to the merged-from list in frontmatter
// and returns the full file bytes. Idempotent: skips entries whose type+id
// (or type+path when id is empty) already appear in the list. Preserves
// the markdown body; mirrors internal/pipeline provenance round-trip behavior.
func InjectMergedFrom(content []byte, newEntries []MergedFromEntry) ([]byte, error) {
	if len(newEntries) == 0 {
		return content, nil
	}

	existing, err := parseMergedFromKeys(content)
	if err != nil {
		existing = map[string]struct{}{}
	}

	var toAdd []MergedFromEntry
	now := time.Now().UTC().Format(time.RFC3339)
	for _, e := range newEntries {
		if e.Type == "" {
			return nil, fmt.Errorf("memory: merged-from entry needs type")
		}
		if e.ID == "" && e.Path == "" {
			return nil, fmt.Errorf("memory: merged-from entry needs id or path")
		}
		if e.Date == "" {
			e.Date = now
		}
		k := mergeKey(&e)
		if _, ok := existing[k]; ok {
			continue
		}
		existing[k] = struct{}{}
		toAdd = append(toAdd, e)
	}
	if len(toAdd) == 0 {
		return content, nil
	}

	fmBytes, body, err := markdown.SplitFrontmatter(content)
	if err != nil {
		return nil, err
	}

	var root yaml.Node
	if len(fmBytes) > 0 {
		if err := yaml.Unmarshal(fmBytes, &root); err != nil {
			return nil, fmt.Errorf("parse frontmatter: %w", err)
		}
	}
	mapping := ensureMappingDocument(&root)

	for _, e := range toAdd {
		var node yaml.Node
		if err := node.Encode(&e); err != nil {
			return nil, fmt.Errorf("encode merged-from entry: %w", err)
		}
		appendToListKey(mapping, "merged-from", &node)
	}

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

func mergeKey(e *MergedFromEntry) string {
	typ := strings.ToLower(strings.TrimSpace(e.Type))
	if e.ID != "" {
		return typ + ":" + strings.TrimSpace(e.ID)
	}
	p := strings.TrimSpace(e.Path)
	p = strings.ReplaceAll(p, "\\", "/")
	return typ + ":path:" + p
}

func parseMergedFromKeys(content []byte) (map[string]struct{}, error) {
	fmBytes, _, err := markdown.SplitFrontmatter(content)
	if err != nil {
		return nil, err
	}
	if len(fmBytes) == 0 {
		return map[string]struct{}{}, nil
	}
	var m map[string]any
	if err := yaml.Unmarshal(fmBytes, &m); err != nil {
		return nil, err
	}
	return mergedKeysFromFM(m)
}

func mergedKeysFromFM(m map[string]any) (map[string]struct{}, error) {
	out := make(map[string]struct{})
	raw, ok := m["merged-from"]
	if !ok {
		return out, nil
	}
	entries, err := normaliseMergedSequence(raw)
	if err != nil {
		return out, err
	}
	for _, e := range entries {
		typ, _ := e["type"].(string)
		if typ == "" {
			continue
		}
		id, _ := e["id"].(string)
		pth, _ := e["path"].(string)
		ee := MergedFromEntry{Type: typ, ID: id, Path: pth}
		out[mergeKey(&ee)] = struct{}{}
	}
	return out, nil
}

// ensureMappingDocument mirrors internal/pipeline/provenance.
func ensureMappingDocument(root *yaml.Node) *yaml.Node {
	if root.Kind == 0 {
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

func appendToListKey(mapping *yaml.Node, keyName string, entry *yaml.Node) {
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
