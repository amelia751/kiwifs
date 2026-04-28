package cmd

import (
	"io/fs"
	"testing"
)

func TestKnowledgeTemplateEmbedded(t *testing.T) {
	t.Parallel()
	paths := []string{
		"templates/knowledge/SCHEMA.md",
		"templates/knowledge/index.md",
		"templates/knowledge/log.md",
		"templates/knowledge/episodes/example-episode.md",
	}
	for _, p := range paths {
		if _, err := fs.Stat(templates, p); err != nil {
			t.Fatalf("embedded template missing %s: %v", p, err)
		}
	}
}
