package memory

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kiwifs/kiwifs/internal/storage"
)

func TestScan_unmerged(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	// Episodic file
	ep := filepath.Join(root, "episodes", "a.md")
	if err := os.MkdirAll(filepath.Dir(ep), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ep, []byte(`---
memory_kind: episodic
episode_id: run-1
---
# run
`), 0644); err != nil {
		t.Fatal(err)
	}
	// Semantic with merged-from
	sem := filepath.Join(root, "concepts", "x.md")
	if err := os.MkdirAll(filepath.Dir(sem), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sem, []byte(`---
memory_kind: semantic
title: x
merged-from:
  - type: episode
    id: run-1
---
# x
`), 0644); err != nil {
		t.Fatal(err)
	}

	s, err := storage.NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := Scan(context.Background(), s, Options{EpisodesPathPrefix: "episodes/"})
	if err != nil {
		t.Fatal(err)
	}
	if rep.EpisodicCount != 1 {
		t.Fatalf("episodic: %d", rep.EpisodicCount)
	}
	if len(rep.Unmerged) != 0 {
		t.Fatalf("unmerged: %+v", rep.Unmerged)
	}
}

func TestScan_pathOnlyMerge(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ep := filepath.Join(root, "episodes", "b.md")
	if err := os.MkdirAll(filepath.Dir(ep), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ep, []byte(`---
memory_kind: episodic
---
# no id
`), 0644); err != nil {
		t.Fatal(err)
	}
	sem := filepath.Join(root, "c.md")
	if err := os.WriteFile(sem, []byte(`---
memory_kind: semantic
merged-from:
  - type: episode
    path: episodes/b.md
---
# c
`), 0644); err != nil {
		t.Fatal(err)
	}

	s, err := storage.NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := Scan(context.Background(), s, Options{EpisodesPathPrefix: "episodes/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Unmerged) != 0 {
		t.Fatalf("expected path merge, unmerged: %+v", rep.Unmerged)
	}
}
