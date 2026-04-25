package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/vectorstore"
)

type fakeVecProvider struct {
	chunks map[string][]vectorstore.Chunk
}

func (f *fakeVecProvider) GetVectors(_ context.Context, path string) ([]vectorstore.Chunk, error) {
	return f.chunks[path], nil
}

func testStore(t *testing.T) storage.Storage {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	return store
}

func writeFile(t *testing.T, store storage.Storage, path string, content string) {
	t.Helper()
	ctx := context.Background()
	if err := store.Write(ctx, path, []byte(content)); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestExportJSONL(t *testing.T) {
	store := testStore(t)
	writeFile(t, store, "students/alice.md", `---
name: Alice
grade: A
score: 95
---
# Alice
Top student.
`)
	writeFile(t, store, "students/bob.md", `---
name: Bob
grade: B
score: 88
---
# Bob
Good student.
`)

	var buf bytes.Buffer
	ctx := context.Background()
	searcher := search.NewGrep(store.(*storage.Local).AbsPath("/"))

	count, err := Export(ctx, store, searcher, nil, Options{
		Format: "jsonl",
		Output: &buf,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if count != 2 {
		t.Fatalf("count=%d, want 2", count)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines=%d, want 2", len(lines))
	}

	var rec Record
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rec.Path == "" {
		t.Fatalf("path is empty")
	}
	if rec.Frontmatter["name"] == nil {
		t.Fatalf("missing name in frontmatter")
	}
	if rec.WordCount == 0 {
		t.Fatalf("word_count should be > 0")
	}
	if rec.Content != "" {
		t.Fatalf("content should be empty without include_content")
	}
}

func TestExportJSONLWithContent(t *testing.T) {
	store := testStore(t)
	writeFile(t, store, "notes/hello.md", `---
title: Hello
---
# Hello World
Some body text here.
`)

	var buf bytes.Buffer
	ctx := context.Background()
	searcher := search.NewGrep(store.(*storage.Local).AbsPath("/"))

	count, err := Export(ctx, store, searcher, nil, Options{
		Format:         "jsonl",
		IncludeContent: true,
		Output:         &buf,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if count != 1 {
		t.Fatalf("count=%d, want 1", count)
	}

	var rec Record
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rec.Content == "" {
		t.Fatalf("content should be present with include_content")
	}
	if !strings.Contains(rec.Content, "Hello World") {
		t.Fatalf("content missing body: %s", rec.Content)
	}
}

func TestExportCSV(t *testing.T) {
	store := testStore(t)
	writeFile(t, store, "students/alice.md", `---
name: Alice
grade: A
---
# Alice
`)
	writeFile(t, store, "students/bob.md", `---
name: Bob
grade: B
---
# Bob
`)

	var buf bytes.Buffer
	ctx := context.Background()
	searcher := search.NewGrep(store.(*storage.Local).AbsPath("/"))

	count, err := Export(ctx, store, searcher, nil, Options{
		Format:  "csv",
		Columns: []string{"name", "grade"},
		Output:  &buf,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if count != 2 {
		t.Fatalf("count=%d, want 2", count)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines=%d, want 3 (header + 2 rows)", len(lines))
	}
	if !strings.Contains(lines[0], "path") {
		t.Fatalf("header missing path: %s", lines[0])
	}
	if !strings.Contains(lines[0], "name") {
		t.Fatalf("header missing name: %s", lines[0])
	}

	hasAlice := false
	hasBob := false
	for _, line := range lines[1:] {
		if strings.Contains(line, "Alice") {
			hasAlice = true
		}
		if strings.Contains(line, "Bob") {
			hasBob = true
		}
	}
	if !hasAlice || !hasBob {
		t.Fatalf("missing data rows: %s", buf.String())
	}
}

func TestExportCSVAutoDetect(t *testing.T) {
	store := testStore(t)
	writeFile(t, store, "items/one.md", `---
title: One
status: active
priority: high
---
# One
`)

	var buf bytes.Buffer
	ctx := context.Background()
	searcher := search.NewGrep(store.(*storage.Local).AbsPath("/"))

	count, err := Export(ctx, store, searcher, nil, Options{
		Format: "csv",
		Output: &buf,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if count != 1 {
		t.Fatalf("count=%d, want 1", count)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	header := lines[0]
	if !strings.Contains(header, "path") {
		t.Fatalf("header missing path: %s", header)
	}
	if !strings.Contains(header, "title") {
		t.Fatalf("header missing title: %s", header)
	}
	if !strings.Contains(header, "status") {
		t.Fatalf("header missing status: %s", header)
	}
}

func TestExportPathPrefix(t *testing.T) {
	store := testStore(t)
	writeFile(t, store, "students/alice.md", `---
name: Alice
---
# Alice
`)
	writeFile(t, store, "teachers/mr-smith.md", `---
name: Mr Smith
---
# Mr Smith
`)

	var buf bytes.Buffer
	ctx := context.Background()
	searcher := search.NewGrep(store.(*storage.Local).AbsPath("/"))

	count, err := Export(ctx, store, searcher, nil, Options{
		Format:     "jsonl",
		PathPrefix: "students/",
		Output:     &buf,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if count != 1 {
		t.Fatalf("count=%d, want 1 (only students/)", count)
	}

	var rec Record
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !strings.HasPrefix(rec.Path, "students/") {
		t.Fatalf("path=%s, want students/ prefix", rec.Path)
	}
}

func TestExportLimit(t *testing.T) {
	store := testStore(t)
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		writeFile(t, store, "items/"+name+".md", "---\nname: "+name+"\n---\n# "+name+"\n")
	}

	var buf bytes.Buffer
	ctx := context.Background()
	searcher := search.NewGrep(store.(*storage.Local).AbsPath("/"))

	count, err := Export(ctx, store, searcher, nil, Options{
		Format: "jsonl",
		Output: &buf,
		Limit:  2,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if count != 2 {
		t.Fatalf("count=%d, want 2", count)
	}
}

func TestExportCSVColumnSubset(t *testing.T) {
	store := testStore(t)
	writeFile(t, store, "data/item.md", `---
name: Test
category: science
priority: high
status: active
---
# Test
`)

	var buf bytes.Buffer
	ctx := context.Background()
	searcher := search.NewGrep(store.(*storage.Local).AbsPath("/"))

	count, err := Export(ctx, store, searcher, nil, Options{
		Format:  "csv",
		Columns: []string{"name", "category"},
		Output:  &buf,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if count != 1 {
		t.Fatalf("count=%d, want 1", count)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines=%d, want 2", len(lines))
	}
	if !strings.Contains(lines[0], "name") || !strings.Contains(lines[0], "category") {
		t.Fatalf("header wrong: %s", lines[0])
	}
	if !strings.Contains(lines[1], "science") {
		t.Fatalf("missing category value: %s", lines[1])
	}
	if strings.Contains(lines[1], "high") || strings.Contains(lines[1], "active") {
		t.Fatalf("priority/status should not be in subset: %s", lines[1])
	}
}

func TestExportEmptyStore(t *testing.T) {
	store := testStore(t)

	var buf bytes.Buffer
	ctx := context.Background()
	searcher := search.NewGrep(store.(*storage.Local).AbsPath("/"))

	count, err := Export(ctx, store, searcher, nil, Options{
		Format: "jsonl",
		Output: &buf,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if count != 0 {
		t.Fatalf("count=%d, want 0", count)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected empty output, got: %s", buf.String())
	}
}

func TestExportWithEmbeddings(t *testing.T) {
	store := testStore(t)
	writeFile(t, store, "notes/test.md", "---\ntitle: Test\n---\n# Test\nSome body text.\n")

	vecs := &fakeVecProvider{
		chunks: map[string][]vectorstore.Chunk{
			"notes/test.md": {
				{ID: "notes/test.md#0", Path: "notes/test.md", ChunkIdx: 0, Text: "chunk zero", Vector: []float32{0.1, 0.2, 0.3}},
				{ID: "notes/test.md#1", Path: "notes/test.md", ChunkIdx: 1, Text: "chunk one", Vector: []float32{0.4, 0.5, 0.6}},
			},
		},
	}

	var buf bytes.Buffer
	ctx := context.Background()
	searcher := search.NewGrep(store.(*storage.Local).AbsPath("/"))

	count, err := Export(ctx, store, searcher, vecs, Options{
		Format:            "jsonl",
		IncludeEmbeddings: true,
		Output:            &buf,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if count != 1 {
		t.Fatalf("count=%d, want 1", count)
	}

	var rec Record
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rec.Embeddings) != 2 {
		t.Fatalf("embeddings=%d, want 2", len(rec.Embeddings))
	}
	if rec.Embeddings[0].ChunkIdx != 0 || rec.Embeddings[1].ChunkIdx != 1 {
		t.Fatalf("unexpected chunk indices: %v", rec.Embeddings)
	}
	if len(rec.Embeddings[0].Vector) != 3 {
		t.Fatalf("vector dims=%d, want 3", len(rec.Embeddings[0].Vector))
	}
}

func TestExportWithEmbeddingsNoVecService(t *testing.T) {
	store := testStore(t)
	writeFile(t, store, "notes/test.md", "---\ntitle: Test\n---\n# Test\n")

	var buf bytes.Buffer
	ctx := context.Background()
	searcher := search.NewGrep(store.(*storage.Local).AbsPath("/"))

	count, err := Export(ctx, store, searcher, nil, Options{
		Format:            "jsonl",
		IncludeEmbeddings: true,
		Output:            &buf,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if count != 1 {
		t.Fatalf("count=%d, want 1", count)
	}

	var rec Record
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rec.Embeddings) != 0 {
		t.Fatalf("embeddings=%d, want 0 (no vec service)", len(rec.Embeddings))
	}
}

func TestExportWithoutEmbeddingsNoRegression(t *testing.T) {
	store := testStore(t)
	writeFile(t, store, "notes/test.md", "---\ntitle: Test\n---\n# Test\n")

	vecs := &fakeVecProvider{
		chunks: map[string][]vectorstore.Chunk{
			"notes/test.md": {
				{ID: "notes/test.md#0", Path: "notes/test.md", ChunkIdx: 0, Text: "chunk", Vector: []float32{0.1}},
			},
		},
	}

	var buf bytes.Buffer
	ctx := context.Background()
	searcher := search.NewGrep(store.(*storage.Local).AbsPath("/"))

	count, err := Export(ctx, store, searcher, vecs, Options{
		Format:            "jsonl",
		IncludeEmbeddings: false,
		Output:            &buf,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if count != 1 {
		t.Fatalf("count=%d, want 1", count)
	}

	var rec Record
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rec.Embeddings) != 0 {
		t.Fatalf("embeddings=%d, want 0 (include_embeddings=false)", len(rec.Embeddings))
	}
}

func TestExportSchemaWriter(t *testing.T) {
	store := testStore(t)
	writeFile(t, store, "notes/test.md", "---\ntitle: Test\n---\n# Test\n")

	vecs := &fakeVecProvider{
		chunks: map[string][]vectorstore.Chunk{
			"notes/test.md": {
				{ID: "notes/test.md#0", Path: "notes/test.md", ChunkIdx: 0, Text: "chunk", Vector: []float32{0.1, 0.2, 0.3}},
			},
		},
	}

	var buf, schemaBuf bytes.Buffer
	ctx := context.Background()
	searcher := search.NewGrep(store.(*storage.Local).AbsPath("/"))

	_, err := Export(ctx, store, searcher, vecs, Options{
		Format:            "jsonl",
		IncludeEmbeddings: true,
		Output:            &buf,
		SchemaWriter:      &schemaBuf,
		EmbeddingDims:     3,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	var schema map[string]any
	if err := json.Unmarshal(schemaBuf.Bytes(), &schema); err != nil {
		t.Fatalf("unmarshal schema: %v", err)
	}
	if schema["format"] != "jsonl" {
		t.Fatalf("schema format=%v, want jsonl", schema["format"])
	}
	if int(schema["embedding_dimensions"].(float64)) != 3 {
		t.Fatalf("schema dims=%v, want 3", schema["embedding_dimensions"])
	}
	if int(schema["record_count"].(float64)) != 1 {
		t.Fatalf("schema record_count=%v, want 1", schema["record_count"])
	}
}
