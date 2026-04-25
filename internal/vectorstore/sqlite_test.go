package vectorstore

import (
	"context"
	"math"
	"testing"
)

func newTestSQLite(t *testing.T) *SQLite {
	t.Helper()
	s, err := NewSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestGetVectorsReturnsChunks(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()

	chunks := []Chunk{
		{ID: "a.md#0", Path: "a.md", ChunkIdx: 0, Text: "hello", Vector: []float32{1, 0, 0}},
		{ID: "a.md#1", Path: "a.md", ChunkIdx: 1, Text: "world", Vector: []float32{0, 1, 0}},
	}
	if err := s.Upsert(ctx, chunks); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, err := s.GetVectors(ctx, "a.md")
	if err != nil {
		t.Fatalf("GetVectors: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d chunks, want 2", len(got))
	}
	if got[0].ChunkIdx != 0 || got[1].ChunkIdx != 1 {
		t.Fatalf("unexpected chunk order: %v", got)
	}
	if got[0].Text != "hello" || got[1].Text != "world" {
		t.Fatalf("unexpected text: %q, %q", got[0].Text, got[1].Text)
	}
	if got[0].Path != "a.md" {
		t.Fatalf("path=%q, want a.md", got[0].Path)
	}
}

func TestGetVectorsEmptyForUnknownPath(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()

	got, err := s.GetVectors(ctx, "nonexistent.md")
	if err != nil {
		t.Fatalf("GetVectors: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d chunks, want 0", len(got))
	}
}

func TestGetVectorsRoundTrip(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()

	original := []float32{0.1, 0.2, 0.3, 0.4, 0.5}
	chunks := []Chunk{
		{ID: "b.md#0", Path: "b.md", ChunkIdx: 0, Text: "test", Vector: original},
	}
	if err := s.Upsert(ctx, chunks); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, err := s.GetVectors(ctx, "b.md")
	if err != nil {
		t.Fatalf("GetVectors: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d chunks, want 1", len(got))
	}

	// Vectors are L2-normalised on write, so compare normalised values.
	norm := normalise(original)
	if len(got[0].Vector) != len(norm) {
		t.Fatalf("vector dims=%d, want %d", len(got[0].Vector), len(norm))
	}
	for i, v := range got[0].Vector {
		if math.Abs(float64(v-norm[i])) > 1e-6 {
			t.Fatalf("vector[%d]=%f, want %f", i, v, norm[i])
		}
	}
}
