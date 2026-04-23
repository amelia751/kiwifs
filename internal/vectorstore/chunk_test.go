package vectorstore

import (
	"strings"
	"testing"
)

func TestChunkRespectsSize(t *testing.T) {
	// 300 chars; size=100 overlap=10 → several chunks, each ≤ size.
	input := strings.Repeat("a", 300)
	chunks := chunk(input, 100, 10)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	for i, c := range chunks {
		if len([]rune(c)) > 100 {
			t.Fatalf("chunk %d exceeded size: %d", i, len([]rune(c)))
		}
	}
}

func TestChunkKeepsShortText(t *testing.T) {
	chunks := chunk("short", 100, 10)
	if len(chunks) != 1 || chunks[0] != "short" {
		t.Fatalf("unexpected chunks: %v", chunks)
	}
}

func TestChunkEmpty(t *testing.T) {
	if got := chunk("", 100, 10); got != nil {
		t.Fatalf("want nil, got %v", got)
	}
}
