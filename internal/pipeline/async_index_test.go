package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kiwifs/kiwifs/internal/search"
)

// stubSearcher records Index/Remove calls for assertions.
type stubSearcher struct {
	mu      sync.Mutex
	indexed map[string]string // path → content
	removed []string
}

func newStubSearcher() *stubSearcher {
	return &stubSearcher{indexed: make(map[string]string)}
}

func (s *stubSearcher) Search(_ context.Context, _ string, _, _ int, _ string) ([]search.Result, error) {
	return nil, nil
}
func (s *stubSearcher) Index(_ context.Context, path string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indexed[path] = string(content)
	return nil
}
func (s *stubSearcher) Remove(_ context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removed = append(s.removed, path)
	delete(s.indexed, path)
	return nil
}
func (s *stubSearcher) Reindex(_ context.Context) (int, error) { return 0, nil }
func (s *stubSearcher) Close() error                           { return nil }

func (s *stubSearcher) snapshotIndexed() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make(map[string]string, len(s.indexed))
	for k, v := range s.indexed {
		cp[k] = v
	}
	return cp
}

func (s *stubSearcher) snapshotRemoved() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]string, len(s.removed))
	copy(cp, s.removed)
	return cp
}

func TestAsyncIndexer_BatchesUpserts(t *testing.T) {
	stub := newStubSearcher()
	ai := NewAsyncIndexer(stub, nil, nil,
		WithIndexBatchWindow(100*time.Millisecond),
		WithIndexBatchMax(200),
	)

	const n = 50
	for i := 0; i < n; i++ {
		ai.Enqueue("doc"+string(rune('A'+i))+".md", []byte("content"))
	}

	ai.Close()

	indexed := stub.snapshotIndexed()
	if len(indexed) != n {
		t.Fatalf("expected %d indexed, got %d", n, len(indexed))
	}
}

func TestAsyncIndexer_HandlesDeletes(t *testing.T) {
	stub := newStubSearcher()
	ai := NewAsyncIndexer(stub, nil, nil,
		WithIndexBatchWindow(100*time.Millisecond),
	)

	ai.Enqueue("keep.md", []byte("keep"))
	ai.EnqueueDelete("gone.md")
	ai.Close()

	indexed := stub.snapshotIndexed()
	if _, ok := indexed["keep.md"]; !ok {
		t.Fatalf("keep.md not indexed")
	}

	removed := stub.snapshotRemoved()
	found := false
	for _, p := range removed {
		if p == "gone.md" {
			found = true
		}
	}
	if !found {
		t.Fatalf("gone.md not removed")
	}
}

func TestAsyncIndexer_JournalAndClear(t *testing.T) {
	stub := newStubSearcher()
	dir := t.TempDir()
	journal := filepath.Join(dir, "unindexed.log")

	ai := NewAsyncIndexer(stub, nil, nil,
		WithIndexBatchWindow(500*time.Millisecond),
		WithIndexJournal(journal),
	)

	ai.Enqueue("j.md", []byte("content"))
	time.Sleep(10 * time.Millisecond)

	data, err := os.ReadFile(journal)
	if err != nil {
		t.Fatalf("journal should exist after Enqueue: %v", err)
	}
	if !strings.Contains(string(data), "j.md") {
		t.Fatalf("journal should contain path, got: %q", data)
	}

	ai.Close()

	if _, err := os.Stat(journal); !os.IsNotExist(err) {
		t.Fatalf("journal should be removed after successful flush, err=%v", err)
	}
}

func TestAsyncIndexer_CloseFlushes(t *testing.T) {
	stub := newStubSearcher()
	ai := NewAsyncIndexer(stub, nil, nil,
		WithIndexBatchWindow(10*time.Second),
	)

	ai.Enqueue("flush.md", []byte("content"))
	ai.Close()

	indexed := stub.snapshotIndexed()
	if _, ok := indexed["flush.md"]; !ok {
		t.Fatalf("file not flushed on Close")
	}
}
