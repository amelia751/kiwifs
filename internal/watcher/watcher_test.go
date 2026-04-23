package watcher

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/versioning"
)

// The watcher funnels changes through the shared pipeline, which means
// SSE subscribers see fsnotify-origin events without any parallel fan-out.
// This test asserts that end-to-end: write a file under --root out of
// band, expect a write event on the hub.
func TestWatcherRoutesThroughPipeline(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	hub := events.NewHub()
	pipe := pipeline.New(store, versioning.NewNoop(), search.NewGrep(dir), nil, hub, nil)

	w, err := New(dir, store, pipe)
	if err != nil {
		t.Fatalf("watcher: %v", err)
	}
	defer w.Close()
	// Shorter debounce keeps the test fast.
	w.debounce = 50 * time.Millisecond
	w.Start()

	ch, err := hub.Subscribe()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer hub.Unsubscribe(ch)

	// Write directly on disk (bypass the API) — fsnotify should pick it up.
	if err := os.WriteFile(filepath.Join(dir, "direct.md"), []byte("body\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Wait for the broadcast.
	var got events.Message
	select {
	case got = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("no SSE event within 2s")
	}
	if got.Op != "write" {
		t.Fatalf("want op=write, got %s", got.Op)
	}
}

// Double-check there's no obvious race under go test -race.
func TestWatcherCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	store, _ := storage.NewLocal(dir)
	pipe := pipeline.New(store, versioning.NewNoop(), search.NewGrep(dir), nil, events.NewHub(), nil)
	w, _ := New(dir, store, pipe)
	w.Start()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = w.Close()
		}()
	}
	wg.Wait()
}
