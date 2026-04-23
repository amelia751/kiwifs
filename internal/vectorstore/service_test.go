package vectorstore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeEmbedder sleeps sleepPer call then returns a trivial one-dimensional
// vector. The sleep is what lets the test observe worker-pool parallelism:
// N serial calls take N*sleepPer, while N parallel workers finish in
// roughly sleepPer regardless of N.
type fakeEmbedder struct {
	sleepPer time.Duration
	calls    atomic.Int64
}

func (f *fakeEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	f.calls.Add(1)
	select {
	case <-time.After(f.sleepPer):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = []float32{1}
	}
	return out, nil
}

func (f *fakeEmbedder) Dimensions() int { return 1 }

// fakeStore counts Upsert and RemoveByPath invocations so tests can check
// whether a skipped path actually made it to the store or not.
type fakeStore struct {
	mu      sync.Mutex
	upserts []string
	removes []string
}

func (s *fakeStore) Upsert(ctx context.Context, chunks []Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range chunks {
		s.upserts = append(s.upserts, c.Path)
	}
	return nil
}
func (s *fakeStore) RemoveByPath(ctx context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removes = append(s.removes, path)
	return nil
}
func (s *fakeStore) Search(context.Context, []float32, int) ([]Result, error) { return nil, nil }
func (s *fakeStore) Reset(context.Context) error                              { return nil }
func (s *fakeStore) Count(context.Context) (int, error)                       { return 0, nil }
func (s *fakeStore) Close() error                                             { return nil }

func (s *fakeStore) upsertCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.upserts)
}

// TestWorkerPoolParallelism verifies that 5 workers drain a 20-job queue in
// ~4× embed latency, not 20×. Without the pool the test would take 20 ×
// sleepPer; with it, 4 × sleepPer (round-up of 20/5).
func TestWorkerPoolParallelism(t *testing.T) {
	emb := &fakeEmbedder{sleepPer: 50 * time.Millisecond}
	store := &fakeStore{}
	svc := NewService("/", nil, emb, store, Options{WorkerCount: 5})
	defer svc.Close()

	start := time.Now()
	const n = 20
	for i := 0; i < n; i++ {
		svc.Enqueue(relPath(i), []byte("hello world"))
	}
	// Wait for all jobs to drain. 5 workers × 50ms/job × 4 batches = 200ms;
	// 500ms leaves plenty of headroom for CI jitter.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if store.upsertCount() >= n {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	elapsed := time.Since(start)
	if store.upsertCount() < n {
		t.Fatalf("only %d/%d upserts landed after %s", store.upsertCount(), n, elapsed)
	}
	// Serial would be 1s+. The pool should finish in well under that.
	if elapsed > 700*time.Millisecond {
		t.Fatalf("pool wasn't parallel enough: %s for %d jobs", elapsed, n)
	}
}

// TestSkipPathShortCircuitsPendingUpsert models the BulkWrite rollback
// case: a path is enqueued, SkipPath marks it, the worker runs, and the
// store never sees the upsert.
func TestSkipPathShortCircuitsPendingUpsert(t *testing.T) {
	// Slow embedder so we have time to call SkipPath before the (single)
	// worker picks the job up.
	emb := &fakeEmbedder{sleepPer: 100 * time.Millisecond}
	store := &fakeStore{}
	// One worker + busy upfront = the target job stays queued long enough
	// for SkipPath to land before run() fires.
	svc := NewService("/", nil, emb, store, Options{WorkerCount: 1})
	defer svc.Close()

	// Occupy the worker so the next Enqueue sits in the buffered channel.
	svc.Enqueue("warm.md", []byte("warmup"))

	svc.Enqueue("bad.md", []byte("rolled-back content"))
	svc.SkipPath("bad.md")

	// Give the worker time to consume both jobs.
	time.Sleep(400 * time.Millisecond)

	store.mu.Lock()
	defer store.mu.Unlock()
	for _, p := range store.upserts {
		if p == "bad.md" {
			t.Fatalf("bad.md was upserted despite SkipPath: %v", store.upserts)
		}
	}
}

func relPath(i int) string {
	return "p-" + string(rune('a'+i%26)) + ".md"
}
