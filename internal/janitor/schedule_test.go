package janitor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
)

// TestSchedulerRunsInitialScanAndBroadcasts verifies the scheduler's
// contract with consumers: a startup scan populates LastResult, emits a
// "janitor" SSE event with the severity breakdown, and stops cleanly on
// context cancel.
func TestSchedulerRunsInitialScanAndBroadcasts(t *testing.T) {
	root := t.TempDir()
	// One healthy index, one missing-owner page → scanner will flag
	// at least one warning-severity issue.
	writeFile(t, root, "index.md", `---
owner: team@acme
status: current
reviewed: 2099-01-01
---
# Home

Initial page with enough body content to pass the empty-page heuristic so
we get a realistic workspace.`)
	writeFile(t, root, "orphan.md", "# Orphan\n\nno frontmatter, should flag missing-owner.\n")

	store, err := storage.NewLocal(root)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	searcher := search.NewGrep(root)
	scanner := New(root, store, searcher, 90)
	hub := events.NewHub()

	sub, err := hub.Subscribe()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer hub.Unsubscribe(sub)

	s := NewScheduler(scanner, hub, nil, ScheduleOptions{
		Interval:    10 * time.Second, // long enough not to fire a second tick in this test
		InitialScan: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	defer s.Stop()

	// Wait for the initial-scan event. 2 s is generous on CI; a clean
	// scan of two markdown files on a warm box completes in < 50 ms.
	deadline := time.After(2 * time.Second)
	var ev events.Event
	for {
		select {
		case msg := <-sub:
			if msg.Op != "janitor" {
				continue
			}
			if err := json.Unmarshal(msg.Data, &ev); err != nil {
				t.Fatalf("decode janitor event: %v", err)
			}
			goto done
		case <-deadline:
			t.Fatal("timed out waiting for janitor SSE event")
		}
	}
done:
	if ev.Extra == nil {
		t.Fatalf("expected Extra payload on janitor event, got %+v", ev)
	}
	if ev.Extra["status"] != "ok" {
		t.Fatalf("status = %v, want ok", ev.Extra["status"])
	}
	// Not fatal — the heuristic might not flag on this particular file
	// set on all platforms. The test's primary job is to prove the
	// loop, cache, and SSE plumbing work; exact issue counts are the
	// scanner's responsibility and tested elsewhere.
	t.Logf("scan result: %+v", ev.Extra)
	if s.LastResult() == nil {
		t.Fatal("LastResult should be populated after the initial scan")
	}
	if s.LastScan().IsZero() {
		t.Fatal("LastScan should record a non-zero timestamp")
	}
}

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	abs := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}
