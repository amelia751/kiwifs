package janitor

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/kiwifs/kiwifs/internal/events"
)

// ScheduleOptions controls the background janitor loop. The zero value is
// not useful — Interval must be positive. Jitter staggers the first tick
// so a fleet of self-hosted servers restarted together don't all scan at
// the same wall-clock minute and thrash the shared storage.
type ScheduleOptions struct {
	// Interval between scheduled scans. Typical production value is 24h
	// ("nightly"). Tests use 1s; Interval ≤ 0 disables scheduled scans.
	Interval time.Duration

	// Jitter is a random-up-to-this delay applied to the first tick.
	// Avoids the thundering-herd problem on a managed fleet. 0 = no
	// jitter (useful in tests).
	Jitter time.Duration

	// InitialScan, when true, runs one scan immediately at startup before
	// entering the periodic loop. Leaves the workspace with a fresh
	// issue list the first time an admin opens the Janitor panel after a
	// restart.
	InitialScan bool
}

// Scheduler wraps a Scanner with a periodic ticker and pushes a summary
// SSE event after each scan. Consumers (the UI notification toast, a
// Slack webhook, Prometheus scrape hook, …) subscribe to the event hub
// and react to "kiwi.janitor" events without needing a direct handle to
// the Scanner.
//
// Life-cycle: call Start() once with the cancel context. The scheduler
// exits cleanly when ctx is cancelled or Stop() is called. A second
// Start on the same Scheduler is a no-op.
type Scheduler struct {
	scanner *Scanner
	hub     *events.Hub
	opts    ScheduleOptions

	mu       sync.Mutex
	started  bool
	stopCh   chan struct{}
	lastScan time.Time
	lastRes  *ScanResult
}

// NewScheduler wires a scanner to an SSE hub. The hub may be nil — the
// scheduler still runs and the ScanResult is cached on the struct for
// the API handler to read.
func NewScheduler(scanner *Scanner, hub *events.Hub, opts ScheduleOptions) *Scheduler {
	return &Scheduler{scanner: scanner, hub: hub, opts: opts, stopCh: make(chan struct{})}
}

// Start runs the scheduling loop in a goroutine and returns immediately.
// Safe to call multiple times; only the first wins.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started || s.scanner == nil || s.opts.Interval <= 0 {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()

	go s.loop(ctx)
}

// Stop signals the loop to exit. Blocks until the in-flight scan (if
// any) finishes — callers that want a bounded-time shutdown should
// cancel the context instead.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return
	}
	select {
	case <-s.stopCh: // already closed
	default:
		close(s.stopCh)
	}
}

// LastResult returns the most recent cached scan result (may be nil).
// The handler uses this so an admin opening the panel mid-day sees last
// night's scan instantly, then a background refresh kicks in.
func (s *Scheduler) LastResult() *ScanResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRes
}

// LastScan reports when the last scheduled scan completed. Zero value
// means no scheduled scan has run yet.
func (s *Scheduler) LastScan() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastScan
}

func (s *Scheduler) loop(ctx context.Context) {
	// Jitter the first tick so a fleet of servers restarted simultaneously
	// don't all hammer storage at the same second.
	if s.opts.Jitter > 0 {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-time.After(time.Duration(int64(s.opts.Jitter) / 2)):
		}
	}
	if s.opts.InitialScan {
		s.runOnce(ctx)
	}
	ticker := time.NewTicker(s.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.runOnce(ctx)
		}
	}
}

// runOnce executes a scan and broadcasts a compact summary via SSE. The
// full issue list is intentionally NOT pushed on the bus — it can be
// thousands of entries and the UI will pull it via the existing
// /janitor endpoint as soon as the summary arrives.
func (s *Scheduler) runOnce(ctx context.Context) {
	scanCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	start := time.Now()
	res, err := s.scanner.Scan(scanCtx)
	if err != nil {
		log.Printf("janitor: scheduled scan failed: %v", err)
		if s.hub != nil {
			s.hub.Broadcast(events.Event{
				Op:    "janitor",
				Actor: "scheduler",
				Extra: map[string]any{
					"status": "error",
					"error":  err.Error(),
				},
			})
		}
		return
	}

	// Cache for handler reads.
	s.mu.Lock()
	s.lastRes = res
	s.lastScan = time.Now()
	s.mu.Unlock()

	duration := time.Since(start).Round(time.Millisecond)
	// Severity breakdown helps the UI render a single-glance badge (red
	// = errors, amber = warnings, green = clean).
	counts := map[string]int{"error": 0, "warning": 0, "info": 0}
	for _, is := range res.Issues {
		counts[is.Severity]++
	}
	if s.hub != nil {
		s.hub.Broadcast(events.Event{
			Op:    "janitor",
			Actor: "scheduler",
			Extra: map[string]any{
				"status":      "ok",
				"scanned":     res.Scanned,
				"healthy":     res.Healthy,
				"issues":      len(res.Issues),
				"errors":      counts["error"],
				"warnings":    counts["warning"],
				"infos":       counts["info"],
				"duration_ms": duration.Milliseconds(),
				"timestamp":   res.Timestamp,
			},
		})
	}
	log.Printf("janitor: scheduled scan found %d issue(s) (%d errors, %d warnings) across %d pages in %s",
		len(res.Issues), counts["error"], counts["warning"], res.Scanned, duration)
}
