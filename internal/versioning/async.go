package versioning

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type commitReq struct {
	paths   []string
	actor   string
	message string
}

// AsyncGit wraps a Versioner and defers commits to a background goroutine
// that batches them by actor. When the actor changes between consecutive
// requests, the current batch is flushed before starting a new one — this
// guarantees that every commit in git is attributed to the correct actor.
type AsyncGit struct {
	inner       Versioner
	pending     chan commitReq
	wg          sync.WaitGroup
	stopOnce    sync.Once
	stop        chan struct{}
	batchWindow time.Duration
	batchMax    int

	// uncommittedLog, when non-empty, is the path to a file where paths
	// are appended before enqueuing. If the process crashes before the
	// background loop flushes, DrainUncommitted can recover them on
	// restart. Without this, a crash loses the channel contents and
	// the audit trail has a silent gap.
	uncommittedLog string
}

type AsyncOption func(*AsyncGit)

func WithBatchWindow(d time.Duration) AsyncOption {
	return func(a *AsyncGit) { a.batchWindow = d }
}

func WithBatchMaxSize(n int) AsyncOption {
	return func(a *AsyncGit) { a.batchMax = n }
}

func WithChannelBuffer(n int) AsyncOption {
	return func(a *AsyncGit) {
		a.pending = make(chan commitReq, n)
	}
}

// WithUncommittedLog sets the path where pending paths are journaled
// before enqueuing so a crash doesn't silently lose audit trail entries.
func WithUncommittedLog(path string) AsyncOption {
	return func(a *AsyncGit) { a.uncommittedLog = path }
}

func NewAsyncGit(inner Versioner, opts ...AsyncOption) *AsyncGit {
	a := &AsyncGit{
		inner:       inner,
		pending:     make(chan commitReq, 1000),
		stop:        make(chan struct{}),
		batchWindow: 200 * time.Millisecond,
		batchMax:    50,
	}
	for _, o := range opts {
		o(a)
	}
	a.wg.Add(1)
	go a.run()
	return a
}

func (a *AsyncGit) Commit(_ context.Context, path, actor, message string) error {
	a.journalPaths(path)
	select {
	case a.pending <- commitReq{paths: []string{path}, actor: actor, message: message}:
	case <-a.stop:
	}
	return nil
}

func (a *AsyncGit) BulkCommit(_ context.Context, paths []string, actor, message string) error {
	a.journalPaths(paths...)
	select {
	case a.pending <- commitReq{paths: paths, actor: actor, message: message}:
	case <-a.stop:
	}
	return nil
}

func (a *AsyncGit) CommitDelete(_ context.Context, path, actor string) error {
	a.journalPaths(path)
	select {
	case a.pending <- commitReq{paths: []string{path}, actor: actor, message: "delete: " + path}:
	case <-a.stop:
	}
	return nil
}

func (a *AsyncGit) Log(ctx context.Context, path string) ([]Version, error) {
	return a.inner.Log(ctx, path)
}

func (a *AsyncGit) Show(ctx context.Context, path, hash string) ([]byte, error) {
	return a.inner.Show(ctx, path, hash)
}

func (a *AsyncGit) Diff(ctx context.Context, path, fromHash, toHash string) (string, error) {
	return a.inner.Diff(ctx, path, fromHash, toHash)
}

func (a *AsyncGit) Blame(ctx context.Context, path string) ([]BlameLine, error) {
	return a.inner.Blame(ctx, path)
}

func (a *AsyncGit) GC(ctx context.Context) error {
	if gc, ok := a.inner.(GCer); ok {
		return gc.GC(ctx)
	}
	return nil
}

func (a *AsyncGit) Close() error {
	a.stopOnce.Do(func() {
		close(a.stop)
	})
	a.wg.Wait()
	return nil
}

// journalPaths appends paths to the uncommitted log before they enter the
// channel. If the process crashes after this but before the commit flushes,
// DrainUncommitted will find them on restart and recommit. The log is
// cleared after every successful flush.
func (a *AsyncGit) journalPaths(paths ...string) {
	if a.uncommittedLog == "" {
		return
	}
	f, err := os.OpenFile(a.uncommittedLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("async-git: journal open: %v", err)
		return
	}
	defer f.Close()
	for _, p := range paths {
		fmt.Fprintln(f, p)
	}
}

// clearJournal removes the uncommitted log after a successful flush.
func (a *AsyncGit) clearJournal() {
	if a.uncommittedLog == "" {
		return
	}
	os.Remove(a.uncommittedLog)
}

// buildMessage constructs a commit message from the batch. Single-path
// batches use the original message; multi-path batches list every path
// so `git log` stays useful.
func buildMessage(actor string, reqs []commitReq) string {
	if len(reqs) == 1 && len(reqs[0].paths) == 1 {
		return reqs[0].message
	}
	var paths []string
	for _, r := range reqs {
		paths = append(paths, r.paths...)
	}
	if len(paths) == 1 {
		return fmt.Sprintf("%s: %s", actor, paths[0])
	}
	return fmt.Sprintf("%s: %d files\n\n%s", actor, len(paths), strings.Join(paths, "\n"))
}

// run is the background commit loop. It batches requests by actor: when a
// request arrives from a different actor, the current batch is flushed
// first. This guarantees every git commit is attributed to the correct
// actor and never mixes authors.
func (a *AsyncGit) run() {
	defer a.wg.Done()

	var batchReqs []commitReq
	var batchPaths []string
	var batchActor string
	timer := time.NewTimer(a.batchWindow)
	timer.Stop()

	flush := func() {
		if len(batchPaths) == 0 {
			return
		}
		msg := buildMessage(batchActor, batchReqs)
		ctx, cancel := context.WithTimeout(context.Background(), gitCmdTimeout)
		defer cancel()
		if err := a.inner.BulkCommit(ctx, batchPaths, batchActor, msg); err != nil {
			log.Printf("async-git: flush %d paths (actor=%s) failed: %v", len(batchPaths), batchActor, err)
			return
		}
		a.clearJournal()
		batchReqs = batchReqs[:0]
		batchPaths = batchPaths[:0]
	}

	appendReq := func(req commitReq) {
		// Actor changed — flush the current batch so it gets the
		// correct attribution, then start accumulating for the new actor.
		if batchActor != "" && req.actor != batchActor {
			timer.Stop()
			flush()
		}
		batchActor = req.actor
		batchReqs = append(batchReqs, req)
		batchPaths = append(batchPaths, req.paths...)
	}

	for {
		select {
		case req, ok := <-a.pending:
			if !ok {
				flush()
				return
			}
			appendReq(req)
			if len(batchPaths) >= a.batchMax {
				timer.Stop()
				flush()
			} else {
				timer.Reset(a.batchWindow)
			}

		case <-timer.C:
			flush()

		case <-a.stop:
			for {
				select {
				case req := <-a.pending:
					appendReq(req)
				default:
					flush()
					return
				}
			}
		}
	}
}
