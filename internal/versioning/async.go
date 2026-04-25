package versioning

import (
	"context"
	"log"
	"sync"
	"time"
)

type commitReq struct {
	paths   []string
	actor   string
	message string
}

type AsyncGit struct {
	inner       Versioner
	pending     chan commitReq
	wg          sync.WaitGroup
	stopOnce    sync.Once
	stop        chan struct{}
	batchWindow time.Duration
	batchMax    int
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
	select {
	case a.pending <- commitReq{paths: []string{path}, actor: actor, message: message}:
	case <-a.stop:
	}
	return nil
}

func (a *AsyncGit) BulkCommit(_ context.Context, paths []string, actor, message string) error {
	select {
	case a.pending <- commitReq{paths: paths, actor: actor, message: message}:
	case <-a.stop:
	}
	return nil
}

func (a *AsyncGit) CommitDelete(_ context.Context, path, actor string) error {
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

// run is the background commit loop. It batches incoming commitReqs and
// flushes them via inner.BulkCommit when the batch window expires or
// the batch hits batchMax.
func (a *AsyncGit) run() {
	defer a.wg.Done()

	var batch []string
	var lastActor string
	timer := time.NewTimer(a.batchWindow)
	timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), gitCmdTimeout)
		defer cancel()
		if err := a.inner.BulkCommit(ctx, batch, lastActor, "async batch commit"); err != nil {
			log.Printf("async-git: flush %d paths failed: %v", len(batch), err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case req, ok := <-a.pending:
			if !ok {
				flush()
				return
			}
			batch = append(batch, req.paths...)
			lastActor = req.actor
			if len(batch) >= a.batchMax {
				timer.Stop()
				flush()
			} else {
				timer.Reset(a.batchWindow)
			}

		case <-timer.C:
			flush()

		case <-a.stop:
			// Drain remaining items from the channel, then flush.
			for {
				select {
				case req := <-a.pending:
					batch = append(batch, req.paths...)
					lastActor = req.actor
				default:
					flush()
					return
				}
			}
		}
	}
}
