package pipeline

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/vectorstore"
)

type indexReq struct {
	path    string
	content []byte
	delete  bool
}

// AsyncIndexer defers search/links/meta indexing to a background goroutine
// that batches work and flushes on a timer or when the batch is full.
// This is the Elasticsearch refresh_interval pattern: writes return
// immediately, the search index catches up within the batch window
// (default 200ms). For a knowledge base, sub-second eventual consistency
// is invisible to both humans and agents.
type AsyncIndexer struct {
	searcher search.Searcher
	linker   links.Linker
	vectors  *vectorstore.Service

	pending     chan indexReq
	wg          sync.WaitGroup
	stopOnce    sync.Once
	stop        chan struct{}
	batchWindow time.Duration
	batchMax    int
	journalPath string
}

type IndexerOption func(*AsyncIndexer)

func WithIndexBatchWindow(d time.Duration) IndexerOption {
	return func(a *AsyncIndexer) { a.batchWindow = d }
}

func WithIndexBatchMax(n int) IndexerOption {
	return func(a *AsyncIndexer) { a.batchMax = n }
}

func WithIndexJournal(path string) IndexerOption {
	return func(a *AsyncIndexer) { a.journalPath = path }
}

func NewAsyncIndexer(
	searcher search.Searcher,
	linker links.Linker,
	vectors *vectorstore.Service,
	opts ...IndexerOption,
) *AsyncIndexer {
	a := &AsyncIndexer{
		searcher:    searcher,
		linker:      linker,
		vectors:     vectors,
		pending:     make(chan indexReq, 2000),
		stop:        make(chan struct{}),
		batchWindow: 200 * time.Millisecond,
		batchMax:    100,
	}
	for _, o := range opts {
		o(a)
	}
	a.wg.Add(1)
	go a.run()
	return a
}

// Enqueue schedules a file for indexing. Returns immediately.
func (a *AsyncIndexer) Enqueue(path string, content []byte) {
	a.journal(path)
	cp := make([]byte, len(content))
	copy(cp, content)
	select {
	case a.pending <- indexReq{path: path, content: cp}:
	case <-a.stop:
	}
}

// EnqueueDelete schedules a file for deindexing. Returns immediately.
func (a *AsyncIndexer) EnqueueDelete(path string) {
	a.journal(path)
	select {
	case a.pending <- indexReq{path: path, delete: true}:
	case <-a.stop:
	}
}

func (a *AsyncIndexer) Close() error {
	a.stopOnce.Do(func() { close(a.stop) })
	a.wg.Wait()
	return nil
}

func (a *AsyncIndexer) journal(path string) {
	if a.journalPath == "" {
		return
	}
	f, err := os.OpenFile(a.journalPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("async-indexer: journal open: %v", err)
		return
	}
	defer f.Close()
	fmt.Fprintln(f, path)
}

func (a *AsyncIndexer) clearJournal() {
	if a.journalPath == "" {
		return
	}
	os.Remove(a.journalPath)
}

func (a *AsyncIndexer) run() {
	defer a.wg.Done()

	var batch []indexReq
	timer := time.NewTimer(a.batchWindow)
	timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		a.flushBatch(batch)
		a.clearJournal()
		batch = batch[:0]
	}

	for {
		select {
		case req, ok := <-a.pending:
			if !ok {
				flush()
				return
			}
			batch = append(batch, req)
			if len(batch) >= a.batchMax {
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
					batch = append(batch, req)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (a *AsyncIndexer) flushBatch(batch []indexReq) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var upserts []indexReq
	var deletes []indexReq
	for _, r := range batch {
		if r.delete {
			deletes = append(deletes, r)
		} else {
			upserts = append(upserts, r)
		}
	}

	if len(upserts) > 0 {
		if bi, ok := a.searcher.(search.BatchIndexer); ok {
			entries := make([]search.IndexEntry, len(upserts))
			for i, r := range upserts {
				entries[i] = search.IndexEntry{Path: r.path, Content: r.content}
			}
			if err := bi.IndexBatch(ctx, entries); err != nil {
				log.Printf("async-indexer: IndexBatch(%d): %v", len(entries), err)
			}
		} else {
			for _, r := range upserts {
				if err := a.searcher.Index(ctx, r.path, r.content); err != nil {
					log.Printf("async-indexer: Index(%s): %v", r.path, err)
				}
			}
		}

		for _, r := range upserts {
			if a.linker != nil {
				if err := a.linker.IndexLinks(ctx, r.path, links.Extract(r.content)); err != nil {
					log.Printf("async-indexer: IndexLinks(%s): %v", r.path, err)
				}
			}
			if meta, ok := a.searcher.(metaIndexer); ok {
				if err := meta.IndexMeta(ctx, r.path, r.content); err != nil {
					log.Printf("async-indexer: IndexMeta(%s): %v", r.path, err)
				}
			}
			if a.vectors != nil {
				a.vectors.Enqueue(r.path, r.content)
			}
		}
	}

	for _, r := range deletes {
		if ra, ok := a.searcher.(allRemover); ok {
			if err := ra.RemoveAll(ctx, r.path); err != nil {
				log.Printf("async-indexer: RemoveAll(%s): %v", r.path, err)
			}
		} else {
			if err := a.searcher.Remove(ctx, r.path); err != nil {
				log.Printf("async-indexer: Remove(%s): %v", r.path, err)
			}
			if a.linker != nil {
				if err := a.linker.RemoveLinks(ctx, r.path); err != nil {
					log.Printf("async-indexer: RemoveLinks(%s): %v", r.path, err)
				}
			}
			if meta, ok := a.searcher.(metaIndexer); ok {
				if err := meta.RemoveMeta(ctx, r.path); err != nil {
					log.Printf("async-indexer: RemoveMeta(%s): %v", r.path, err)
				}
			}
		}
		if a.vectors != nil {
			a.vectors.EnqueueDelete(r.path)
		}
	}

	log.Printf("async-indexer: flushed %d upserts + %d deletes", len(upserts), len(deletes))
}
