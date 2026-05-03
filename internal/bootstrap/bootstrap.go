package bootstrap

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/kiwifs/kiwifs/internal/api"
	"github.com/kiwifs/kiwifs/internal/comments"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/janitor"
	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/rbac"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/tracing"
	"github.com/kiwifs/kiwifs/internal/vectorstore"
	"github.com/kiwifs/kiwifs/internal/versioning"
)

func janitorInterval(cfg *config.Config) time.Duration {
	raw := cfg.Janitor.Interval
	if raw == "" {
		return 24 * time.Hour
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		log.Printf("janitor: invalid interval %q, using 24h: %v", raw, err)
		return 24 * time.Hour
	}
	if d < 0 {
		return 0
	}
	return d
}

type Stack struct {
	Name         string
	Root         string
	Config       *config.Config
	Store        storage.Storage
	Versioner    versioning.Versioner
	Searcher     search.Searcher
	Linker       links.Linker
	LinkResolver *links.Resolver
	Hub          *events.Hub
	Pipeline     *pipeline.Pipeline
	Vectors      *vectorstore.Service
	Comments     *comments.Store
	Server       *api.Server
	JanitorSched *janitor.Scheduler
	Emitter      tracing.Emitter
}

func Build(name, root string, cfg *config.Config) (*Stack, error) {
	prefix := logPrefix(name)

	store, err := storage.NewLocal(root)
	if err != nil {
		return nil, fmt.Errorf("%sstorage init: %w", prefix, err)
	}

	ver := buildVersioner(prefix, root, cfg)
	searcher := buildSearcher(prefix, root, store, cfg)

	vectors, verr := vectorstore.Build(root, store, cfg.Search.Vector)
	if verr != nil {
		log.Printf("%svector search disabled (%v)", prefix, verr)
		vectors = nil
	} else if vectors != nil {
		log.Printf("%svector search: provider=%s store=%s — enabled",
			prefix, cfg.Search.Vector.Embedder.Provider, cfg.Search.Vector.Store.Provider)
	}

	var linker links.Linker
	if l, ok := searcher.(links.Linker); ok {
		linker = l
	}

	linkResolver := links.NewResolver(func(ctx context.Context, fn func(path string)) error {
		return storage.Walk(ctx, store, "/", func(e storage.Entry) error {
			fn(e.Path)
			return nil
		})
	})

	hub := events.NewHub()
	pipe := pipeline.New(store, ver, searcher, linker, hub, vectors, root)

	asyncIdxEnabled := cfg.Search.AsyncIndex == nil || *cfg.Search.AsyncIndex
	if asyncIdxEnabled && cfg.Search.Engine != "grep" {
		var idxOpts []pipeline.IndexerOption
		if cfg.Search.IndexWindowMs > 0 {
			idxOpts = append(idxOpts, pipeline.WithIndexBatchWindow(
				time.Duration(cfg.Search.IndexWindowMs)*time.Millisecond))
		}
		if cfg.Search.IndexBatchMax > 0 {
			idxOpts = append(idxOpts, pipeline.WithIndexBatchMax(cfg.Search.IndexBatchMax))
		}
		idxJournal := filepath.Join(root, ".kiwi", "state", "unindexed.log")
		idxOpts = append(idxOpts, pipeline.WithIndexJournal(idxJournal))
		pipe.AsyncIdx = pipeline.NewAsyncIndexer(searcher, linker, vectors, idxOpts...)
		log.Printf("%sasync indexing enabled (window=%dms)",
			prefix, func() int {
				if cfg.Search.IndexWindowMs > 0 {
					return cfg.Search.IndexWindowMs
				}
				return 200
			}())
	}

	pipe.OnInvalidate = func() { linkResolver.MarkDirty() }

	cstore, err := comments.New(root)
	if err != nil {
		if vectors != nil {
			_ = vectors.Close()
		}
		_ = searcher.Close()
		return nil, fmt.Errorf("%scomments store: %w", prefix, err)
	}

	shares, err := rbac.NewShareStore(root)
	if err != nil {
		log.Printf("%sshare links disabled (%v)", prefix, err)
		shares = nil
	}

	em := tracing.NewEmitter(cfg.Tracing.IsEnabled(), cfg.Tracing.Output, cfg.Tracing.File)

	server := api.NewServer(cfg, pipe, vectors, cstore, shares, linkResolver, em)

	var janitorSched *janitor.Scheduler
	if iv := janitorInterval(cfg); iv > 0 {
		staleDays := cfg.Janitor.StaleDays
		if staleDays <= 0 {
			staleDays = janitor.DefaultStaleDays
		}
		scanner := janitor.New(root, store, searcher, staleDays)
		opts := janitor.ScheduleOptions{
			Interval:    iv,
			Jitter:      60 * time.Second,
			InitialScan: cfg.Janitor.StartupScan,
		}
		janitorSched = janitor.NewScheduler(scanner, hub, ver, opts)
		server.SetJanitorScheduler(janitorSched)
	}

	stack := &Stack{
		Name:         name,
		Root:         root,
		Config:       cfg,
		Store:        store,
		Versioner:    ver,
		Searcher:     searcher,
		Linker:       linker,
		LinkResolver: linkResolver,
		Hub:          hub,
		Pipeline:     pipe,
		Vectors:      vectors,
		Comments:     cstore,
		Server:       server,
		JanitorSched: janitorSched,
		Emitter:      em,
	}

	pipe.DrainUncommitted(context.Background())

	if rs, ok := searcher.(search.Resyncer); ok {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			start := time.Now()
			added, removed, rerr := rs.Resync(ctx)
			if rerr != nil {
				log.Printf("%ssearch: resync failed: %v", prefix, rerr)
				return
			}
			if added == 0 && removed == 0 {
				return
			}
			log.Printf("%ssearch: resync reconciled %d added, %d removed in %s",
				prefix, added, removed, time.Since(start).Round(time.Millisecond))
		}()
	}

	if vectors != nil {
		go stack.reindexIfEmpty()
	}

	return stack, nil
}

func (s *Stack) Close() error {
	var firstErr error
	// Flush async indexer before closing the searcher it writes to.
	if s.Pipeline != nil && s.Pipeline.AsyncIdx != nil {
		if err := s.Pipeline.AsyncIdx.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if c, ok := s.Versioner.(interface{ Close() error }); ok {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.Vectors != nil {
		if err := s.Vectors.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.Searcher != nil {
		if err := s.Searcher.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func buildVersioner(prefix, root string, cfg *config.Config) versioning.Versioner {
	switch cfg.Versioning.Strategy {
	case "git":
		v, err := versioning.NewGit(root)
		if err != nil {
			log.Printf("%sgit versioning unavailable (%v) — running without versioning", prefix, err)
			return versioning.NewNoop()
		}
		asyncEnabled := cfg.Versioning.AsyncCommit == nil || *cfg.Versioning.AsyncCommit
		if asyncEnabled {
			var opts []versioning.AsyncOption
			if cfg.Versioning.BatchWindowMs > 0 {
				opts = append(opts, versioning.WithBatchWindow(time.Duration(cfg.Versioning.BatchWindowMs)*time.Millisecond))
			}
			if cfg.Versioning.BatchMaxSize > 0 {
				opts = append(opts, versioning.WithBatchMaxSize(cfg.Versioning.BatchMaxSize))
			}
			ulog := filepath.Join(root, ".kiwi", "state", "uncommitted.log")
			opts = append(opts, versioning.WithUncommittedLog(ulog))
			log.Printf("%sgit versioning: async commits enabled", prefix)
			return versioning.NewAsyncGit(v, opts...)
		}
		return v
	case "cow":
		cow, err := versioning.NewCow(root)
		if err != nil {
			log.Printf("%scow versioning unavailable (%v) — running without versioning", prefix, err)
			return versioning.NewNoop()
		}
		if cfg.Versioning.MaxVersions != 0 {
			cow.MaxVersions = cfg.Versioning.MaxVersions
		}
		return cow
	default:
		return versioning.NewNoop()
	}
}

func buildSearcher(prefix, root string, store storage.Storage, cfg *config.Config) search.Searcher {
	switch cfg.Search.Engine {
	case "sqlite", "fts5":
		sq, err := search.NewSQLite(root, store, cfg.Dataview.CustomFields)
		if err != nil {
			log.Printf("%ssqlite search unavailable (%v) — falling back to grep", prefix, err)
			return search.NewGrep(root)
		}
		return sq
	default:
		return search.NewGrep(root)
	}
}

func (s *Stack) reindexIfEmpty() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	prefix := logPrefix(s.Name)
	n, err := s.Vectors.Count(ctx)
	if err != nil {
		log.Printf("%svectorstore: count: %v", prefix, err)
		return
	}
	if n > 0 {
		return
	}
	log.Printf("%svectorstore: empty — reindexing in background", prefix)
	start := time.Now()
	count, err := s.Vectors.Reindex(ctx)
	if err != nil {
		log.Printf("%svectorstore: reindex: %v", prefix, err)
		return
	}
	log.Printf("%svectorstore: reindexed %d files in %s",
		prefix, count, time.Since(start).Round(time.Millisecond))
}

func logPrefix(name string) string {
	if name == "" || name == "default" {
		return ""
	}
	return "[" + name + "] "
}
