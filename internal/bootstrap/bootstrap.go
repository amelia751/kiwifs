// Package bootstrap assembles the KiwiFS dependency graph for a single
// knowledge space. Both the top-level serve command and the multi-space
// Manager route through Build so error-handling policy ("git unavailable
// → degrade to Noop"), component ordering, and teardown stay consistent.
package bootstrap

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kiwifs/kiwifs/internal/api"
	"github.com/kiwifs/kiwifs/internal/comments"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/vectorstore"
	"github.com/kiwifs/kiwifs/internal/versioning"
)

// Stack is the set of live components backing one knowledge space.
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
	Vectors      *vectorstore.Service // nil when disabled or build failed
	Comments     *comments.Store
	Server       *api.Server
}

// Build assembles every dependency for one space. name is used as a log
// prefix so multi-space deployments can tell which space emitted what;
// use "default" (or "") for the single-space case to keep logs unprefixed.
//
// Fatal failures (storage, comments store) return an error. Soft failures
// (git unavailable, sqlite FTS unavailable, vector store unreachable) log
// a warning and degrade — the server still starts.
func Build(name, root string, cfg *config.Config) (*Stack, error) {
	prefix := logPrefix(name)

	store, err := storage.NewLocal(root)
	if err != nil {
		return nil, fmt.Errorf("%sstorage init: %w", prefix, err)
	}

	ver := buildVersioner(prefix, root, cfg)
	searcher := buildSearcher(prefix, root, store, cfg)

	// Vector search is optional. Build returns (nil, nil) when
	// [search.vector] is disabled; only a configuration error surfaces
	// here and it's non-fatal — the /search/semantic endpoint will 503.
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

	pipe.OnInvalidate = func() { linkResolver.MarkDirty() }

	cstore, err := comments.New(root)
	if err != nil {
		// Unwind anything that was already opened so the caller doesn't
		// have to care about partial construction.
		if vectors != nil {
			_ = vectors.Close()
		}
		_ = searcher.Close()
		return nil, fmt.Errorf("%scomments store: %w", prefix, err)
	}

	server := api.NewServer(cfg, pipe, vectors, cstore, linkResolver)

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
	}

	pipe.DrainUncommitted(context.Background())

	if vectors != nil {
		go stack.reindexIfEmpty()
	}

	return stack, nil
}

// Close tears down components in reverse dependency order. It returns the
// first error but still attempts to close every component so a partially
// broken stack doesn't leak file handles or sqlite connections.
func (s *Stack) Close() error {
	var firstErr error
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
		sq, err := search.NewSQLite(root, store)
		if err != nil {
			log.Printf("%ssqlite search unavailable (%v) — falling back to grep", prefix, err)
			return search.NewGrep(root)
		}
		return sq
	default:
		return search.NewGrep(root)
	}
}

// reindexIfEmpty mirrors the FTS bootstrap: on fresh deploys the vector
// store is empty, so kick off one background reindex so semantic search
// works without a manual reindex step. Fire-and-forget — the 10-minute
// timeout caps runaway embeddings and failures stay in the log.
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
