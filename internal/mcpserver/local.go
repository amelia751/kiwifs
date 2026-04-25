package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/kiwifs/kiwifs/internal/bootstrap"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/dataview"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/vectorstore"
)

type LocalBackend struct {
	root   string
	stack  *bootstrap.Stack
	dvExec *dataview.Executor

	once sync.Once
	err  error
}

func NewLocalBackend(root string) *LocalBackend {
	return &LocalBackend{root: root}
}

func (b *LocalBackend) init() error {
	b.once.Do(func() {
		abs, err := filepath.Abs(b.root)
		if err != nil {
			b.err = fmt.Errorf("resolve root: %w", err)
			return
		}
		b.root = abs

		cfgPath := filepath.Join(abs, ".kiwi", "config.toml")
		var cfg *config.Config
		if _, serr := os.Stat(cfgPath); serr == nil {
			cfg, _ = config.Load(abs)
		}
		if cfg == nil {
			cfg = &config.Config{}
		}
		cfg.Storage.Root = abs

		stack, err := bootstrap.Build("mcp", abs, cfg)
		if err != nil {
			b.err = fmt.Errorf("bootstrap: %w", err)
			return
		}
		b.stack = stack

		if sq, ok := b.stack.Searcher.(*search.SQLite); ok {
			b.dvExec = dataview.NewExecutor(sq.ReadDB())
			timeout := 5 * time.Second
			maxRows := 10000
			if t, err := time.ParseDuration(cfg.Dataview.QueryTimeout); err == nil && t > 0 {
				timeout = t
			}
			if cfg.Dataview.MaxScanRows > 0 {
				maxRows = cfg.Dataview.MaxScanRows
			}
			b.dvExec.SetLimits(maxRows, timeout)
		}
	})
	return b.err
}

func (b *LocalBackend) ReadFile(ctx context.Context, path string) (string, string, error) {
	if err := b.init(); err != nil {
		return "", "", err
	}
	content, err := b.stack.Store.Read(ctx, path)
	if err != nil {
		return "", "", err
	}
	etag := pipeline.ETag(content)
	return string(content), etag, nil
}

func (b *LocalBackend) WriteFile(ctx context.Context, path, content, actor, provenance string) (string, error) {
	if err := b.init(); err != nil {
		return "", err
	}
	body := []byte(content)
	if provType, provID, ok := pipeline.ParseProvenanceHeader(provenance); ok {
		injected, perr := pipeline.InjectProvenance(body, provType, provID, actor)
		if perr != nil {
			return "", fmt.Errorf("provenance: %w", perr)
		}
		body = injected
	}
	res, err := b.stack.Pipeline.Write(ctx, path, body, actor)
	if err != nil {
		return "", err
	}
	return res.ETag, nil
}

func (b *LocalBackend) DeleteFile(ctx context.Context, path, actor string) error {
	if err := b.init(); err != nil {
		return err
	}
	return b.stack.Pipeline.Delete(ctx, path, actor)
}

func (b *LocalBackend) Tree(ctx context.Context, path string) (json.RawMessage, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	tree, err := storage.BuildTree(ctx, b.stack.Store, path, 10)
	if err != nil {
		return nil, err
	}
	return json.Marshal(tree)
}

func (b *LocalBackend) Search(ctx context.Context, query string, limit, offset int, pathPrefix string) ([]SearchResult, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	results, err := b.stack.Searcher.Search(ctx, query, limit, offset, pathPrefix)
	if err != nil {
		return nil, err
	}
	out := make([]SearchResult, len(results))
	for i, r := range results {
		snippet := r.Snippet
		snippet = stripMarkTags(snippet)
		out[i] = SearchResult{
			Path:    r.Path,
			Snippet: snippet,
			Score:   r.Score,
		}
	}
	return out, nil
}

var markTagRe = regexp.MustCompile(`</?mark>`)

func stripMarkTags(s string) string {
	return markTagRe.ReplaceAllString(s, "")
}

func (b *LocalBackend) SearchSemantic(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	if b.stack.Vectors == nil {
		return nil, fmt.Errorf("semantic search is not enabled")
	}
	if limit <= 0 {
		limit = vectorstore.DefaultTopK
	}
	results, err := b.stack.Vectors.Search(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	out := make([]SearchResult, len(results))
	for i, r := range results {
		out[i] = SearchResult{
			Path:    r.Path,
			Snippet: r.Snippet,
			Score:   r.Score,
		}
	}
	return out, nil
}

type metaQuerier interface {
	QueryMeta(ctx context.Context, filters []search.MetaFilter, sort, order string, limit, offset int) ([]search.MetaResult, error)
}

func (b *LocalBackend) QueryMeta(ctx context.Context, filters []string, sort, order string, limit, offset int) ([]MetaResult, error) {
	return b.QueryMetaOr(ctx, filters, nil, sort, order, limit, offset)
}

type orMetaQuerier interface {
	QueryMetaOr(ctx context.Context, andFilters, orFilters []search.MetaFilter, sort, order string, limit, offset int) ([]search.MetaResult, error)
}

func (b *LocalBackend) QueryMetaOr(ctx context.Context, andFilters, orFilters []string, sort, order string, limit, offset int) ([]MetaResult, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	mq, ok := b.stack.Searcher.(orMetaQuerier)
	if !ok {
		return nil, fmt.Errorf("metadata index requires sqlite search backend")
	}

	parsedAnd := make([]search.MetaFilter, 0, len(andFilters))
	for _, raw := range andFilters {
		f, err := search.ParseMetaFilter(raw)
		if err != nil {
			return nil, err
		}
		parsedAnd = append(parsedAnd, f)
	}

	parsedOr := make([]search.MetaFilter, 0, len(orFilters))
	for _, raw := range orFilters {
		f, err := search.ParseMetaFilter(raw)
		if err != nil {
			return nil, err
		}
		parsedOr = append(parsedOr, f)
	}

	results, err := mq.QueryMetaOr(ctx, parsedAnd, parsedOr, sort, order, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]MetaResult, len(results))
	for i, r := range results {
		fm, _ := json.Marshal(r.Frontmatter)
		out[i] = MetaResult{Path: r.Path, Frontmatter: fm}
	}
	return out, nil
}

func (b *LocalBackend) ViewRefresh(ctx context.Context, path string) (bool, error) {
	if err := b.init(); err != nil {
		return false, err
	}
	if b.dvExec == nil {
		return false, fmt.Errorf("view refresh requires sqlite search backend")
	}
	return dataview.RegenerateView(ctx, b.stack.Store, b.dvExec, path)
}

func (b *LocalBackend) QueryDQL(ctx context.Context, dql string, limit, offset int) (*QueryResult, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	if b.dvExec == nil {
		return nil, fmt.Errorf("dataview requires sqlite search backend")
	}
	result, err := b.dvExec.Query(ctx, dql, limit, offset)
	if err != nil {
		return nil, err
	}
	qr := &QueryResult{
		Columns: result.Columns,
		Rows:    result.Rows,
		Total:   result.Total,
		HasMore: result.HasMore,
	}
	for _, g := range result.Groups {
		qr.Groups = append(qr.Groups, GroupResult{Key: g.Key, Count: g.Count})
	}
	return qr, nil
}

func (b *LocalBackend) Versions(ctx context.Context, path string) ([]Version, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	vers, err := b.stack.Versioner.Log(ctx, path)
	if err != nil {
		return nil, err
	}
	out := make([]Version, len(vers))
	for i, v := range vers {
		out[i] = Version{Hash: v.Hash, Date: v.Date, Author: v.Author, Message: v.Message}
	}
	return out, nil
}

func (b *LocalBackend) BulkWrite(ctx context.Context, files []BulkFile, actor, provenance string) (map[string]string, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	pipeFiles := make([]struct {
		Path    string
		Content []byte
	}, len(files))
	for i, f := range files {
		body := []byte(f.Content)
		if provType, provID, ok := pipeline.ParseProvenanceHeader(provenance); ok {
			injected, perr := pipeline.InjectProvenance(body, provType, provID, actor)
			if perr != nil {
				return nil, fmt.Errorf("provenance on %s: %w", f.Path, perr)
			}
			body = injected
		}
		pipeFiles[i].Path = f.Path
		pipeFiles[i].Content = body
	}
	results, err := b.stack.Pipeline.BulkWrite(ctx, pipeFiles, actor, "")
	if err != nil {
		return nil, err
	}
	etags := make(map[string]string, len(results))
	for _, r := range results {
		etags[r.Path] = r.ETag
	}
	return etags, nil
}

func (b *LocalBackend) Backlinks(ctx context.Context, path string) ([]Backlink, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	if b.stack.Linker == nil {
		return []Backlink{}, nil
	}
	entries, err := b.stack.Linker.Backlinks(ctx, path)
	if err != nil {
		return nil, err
	}
	out := make([]Backlink, len(entries))
	for i, e := range entries {
		out[i] = Backlink{Path: e.Path, Count: e.Count}
	}
	return out, nil
}

func (b *LocalBackend) PublicURL() string {
	if b.stack == nil {
		return ""
	}
	return b.stack.Config.ResolvedPublicURL()
}

func (b *LocalBackend) ResolveWikiLinks(ctx context.Context, content string) string {
	if b.stack == nil || b.stack.LinkResolver == nil {
		return content
	}
	publicURL := b.stack.Config.ResolvedPublicURL()
	return b.stack.LinkResolver.Resolve(ctx, content, publicURL)
}

func (b *LocalBackend) Health(_ context.Context) error {
	return b.init()
}

func (b *LocalBackend) Close() error {
	if b.stack != nil {
		return b.stack.Close()
	}
	return nil
}

func formatSize(bytes int64) string {
	switch {
	case bytes >= 1024*1024:
		return fmt.Sprintf("%.1f MB", float64(bytes)/1024/1024)
	case bytes >= 1024:
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
