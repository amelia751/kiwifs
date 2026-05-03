package mcpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kiwifs/kiwifs/internal/bootstrap"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/dataview"
	"github.com/kiwifs/kiwifs/internal/janitor"
	"github.com/kiwifs/kiwifs/internal/memory"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/tracing"
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

func (b *LocalBackend) Changes(ctx context.Context, since string, limit int) (*ChangesResult, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	if since != "" {
		for _, c := range since {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return nil, fmt.Errorf("invalid since: must be a hex commit hash")
			}
		}
		if len(since) < 4 || len(since) > 40 {
			return nil, fmt.Errorf("invalid since: must be 4–40 hex characters")
		}
	}

	var args []string
	if since != "" {
		args = []string{"log", "--format=%H|%an|%at|%s", fmt.Sprintf("%s..HEAD", since), fmt.Sprintf("-%d", limit)}
	} else {
		args = []string{"log", "--format=%H|%an|%at|%s", fmt.Sprintf("-%d", limit)}
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = b.root
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr := string(exitErr.Stderr)
			if strings.Contains(stderr, "unknown revision") {
				return nil, fmt.Errorf("unknown sequence")
			}
			if strings.Contains(stderr, "does not have any commits") {
				return &ChangesResult{Changes: []Change{}, LastSeq: ""}, nil
			}
		}
		return nil, fmt.Errorf("git log: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	changes := make([]Change, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 4)
		if len(parts) < 4 {
			continue
		}
		hash, author, tsStr, subject := parts[0], parts[1], parts[2], parts[3]
		ts, _ := strconv.ParseInt(tsStr, 10, 64)
		action, path := parseLocalCommitSubject(subject)
		changes = append(changes, Change{
			Seq:       hash,
			Path:      path,
			Action:    action,
			Actor:     author,
			Timestamp: time.Unix(ts, 0).UTC().Format(time.RFC3339),
		})
	}

	lastSeq := ""
	if len(changes) > 0 {
		lastSeq = changes[0].Seq
	}
	return &ChangesResult{Changes: changes, LastSeq: lastSeq}, nil
}

func parseLocalCommitSubject(subject string) (action, path string) {
	subject = strings.TrimSpace(subject)
	if idx := strings.Index(subject, ": "); idx >= 0 {
		subject = subject[idx+2:]
	}
	subject = strings.TrimSpace(subject)
	parts := strings.SplitN(subject, " ", 2)
	if len(parts) == 2 {
		act := strings.ToLower(parts[0])
		path = strings.TrimSpace(parts[1])
		switch act {
		case "write", "create", "update":
			action = "write"
		case "delete", "remove":
			action = "delete"
		case "rename", "move":
			action = "rename"
			if idx := strings.Index(path, " → "); idx >= 0 {
				path = strings.TrimSpace(path[idx+len(" → "):])
			}
		case "bulk":
			action = "write"
			path = ""
		default:
			action = "write"
		}
		return action, path
	}
	return "write", subject
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
	tracing.Record(ctx, tracing.Event{Kind: tracing.KindRead, Path: path, ETag: etag})
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
	tracing.Record(ctx, tracing.Event{Kind: tracing.KindWrite, Path: path, ETag: res.ETag})
	return res.ETag, nil
}

func (b *LocalBackend) DeleteFile(ctx context.Context, path, actor string) error {
	if err := b.init(); err != nil {
		return err
	}
	err := b.stack.Pipeline.Delete(ctx, path, actor)
	if err == nil {
		tracing.Record(ctx, tracing.Event{Kind: tracing.KindDelete, Path: path})
	}
	return err
}

func (b *LocalBackend) Append(ctx context.Context, path, content, separator, actor string) (string, error) {
	if err := b.init(); err != nil {
		return "", err
	}
	if actor == "" {
		actor = "mcp-agent"
	}
	res, err := b.stack.Pipeline.Append(ctx, path, content, separator, actor)
	if err != nil {
		return "", err
	}
	return res.ETag, nil
}

func (b *LocalBackend) Rename(ctx context.Context, from, to, actor string) (string, error) {
	if err := b.init(); err != nil {
		return "", err
	}
	res, err := b.stack.Pipeline.Rename(ctx, from, to, actor)
	if err != nil {
		return "", err
	}
	return res.ETag, nil
}

func (b *LocalBackend) RenameWithLinks(ctx context.Context, from, to, actor string, updateLinks bool) (string, []string, error) {
	if err := b.init(); err != nil {
		return "", nil, err
	}
	res, updated, err := b.stack.Pipeline.RenameWithLinks(ctx, from, to, actor, updateLinks)
	if err != nil {
		return "", nil, err
	}
	return res.ETag, updated, nil
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
	tracing.Record(ctx, tracing.Event{Kind: tracing.KindSearch, Query: query, HitCount: len(out)})
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

func (b *LocalBackend) QueryMetaOr(ctx context.Context, andFilters, orFilters []string, sort, order string, limit, offset int, paths ...string) ([]MetaResult, error) {
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

	if len(paths) > 0 {
		return b.queryMetaByPaths(ctx, paths)
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

func (b *LocalBackend) queryMetaByPaths(ctx context.Context, paths []string) ([]MetaResult, error) {
	sq, ok := b.stack.Searcher.(*search.SQLite)
	if !ok {
		return nil, fmt.Errorf("paths filter requires sqlite search backend")
	}
	placeholders := make([]string, len(paths))
	args := make([]any, len(paths))
	for i, p := range paths {
		placeholders[i] = "?"
		args[i] = p
	}
	query := fmt.Sprintf("SELECT path, frontmatter FROM file_meta WHERE path IN (%s)", strings.Join(placeholders, ","))
	rows, err := sq.ReadDB().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MetaResult
	for rows.Next() {
		var path, fmStr string
		if err := rows.Scan(&path, &fmStr); err != nil {
			return nil, err
		}
		out = append(out, MetaResult{Path: path, Frontmatter: json.RawMessage(fmStr)})
	}
	if out == nil {
		out = []MetaResult{}
	}
	return out, rows.Err()
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
	tracing.Record(ctx, tracing.Event{Kind: tracing.KindDQL, Query: dql, HitCount: result.Total})
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
	tracing.Record(ctx, tracing.Event{Kind: tracing.KindVersions, Path: path, HitCount: len(out)})
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

func (b *LocalBackend) Aggregate(ctx context.Context, groupBy, calc, where, pathPrefix string) (map[string]map[string]any, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	sq, ok := b.stack.Searcher.(*search.SQLite)
	if !ok {
		return nil, fmt.Errorf("aggregate requires sqlite search backend")
	}

	calcs, err := parseCalcSpecsLocal(calc)
	if err != nil {
		return nil, err
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("SELECT json_extract(frontmatter, '$.%s') AS grp", groupBy))
	for _, cs := range calcs {
		switch cs.fn {
		case "count":
			sb.WriteString(", COUNT(*) AS agg_count")
		case "avg":
			sb.WriteString(fmt.Sprintf(", AVG(json_extract(frontmatter, '$.%s'))", cs.field))
		case "sum":
			sb.WriteString(fmt.Sprintf(", SUM(json_extract(frontmatter, '$.%s'))", cs.field))
		case "min":
			sb.WriteString(fmt.Sprintf(", MIN(json_extract(frontmatter, '$.%s'))", cs.field))
		case "max":
			sb.WriteString(fmt.Sprintf(", MAX(json_extract(frontmatter, '$.%s'))", cs.field))
		}
	}
	sb.WriteString(" FROM file_meta")

	var conditions []string
	var args []any
	if pathPrefix != "" {
		conditions = append(conditions, "path LIKE ? || '%'")
		args = append(args, pathPrefix)
	}
	if where != "" {
		conditions = append(conditions, where)
	}
	if len(conditions) > 0 {
		sb.WriteString(" WHERE " + strings.Join(conditions, " AND "))
	}
	sb.WriteString(fmt.Sprintf(" GROUP BY json_extract(frontmatter, '$.%s')", groupBy))

	rows, err := sq.ReadDB().QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	groups := make(map[string]map[string]any)
	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		key := fmt.Sprint(vals[0])
		if key == "<nil>" {
			key = "(none)"
		}
		bucket := make(map[string]any)
		for i, cs := range calcs {
			bucket[cs.label()] = vals[i+1]
		}
		groups[key] = bucket
	}
	return groups, rows.Err()
}

type localCalcSpec struct {
	fn    string
	field string
}

func (cs localCalcSpec) label() string {
	if cs.field == "" {
		return cs.fn
	}
	return cs.fn + ":" + cs.field
}

func parseCalcSpecsLocal(raw string) ([]localCalcSpec, error) {
	if raw == "" {
		return []localCalcSpec{{fn: "count"}}, nil
	}
	parts := strings.Split(raw, ",")
	specs := make([]localCalcSpec, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if p == "count" {
			specs = append(specs, localCalcSpec{fn: "count"})
			continue
		}
		fn, field, ok := strings.Cut(p, ":")
		if !ok || field == "" {
			return nil, fmt.Errorf("invalid calc %q", p)
		}
		specs = append(specs, localCalcSpec{fn: fn, field: field})
	}
	if len(specs) == 0 {
		return []localCalcSpec{{fn: "count"}}, nil
	}
	return specs, nil
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
	resolved := b.stack.LinkResolver.Resolve(ctx, content, publicURL)
	tracing.Record(ctx, tracing.Event{Kind: tracing.KindLinkResolve, Detail: "wiki-links resolved"})
	return resolved
}

func (b *LocalBackend) Analytics(ctx context.Context, scope string, staleThreshold int) (json.RawMessage, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	sq, ok := b.stack.Searcher.(*search.SQLite)
	if !ok {
		return nil, fmt.Errorf("analytics requires sqlite search backend")
	}
	resp, err := buildLocalAnalytics(ctx, sq, b.stack.JanitorSched, scope, staleThreshold)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp)
}

func (b *LocalBackend) MemoryReport(ctx context.Context, episodesPrefix string) (json.RawMessage, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	opt := memory.Options{}
	if episodesPrefix != "" {
		opt.EpisodesPathPrefix = episodesPrefix
	} else if b.stack.Config != nil && b.stack.Config.Memory.EpisodesPathPrefix != "" {
		opt.EpisodesPathPrefix = b.stack.Config.Memory.EpisodesPathPrefix
	}
	rep, err := memory.Scan(ctx, b.stack.Store, opt)
	if err != nil {
		return nil, err
	}
	return json.Marshal(rep)
}

func (b *LocalBackend) HealthCheckPage(ctx context.Context, path string) (json.RawMessage, error) {
	if err := b.init(); err != nil {
		return nil, err
	}
	sq, ok := b.stack.Searcher.(*search.SQLite)
	if !ok {
		return nil, fmt.Errorf("health check requires sqlite search backend")
	}
	resp, err := buildLocalHealthCheck(ctx, sq, b.stack.JanitorSched, path)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp)
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

type localAnalytics struct {
	TotalPages int                `json:"total_pages"`
	TotalWords int                `json:"total_words"`
	Health     localHealthStats   `json:"health"`
	Coverage   localCoverageStats `json:"coverage"`
	TopUpdated []localPageStat    `json:"top_updated"`
}

type localIssueGroup struct {
	Count int      `json:"count"`
	Paths []string `json:"paths,omitempty"`
}

type localHealthStats struct {
	Stale         localIssueGroup `json:"stale"`
	Orphans       localIssueGroup `json:"orphans"`
	BrokenLinks   localIssueGroup `json:"broken_links"`
	Empty         localIssueGroup `json:"empty"`
	NoFrontmatter localIssueGroup `json:"no_frontmatter"`
}

type localCoverageStats struct {
	PagesWithLinks    int     `json:"pages_with_links"`
	PagesWithoutLinks int     `json:"pages_without_links"`
	AvgLinksPerPage   float64 `json:"avg_links_per_page"`
}

type localPageStat struct {
	Path      string `json:"path"`
	UpdatedAt string `json:"updated_at"`
}

func buildLocalAnalytics(ctx context.Context, sq *search.SQLite, sched *janitor.Scheduler, scope string, staleThreshold int) (*localAnalytics, error) {
	db := sq.ReadDB()
	resp := &localAnalytics{}

	scopeSQL := ""
	var scopeArgs []any
	if scope != "" {
		scopeSQL = " WHERE path LIKE ? || '%'"
		scopeArgs = append(scopeArgs, scope)
	}

	var totalWordsNull *float64
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), SUM(json_extract(frontmatter, '$._word_count')) FROM file_meta`+scopeSQL,
		scopeArgs...,
	).Scan(&resp.TotalPages, &totalWordsNull)
	if err != nil {
		return nil, err
	}
	if totalWordsNull != nil {
		resp.TotalWords = int(*totalWordsNull)
	}

	if sd, ok := interface{}(sq).(search.StaleDetector); ok {
		stale, serr := sd.StalePages(ctx, staleThreshold)
		if serr == nil {
			for _, s := range stale {
				if scope == "" || localHasPrefix(s.Path, scope) {
					resp.Health.Stale.Count++
					resp.Health.Stale.Paths = append(resp.Health.Stale.Paths, s.Path)
				}
			}
		}
	}

	if sched != nil {
		if scan := sched.LastResult(); scan != nil {
			for _, issue := range scan.Issues {
				if scope != "" && !localHasPrefix(issue.Path, scope) {
					continue
				}
				switch issue.Kind {
				case janitor.IssueOrphan:
					resp.Health.Orphans.Count++
					resp.Health.Orphans.Paths = append(resp.Health.Orphans.Paths, issue.Path)
				case janitor.IssueBrokenLink:
					resp.Health.BrokenLinks.Count++
					resp.Health.BrokenLinks.Paths = append(resp.Health.BrokenLinks.Paths, issue.Path)
				case janitor.IssueEmptyPage:
					resp.Health.Empty.Count++
					resp.Health.Empty.Paths = append(resp.Health.Empty.Paths, issue.Path)
				}
			}
		}
	}

	nfSQL := `SELECT COUNT(*) FROM file_meta WHERE json_extract(frontmatter, '$._has_frontmatter') = 0 OR json_extract(frontmatter, '$._has_frontmatter') IS NULL`
	if scope != "" {
		nfSQL += ` AND path LIKE ? || '%'`
	}
	var nfCount int
	if scope != "" {
		_ = db.QueryRowContext(ctx, nfSQL, scope).Scan(&nfCount)
	} else {
		_ = db.QueryRowContext(ctx, nfSQL).Scan(&nfCount)
	}
	resp.Health.NoFrontmatter = localIssueGroup{Count: nfCount}

	buildLocalCoverage(ctx, db, scopeSQL, scopeArgs, resp)

	topSQL := `SELECT path, updated_at FROM file_meta` + scopeSQL + ` ORDER BY updated_at DESC LIMIT 10`
	rows, err := db.QueryContext(ctx, topSQL, scopeArgs...)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var path, updatedAt string
			if rows.Scan(&path, &updatedAt) == nil {
				resp.TopUpdated = append(resp.TopUpdated, localPageStat{Path: path, UpdatedAt: updatedAt})
			}
		}
	}

	if resp.TopUpdated == nil {
		resp.TopUpdated = []localPageStat{}
	}
	if resp.Health.Stale.Paths == nil {
		resp.Health.Stale.Paths = []string{}
	}
	if resp.Health.Orphans.Paths == nil {
		resp.Health.Orphans.Paths = []string{}
	}
	if resp.Health.BrokenLinks.Paths == nil {
		resp.Health.BrokenLinks.Paths = []string{}
	}
	if resp.Health.Empty.Paths == nil {
		resp.Health.Empty.Paths = []string{}
	}
	return resp, nil
}

func buildLocalCoverage(ctx context.Context, db *sql.DB, scopeSQL string, scopeArgs []any, resp *localAnalytics) {
	row := db.QueryRowContext(ctx,
		`SELECT
			COUNT(CASE WHEN COALESCE(json_extract(frontmatter, '$._link_count'), 0) > 0 THEN 1 END),
			COUNT(CASE WHEN COALESCE(json_extract(frontmatter, '$._link_count'), 0) = 0 THEN 1 END),
			COALESCE(AVG(json_extract(frontmatter, '$._link_count')), 0)
		FROM file_meta`+scopeSQL,
		scopeArgs...,
	)
	_ = row.Scan(&resp.Coverage.PagesWithLinks, &resp.Coverage.PagesWithoutLinks, &resp.Coverage.AvgLinksPerPage)
}

func localHasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

type localHealthCheck struct {
	Path            string   `json:"path"`
	WordCount       int      `json:"word_count"`
	LinkCount       int      `json:"link_count"`
	BacklinkCount   int      `json:"backlink_count"`
	DaysSinceUpdate float64  `json:"days_since_update"`
	QualityScore    *float64 `json:"quality_score,omitempty"`
	Issues          []string `json:"issues"`
}

func buildLocalHealthCheck(ctx context.Context, sq *search.SQLite, sched *janitor.Scheduler, path string) (*localHealthCheck, error) {
	db := sq.ReadDB()
	resp := &localHealthCheck{Path: path, Issues: []string{}}

	var fm string
	var updatedAt string
	err := db.QueryRowContext(ctx,
		`SELECT frontmatter, updated_at FROM file_meta WHERE path = ?`, path,
	).Scan(&fm, &updatedAt)
	if err != nil {
		return resp, nil
	}

	var parsed map[string]any
	if json.Unmarshal([]byte(fm), &parsed) == nil {
		if v, ok := parsed["_word_count"]; ok {
			resp.WordCount = localToInt(v)
		}
		if v, ok := parsed["_link_count"]; ok {
			resp.LinkCount = localToInt(v)
		}
		if v, ok := parsed["_backlink_count"]; ok {
			resp.BacklinkCount = localToInt(v)
		}
		if v, ok := parsed["quality_score"]; ok {
			f := localToFloat64(v)
			resp.QualityScore = &f
		}
	}

	if updatedAt != "" {
		if t, err := time.Parse(time.RFC3339, updatedAt); err == nil {
			resp.DaysSinceUpdate = time.Since(t).Hours() / 24
		}
	}

	if sched != nil {
		if scan := sched.LastResult(); scan != nil {
			for _, issue := range scan.Issues {
				if issue.Path == path {
					resp.Issues = append(resp.Issues, issue.Kind+": "+issue.Message)
				}
			}
		}
	}
	return resp, nil
}

func localToInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	}
	return 0
}

func localToFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
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
