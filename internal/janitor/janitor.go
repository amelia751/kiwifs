package janitor

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/markdown"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
)

const DefaultStaleDays = 90

const (
	IssueStale         = "stale"
	IssueOrphan        = "orphan"
	IssueDuplicate     = "duplicate"
	IssueContradiction = "contradiction"
	IssueMissingOwner  = "missing-owner"
	IssueMissingStatus = "missing-status"
	IssueEmptyPage     = "empty-page"
	IssueBrokenLink    = "broken-link"
	IssueNoReviewDate  = "no-review-date"
	IssueDecisionFound = "decision-found"
)

type Issue struct {
	Kind       string   `json:"kind"`
	Path       string   `json:"path"`
	Message    string   `json:"message"`
	Related    []string `json:"related,omitempty"`
	Suggestion string   `json:"suggestion,omitempty"`
	Severity   string   `json:"severity"`
}

type ScanResult struct {
	Issues    []Issue `json:"issues"`
	Scanned   int     `json:"scanned"`
	Healthy   int     `json:"healthy"`
	Timestamp string  `json:"timestamp"`
}

// Summary renders a compact human-readable report.
func (r *ScanResult) Summary() string {
	if r == nil {
		return "no scan result"
	}
	var b strings.Builder
	if len(r.Issues) == 0 {
		fmt.Fprintf(&b, "kiwifs janitor: clean — %d pages scanned, all healthy\n", r.Scanned)
		return b.String()
	}
	fmt.Fprintf(&b, "kiwifs janitor: %d issue(s) across %d pages (%d healthy)\n", len(r.Issues), r.Scanned, r.Healthy)
	sort.Slice(r.Issues, func(i, j int) bool {
		if r.Issues[i].Severity != r.Issues[j].Severity {
			return severityRank(r.Issues[i].Severity) > severityRank(r.Issues[j].Severity)
		}
		if r.Issues[i].Kind != r.Issues[j].Kind {
			return r.Issues[i].Kind < r.Issues[j].Kind
		}
		return r.Issues[i].Path < r.Issues[j].Path
	})
	for _, is := range r.Issues {
		fmt.Fprintf(&b, "  [%s] %-16s %s — %s\n", is.Severity, is.Kind, is.Path, is.Message)
		if is.Suggestion != "" {
			fmt.Fprintf(&b, "         suggestion: %s\n", is.Suggestion)
		}
	}
	return b.String()
}

func severityRank(s string) int {
	switch s {
	case "error":
		return 3
	case "warning":
		return 2
	default:
		return 1
	}
}

// HasErrors reports whether any issue has error severity.
func (r *ScanResult) HasErrors() bool {
	for _, is := range r.Issues {
		if is.Severity == "error" {
			return true
		}
	}
	return false
}

type Scanner struct {
	root      string
	store     storage.Storage
	searcher  search.Searcher
	staleDays int
}

func New(root string, store storage.Storage, searcher search.Searcher, staleDays int) *Scanner {
	return &Scanner{
		root:      root,
		store:     store,
		searcher:  searcher,
		staleDays: staleDays,
	}
}

type pageInfo struct {
	path        string
	content     []byte
	frontmatter map[string]any
	bodyText    string
	title       string
	wikiLinks   []string
}

var decisionPatterns = regexp.MustCompile(
	`(?i)(?:we decided|decision:|chose .+ over .+|agreed to|the decision was)`,
)

func (s *Scanner) Scan(ctx context.Context) (*ScanResult, error) {
	pages, err := s.collectPages(ctx)
	if err != nil {
		return nil, fmt.Errorf("janitor: walk: %w", err)
	}

	result := &ScanResult{
		Scanned:   len(pages),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	existingPaths := make(map[string]bool, len(pages))
	for _, p := range pages {
		existingPaths[strings.ToLower(p.path)] = true
		for _, form := range links.TargetForms(p.path) {
			existingPaths[strings.ToLower(form)] = true
		}
	}

	pagesWithIssues := make(map[string]bool)

	for _, p := range pages {
		issues := s.checkPage(ctx, p, existingPaths)
		for _, is := range issues {
			pagesWithIssues[is.Path] = true
		}
		result.Issues = append(result.Issues, issues...)
	}

	result.Issues = append(result.Issues, s.checkOrphans(ctx, pages)...)
	result.Issues = append(result.Issues, s.checkDuplicates(pages)...)
	result.Issues = append(result.Issues, s.checkContradictions(pages)...)

	for _, is := range result.Issues {
		pagesWithIssues[is.Path] = true
	}
	result.Healthy = result.Scanned - len(pagesWithIssues)
	if result.Healthy < 0 {
		result.Healthy = 0
	}

	return result, nil
}

func (s *Scanner) collectPages(ctx context.Context) ([]pageInfo, error) {
	var pages []pageInfo
	err := storage.Walk(ctx, s.store, "/", func(e storage.Entry) error {
		raw, rerr := s.store.Read(ctx, e.Path)
		if rerr != nil {
			return nil
		}
		fm, _ := markdown.Frontmatter(raw)
		body := markdown.BodyAfterFrontmatter(raw)
		title := extractTitle(fm, raw)

		pages = append(pages, pageInfo{
			path:        e.Path,
			content:     raw,
			frontmatter: fm,
			bodyText:    body,
			title:       title,
			wikiLinks:   links.Extract(raw),
		})
		return nil
	})
	return pages, err
}

func (s *Scanner) checkPage(ctx context.Context, p pageInfo, existing map[string]bool) []Issue {
	var issues []Issue

	// Empty page
	if len(strings.TrimSpace(p.bodyText)) < 50 {
		issues = append(issues, Issue{
			Kind:       IssueEmptyPage,
			Path:       p.path,
			Message:    "page has less than 50 characters of content",
			Severity:   "warning",
			Suggestion: "add meaningful content or remove the page",
		})
	}

	// Missing owner
	if _, ok := p.frontmatter["owner"]; !ok {
		issues = append(issues, Issue{
			Kind:       IssueMissingOwner,
			Path:       p.path,
			Message:    "no owner field in frontmatter",
			Severity:   "info",
			Suggestion: "add `owner: <name>` to the YAML frontmatter",
		})
	}

	// Missing status
	if _, ok := p.frontmatter["status"]; !ok {
		issues = append(issues, Issue{
			Kind:       IssueMissingStatus,
			Path:       p.path,
			Message:    "no status field in frontmatter",
			Severity:   "info",
			Suggestion: "add `status: draft|published|archived` to the YAML frontmatter",
		})
	}

	// Stale detection
	issues = append(issues, s.checkStale(p)...)

	// No review date (has owner but no next-review)
	if _, hasOwner := p.frontmatter["owner"]; hasOwner {
		if _, hasReview := p.frontmatter["next-review"]; !hasReview {
			issues = append(issues, Issue{
				Kind:       IssueNoReviewDate,
				Path:       p.path,
				Message:    "has an owner but no next-review date",
				Severity:   "info",
				Suggestion: "add `next-review: YYYY-MM-DD` to the YAML frontmatter",
			})
		}
	}

	// Broken links
	for _, target := range links.Unique(p.wikiLinks) {
		if !existing[strings.ToLower(target)] {
			issues = append(issues, Issue{
				Kind:     IssueBrokenLink,
				Path:     p.path,
				Message:  fmt.Sprintf("[[%s]] doesn't resolve to any file", target),
				Related:  []string{target},
				Severity: "error",
			})
		}
	}

	// Decision language in non-decisions path
	if !strings.HasPrefix(p.path, "decisions/") && decisionPatterns.MatchString(p.bodyText) {
		issues = append(issues, Issue{
			Kind:       IssueDecisionFound,
			Path:       p.path,
			Message:    "contains decision language but is not in decisions/",
			Severity:   "info",
			Suggestion: "consider extracting the decision into decisions/ using the decision template",
		})
	}

	return issues
}

func (s *Scanner) checkStale(p pageInfo) []Issue {
	now := time.Now()
	var issues []Issue

	reviewed, ok := fmDateField(p.frontmatter, "reviewed")
	if !ok {
		reviewed, ok = fmDateField(p.frontmatter, "last-reviewed")
	}
	if ok {
		if now.Sub(reviewed).Hours()/24 > float64(s.staleDays) {
			issues = append(issues, Issue{
				Kind:       IssueStale,
				Path:       p.path,
				Message:    fmt.Sprintf("last reviewed %s (%d+ days ago)", reviewed.Format("2006-01-02"), s.staleDays),
				Severity:   "warning",
				Suggestion: "review the page and update the `reviewed` date",
			})
		}
	}

	if nextReview, ok := fmDateField(p.frontmatter, "next-review"); ok {
		if now.After(nextReview) {
			issues = append(issues, Issue{
				Kind:       IssueStale,
				Path:       p.path,
				Message:    fmt.Sprintf("next-review date %s is in the past", nextReview.Format("2006-01-02")),
				Severity:   "warning",
				Suggestion: "review the page and set a new next-review date",
			})
		}
	}

	return issues
}

func (s *Scanner) checkOrphans(ctx context.Context, pages []pageInfo) []Issue {
	linker, ok := s.searcher.(links.Linker)
	if !ok {
		return nil
	}

	var issues []Issue
	for _, p := range pages {
		if p.path == "index.md" || p.path == "SCHEMA.md" || p.path == "log.md" {
			continue
		}
		entries, err := linker.Backlinks(ctx, p.path)
		if err != nil {
			continue
		}
		if len(entries) == 0 {
			issues = append(issues, Issue{
				Kind:       IssueOrphan,
				Path:       p.path,
				Message:    "no inbound wiki links point to this page",
				Severity:   "info",
				Suggestion: "link to this page from index.md or a related page",
			})
		}
	}
	return issues
}

func (s *Scanner) checkDuplicates(pages []pageInfo) []Issue {
	type titleEntry struct {
		path  string
		title string
	}
	var titled []titleEntry
	for _, p := range pages {
		if p.title != "" {
			titled = append(titled, titleEntry{path: p.path, title: strings.ToLower(p.title)})
		}
	}

	var issues []Issue
	seen := make(map[string]bool)
	for i := 0; i < len(titled); i++ {
		for j := i + 1; j < len(titled); j++ {
			if titled[i].title == titled[j].title {
				key := titled[i].path + "~" + titled[j].path
				if seen[key] {
					continue
				}
				seen[key] = true
				issues = append(issues, Issue{
					Kind:       IssueDuplicate,
					Path:       titled[i].path,
					Message:    fmt.Sprintf("identical title %q also found in %s", titled[i].title, titled[j].path),
					Related:    []string{titled[j].path},
					Severity:   "warning",
					Suggestion: "merge or disambiguate these pages",
				})
			}
		}
	}
	return issues
}

func (s *Scanner) checkContradictions(pages []pageInfo) []Issue {
	type sotPage struct {
		path string
		tags []string
	}
	var sotPages []sotPage
	for _, p := range pages {
		if sot, ok := p.frontmatter["source-of-truth"]; ok {
			if b, ok := sot.(bool); ok && b {
				tags := extractTags(p.frontmatter)
				sotPages = append(sotPages, sotPage{path: p.path, tags: tags})
			}
		}
	}

	var issues []Issue
	for i := 0; i < len(sotPages); i++ {
		for j := i + 1; j < len(sotPages); j++ {
			if overlaps := tagOverlap(sotPages[i].tags, sotPages[j].tags); len(overlaps) > 0 {
				issues = append(issues, Issue{
					Kind:       IssueContradiction,
					Path:       sotPages[i].path,
					Message:    fmt.Sprintf("both claim source-of-truth with overlapping tags %v; also: %s", overlaps, sotPages[j].path),
					Related:    []string{sotPages[j].path},
					Severity:   "error",
					Suggestion: "resolve which page is the authoritative source for these topics",
				})
			}
		}
	}
	return issues
}

func extractTitle(fm map[string]any, content []byte) string {
	if t, ok := fm["title"]; ok {
		if s, ok := t.(string); ok && s != "" {
			return s
		}
	}
	parsed, err := markdown.Parse(content)
	if err != nil {
		return ""
	}
	for _, h := range parsed.Headings {
		if h.Level == 1 {
			return h.Text
		}
	}
	return ""
}

func extractTags(fm map[string]any) []string {
	val, ok := fm["tags"]
	if !ok {
		return nil
	}
	switch v := val.(type) {
	case []any:
		tags := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				tags = append(tags, strings.ToLower(s))
			}
		}
		return tags
	case string:
		if v != "" {
			return []string{strings.ToLower(v)}
		}
	}
	return nil
}

func tagOverlap(a, b []string) []string {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	set := make(map[string]bool, len(a))
	for _, t := range a {
		set[t] = true
	}
	var overlap []string
	for _, t := range b {
		if set[t] {
			overlap = append(overlap, t)
		}
	}
	return overlap
}

func fmDateField(fm map[string]any, key string) (time.Time, bool) {
	val, ok := fm[key]
	if !ok {
		return time.Time{}, false
	}
	switch v := val.(type) {
	case string:
		for _, layout := range []string{"2006-01-02", time.RFC3339, "2006-01-02T15:04:05Z"} {
			if t, err := time.Parse(layout, v); err == nil {
				return t, true
			}
		}
	case time.Time:
		return v, true
	}
	return time.Time{}, false
}
