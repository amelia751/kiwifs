package importer

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kiwifs/kiwifs/internal/pipeline"
	"gopkg.in/yaml.v3"
)

// Record is one row/document from a data source, ready to be written as a
// markdown file in the knowledge base.
type Record struct {
	SourceID   string
	SourceDSN  string
	Table      string
	Fields     map[string]any
	PrimaryKey string
}

// Source streams records from an external data source.
type Source interface {
	Name() string
	Stream(ctx context.Context) (<-chan Record, <-chan error)
	Close() error
}

// Options controls the import pipeline behaviour.
type Options struct {
	Prefix   string // path prefix in kiwifs (default: table/collection name)
	IDColumn string // column to use as filename (default: auto-detect primary key)
	Columns  []string
	DryRun   bool
	Limit    int
	Actor    string
}

// Stats is returned by Run with import counts.
type Stats struct {
	Imported int
	Skipped  int
	Errors   []string
}

// Run streams records from src, converts each to a markdown file, and writes
// them through the pipeline. Idempotent: files with matching _source_id are
// skipped if unchanged.
func Run(ctx context.Context, src Source, pipe *pipeline.Pipeline, opts Options) (*Stats, error) {
	records, errs := src.Stream(ctx)
	stats := &Stats{}
	actor := opts.Actor
	if actor == "" {
		actor = "import"
	}
	prefix := opts.Prefix
	if prefix == "" {
		prefix = src.Name()
	}
	count := 0

	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if err != nil {
				stats.Errors = append(stats.Errors, err.Error())
			}
		case rec, ok := <-records:
			if !ok {
				return stats, nil
			}
			if opts.Limit > 0 && count >= opts.Limit {
				return stats, nil
			}

			fields := rec.Fields
			if len(opts.Columns) > 0 {
				fields = filterColumns(fields, opts.Columns)
			}

			pk := rec.PrimaryKey
			if opts.IDColumn != "" {
				if v, ok := fields[opts.IDColumn]; ok {
					pk = fmt.Sprintf("%v", v)
				}
			}
			if pk == "" {
				pk = fmt.Sprintf("row_%d", count)
			}

			path := fmt.Sprintf("%s/%s.md", prefix, sanitizePath(pk))

			fm := make(map[string]any, len(fields)+3)
			for k, v := range fields {
				fm[k] = v
			}
			fm["_source"] = src.Name()
			fm["_source_id"] = rec.SourceID
			fm["_imported_at"] = time.Now().UTC().Format(time.RFC3339)

			title := pk
			if t, ok := fields["title"].(string); ok && t != "" {
				title = t
			} else if t, ok := fields["name"].(string); ok && t != "" {
				title = t
			}

			content := renderMarkdown(fm, title, rec.Table, rec.SourceID)

			if !opts.DryRun {
				existing, rerr := pipe.Store.Read(ctx, path)
				if rerr == nil {
					existingID := extractSourceID(existing)
					if existingID == rec.SourceID && contentUnchanged(existing, fields) {
						stats.Skipped++
						count++
						continue
					}
				}
				if _, err := pipe.Write(ctx, path, content, actor); err != nil {
					stats.Errors = append(stats.Errors, fmt.Sprintf("%s: %v", path, err))
					count++
					continue
				}
			}
			stats.Imported++
			count++
		}
	}
}

func renderMarkdown(fm map[string]any, title, table, id string) []byte {
	var buf bytes.Buffer
	buf.WriteString("---\n")
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	_ = enc.Encode(fm)
	_ = enc.Close()
	buf.WriteString("---\n\n")
	fmt.Fprintf(&buf, "# %s\n\n", title)
	fmt.Fprintf(&buf, "> Auto-imported from %s (row %s)\n", table, id)
	return buf.Bytes()
}

func sanitizePath(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "..", "_")
	return s
}

func filterColumns(fields map[string]any, columns []string) map[string]any {
	out := make(map[string]any, len(columns))
	for _, c := range columns {
		if v, ok := fields[c]; ok {
			out[c] = v
		}
	}
	return out
}

func extractSourceID(content []byte) string {
	fm := extractFrontmatter(content)
	if fm == "" {
		return ""
	}
	for _, line := range strings.Split(fm, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "_source_id:") {
			v := strings.TrimSpace(strings.TrimPrefix(line, "_source_id:"))
			return strings.Trim(v, `"'`)
		}
	}
	return ""
}

func contentUnchanged(existing []byte, newFields map[string]any) bool {
	existingFM := extractFrontmatter(existing)
	if existingFM == "" {
		return false
	}
	var existingMap map[string]any
	if err := yaml.Unmarshal([]byte(existingFM), &existingMap); err != nil {
		return false
	}
	for k, newVal := range newFields {
		oldVal, ok := existingMap[k]
		if !ok {
			return false
		}
		if fmt.Sprintf("%v", oldVal) != fmt.Sprintf("%v", newVal) {
			return false
		}
	}
	return true
}

func extractFrontmatter(content []byte) string {
	s := string(content)
	if !strings.HasPrefix(s, "---\n") {
		return ""
	}
	end := strings.Index(s[4:], "\n---\n")
	if end < 0 {
		return ""
	}
	return s[4 : 4+end]
}
