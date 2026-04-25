package exporter

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/kiwifs/kiwifs/internal/links"
	"github.com/kiwifs/kiwifs/internal/markdown"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/vectorstore"
)

// Options controls the export behaviour.
type Options struct {
	Format            string   // "jsonl" | "csv"
	PathPrefix        string   // scope to a subdirectory
	Columns           []string // frontmatter fields (CSV mode)
	IncludeContent    bool
	IncludeLinks      bool
	IncludeGitMeta    bool
	IncludeEmbeddings bool
	Output            io.Writer
	SchemaWriter      io.Writer // if set, writes a schema.json sidecar for ML compatibility
	EmbeddingModel    string    // model name for schema metadata
	EmbeddingDims     int       // vector dimensions for schema metadata
	Limit             int
}

// Record is one exported file.
type Record struct {
	Path        string         `json:"path"`
	Content     string         `json:"content,omitempty"`
	Frontmatter map[string]any `json:"frontmatter"`
	LinksOut    []string       `json:"links_out,omitempty"`
	LinksIn     []string       `json:"links_in,omitempty"`
	WordCount   int            `json:"word_count"`
	CreatedAt   string         `json:"created_at,omitempty"`
	UpdatedAt   string         `json:"updated_at,omitempty"`
	Embeddings  []EmbChunk     `json:"embeddings,omitempty"`
}

// EmbChunk is a single embedding chunk for ML export.
type EmbChunk struct {
	ChunkIdx int       `json:"chunk_idx"`
	Text     string    `json:"text"`
	Vector   []float32 `json:"vector"`
}

// EmbeddingProvider retrieves stored embedding vectors for a file path.
type EmbeddingProvider interface {
	GetVectors(ctx context.Context, path string) ([]vectorstore.Chunk, error)
}

// backlinkQuerier is satisfied by search.SQLite.
type backlinkQuerier interface {
	Backlinks(ctx context.Context, target string) ([]links.Entry, error)
}

// Export walks the store, builds Records, and writes them in the requested format.
func Export(ctx context.Context, store storage.Storage, searcher search.Searcher, vecs EmbeddingProvider, opts Options) (int, error) {
	if opts.Output == nil {
		return 0, fmt.Errorf("output writer is required")
	}

	var bq backlinkQuerier
	if opts.IncludeLinks {
		bq, _ = searcher.(backlinkQuerier)
	}

	count := 0
	var csvWriter *csv.Writer
	var csvHeaders []string

	if opts.Format == "csv" {
		csvWriter = csv.NewWriter(opts.Output)
		if len(opts.Columns) > 0 {
			csvHeaders = append([]string{"path"}, opts.Columns...)
			csvWriter.Write(csvHeaders)
		}
	}

	err := storage.Walk(ctx, store, "/", func(e storage.Entry) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if opts.Limit > 0 && count >= opts.Limit {
			return nil
		}
		if opts.PathPrefix != "" && !strings.HasPrefix(e.Path, opts.PathPrefix) {
			return nil
		}

		content, err := store.Read(ctx, e.Path)
		if err != nil {
			return nil
		}

		parsed, _ := markdown.Parse(content)
		fm := parsed.Frontmatter
		if fm == nil {
			fm = map[string]any{}
		}

		body := markdown.BodyAfterFrontmatter(content)
		wordCount := len(strings.Fields(body))

		rec := Record{
			Path:        e.Path,
			Frontmatter: fm,
			WordCount:   wordCount,
		}

		if opts.IncludeContent {
			rec.Content = string(content)
		}

		if opts.IncludeLinks {
			rec.LinksOut = links.Extract(content)
			if bq != nil {
				entries, err := bq.Backlinks(ctx, e.Path)
				if err == nil {
					for _, en := range entries {
						rec.LinksIn = append(rec.LinksIn, en.Path)
					}
				}
			}
		}

		if opts.IncludeEmbeddings && vecs != nil {
			chunks, err := vecs.GetVectors(ctx, e.Path)
			if err == nil && len(chunks) > 0 {
				for _, c := range chunks {
					rec.Embeddings = append(rec.Embeddings, EmbChunk{
						ChunkIdx: c.ChunkIdx,
						Text:     c.Text,
						Vector:   c.Vector,
					})
				}
			}
		}

		switch opts.Format {
		case "jsonl":
			data, err := json.Marshal(rec)
			if err != nil {
				return nil
			}
			opts.Output.Write(data)
			opts.Output.Write([]byte("\n"))

		case "csv":
			if csvHeaders == nil {
				csvHeaders = autoDetectColumns(fm)
				csvWriter.Write(csvHeaders)
			}
			row := make([]string, len(csvHeaders))
			row[0] = e.Path
			for i, col := range csvHeaders[1:] {
				row[i+1] = flattenValue(getNestedField(fm, col))
			}
			csvWriter.Write(row)
		}

		count++
		return nil
	})

	if csvWriter != nil {
		csvWriter.Flush()
	}

	if opts.IncludeEmbeddings && opts.SchemaWriter != nil {
		writeSchema(opts, count)
	}

	return count, err
}

func writeSchema(opts Options, recordCount int) {
	model := opts.EmbeddingModel
	if model == "" {
		model = "all-MiniLM-L6-v2"
	}
	dims := opts.EmbeddingDims
	if dims <= 0 {
		dims = 384
	}
	schema := map[string]any{
		"format": opts.Format,
		"fields": map[string]any{
			"path":        map[string]any{"type": "string", "description": "File path in knowledge base"},
			"frontmatter": map[string]any{"type": "object", "description": "Structured metadata"},
			"content":     map[string]any{"type": "string", "description": "Full markdown content"},
			"word_count":  map[string]any{"type": "integer"},
			"embeddings": map[string]any{
				"type": "array",
				"items": map[string]any{
					"chunk_idx": map[string]any{"type": "integer"},
					"text":      map[string]any{"type": "string"},
					"vector":    map[string]any{"type": "array", "items": map[string]any{"type": "float32"}, "dimensions": dims},
				},
			},
		},
		"embedding_model":      model,
		"embedding_dimensions": dims,
		"exported_at":          time.Now().UTC().Format(time.RFC3339),
		"record_count":         recordCount,
	}
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return
	}
	opts.SchemaWriter.Write(data)
	opts.SchemaWriter.Write([]byte("\n"))
}

func autoDetectColumns(fm map[string]any) []string {
	cols := []string{"path"}
	keys := make([]string, 0, len(fm))
	for k := range fm {
		if !strings.HasPrefix(k, "_") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return append(cols, keys...)
}

func getNestedField(fm map[string]any, key string) any {
	parts := strings.Split(key, ".")
	var current any = fm
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = m[part]
	}
	return current
}

func flattenValue(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case []any:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = flattenValue(item)
		}
		return strings.Join(parts, ", ")
	case map[string]any:
		data, _ := json.Marshal(val)
		return string(data)
	default:
		return fmt.Sprintf("%v", val)
	}
}
