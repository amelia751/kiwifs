package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kiwifs/kiwifs/internal/bootstrap"
	"github.com/kiwifs/kiwifs/internal/config"
	"github.com/kiwifs/kiwifs/internal/exporter"
	"github.com/spf13/cobra"
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export knowledge base files to JSONL or CSV",
	Example: `  kiwifs export --format jsonl --output students.jsonl
  kiwifs export --format jsonl --path students/ --include-content
  kiwifs export --format csv --columns name,status,grade --output students.csv
  kiwifs export --format jsonl  # stdout`,
	RunE: runExport,
}

func init() {
	rootCmd.AddCommand(exportCmd)

	exportCmd.Flags().StringP("root", "r", "./knowledge", "knowledge root directory")
	exportCmd.Flags().String("format", "jsonl", "output format: jsonl | csv")
	exportCmd.Flags().StringP("output", "o", "", "output file (default: stdout)")
	exportCmd.Flags().String("path", "", "scope to a subdirectory")
	exportCmd.Flags().String("columns", "", "comma-separated frontmatter fields (CSV mode)")
	exportCmd.Flags().Bool("include-content", false, "include full markdown content")
	exportCmd.Flags().Bool("include-links", false, "include outgoing and incoming links")
	exportCmd.Flags().Bool("include-embeddings", false, "include vector embeddings")
	exportCmd.Flags().Int("limit", 0, "max files to export (0 = unlimited)")
}

func runExport(cmd *cobra.Command, _ []string) error {
	root, _ := cmd.Flags().GetString("root")
	format, _ := cmd.Flags().GetString("format")
	output, _ := cmd.Flags().GetString("output")
	path, _ := cmd.Flags().GetString("path")
	columnsStr, _ := cmd.Flags().GetString("columns")
	includeContent, _ := cmd.Flags().GetBool("include-content")
	includeLinks, _ := cmd.Flags().GetBool("include-links")
	includeEmb, _ := cmd.Flags().GetBool("include-embeddings")
	limit, _ := cmd.Flags().GetInt("limit")

	if format != "jsonl" && format != "csv" {
		return fmt.Errorf("unsupported format: %s (use jsonl or csv)", format)
	}

	cfg, err := config.Load(root)
	if err != nil {
		cfg = &config.Config{}
	}
	cfg.Storage.Root = root

	stack, err := bootstrap.Build("export", root, cfg)
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	defer stack.Close()

	var columns []string
	if columnsStr != "" {
		columns = strings.Split(columnsStr, ",")
		for i := range columns {
			columns[i] = strings.TrimSpace(columns[i])
		}
	}

	var w *os.File
	if output != "" {
		w, err = os.Create(output)
		if err != nil {
			return fmt.Errorf("create output: %w", err)
		}
		defer w.Close()
	} else {
		w = os.Stdout
	}

	opts := exporter.Options{
		Format:            format,
		PathPrefix:        path,
		Columns:           columns,
		IncludeContent:    includeContent,
		IncludeLinks:      includeLinks,
		IncludeEmbeddings: includeEmb,
		Output:            w,
		Limit:             limit,
	}

	if includeEmb && output != "" {
		ext := filepath.Ext(output)
		schemaPath := strings.TrimSuffix(output, ext) + ".schema.json"
		sf, err := os.Create(schemaPath)
		if err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
		defer sf.Close()
		opts.SchemaWriter = sf
	}

	ctx := cmd.Context()
	count, err := exporter.Export(ctx, stack.Store, stack.Searcher, stack.Vectors, opts)
	if err != nil {
		return fmt.Errorf("export: %w", err)
	}

	if output != "" {
		fmt.Fprintf(os.Stderr, "Exported %d files to %s\n", count, output)
	}
	return nil
}
