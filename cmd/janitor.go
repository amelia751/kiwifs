package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kiwifs/kiwifs/internal/janitor"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/spf13/cobra"
)

var janitorCmd = &cobra.Command{
	Use:   "janitor",
	Short: "Scan a knowledge base for stale, orphaned, or broken pages",
	Long: `Scan the knowledge folder at --root for hygiene issues.

Reports:
  - stale          — page not reviewed within --stale-days
  - orphan         — page with no inbound wiki links
  - duplicate      — pages with identical titles
  - contradiction  — conflicting source-of-truth claims
  - missing-owner  — page without an owner field
  - missing-status — page without a status field
  - empty-page     — page with < 50 chars of content
  - broken-link    — wiki link target doesn't exist
  - no-review-date — has owner but no next-review
  - decision-found — meeting note contains decision language

Exits 0 on a clean run, 1 if any error-severity issues are found.`,
	Example: `  kiwifs janitor --root ~/my-knowledge
  kiwifs janitor --root /data/knowledge --stale-days 60 --json`,
	RunE: runJanitor,
}

func init() {
	janitorCmd.Flags().StringP("root", "r", "./knowledge", "knowledge root directory")
	janitorCmd.Flags().Int("stale-days", 90, "days before a page is considered stale")
	janitorCmd.Flags().Bool("json", false, "emit JSON instead of the human summary")
	rootCmd.AddCommand(janitorCmd)
}

func runJanitor(cmd *cobra.Command, args []string) error {
	root, _ := cmd.Flags().GetString("root")
	staleDays, _ := cmd.Flags().GetInt("stale-days")
	asJSON, _ := cmd.Flags().GetBool("json")

	abs, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("janitor: %w", err)
	}

	store, err := storage.NewLocal(abs)
	if err != nil {
		return fmt.Errorf("janitor: open storage: %w", err)
	}
	var searcher search.Searcher
	sq, sqerr := search.NewSQLite(abs, store)
	if sqerr == nil {
		defer sq.Close()
		searcher = sq
	}

	scanner := janitor.New(abs, store, searcher, staleDays)
	result, err := scanner.Scan(cmd.Context())
	if err != nil {
		return fmt.Errorf("janitor: %w", err)
	}

	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(result); err != nil {
			return err
		}
	} else {
		fmt.Print(result.Summary())
	}

	if result.HasErrors() {
		os.Exit(1)
	}
	return nil
}
