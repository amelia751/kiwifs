package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/kiwifs/kiwifs/internal/schema"
	"github.com/spf13/cobra"
)

var lintCmd = &cobra.Command{
	Use:   "lint",
	Short: "Validate a knowledge directory against its SCHEMA.md",
	Long: `Validate the folder at --root against its SCHEMA.md.

Reports:
  - orphan     — SCHEMA.md references a [[page]] that doesn't exist
  - broken-link — a page contains a [[page]] that doesn't resolve
  - empty-file  — a .md file has no content
  - missing-schema — SCHEMA.md itself is missing

Exits 0 on a clean run, 1 if any issues are found.`,
	Example: `  kiwifs lint --root ~/my-knowledge
  kiwifs lint --root /data/knowledge --json`,
	RunE: runLint,
}

func init() {
	lintCmd.Flags().StringP("root", "r", "./knowledge", "knowledge root directory")
	lintCmd.Flags().Bool("json", false, "emit JSON instead of the human summary")
	rootCmd.AddCommand(lintCmd)
}

func runLint(cmd *cobra.Command, args []string) error {
	root, _ := cmd.Flags().GetString("root")
	asJSON, _ := cmd.Flags().GetBool("json")

	result, err := schema.Lint(root)
	if err != nil {
		return fmt.Errorf("lint: %w", err)
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

	if len(result.Issues) > 0 {
		// Non-zero exit so CI can gate on lint; don't print the error
		// since we've already emitted a human-readable summary.
		os.Exit(1)
	}
	return nil
}
