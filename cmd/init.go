package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a knowledge directory",
	Example: `  kiwifs init --root ~/my-knowledge
  kiwifs init --root ~/my-knowledge --template agent-knowledge
  kiwifs init --root ~/my-knowledge --template team-wiki`,
	RunE: runInit,
}

func init() {
	initCmd.Flags().StringP("root", "r", "./knowledge", "directory to initialize")
	initCmd.Flags().String("template", "agent-knowledge", "template: agent-knowledge | team-wiki | runbook | research | blank")
}

func runInit(cmd *cobra.Command, args []string) error {
	root, _ := cmd.Flags().GetString("root")
	template, _ := cmd.Flags().GetString("template")

	if err := os.MkdirAll(root, 0755); err != nil {
		return fmt.Errorf("create root: %w", err)
	}

	switch template {
	case "agent-knowledge":
		if err := initAgentKnowledge(root); err != nil {
			return err
		}
	case "blank":
		// just the directory
	default:
		if err := initAgentKnowledge(root); err != nil {
			return err
		}
	}

	kiwiDir := filepath.Join(root, ".kiwi")
	if err := os.MkdirAll(kiwiDir, 0755); err != nil {
		return fmt.Errorf("create .kiwi: %w", err)
	}

	configPath := filepath.Join(kiwiDir, "config.toml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		configContent := `[server]
port = 3333
host = "0.0.0.0"

[storage]
root = "."

[search]
engine = "grep"

[versioning]
strategy = "git"

[auth]
type = "none"
`
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			return fmt.Errorf("write config: %w", err)
		}
	}

	fmt.Printf("Initialized knowledge at %s (template: %s)\n", root, template)
	fmt.Printf("Run: kiwifs serve --root %s\n", root)
	return nil
}

func writeFileIfMissing(path, content string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		return os.WriteFile(path, []byte(content), 0644)
	}
	return nil
}

func initAgentKnowledge(root string) error {
	files := map[string]string{
		"SCHEMA.md": `# Schema

This knowledge base follows the agent-knowledge pattern.

## Ingest
When adding new information:
1. Create or update a page in the appropriate folder
2. Update index.md with a link to the new page
3. Append a line to log.md: ` + "`" + `- YYYY-MM-DD: <summary>` + "`" + `

## Query
To answer a question:
1. Check index.md for relevant pages
2. Read the relevant pages
3. If the answer warrants a new page, create it in concepts/

## Lint
To audit the knowledge base:
1. Check for orphan pages (linked in index.md but file missing)
2. Check for stale content (no updates in 30+ days)
3. Check for contradictions across pages
`,
		"index.md": `# Knowledge Index

> Auto-maintained table of contents. Update this file when adding new pages.

## Concepts
_No concepts yet. Add pages to concepts/ and link them here._

## Entities
_No entities yet. Add pages to entities/ and link them here._

## Reports
_No reports yet. Add pages to reports/ and link them here._

## Log
See [log.md](log.md) for the chronological record.
`,
		"log.md": `# Log

Chronological record of knowledge additions.

`,
		"concepts/.gitkeep":  "",
		"entities/.gitkeep":  "",
		"reports/.gitkeep":   "",
	}

	for relPath, content := range files {
		fullPath := filepath.Join(root, relPath)
		if err := writeFileIfMissing(fullPath, content); err != nil {
			return fmt.Errorf("init file %s: %w", relPath, err)
		}
	}
	return nil
}
