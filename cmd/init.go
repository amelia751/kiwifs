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
	case "team-wiki":
		if err := initTeamWiki(root); err != nil {
			return err
		}
	case "runbook":
		if err := initRunbook(root); err != nil {
			return err
		}
	case "research":
		if err := initResearch(root); err != nil {
			return err
		}
	case "blank":
		// just the directory
	default:
		return fmt.Errorf("unknown template %q (want agent-knowledge | team-wiki | runbook | research | blank)", template)
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
engine = "sqlite"

[versioning]
strategy = "git"

[auth]
type = "none"
# type = "apikey"  → single global key via api_key below
# type = "perspace" → per-space keys via [[auth.api_keys]] below
# type = "oidc"    → JWT validation via [auth.oidc] below

# Single global API key (auth.type = "apikey"):
# api_key = "your-secret-key"

# Per-space API keys (auth.type = "perspace"):
# [[auth.api_keys]]
# key   = "kiwi_proj_abc123"
# space = "project-alpha"
# actor = "jetrun-agent"

# OIDC / OAuth JWT validation (auth.type = "oidc"):
# [auth.oidc]
# issuer    = "https://accounts.google.com"
# client_id = "your-client-id"
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

## Provenance

Every agent-written page should declare where its contents came from via a
` + "`derived-from`" + ` list in the YAML frontmatter:

` + "```yaml" + `
---
status: published
derived-from:
  - type: run          # run | commit | import | manual | agent
    id: run-249
    commit: a1b2c3d    # optional — git SHA the run exercised
    date: 2026-04-21T09:14Z
    actor: agent:exec_abc
    note: "extracted from turn summary"  # optional
---
` + "```" + `

Writers can skip the frontmatter entirely and instead pass an
` + "`X-Provenance: <type>:<id>`" + ` header on PUT / bulk. KiwiFS appends a
normalised entry to ` + "`derived-from`" + ` before indexing, so the
server-side file always has the full record.

Query provenance through the metadata index:

    GET /api/kiwi/meta?where=$.derived-from[*].id=run-249
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

func initTeamWiki(root string) error {
	files := map[string]string{
		"SCHEMA.md": `# Schema — Team Wiki

Flat structure for a team replacing Confluence/Notion. Every top-level
folder is a functional area; every ` + "`index.md`" + ` is the landing page
for its folder.

## Conventions
- Link between pages with ` + "`[[wiki links]]`" + `.
- Keep pages short. Split when a page exceeds ~300 lines.
- Runbooks live under ` + "`engineering/runbooks/`" + `; specs under
  ` + "`product/specs/`" + `.
`,
		"index.md": `# Team Wiki

Welcome. Start in [[getting-started]] or browse by area.

## Areas
- [[engineering/architecture|Architecture]]
- [[engineering/deployment|Deployment]]
- [[product/roadmap|Roadmap]]
- [[onboarding/index|Onboarding]]
`,
		"getting-started.md": `# Getting Started

_One-pager for new teammates. Link to the most important reads first._
`,
		"engineering/architecture.md":        "# Architecture\n\n_Overview of services, data flow, and key decisions._\n",
		"engineering/deployment.md":          "# Deployment\n\n_How code reaches production._\n",
		"engineering/runbooks/.gitkeep":      "",
		"product/roadmap.md":                 "# Roadmap\n\n_What's shipping this quarter._\n",
		"product/specs/.gitkeep":             "",
		"onboarding/index.md":                "# Onboarding\n\n_Reading list + checklist for week 1._\n",
	}
	return writeTemplateFiles(root, files)
}

func initRunbook(root string) error {
	files := map[string]string{
		"SCHEMA.md": `# Schema — Runbook

Operational knowledge for on-call and platform teams.

## Conventions
- Every incident gets a file ` + "`incidents/YYYY-MM-DD-<slug>.md`" + ` copied from
  ` + "`incidents/template.md`" + `.
- Every procedure lives in ` + "`procedures/`" + ` and is linkable from
  on-call playbooks via ` + "`[[procedure-name]]`" + `.
- Postmortems live in ` + "`postmortems/`" + ` and link back to the incident.
`,
		"index.md": `# Runbooks

## Incidents
_See [[incidents/template|incident template]]._

## Procedures
_Common operational tasks._

## Postmortems
_Root cause analyses._
`,
		"incidents/template.md": `# Incident — <short title>

- **Date:** YYYY-MM-DD
- **Severity:** Sev1 / Sev2 / Sev3
- **On-call:**
- **Status:** open / mitigated / resolved

## Timeline
- HH:MM — detected
- HH:MM — mitigated
- HH:MM — resolved

## Impact

## Root cause

## Follow-ups
- [ ]
`,
		"procedures/deploy-rollback.md": "# Deploy Rollback\n\n_Steps to roll back the most recent deploy._\n",
		"procedures/scale-up.md":        "# Scale Up\n\n_Steps to add capacity during a traffic spike._\n",
		"procedures/rotate-secrets.md":  "# Rotate Secrets\n\n_Credential rotation workflow._\n",
		"postmortems/.gitkeep":          "",
	}
	return writeTemplateFiles(root, files)
}

func initResearch(root string) error {
	files := map[string]string{
		"SCHEMA.md": `# Schema — Research

Literature notes, experiment logs, and analysis for researchers.

## Conventions
- One paper per file in ` + "`literature/`" + `, named after the paper's slug.
- One experiment per file in ` + "`experiments/`" + `, prefixed ` + "`exp-NNN-`" + `
  with a zero-padded sequence.
- Free-form working notes live in ` + "`notes/`" + `.
`,
		"index.md": `# Research

## Literature
_Reading notes, one per paper._

## Experiments
_Experiment logs, prefixed ` + "`exp-NNN-<slug>.md`" + `._

## Notes
_Free-form working notes._
`,
		"literature/.gitkeep": "",
		"experiments/exp-001-baseline.md": `# Experiment 001 — Baseline

- **Date:**
- **Hypothesis:**
- **Setup:**
- **Result:**
- **Takeaway:**
`,
		"notes/.gitkeep": "",
	}
	return writeTemplateFiles(root, files)
}

func writeTemplateFiles(root string, files map[string]string) error {
	for relPath, content := range files {
		fullPath := filepath.Join(root, relPath)
		if err := writeFileIfMissing(fullPath, content); err != nil {
			return fmt.Errorf("init file %s: %w", relPath, err)
		}
	}
	return nil
}
