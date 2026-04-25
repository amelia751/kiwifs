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

	gitignorePath := filepath.Join(root, ".gitignore")
	if _, err := os.Stat(gitignorePath); os.IsNotExist(err) {
		gitignoreContent := `# KiwiFS — rebuildable state (SQLite indexes, WAL, vector cache)
.kiwi/state/
`
		if err := os.WriteFile(gitignorePath, []byte(gitignoreContent), 0644); err != nil {
			return fmt.Errorf("write .gitignore: %w", err)
		}
	}

	templatesDir := filepath.Join(kiwiDir, "templates")
	if err := os.MkdirAll(templatesDir, 0755); err != nil {
		return fmt.Errorf("create .kiwi/templates: %w", err)
	}
	if err := writeFileIfMissing(filepath.Join(templatesDir, "decision.md"), decisionTemplate); err != nil {
		return fmt.Errorf("write decision template: %w", err)
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
# actor = "my-agent"

# OIDC / OAuth JWT validation (auth.type = "oidc"):
# [auth.oidc]
# issuer    = "https://accounts.google.com"
# client_id = "your-client-id"
`
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			return fmt.Errorf("write config: %w", err)
		}
	}

	if err := writeWorkflowTemplates(root); err != nil {
		return err
	}

	fmt.Printf("Initialized knowledge at %s (template: %s)\n", root, template)
	fmt.Printf("Run: kiwifs serve --root %s\n", root)
	return nil
}

const decisionTemplate = `---
type: decision
status: active
date: YYYY-MM-DD
owner: 
decision: 
alternatives:
  - option: 
    pros: 
    cons: 
impact: 
reversal-conditions: 
linked-docs: []
linked-meetings: []
tags: []
---

# [Decision Title]

## Context
What is the situation that requires this decision?

## Decision
What was decided?

## Alternatives Considered
What other options were evaluated?

## Consequences
What are the expected outcomes?

## Reversal Conditions
Under what circumstances should this decision be revisited?
`

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

## Decisions
When a decision is made:
1. Create a page in decisions/ using the decision template
2. Fill in all fields: decision, alternatives, impact, reversal conditions
3. Link to related docs and meetings
4. Update index.md with a link
5. Append to log.md: ` + "`" + `- YYYY-MM-DD: Decision: <summary>` + "`" + `

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

> Start at [[welcome]] if this is your first time. The rest of this file
> is an auto-maintained table of contents — update it when you add new
> pages.

## Concepts
- [[concepts/wikilinks|How wikilinks work]]
- [[concepts/frontmatter|Frontmatter cheat sheet]]
_Add more concept pages to concepts/ and link them here._

## Entities
_No entities yet. Add pages to entities/ and link them here._

## Reports
_No reports yet. Add pages to reports/ and link them here._

## Log
See [log.md](log.md) for the chronological record.
`,
		"welcome.md": `---
title: Welcome to KiwiFS
tags: [onboarding]
---

# Welcome to KiwiFS

KiwiFS is a knowledge base that lives as **markdown files on disk**.
That means every page you write here is a normal ` + "`.md`" + ` file you
can edit with any tool, commit with git, and share with an AI agent.

## Three things to try right now

1. **Follow a wikilink.** Click → [[concepts/wikilinks]] to see how pages
   reference each other. You can type ` + "`[[`" + ` anywhere inside an
   editor to search for a target.
2. **Write your first note.** Use the "New page" button in the sidebar,
   or drop a markdown file into the folder on disk — it'll appear in the
   tree within a second.
3. **See the graph.** Click the graph icon in the toolbar to visualise
   which pages link to which. With just a handful of notes it'll start
   looking useful.

## Longer reads

- [[concepts/frontmatter]] — the metadata header at the top of each page
- [[SCHEMA|Schema]] — how agents should extend this knowledge base
- [[log|Log]] — the running changelog new facts get appended to

When you're comfortable, delete this page and the sample pages under
` + "`concepts/`" + `. KiwiFS won't complain.
`,
		"concepts/wikilinks.md": `---
title: How wikilinks work
tags: [onboarding, concepts]
---

# How wikilinks work

Inside any page, write ` + "`[[target-page]]`" + ` to link to another
page. KiwiFS resolves the target by:

1. exact path (` + "`[[concepts/wikilinks]]`" + ` → ` + "`concepts/wikilinks.md`" + `)
2. basename match across the tree (` + "`[[wikilinks]]`" + ` works too)
3. title match from the page's frontmatter

You can give the link a different label with a pipe:

    [[concepts/wikilinks|how linking works]]

→ [[concepts/wikilinks|how linking works]]

Broken links render with a dashed underline so they're easy to spot.
Fixing a link is as simple as creating the target page or renaming
along the way.

See also: [[concepts/frontmatter]], [[SCHEMA]].
`,
		"concepts/frontmatter.md": `---
title: Frontmatter cheat sheet
tags: [onboarding, concepts]
---

# Frontmatter cheat sheet

Every page can start with a YAML block between ` + "`---`" + ` fences.
KiwiFS uses frontmatter for metadata, search, and workflow triggers.

    ---
    title: My Page
    status: published       # draft | review | published | deprecated
    trust: verified         # suggestion | validated | verified | source-of-truth
    tags: [runbook, on-call]
    owner: alice@example.com
    last-reviewed: 2026-04-21
    ---

Useful fields KiwiFS understands out of the box:

- **title / tags** — surfaced in search, sidebar, and badges.
- **trust** — bumps the page in default search ranking (see
  [[SCHEMA]]).
- **last-reviewed** — the Janitor flags anything older than 90 days
  (configurable per space) as stale.
- **due-date / tasks / approval** — turns the page into a workflow
  page rendered with checklists and reminders.
- **derived-from** — provenance stamps for agent-generated content.
`,
		"log.md": `# Log

Chronological record of knowledge additions.

`,
		"concepts/.gitkeep":  "",
		"entities/.gitkeep":  "",
		"reports/.gitkeep":   "",
		"decisions/.gitkeep": "",
		"decisions/_template.md": decisionTemplate,
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

func writeWorkflowTemplates(root string) error {
	templates := map[string]string{
		".kiwi/templates/onboarding.md": `---
title: "New Hire Onboarding: [Name]"
type: onboarding
owner: 
status: draft
tasks:
  - id: t1
    title: "Create company email"
    status: todo
    assignee: IT
  - id: t2
    title: "Set up GitHub access"
    status: todo
    assignee: Engineering
  - id: t3
    title: "Add to Slack channels"
    status: todo
    assignee: HR
  - id: t4
    title: "Schedule intro meetings"
    status: todo
    assignee: Manager
  - id: t5
    title: "Complete security training"
    status: todo
    assignee: New Hire
  - id: t6
    title: "Review company handbook"
    status: todo
    assignee: New Hire
approval:
  status: pending
due-date: 
tags: [onboarding, hr]
---

# New Hire Onboarding: [Name]

## Welcome!
Welcome to the team! This checklist will guide you through your first week.

## Tasks
Tasks are tracked in the frontmatter above and rendered by the workflow UI.

## Notes
Add any onboarding notes here.
`,
		".kiwi/templates/product-spec.md": `---
title: "[Feature Name] Spec"
type: spec
owner: 
status: draft
tasks:
  - id: t1
    title: "Requirements defined"
    status: todo
  - id: t2
    title: "Design approved"
    status: todo
  - id: t3
    title: "Engineering tasks created"
    status: todo
  - id: t4
    title: "QA plan written"
    status: todo
  - id: t5
    title: "Launch checklist complete"
    status: todo
approval:
  status: pending
tags: [product, spec]
---

# [Feature Name] Spec

## Problem
What problem does this solve?

## Solution
What are we building?

## Requirements
- [ ] Requirement 1
- [ ] Requirement 2

## Design
Link to design docs.

## Launch Checklist
- [ ] Feature flag created
- [ ] Monitoring configured
- [ ] Docs updated
- [ ] Stakeholders notified
`,
	}
	for relPath, content := range templates {
		fullPath := filepath.Join(root, relPath)
		if err := writeFileIfMissing(fullPath, content); err != nil {
			return fmt.Errorf("init workflow template %s: %w", relPath, err)
		}
	}
	return nil
}
