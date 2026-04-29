---
title: Frontmatter cheat sheet
tags: [onboarding, concepts]
---

# Frontmatter cheat sheet

Every page can start with a YAML block between `---` fences.
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
