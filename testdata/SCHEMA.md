# Schema

This knowledge base follows the agent-knowledge pattern.

## Ingest
When adding new information:
1. Create or update a page in the appropriate folder
2. Update index.md with a link to the new page
3. Append a line to log.md: `- YYYY-MM-DD: <summary>`

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
`derived-from` list in the YAML frontmatter:

```yaml
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
```

Writers can skip the frontmatter entirely and instead pass an
`X-Provenance: <type>:<id>` header on PUT / bulk. KiwiFS appends a
normalised entry to `derived-from` before indexing, so the
server-side file always has the full record.

Query provenance through the metadata index:

    GET /api/kiwi/meta?where=$.derived-from[*].id=run-249
