# Roadmap

Where KiwiFS is headed. Updated as priorities shift.

This is a living document — not a promise. If you want to work on something here, open an issue first so we can coordinate.

---

## v0.1 — "It works" (current)

The foundation. A single Go binary that serves markdown files with a web UI, git versioning, full-text + vector search, and multi-protocol access.

- [x] REST API (file CRUD, tree, search, versions, diff, blame, SSE)
- [x] Web UI (BlockNote editor, wiki links, backlinks, graph view, Cmd+K search, ToC, comments)
- [x] Git versioning (atomic commits, audit trail, conflict detection via ETags)
- [x] SQLite FTS5 search (BM25 ranked) + pluggable vector search
- [x] NFS, S3, WebDAV, FUSE access protocols
- [x] Structured metadata index (`file_meta` JSON column, queryable frontmatter)
- [x] Provenance tracking (`X-Provenance` header → frontmatter injection)
- [x] Binary asset uploads (images, PDFs alongside markdown)
- [x] Multi-space support (one server, multiple knowledge bases)
- [x] Go library (`pkg/kiwi`) — embed KiwiFS in any Go app

---

## v0.2 — Embeddable

Make KiwiFS easy to plug into other apps. This is what turns it from a tool into a platform.

- [x] **Permalinks** — `public_url` config, HTML5 history routing (`/page/{path}`), permalink field in API responses, wiki link → real URL resolution for external contexts (PR comments, Slack, exports)
- [ ] **React component library** (`kiwifs-ui` on npm) — `<KiwiProvider>`, `<KiwiTree>`, `<KiwiPage>`, `<KiwiEditor>`, `<KiwiSearch>`, `<KiwiGraph>` as standalone components
- [x] **MCP server** (`kiwifs mcp`) — Model Context Protocol for AI agents (Claude, Cursor, etc.)
- [x] **Permalinks** — shareable page URLs with SPA deep linking, `X-Permalink` header, wiki-link resolution
- [ ] **Dataview** — computed views over frontmatter (Obsidian Dataview as a server). Hand-written Pratt parser for DQL, `json_extract`-based SQLite queries, TABLE/LIST/COUNT output, aggregation, GROUP BY. REST `GET /api/kiwi/query`, MCP `kiwi_query`, CLI `kiwifs query`. Computed view files with `kiwi-view: true` frontmatter.
- [x] **Episodic vs semantic memory** — `merged-from` frontmatter, `memory_kind`, `[memory]` config, `kiwifs memory report`, REST `GET /api/kiwi/memory/report`, MCP `kiwi_memory_report`, `internal/memory` helpers (`docs/MEMORY.md`), default `knowledge` init template includes `episodes/`
- [ ] **Pipeline hooks** (Go) — `OnBeforeWrite`, `OnAfterWrite` callbacks for custom validation/notifications
- [ ] **JS hooks** — `.kiwi/hooks/*.js` scripts via embedded runtime, no recompile needed

## v0.2.1 — Data durability & backup

Your data must survive anything — process crash, container kill, host failure. KiwiFS is Obsidian-model (files are the truth, everything else rebuilds), and now ships with built-in backup.

- [ ] **Atomic file writes** — temp-file-then-rename in storage layer, eliminates torn writes on crash
- [ ] **Track `.kiwi/` user data in git** — comments, config, templates are user-created data that must not be silently lost
- [x] **`[backup]` config** — `remote = "git@github.com:user/kb.git"`, `interval = "5m"`, background goroutine pushes to any git remote
- [x] **`kiwifs backup`** — one-shot CLI command for manual push, cron jobs, pre-maintenance scripts
- [x] **`kiwifs restore`** — `git clone` + auto-reindex, one command to recover from any git remote
- [ ] **Uncommitted path tracking** — if git commit fails after file write, track and retry so history is never silently lost

---

## v0.3 — Import & export

You can't replace Confluence if you can't migrate from it.

- [ ] `kiwifs import --from obsidian` — copy vault, rewrite `![[image]]` paths
- [ ] `kiwifs import --from notion` — parse exported markdown + CSV, fix internal links
- [ ] `kiwifs import --from confluence` — convert XHTML storage format to markdown
- [ ] `kiwifs export --format mkdocs` / `--format docusaurus` — generate static doc sites

## v0.4 — Webhooks & analytics

Outbound integration and content health signals.

- [ ] **Webhooks** — POST to Slack/CI/custom URLs on write/delete events, HMAC signing, retry with backoff
- [ ] **Content analytics** — page views, stale page detection, failed search queries, orphan pages

## v0.5 — Access control & governance

Enterprise features for teams that need enforced boundaries.

- [ ] **RBAC permissions** (Casbin) — per-space role-based access, JWT/API key/OIDC identity
- [ ] **Content lifecycle** — retention policies, legal holds, auto-archival
- [ ] **Editorial states** — draft → review → published workflow via frontmatter

---

## How to contribute to the roadmap

1. **Pick something** — find an item above that interests you
2. **Open an issue** — describe your approach, we'll discuss before you code
3. **Start small** — even one bullet point from a section is a meaningful PR
4. **Suggest new items** — open a [Discussion](https://github.com/kiwifs/kiwifs/discussions) if you think something is missing

Items labeled [`good first issue`](https://github.com/kiwifs/kiwifs/labels/good%20first%20issue) are specifically scoped for new contributors.

---

*Last updated: April 2026*
