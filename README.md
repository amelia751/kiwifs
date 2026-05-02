<p align="center">
  <img src="kiwifs.png" alt="KiwiFS" width="200" />
</p>

<h1 align="center">KiwiFS</h1>

<p align="center">
  <strong>Knowledge filesystem agents can write to, search, query, and trust.</strong>
</p>

<p align="center">
  Agents write with <code>cat</code>. Humans read in a wiki. Git versions everything. One binary, zero config.
</p>

<p align="center">
  <a href="https://github.com/kiwifs/kiwifs/actions/workflows/ci.yml"><img src="https://github.com/kiwifs/kiwifs/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-BSL--1.1-blue" alt="License: BSL 1.1"></a>
  <a href="https://github.com/kiwifs/kiwifs"><img src="https://img.shields.io/badge/go-1.25-00ADD8?logo=go&logoColor=white" alt="Go 1.25"></a>
  <a href="https://github.com/kiwifs/kiwifs"><img src="https://img.shields.io/badge/single_binary-yes-green" alt="Single Binary"></a>
  <a href="https://github.com/kiwifs/kiwifs"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen" alt="PRs Welcome"></a>
</p>

<p align="center">
  <a href="https://kiwifs.mintlify.app">Docs</a> · <a href="FAQ.md">FAQ</a> · <a href="ROADMAP.md">Roadmap</a> · <a href="CONTRIBUTING.md">Contributing</a>
</p>

```bash
curl -fsSL https://raw.githubusercontent.com/kiwifs/kiwifs/main/install.sh | sh
kiwifs init ./knowledge && kiwifs serve --root ./knowledge
# Open http://localhost:3333
```

**Or with Docker:**

```bash
docker run -p 3333:3333 -v ./knowledge:/data ameliaanhlam/kiwifs
```

---

## The problem

Knowledge filesystems for agents are an emerging primitive — but files are just files. Or are they?

Current solutions fall into one of these camps:

- **Database tables pretending to be files** — no search, no versioning, no human interface. You get `read()` and `write()`, nothing else.
- **Read-only retrieval layers** — agents can search but can't write. The filesystem is a one-way mirror.
- **Flat markdown logs** — no structure, no queries, no importance scoring. The naive approach everyone outgrows.
- **Ephemeral sandboxes** — agent dies, files die. No persistence across sessions.
- **Proprietary SaaS** — locked to a vendor's ecosystem. Can't self-host, can't extend.

A real knowledge filesystem needs to be all of these at once:

- **Writable** — agents write with `cat`, `echo`, `curl`, or MCP tools. Not read-only, not API-only.
- **Searchable** — full-text (BM25) and semantic (vector) over the same files.
- **Queryable** — structured queries over typed metadata, not just keyword matching.
- **Trustworthy** — every write is a Git commit. Immutable audit trail. Crash recovery. Blame.
- **Human-readable** — a web UI with wiki links, backlinks, and graph view. Not pgAdmin.

KiwiFS is all five.

```
AGENT                              HUMAN
─────                              ─────
cat /kiwi/concepts/auth.md         Web UI (like Obsidian Publish
grep -r "timeout" /kiwi/             + Notion's block editor)
echo "# Report" > /kiwi/r.md      wiki links, graph view, search

       ↕                                ↕
     ┌──────────────────────────────────────┐
     │         Markdown files in folders    │
     │         (the single source of truth) │
     └──────────────────────────────────────┘
       ↕                ↕              ↕
    Git versioning   FTS5 + vector   SSE events
    (audit trail)    (search index)  (live updates)
```

## Why files, not a database

This is the core design decision. Every other choice follows from it.

**Files are the only format that is simultaneously human-readable, agent-native, and tool-agnostic.** `cat file.md` works in every shell, every container, every sandbox, every CI pipeline. No driver. No connection string. No SDK. The agent doesn't need to learn your API — it already knows the filesystem from training data.

A Postgres table is invisible to both agents and humans without custom tooling. A JSON blob requires parsing. A proprietary format requires a client library. Markdown files require nothing.

But raw files alone aren't enough. You need versioning, search, concurrency control, a web UI. KiwiFS layers database-like guarantees **on top of** files:

| Need | How KiwiFS solves it |
|---|---|
| **Versioning** | Git — every write is an atomic commit. Crash recovery, blame, diff, point-in-time restore. |
| **Search** | SQLite FTS5 (BM25 ranked) + pluggable vector search. Rebuildable from files anytime. |
| **Concurrency** | Optimistic locking via ETags (git blob hash). Standard HTTP `If-Match` / `409 Conflict`. |
| **Structured queries** | Frontmatter → SQLite `file_meta` table. Query by field, sort, filter. |
| **Audit trail** | Git commit log. SHA-1 hash chain. Tamper = broken chain. |
| **Real-time sync** | SSE broadcast on every write/delete. UI updates live. |

The files are the truth. Everything else is a derivative index you can rebuild.

> **"I already have this in Postgres."**
>
> Postgres stores your data. KiwiFS makes it *accessible* — to agents via `cat`/`grep`/NFS/S3, to humans via a web UI with wiki links and graph view, to auditors via `git blame`. If your agent's knowledge lives in Postgres, your humans need a custom UI to review it, your agents need SQL to query it, and you have no audit trail. KiwiFS gives you all three interfaces over the same files.

## The LLM Wiki pattern

KiwiFS implements [Karpathy's LLM Wiki](https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f) as production infrastructure. The pattern: raw sources in, compiled wiki out, agent maintains it over time.

```bash
kiwifs init --template knowledge
```

```
knowledge/
├── SCHEMA.md          # Agent instructions: ingest, query, lint
├── index.md           # Auto-maintained table of contents
├── log.md             # Append-only chronological record
├── concepts/          # One page per concept (agent-created)
├── entities/          # One page per named entity
├── episodes/          # Per-run episodic notes (memory_kind, episode_id)
└── reports/           # Chronological reports
```

The agent reads `SCHEMA.md` to understand how to maintain the wiki. Three operations:

- **Ingest** — process a new source, create/update wiki pages, update index + log.
- **Query** — search the wiki to answer a question.
- **Lint** — audit for orphan pages, broken links, contradictions, stale content.

Other templates: `wiki`, `runbook`, `research`, or start blank with `kiwifs init`.

---

## Features

### Web UI

Embedded in the binary via `go:embed`. No separate frontend deploy, no Node runtime. Obsidian's knowledge features (wiki links, backlinks, graph view) with a Notion-style block editor.

- **WYSIWYG editor** — block-based (BlockNote), drag handles, 15+ block types, slash commands
- **`[[Wiki links]]` + backlinks** — type `[[auth]]`, resolves to `concepts/authentication.md`. Backlinks panel shows "linked from 3 pages." This is Obsidian's core feature — notes are connected, not isolated.
- **Knowledge graph** — visual map of all pages and their connections (Sigma.js + ForceAtlas2). Same organic clustering as Obsidian's graph view.
- **Cmd+K search** — full-text with highlighted matches
- **Breadcrumbs** — `Knowledge > Concepts > Authentication`
- **Table of contents** — auto-generated, sticky sidebar, scroll tracking
- **Inline comments** — select text, add annotation. Stored in `.kiwi/comments/`, not in the markdown
- **Dark mode** — toggle a CSS class
- **Themeable** — CSS variables, Tailwind-based, drop into any shadcn/ui project

Built on shadcn/ui + Radix. Accessible. Beautiful by default. Fully customizable.

### Agent interface

When your agent has a real filesystem mount:

```bash
cat /kiwi/concepts/authentication.md
grep -r "timeout" /kiwi/
ls /kiwi/reports/
echo "# New finding" > /kiwi/reports/finding-042.md
```

When a real mount isn't available, agents use the REST API or MCP tools instead.

### MCP (Model Context Protocol)

```bash
kiwifs mcp --root ~/knowledge          # in-process, no server needed
kiwifs mcp --remote http://host:3333   # proxy to a running KiwiFS server
```

20 tools: `kiwi_read`, `kiwi_write`, `kiwi_search`, `kiwi_tree`, `kiwi_query_meta`, `kiwi_delete`, `kiwi_bulk_write`, `kiwi_rename`, `kiwi_query`, `kiwi_aggregate`, `kiwi_import`, `kiwi_export`, `kiwi_analytics`, `kiwi_memory_report`, `kiwi_view_refresh`, `kiwi_health_check`, `kiwi_changes`, `kiwi_append`, `kiwi_search_semantic`, `kiwi_backlinks`. Plus resources (`kiwi://schema`, `kiwi://file/{path}`, `kiwi://tree/{path}`).

**Claude Desktop / Cursor:**
```json
{
  "mcpServers": {
    "kiwifs": {
      "command": "kiwifs",
      "args": ["mcp", "--root", "/path/to/knowledge"]
    }
  }
}
```

### Search (three tiers)

```bash
kiwifs serve --search grep      # Tier 1: zero deps, exact match
kiwifs serve --search sqlite    # Tier 2: SQLite FTS5, BM25 ranked (default)
kiwifs serve                    # Tier 3: + vector search (if configured)
```

```
GET /api/kiwi/search?q=payment+timeout           → BM25 ranked results
GET /api/kiwi/search?q="connection reset" AND ws  → boolean + phrase search
POST /api/kiwi/search/semantic                    → vector similarity search
```

Vector search is pluggable — mix any embedder with any vector store:

| Embedder | Vector Store |
|---|---|
| OpenAI, Ollama, Cohere, Vertex AI, Bedrock, custom HTTP | sqlite-vec (default), Qdrant, pgvector, Pinecone, Weaviate, Milvus |

Default (sqlite-vec + OpenAI) needs one env var and zero infrastructure. For fully offline setups: Ollama + sqlite-vec, everything runs locally.

### Git versioning

Every write is an atomic git commit. Users never see Git — the API abstracts it.

```
GET /api/kiwi/versions?path=concepts/auth.md   → commit history
GET /api/kiwi/diff?path=auth.md&from=a1b&to=c3d → unified diff
GET /api/kiwi/blame?path=concepts/auth.md       → per-line attribution
```

What Git gives you for free: crash recovery, immutable audit trail (SHA-1 hash chain), point-in-time restore, tamper detection, replication via `git push`.

### Access protocols

Every protocol flows through the same storage layer. Every write — regardless of how it enters — gets a git commit, a search index update, and an SSE broadcast.

```
┌─────────────────────────────────────────┐
│              KiwiFS Server              │
│                                         │
│  REST API  │  NFS   │  S3    │ WebDAV   │
│   :3333    │ :2049  │ :3334  │  :3335   │
│            └────┬───┘        │          │
│       ──────────┴────────────┘          │
│    Storage → Git → Index → SSE          │
└─────────────────────────────────────────┘
```

| Protocol | Use case | Example |
|---|---|---|
| **REST API** | Web frontend, scripts | `curl localhost:3333/api/kiwi/file?path=index.md` |
| **MCP** | AI agents (Claude, Cursor, custom) | `kiwifs mcp --root ~/knowledge` |
| **NFS** | Docker, Kubernetes (native mount, no FUSE, no privileged) | `docker run --mount type=nfs,...` |
| **S3** | Backup, data pipelines, any tool that "supports S3" | `aws s3 sync s3://knowledge/ /backup/` |
| **WebDAV** | Windows mapped drives, legacy tools | Map Network Drive in Explorer |
| **FUSE** | Developer workstations, remote mount as local folder | `kiwifs mount --remote http://server:3333 ~/kiwi` |

### Structured metadata

Frontmatter from every markdown file is mirrored into a SQLite table. Query it:

```
GET /api/kiwi/meta?where=$.status=published&where=$.priority=high&sort=$.updated&order=desc
```

### DataView Query Language (DQL)

A query language for your knowledge base — think Obsidian Dataview, but server-side:

```bash
kiwifs query 'TABLE title, status, priority FROM "concepts" WHERE status = "draft" SORT priority DESC'
```

```
GET /api/kiwi/query?q=TABLE title, status FROM "reports" WHERE priority = "high"
```

Supports `TABLE`, `LIST`, `COUNT`, `DISTINCT` queries with `WHERE`, `SORT`, `GROUP BY`, `FLATTEN`, and implicit fields (`_path`, `_updated`, `_size`). Expressions, functions, and boolean logic all work.

**Computed views** — markdown files with `kiwi-view: true` in frontmatter auto-refresh their body from a DQL query:

```bash
kiwifs view create --query 'TABLE title, status FROM "concepts"' --output views/concepts.md
kiwifs view refresh   # re-run all view queries
```

### Aggregation

SQL-style aggregates over frontmatter fields:

```bash
kiwifs aggregate --group status --calc count,avg:priority
```

```
GET /api/kiwi/query/aggregate?group_by=status&calc=count,avg:priority
```

Functions: `count`, `avg`, `sum`, `min`, `max`. Optional `--where` filters and `--path-prefix` scoping.

### Computed frontmatter

Define expressions in config that are evaluated at index time and stored as virtual frontmatter fields:

```toml
# .kiwi/config.toml
[dataview]
computed_fields.age_days = "days_since(updated)"
computed_fields.is_long = "len(body) > 5000"
computed_fields.priority_score = "priority * 10 + len(tags)"
```

These fields appear in DQL queries and meta API responses alongside real frontmatter.

### Data import

Bulk-import data from 18 sources into your knowledge base. Each row becomes a markdown file with structured frontmatter:

```bash
kiwifs import --from postgres --dsn "postgres://..." --table users --root ./knowledge
kiwifs import --from csv --path data.csv --root ./knowledge
kiwifs import --from json --url https://api.example.com/data --root ./knowledge
```

| Category | Sources |
|---|---|
| **Databases** | PostgreSQL, MySQL, SQLite, MongoDB, DynamoDB, Redis, Elasticsearch |
| **Files** | CSV, JSON, JSONL, YAML, Excel |
| **SaaS** | Notion, Airtable, Google Sheets, Confluence |
| **Knowledge** | Obsidian vaults, Firebase/Firestore |

Features: idempotent upserts (re-importing skips unchanged rows), `--dry-run`, `--columns` filtering, `--primary-key` control, `_source` / `_source_id` tracking in frontmatter.

### Data export

Export your knowledge base to machine-readable formats for ML pipelines, backups, or analysis:

```bash
kiwifs export --format jsonl --output knowledge.jsonl
kiwifs export --format csv --include-embeddings --output dataset.csv
```

```
GET /api/kiwi/export?format=jsonl&include_content=true&include_embeddings=true
```

Formats: JSONL, CSV. Optional: `--include-content` (full markdown body), `--include-links` (wiki link graph), `--include-embeddings` (vector embeddings), `--columns` filtering. Writes a `.schema.json` sidecar when exporting embeddings.

### Analytics dashboard

Knowledge health metrics at a glance:

```bash
kiwifs analytics                     # text summary
kiwifs analytics --format json       # structured output
```

```
GET /api/kiwi/analytics → { total_pages, stale_pages, orphans, broken_links, ... }
GET /api/kiwi/health-check?path=concepts/auth.md → per-page health
```

Reports: total pages, stale page count + paths, orphan pages, broken links, empty pages, pages without frontmatter, link coverage percentage, recently updated pages.

### Provenance tracking

Know which agent run produced which knowledge:

```bash
curl -X PUT localhost:3333/api/kiwi/file?path=report.md \
  -H "X-Actor: agent:exec_abc" \
  -H "X-Provenance: run:run-249" \
  -d "# Run 249 Report..."
```

KiwiFS injects `derived-from` into the frontmatter automatically. Query later: "show me every page produced by run-249."

### Episodic and central memory

Model **per-run / episodic** notes separately from **semantic** concept pages, and use frontmatter `merged-from` to record which episodes were consolidated into a page. Run `kiwifs memory report` to list episodic files that are not yet referenced from any `merged-from` (for CI, dashboards, and merge jobs). Conventions, `[memory]` config, and a Go API live in [docs/MEMORY.md](docs/MEMORY.md).

### Multi-space

One server, multiple independent knowledge bases:

```
GET /api/kiwi/{space}/tree
GET /api/kiwi/{space}/file?path=...
```

Each space has its own root directory, git repo, search index. Spaces map to NFS exports and S3 buckets.

### Real-time events

```
GET /api/kiwi/events → SSE stream

event: write
data: {"path":"reports/finding-042.md","actor":"agent:exec_abc"}
```

UI updates live when knowledge changes. No polling.

### Permalinks

Set `public_url` in config and every API response includes stable, shareable URLs:

```
https://wiki.mycompany.com/page/concepts/authentication.md
```

- **SPA deep linking** — `/page/{path}` routes via HTML5 history (no `#` fragments)
- **Wiki link resolution** — `[[auth]]` resolves to the full permalink URL for external contexts (Slack, PR comments, exports)
- **X-Permalink header** — every file read returns the permalink in the response header
- **`KIWI_PUBLIC_URL` env var** — override config for Docker/CI without editing TOML

### Knowledge health

Built-in janitor scans your knowledge base for quality issues:

- **Stale page detection** — pages not reviewed within a configurable window (default 90 days)
- **Contradiction finder** — pages with conflicting claims on the same topic
- **Trust-ranked search** — pages marked `status: verified` or `source-of-truth: true` rank higher
- **Scheduled scans** — background janitor runs on a configurable interval (default 24h)
- **Share links** — password-protected public access to specific pages (bcrypt-hashed)

### CLI

Every feature is accessible via `kiwifs <command>`:

| Command | What it does |
|---|---|
| `kiwifs serve` | Start the server (REST API + web UI + optional NFS/S3/WebDAV) |
| `kiwifs init` | Scaffold a knowledge base from a template (`knowledge`, `wiki`, `runbook`, `research`, or blank) |
| `kiwifs mcp` | Start a Model Context Protocol server (for Claude, Cursor, etc.) |
| `kiwifs query` | Run a DQL query against the local index |
| `kiwifs import` | Bulk-import from 18 data sources (Postgres, CSV, Notion, etc.) |
| `kiwifs export` | Export knowledge base to JSONL or CSV |
| `kiwifs aggregate` | Run SQL aggregates (count, avg, sum, min, max) over frontmatter |
| `kiwifs analytics` | Knowledge health dashboard (stale, orphans, broken links) |
| `kiwifs view` | Manage computed views (create, refresh, list) |
| `kiwifs mount` | FUSE-mount a remote KiwiFS server as a local folder |
| `kiwifs reindex` | Rebuild search indexes from files (FTS5 + vector + metadata) |
| `kiwifs lint` | Validate knowledge base (orphan pages, broken links, missing frontmatter) |
| `kiwifs backup` | Push to a git remote for off-site backup |
| `kiwifs restore` | Clone from a git remote and rebuild indexes |
| `kiwifs janitor` | Run a knowledge health scan (stale pages, contradictions, orphans) |
| `kiwifs memory` | Report episodic vs `merged-from` coverage (see [docs/MEMORY.md](docs/MEMORY.md)) |

All commands support `--help` for full flag reference.

---

## Quickstart

### 1. Install

```bash
# One-line install (macOS / Linux)
curl -fsSL https://raw.githubusercontent.com/kiwifs/kiwifs/main/install.sh | sh
```

Or run with Docker:

```bash
docker run -v ./knowledge:/data -p 3333:3333 ameliaanhlam/kiwifs
```

Or build from source (requires Go 1.25+ and Node.js 20+):

```bash
git clone https://github.com/kiwifs/kiwifs.git && cd kiwifs
cd ui && npm install && npm run build && cd ..
go build -o kiwifs .
```

### 2. Initialize

```bash
kiwifs init --template knowledge --root ./knowledge
# Creates SCHEMA.md, index.md, log.md, concepts/, entities/, reports/, episodes/
```

### 3. Serve

```bash
kiwifs serve --root ./knowledge
# REST API on :3333, web UI at http://localhost:3333
```

### 4. Write from an agent

```bash
# Via filesystem (if mounted):
echo "# Authentication\n\nOAuth2 + JWT..." > ./knowledge/concepts/auth.md

# Via API:
curl -X PUT 'localhost:3333/api/kiwi/file?path=concepts/auth.md' \
  -H "X-Actor: my-agent" \
  -d "# Authentication\n\nOAuth2 + JWT..."
```

### 5. Browse in the web UI

Open `http://localhost:3333`. See `concepts/auth.md` rendered as a styled page with wiki links, backlinks, and table of contents.

---

## Configuration

```toml
# .kiwi/config.toml

[server]
port = 3333
host = "0.0.0.0"
public_url = "https://wiki.mycompany.com"  # enables permalinks

[storage]
root = "/data/knowledge"

[search]
engine = "sqlite"                # grep | sqlite | vector

[search.vector]
enabled = true
# Optional: lower for small CPUs/local embedders, default is 5.
worker_count = 1

[search.vector.embedder]
provider = "ollama"              # openai | ollama | cohere | bedrock | vertex | http
model = "nomic-embed-text"
# Optional for Ollama: Go duration string, default is 30s.
timeout = "120s"

[search.vector.store]
provider = "sqlite-vec"          # sqlite-vec | qdrant | pgvector | pinecone | weaviate | milvus

[versioning]
strategy = "git"                 # git | cow | none

[memory]
# episodes_path_prefix = "episodes/"   # optional; default episodes/

[auth]
type = "none"                    # none | apikey | perspace | oidc
```

CLI flags override config: `kiwifs serve --port 4000 --search sqlite --versioning git`.

---

## Deployment

### Docker

```bash
# Pull from Docker Hub
docker run -v ./knowledge:/data -p 3333:3333 ameliaanhlam/kiwifs

# Or build locally
docker build -t kiwifs .
docker run -v ./knowledge:/data -p 3333:3333 kiwifs
```

### Docker Compose (with vector search)

See `docker-compose.yml` in the repo for a ready-to-use setup with optional pgvector sidecar.

### Embedded in your app (Go library)

```go
import "github.com/kiwifs/kiwifs/pkg/kiwi"

srv, err := kiwi.New("/data/knowledge", kiwi.WithSearch("sqlite"))
if err != nil { log.Fatal(err) }
defer srv.Close()

// Mount as an HTTP handler alongside your own routes
mux.Handle("/knowledge/", http.StripPrefix("/knowledge", srv.Handler()))

// Or run standalone
log.Fatal(srv.ListenAndServe(":3333"))
```

### With NFS (Docker / Kubernetes)

```bash
kiwifs serve --root /data/knowledge --nfs --nfs-port 2049
```

```yaml
# Kubernetes PersistentVolume
apiVersion: v1
kind: PersistentVolume
spec:
  nfs:
    server: kiwifs.internal
    path: /
```

---

## REST API

```
GET    /health                              → {"status":"ok"}

GET    /api/kiwi/tree?path=                 → directory tree (JSON)
GET    /api/kiwi/file?path=                 → raw markdown + ETag
PUT    /api/kiwi/file?path=                 → write + git commit + re-index
DELETE /api/kiwi/file?path=                 → delete + git commit
POST   /api/kiwi/bulk                       → multi-file write, one commit
POST   /api/kiwi/rename                     → atomic rename ({"from":"...","to":"..."})
POST   /api/kiwi/file/append?path=          → append content to file

GET    /api/kiwi/changes?since=&limit=      → git-based change feed
GET    /api/kiwi/search?q=                  → full-text search (BM25)
POST   /api/kiwi/search/semantic            → vector search

GET    /api/kiwi/versions?path=             → git log for file
GET    /api/kiwi/version?path=&version=     → content at commit
GET    /api/kiwi/diff?path=&from=&to=       → unified diff
GET    /api/kiwi/blame?path=                → per-line attribution

GET    /api/kiwi/meta?where=$.field=val     → structured query over frontmatter
GET    /api/kiwi/backlinks?path=            → pages that link to this page
GET    /api/kiwi/toc?path=                  → heading outline
GET    /api/kiwi/events                     → SSE stream
POST   /api/kiwi/resolve-links             → resolve [[wiki-links]] to permalinks

GET    /api/kiwi/stale                      → pages past their review date
GET    /api/kiwi/contradictions             → pages with conflicting claims
GET    /api/kiwi/search/verified            → trust-ranked search (verified pages boosted)
GET    /api/kiwi/janitor                    → knowledge health scan
GET    /api/kiwi/memory/report              → episodic vs merged-from coverage (JSON)

GET    /api/kiwi/query?q=                   → DQL query (TABLE, LIST, COUNT, DISTINCT)
GET    /api/kiwi/query/aggregate            → aggregation (count, avg, sum, min, max)
POST   /api/kiwi/view/refresh              → refresh computed views
POST   /api/kiwi/import                    → bulk import from data source
GET    /api/kiwi/export                    → export to JSONL/CSV stream
GET    /api/kiwi/analytics                 → knowledge health dashboard
GET    /api/kiwi/health-check?path=        → per-page health metrics

POST   /api/kiwi/share                     → create a share link (password-protected)
GET    /api/kiwi/share                     → list active share links
DELETE /api/kiwi/share/:id                 → revoke a share link

POST   /api/kiwi/assets                     → upload binary asset (images, PDFs)
```

Writes accept `X-Actor` (git attribution), `X-Provenance` (lineage tracking), and `If-Match` (optimistic locking — 409 on conflict).

---

## Design principles

1. **Files are the source of truth.** Every artifact is a plain markdown file. No proprietary format. Delete the search index — the files remain. `cat file.md` always works.

2. **Two interfaces, one truth.** The web UI and the agent filesystem read/write the same files. No sync. No eventual consistency. One folder, two ways to access it.

3. **Search is derivative.** The FTS5 index, the vector index, the metadata table — all rebuildable from files. `kiwifs reindex` and you're back. The folder is the truth, never the index.

4. **Storage-agnostic.** KiwiFS depends on `open()`, `read()`, `write()`, `listdir()`. It doesn't care if the folder is on a laptop SSD, NFS, EFS, JuiceFS, or a FUSE-mounted S3 bucket.

5. **Git as the WAL.** Instead of building a custom write-ahead log, every write is a git commit. Crash recovery, audit trail, tamper detection, replication — all for free.

6. **Embeddable.** The Go library (`pkg/kiwi`) lets you embed KiwiFS in any Go application. The web UI components (`<KiwiTree />`, `<KiwiPage />`, `<KiwiEditor />`, `<KiwiSearch />`) are built for future standalone use as an npm package.

---

## POSIX compliance

KiwiFS stores real files on a real filesystem — not blobs in a database. The degree of POSIX compliance depends on the access path:

| Access path | POSIX level | Notes |
|---|---|---|
| **Direct filesystem** | Full | Real files, crash-safe atomic writes, mmap works |
| **NFS mount** | Near-full | Userspace NFSv3, symlinks, open-unlink, advisory locking, stable handles across restarts |
| **FUSE mount** | Near-full | Remote FUSE client, symlinks, directory rename, sub-second mtime, O_APPEND |
| **WebDAV** | Partial | MOVE/COPY/MKCOL/DELETE, buffered writes with spill-to-disk |
| **REST API** | N/A | HTTP semantics (ETag concurrency, not POSIX) |
| **S3 API** | N/A | S3-compatible, not POSIX |
| **MCP** | N/A | Tool calls, not file ops |

### What works

| POSIX semantic | NFS | FUSE | How |
|---|---|---|---|
| **Atomic writes** | Yes | Yes | `write → fsync → rename(tmp, target) → fsync(dir)` — the gold-standard crash-safe pattern |
| **rename(2)** | Yes | Yes | Files via pipeline (atomic); directories via bulk endpoint |
| **O_APPEND** | Yes | Yes | FUSE fetches existing content on open, writes at correct EOF offset |
| **O_TRUNC / ftruncate** | Yes | Yes | NFS `Truncate()` with 64MB bounds; FUSE `Setattr` with `FATTR_SIZE` |
| **Symlinks** | Yes | Yes | Real `os.Symlink` on NFS; `Content-Type: application/x-symlink` on FUSE + REST |
| **readlink** | Yes | Yes | NFS via `os.Readlink`; FUSE via `/api/kiwi/readlink`; REST API endpoint |
| **Open-then-delete** | Yes | — | NFS defers deletion until last file handle closes (POSIX unlink semantics) |
| **fsync** | Yes | Yes | NFS `Sync()` pushes through pipeline; FUSE `Fsync()` PUTs to server |
| **Sub-second mtime** | — | Yes | FUSE `Getattr` reports `Mtimensec` from `Last-Modified` header |
| **Advisory locking** | Yes | — | NFS has process-local `Lock()`/`Unlock()` per file handle |
| **Directory rename** | Yes | Yes | FUSE calls `/api/kiwi/rename-dir`; NFS uses `os.Rename` + bulk re-index |
| **readdir** | Yes | Yes | Both hide internal dirs (`.git`, `.kiwi`) |
| **stat** | Yes | Yes | Size, mode, mtime — real values, not synthetic |
| **EFBIG on oversize** | Yes | Yes | 64MB `maxFileSize` limit returns proper errno / HTTP error |
| **mmap** | — | Passthrough | Works on NFS mount (kernel handles it); FUSE is over HTTP so no kernel mmap |
| **Path safety** | Yes | Yes | `GuardPath` blocks traversal, null bytes, control chars, 255-byte segment limit |

### Concurrency & durability

- **Optimistic locking** — ETags (content hash). Writes with `If-Match` headers get HTTP 409 on conflict. `If-Match: *` is handled per RFC 7232 §3.1.
- **Serialized writes** — the pipeline serializes all writes through a single mutex, so concurrent writers are safely queued regardless of protocol.
- **Single-instance guard** — `flock(2)` on `.kiwi/server.lock` prevents two servers from sharing the same data directory (SQLite + git corruption).
- **Crash recovery** — stale `index.lock` files are cleaned by a background watcher (10s interval, 60s threshold). Git subprocesses receive SIGTERM before SIGKILL, giving them a chance to release locks.
- **Line-ending integrity** — `core.autocrlf=false` + `* -text` in `.gitattributes` ensures ETags always match raw content. Writes to `.gitattributes` are blocked by the API.
- **Frontmatter bomb protection** — YAML frontmatter blocks exceeding 64KB are silently treated as empty (headings still extracted).
- **Stable NFS handles** — file handles are derived from `SHA-256(namespaceUUID + path)`, surviving server restarts. No more ESTALE.

### What is intentionally not supported

| POSIX semantic | Why |
|---|---|
| **Hard links** | Would break git versioning (one blob, multiple paths) |
| **chmod / chown** | No user/group model — auth is API-key / OIDC, not POSIX uid/gid |
| **POSIX ACLs** | Same reason — access control is at the HTTP/space level |
| **Extended attributes (xattr)** | Frontmatter serves the same purpose |
| **Distributed locking** | Locks are process-local; use `If-Match` for cross-client concurrency |

---

## How it compares

| | KiwiFS | agent-vfs | ChromaFS | Obsidian | Outline | Confluence |
|---|---|---|---|---|---|---|
| **Writable** (agents can create/update) | Yes | Yes | No (read-only) | Local only | API only | No |
| **Agent-native** (`cat`/`grep`/NFS) | Yes | Virtual only | Read-only | Local only | API only | No |
| **Web UI** (Notion-like) | Yes | No | No | Desktop | Yes | Yes |
| **Versioned** (git audit trail) | Yes | No | No | No | Limited | Plugin ($$$) |
| **Searchable** (FTS + vector) | Yes | No | Chroma | Plugins | Yes | Yes |
| **Query language** (DQL) | Yes | No | No | Plugin | No | No |
| **Data import** (18 sources) | Yes | No | No | No | API | No |
| **Export** (JSONL/CSV + embeddings) | Yes | No | No | No | Markdown | PDF only |
| **Single binary** | Yes | No | No | No | No | No (SaaS) |
| **Embeddable** (Go library) | Yes | No | No | No | No | No |
| **Self-hosted** | Yes | Yes | Yes | N/A | Yes | No |
| **Files are the truth** | Yes | DB is truth | DB is truth | Yes | Postgres | Proprietary DB |

---

## Who this is for

**AI agent builders** — Give your agent persistent, searchable memory it accesses with `cat`. Users browse the same files in a web UI with wiki links and graph view. Knowledge compounds across sessions instead of vanishing.

**Teams replacing Confluence / Notion** — Obsidian's file-first philosophy with a web UI anyone on your team can use. `git clone` your entire wiki. Self-hosted. No vendor lock-in.

**Compliance-heavy industries** — Every change is a git commit with SHA-1 hash chain. Immutable audit trail. `git blame` for per-line attribution. An auditor can verify with standard tools.

**DevOps / platform teams** — Runbooks that agents update after every incident. Humans review in the UI. No more docs that rot after three months.

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  KiwiFS                                    single Go binary
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │  Web UI (embedded via go:embed)                    │  │
│  │  shadcn/ui · BlockNote · react-markdown · Sigma.js │  │
│  └────────────────────┬───────────────────────────────┘  │
│                       │                                  │
│  ┌────────────────────▼───────────────────────────────┐  │
│  │  Access: REST :3333 · NFS :2049 · S3 :3334 · WebDAV│  │
│  └────────────────────┬───────────────────────────────┘  │
│                       │                                  │
│  ┌────────────────────▼───────────────────────────────┐  │
│  │  Core                                              │  │
│  │  Storage · Git versioning · FTS5 + Vector search   │  │
│  │  Watcher (fsnotify) · SSE events · Schema/lint     │  │
│  └────────────────────┬───────────────────────────────┘  │
│                       │                                  │
│  ┌────────────────────▼───────────────────────────────┐  │
│  │  .git/ (audit WAL)  ·  .kiwi/state/ (indexes)      │  │
│  └────────────────────┬───────────────────────────────┘  │
│                       │                                  │
│  ┌────────────────────▼───────────────────────────────┐  │
│  │  Filesystem: local · NFS · EFS · JuiceFS · FUSE-S3 │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

## Inspired by

- **[PocketBase](https://pocketbase.io)** — single Go binary, zero config, just works. KiwiFS brings the same philosophy to knowledge infrastructure.
- **[Obsidian](https://obsidian.md)** — files are the database. Wiki links. Graph view. KiwiFS is Obsidian for the web — plus an agent interface and an API.
- **[Karpathy's LLM Wiki](https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f)** — raw sources in, compiled wiki out, agent maintains it. KiwiFS is the production runtime for this pattern.
- **[Mintlify ChromaFS](https://www.mintlify.com/blog/how-we-built-a-virtual-filesystem-for-our-assistant)** — the filesystem is the best agent interface. Agents already know `cat`/`grep`/`ls`.
- **[Confluence](https://www.atlassian.com/software/confluence)** / **[Notion](https://notion.so)** — great UIs, but your content is locked in their database. KiwiFS gives you the editing experience without the lock-in.

## License

[Business Source License 1.1](LICENSE) — free to use, self-host, and modify. The only restriction: you can't offer KiwiFS as a commercial hosted service. Each release converts to Apache 2.0 after 4 years.

If you want to offer KiwiFS as a managed service or need a commercial license, [get in touch](mailto:amelia.anh.lam@gmail.com).

"KiwiFS" and the KiwiFS logo are trademarks of the KiwiFS Authors. See [LICENSE](LICENSE) for trademark usage guidelines.

## Contributors

<a href="https://github.com/kiwifs/kiwifs/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=kiwifs/kiwifs&columns=6&max=20&v=2" width="200" />
</a>
