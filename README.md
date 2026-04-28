<p align="center">
  <img src="kiwifs.png" alt="KiwiFS" width="200" />
</p>

<h1 align="center">KiwiFS</h1>

<p align="center">
  <strong>The knowledge server.</strong>
</p>

<p align="center">
  PocketBase for Knowledge — one Go binary, zero config. Obsidian's file-first philosophy. Agents write with <code>cat</code>. Humans read in a wiki. Git versions everything.
</p>

<p align="center">
  <a href="https://github.com/kiwifs/kiwifs/actions/workflows/ci.yml"><img src="https://github.com/kiwifs/kiwifs/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-BSL--1.1-blue" alt="License: BSL 1.1"></a>
  <a href="https://github.com/kiwifs/kiwifs"><img src="https://img.shields.io/badge/go-1.25-00ADD8?logo=go&logoColor=white" alt="Go 1.25"></a>
  <a href="https://github.com/kiwifs/kiwifs"><img src="https://img.shields.io/badge/single_binary-yes-green" alt="Single Binary"></a>
  <a href="https://github.com/kiwifs/kiwifs"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen" alt="PRs Welcome"></a>
</p>

```bash
curl -fsSL https://raw.githubusercontent.com/kiwifs/kiwifs/main/install.sh | sh
kiwifs init ./knowledge && kiwifs serve --root ./knowledge
# Open http://localhost:3333
```

---

## The problem

Every agent memory project gives you **storage**. A database table. A vector index. A key-value store.

But storage is only one layer. In practice you need:

- **An agent interface** — agents already know `cat`, `grep`, `ls`. They don't need a custom SDK.
- **A human interface** — someone has to review what the agent wrote. An Obsidian-like UI, not pgAdmin.
- **An audit trail** — who changed what, when, and why. Cryptographic, not "trust the vendor."
- **Search** — full-text (BM25) and semantic (vector), over the same files.

No existing tool does all four. Obsidian has files but no web UI and no agent interface. Confluence has a web UI but your content is trapped in a database. Outline has a web UI but content lives in Postgres. agent-vfs gives you a virtual FS but no UI, no versioning, no search.

KiwiFS is the full stack.

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

7 tools: `kiwi_read`, `kiwi_write`, `kiwi_search`, `kiwi_tree`, `kiwi_query_meta`, `kiwi_delete`, `kiwi_bulk_write`. Plus resources (`kiwi://schema`, `kiwi://file/{path}`, `kiwi://tree/{path}`).

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

### Provenance tracking

Know which agent run produced which knowledge:

```bash
curl -X PUT localhost:3333/api/kiwi/file?path=report.md \
  -H "X-Actor: agent:exec_abc" \
  -H "X-Provenance: run:run-249" \
  -d "# Run 249 Report..."
```

KiwiFS injects `derived-from` into the frontmatter automatically. Query later: "show me every page produced by run-249."

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
| `kiwifs mount` | FUSE-mount a remote KiwiFS server as a local folder |
| `kiwifs reindex` | Rebuild search indexes from files (FTS5 + vector + metadata) |
| `kiwifs lint` | Validate knowledge base (orphan pages, broken links, missing frontmatter) |
| `kiwifs backup` | Push to a git remote for off-site backup |
| `kiwifs restore` | Clone from a git remote and rebuild indexes |
| `kiwifs janitor` | Run a knowledge health scan (stale pages, contradictions, orphans) |

All commands support `--help` for full flag reference.

---

## Quickstart

### 1. Install

```bash
# One-line install (macOS / Linux)
curl -fsSL https://raw.githubusercontent.com/kiwifs/kiwifs/main/install.sh | sh
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
# Creates SCHEMA.md, index.md, log.md, concepts/, entities/, reports/
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

[search.vector.embedder]
provider = "openai"              # openai | ollama | cohere | bedrock | vertex | http
model = "text-embedding-3-small"
api_key = "${OPENAI_API_KEY}"

[search.vector.store]
provider = "sqlite-vec"          # sqlite-vec | qdrant | pgvector | pinecone | weaviate | milvus

[versioning]
strategy = "git"                 # git | cow | none

[auth]
type = "none"                    # none | apikey | perspace | oidc
```

CLI flags override config: `kiwifs serve --port 4000 --search sqlite --versioning git`.

---

## Deployment

### Docker

```bash
# Build locally (pre-built images coming soon)
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

## How it compares

| | KiwiFS | agent-vfs | ChromaFS | Obsidian | Outline | Confluence |
|---|---|---|---|---|---|---|
| **Agent-native** (`cat`/`grep`/NFS) | Yes | Virtual only | Read-only | Local only | API only | No |
| **Web UI** (Notion-like) | Yes | No | No | Desktop | Yes | Yes |
| **Versioned** (git audit trail) | Yes | No | No | No | Limited | Plugin ($$$) |
| **Searchable** (FTS + vector) | Yes | No | Chroma | Plugins | Yes | Yes |
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

- **[PocketBase](https://pocketbase.io)** — single Go binary, zero config, just works. KiwiFS is PocketBase for knowledge.
- **[Obsidian](https://obsidian.md)** — files are the database. Wiki links. Graph view. KiwiFS is Obsidian for the web — plus an agent interface and an API.
- **[Karpathy's LLM Wiki](https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f)** — raw sources in, compiled wiki out, agent maintains it. KiwiFS is the production runtime for this pattern.
- **[Mintlify ChromaFS](https://www.mintlify.com/blog/how-we-built-a-virtual-filesystem-for-our-assistant)** — the filesystem is the best agent interface. Agents already know `cat`/`grep`/`ls`.
- **[Confluence](https://www.atlassian.com/software/confluence)** / **[Notion](https://notion.so)** — great UIs, but your content is locked in their database. KiwiFS gives you the editing experience without the lock-in.

## License

[Business Source License 1.1](LICENSE) — free to use, self-host, and modify. The only restriction: you can't offer KiwiFS as a commercial hosted service. Each release converts to Apache 2.0 after 4 years.

If you want to offer KiwiFS as a managed service or need a commercial license, [get in touch](mailto:amelia.anh.lam@gmail.com).

"KiwiFS" and the KiwiFS logo are trademarks of the KiwiFS Authors. See [LICENSE](LICENSE) for trademark usage guidelines.

## Contributors

<a href="https://github.com/PranavChahal"><img src="https://avatars.githubusercontent.com/u/76513953?v=4" width="50" height="50" alt="Pranav" style="border-radius:50%"></a>
