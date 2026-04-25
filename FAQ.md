# Frequently Asked Questions

## General

### What is KiwiFS?

KiwiFS is a knowledge server — a single Go binary that serves markdown files with a web UI, git versioning, full-text + vector search, and multi-protocol access (REST, NFS, S3, WebDAV, FUSE, MCP). Think PocketBase for knowledge, or Obsidian with a web UI and an agent interface.

### How is this different from Obsidian?

Obsidian is a desktop app. KiwiFS is a server. Obsidian's core insight — files are the database, wiki links connect them — is brilliant. KiwiFS takes that insight and adds: a web UI anyone can access (no app install), an agent interface (agents write with `cat`, not a custom SDK), git versioning (immutable audit trail), and full-text + vector search over the same files.

### How is this different from Outline / Notion / Confluence?

Those tools store your content in a proprietary database. You need their UI to read it, their API to query it, and their export tool to leave. KiwiFS stores everything as plain markdown files. `cat file.md` always works. `git clone` gets you everything. No lock-in.

### Do I need to know Git?

No. Git runs under the hood — every write is an atomic commit — but users never interact with it directly. The API and UI abstract it completely. Git is the audit trail, not a user interface.

### Is KiwiFS production-ready?

KiwiFS is in active development (v0.1). The core is stable — file CRUD, search, versioning, web UI, MCP, and all access protocols work. We use it in production internally. That said, APIs may evolve before v1.0.

---

## Installation & Setup

### What are the system requirements?

Just the binary. KiwiFS is a single statically-linked Go binary with zero runtime dependencies. SQLite is embedded (pure Go, no CGo). The web UI is embedded via `go:embed`. It runs on macOS, Linux, and in Docker.

### How do I install it?

```bash
# One-line install (macOS / Linux) — downloads the pre-built binary
curl -fsSL https://raw.githubusercontent.com/amelia751/kiwifs/main/install.sh | sh
```

Or build from source (requires Go 1.25+ and Node.js 20+):

```bash
git clone https://github.com/amelia751/kiwifs.git && cd kiwifs
cd ui && npm install && npm run build && cd ..
go build -o kiwifs .
```

Docker (build locally):

```bash
docker build -t kiwifs .
docker run -v ./knowledge:/data -p 3333:3333 kiwifs
```

### What's in `.kiwi/`?

The `.kiwi/` directory inside your knowledge root stores derived state — search indexes, comment data, config. It is **not** your content. You can delete it and KiwiFS will rebuild it from files on next startup (`kiwifs reindex`).

```
.kiwi/
├── config.toml       # Server and search configuration
├── state/
│   └── search.db     # SQLite FTS5 + metadata + vector indexes
├── comments/         # Inline comment annotations (JSON)
└── templates/        # Page templates for slash commands
```

---

## Agent Integration

### How do agents write to KiwiFS?

Three ways, depending on what your agent has access to:

1. **Filesystem** — if you mount KiwiFS via NFS or FUSE, the agent uses `cat`, `echo`, `grep`, `ls` directly. No SDK.
2. **REST API** — `curl -X PUT localhost:3333/api/kiwi/file?path=page.md -d "content"`.
3. **MCP** — `kiwifs mcp --root ~/knowledge` gives Claude, Cursor, or any MCP-compatible agent structured tools (`kiwi_read`, `kiwi_write`, `kiwi_search`, etc.).

### What is MCP and why does KiwiFS support it?

[Model Context Protocol](https://modelcontextprotocol.io) is a standard for connecting AI agents to external tools. KiwiFS's MCP server exposes 7 tools and 3 resources, so any MCP-compatible agent can read, write, search, and query your knowledge base without custom integration code.

### Can agents use KiwiFS without a running server?

Yes. `kiwifs mcp --root ~/knowledge` runs entirely in-process — it opens the knowledge directory directly, no HTTP server needed. This is ideal for local agent setups where you don't want to run a separate service.

### How does provenance tracking work?

When an agent writes, it can include `X-Provenance` and `X-Actor` headers. KiwiFS injects this into the file's YAML frontmatter as a `derived-from` entry. You can later query "what did run-249 produce?" via the metadata API.

---

## Search

### What search backends are available?

Three tiers, configurable at startup:

| Tier | Engine | Use case |
|---|---|---|
| 1 | `grep` | Zero deps, exact match, tiny knowledge bases |
| 2 | `sqlite` (default) | SQLite FTS5, BM25 ranked, handles thousands of files |
| 3 | `vector` | Semantic similarity via embeddings, on top of tier 2 |

### Can I run vector search without an API key?

Yes. Use Ollama (`provider = "ollama"`) with sqlite-vec as the vector store. Ollama runs locally on your machine, and sqlite-vec is embedded in the binary — no external API calls, fully offline.

### How do I rebuild the search index?

```bash
kiwifs reindex --root ./knowledge
```

This rebuilds FTS5, vector embeddings, metadata, and wiki link indexes from the files on disk. The files are always the source of truth.

---

## Web UI

### Can I embed the UI in my own app?

The web UI is built as React components (`<KiwiTree />`, `<KiwiPage />`, `<KiwiEditor />`, `<KiwiSearch />`, `<KiwiGraph />`), currently embedded in the binary via `go:embed`. A standalone `kiwifs-ui` npm package for embedding in your own React app is on the roadmap — see [ROADMAP.md](ROADMAP.md).

### Can I customize the theme?

Yes. The UI uses CSS variables prefixed with `--kiwi-`. KiwiFS ships with several built-in themes (kiwi, ocean, forest, sunset, neutral) and you can create your own. It's built on shadcn/ui + Tailwind, so it drops into any shadcn project.

### Does the web UI work on mobile?

The UI is responsive. On viewports ≤900 px (phone-sized), the sidebar becomes a hamburger drawer with a backdrop scrim; tap a page and the drawer auto-closes. Reading is first-class. Editing works but a full keyboard (iPad with attached keyboard, or a laptop) is still a noticeably better experience.

### Is the UI accessible?

Every icon-only button in the toolbar has an `aria-label` and a `title` tooltip; toggles expose `aria-pressed`. Error banners translate common backend errors (401, 404, 409, 412, 413, 429, `sql: no rows in result set`) into human sentences with an expandable "Technical details" section for developers.

### Do I get a first-run tour?

Yes — the UI shows a five-card tour the first time you load it. It covers the sidebar, wikilinks, the graph, the Knowledge Janitor, and trust/workflow frontmatter. Dismissal is per-browser (localStorage). Reset with:

```js
localStorage.removeItem("kiwifs-tour-dismissed");
```

---

## Reverse proxies and SSE

### I'm behind nginx / Cloudflare / an ALB. Do events still work?

They do, but you have to disable response buffering on the SSE route or clients will only see events every ~30 s when the proxy flushes. KiwiFS sends `Cache-Control: no-cache` and an event heartbeat, but most proxies need explicit opt-outs as well.

**nginx**

```nginx
location /api/kiwi/events {
  proxy_pass http://kiwifs-backend;
  proxy_http_version 1.1;
  proxy_set_header Connection "";
  proxy_buffering off;
  proxy_cache off;
  proxy_read_timeout 1h;
  chunked_transfer_encoding on;
}
```

**Caddy**

```caddy
kiwi.example.com {
  reverse_proxy /api/kiwi/events kiwifs-backend {
    flush_interval -1
    transport http {
      read_buffer 0
    }
  }
  reverse_proxy kiwifs-backend
}
```

**Cloudflare**

The standard Cloudflare proxy buffers SSE. Either bypass the proxy
(grey-cloud the DNS record) for the KiwiFS host, or add a **Cache
Rule** that sets Cache Level to "Bypass" for `/api/kiwi/events`. Free
tier works either way; Enterprise can use the `Cache-Control:
no-transform` header we already send.

**AWS ALB**

Set the target group's **HTTP keep-alive** to at least the expected
SSE idle window (default is 60 s — bump it to 3600 s for long-lived
editor sessions) and disable any "sticky sessions" LB features that
buffer responses. ALBs don't buffer SSE bodies themselves, but an
idle-timeout drop manifests as "events pause at exactly 60 s".

### Does the SPA path need special handling?

Only the fallback route. The UI is served from `/` with a SPA
catch-all that returns `index.html`, so make sure the proxy doesn't
treat unknown routes as 404. Example nginx:

```nginx
location / {
  proxy_pass http://kiwifs-backend;
  # Fall back to the SPA for any unknown path; the backend already does
  # this, so we just need to not intercept.
}
```

### Why do I see "events pause then resume"?

Three usual suspects, in order of likelihood:

1. A proxy buffering the response (see the nginx / ALB examples above).
2. A corporate proxy killing idle HTTP/1.1 connections. KiwiFS sends a
   `:keep-alive` comment every 15 s specifically to keep these hops
   happy — if your proxy strips comments, raise
   `[server] sse_heartbeat = "5s"` in config.
3. The browser tab backgrounded. Chrome throttles timers on hidden
   tabs; EventSource reconnects on focus. This is normal.

### Can I use EventSource through an auth header?

Browsers don't let EventSource attach custom headers. KiwiFS supports
`?token=<bearer>` as a query-string fallback for the `events` endpoint
exactly for this reason — the server echoes it into the same
middleware the Authorization header would hit. Use HTTPS in
production so the token isn't logged by intermediaries.

### How do I know presence + janitor events made it through?

The `/metrics` endpoint exposes a live gauge:

```
kiwifs_sse_subscribers 4
```

If the number is zero but you expect clients, the proxy isn't
keeping the stream alive. Curl the endpoint directly to verify:

```bash
curl -N -H "Authorization: Bearer $KIWI_API_KEY" \
  https://kiwi.example.com/api/kiwi/events
```

You should see `:keep-alive` comments every 15 s and real events when
you write to a page in another tab.

---

## Access Protocols

### When should I use NFS vs S3 vs WebDAV vs FUSE?

| Protocol | Best for |
|---|---|
| **NFS** | Docker/Kubernetes native mounts — no FUSE, no privileged containers |
| **S3** | Backup tools, data pipelines, anything that "supports S3" |
| **WebDAV** | Windows mapped drives, legacy tools |
| **FUSE** | Developer workstations — mount a remote KiwiFS as a local folder |
| **REST API** | Web frontends, scripts, CI/CD |
| **MCP** | AI agents (Claude, Cursor, custom) |

### Do all protocols go through the same pipeline?

Yes. Every write — regardless of how it enters (REST, NFS, S3, WebDAV, FUSE, MCP) — flows through the same pipeline: storage write → git commit → search index update → SSE broadcast. Consistency is guaranteed.

---

## Data & Durability

### What happens if KiwiFS crashes mid-write?

Every write is an atomic git commit. If KiwiFS crashes, git's reflog provides crash recovery. On restart, KiwiFS detects and recovers from interrupted state.

### How do I back up my knowledge base?

Your knowledge base is a folder of markdown files with a `.git` directory. You can:

- `git push` to any git remote (GitHub, GitLab, your own server)
- `kiwifs backup` for one-shot push to a configured git remote
- Configure `[backup]` in `.kiwi/config.toml` for automatic scheduled pushes
- `aws s3 sync` via the S3 protocol
- Plain `rsync` or `cp` — the files are the truth

### Can I migrate from Obsidian / Notion / Confluence?

Import tooling (`kiwifs import --from obsidian|notion|confluence`) is on the [roadmap](ROADMAP.md). In the meantime, Obsidian vaults work almost directly — copy the `.md` files and run `kiwifs reindex`. Notion and Confluence exports need manual link fixup.

---

## Deployment

### What's the recommended production setup?

```bash
# Build the image
docker build -t kiwifs .

# Run with persistent storage
docker run -d --restart always \
  -v /data/knowledge:/data \
  -p 3333:3333 \
  kiwifs serve --root /data --search sqlite --versioning git
```

For persistent vector search with pgvector, see the `docker-compose.yml` in the repo.

### Can I run multiple knowledge bases on one server?

Yes. Multi-space support lets one server host multiple independent knowledge bases, each with its own root directory, git repo, and search index:

```
GET /api/kiwi/{space}/tree
GET /api/kiwi/{space}/file?path=...
```

---

## License

### Can I use KiwiFS commercially?

Yes. KiwiFS is licensed under [BSL 1.1](LICENSE). You can use, self-host, modify, and embed it in commercial products. The only restriction: you cannot offer KiwiFS itself as a commercial hosted service (i.e., selling "KiwiFS-as-a-Service"). Each release converts to Apache 2.0 after 4 years.

### Can I contribute?

Absolutely. See [CONTRIBUTING.md](CONTRIBUTING.md). By contributing, you agree that your contributions are licensed under BSL 1.1 (converting to Apache 2.0 per the license terms).
