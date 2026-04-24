# KiwiFS — The Knowledge Filesystem

A filesystem-based knowledge system. Agents write with `cat`. Humans read in the web UI. Same files.

One binary. Storage-agnostic. Git-versioned. Embeddable.

> **Note:** This npm package is not yet published. For now, build from source — see the [main README](https://github.com/amelia751/kiwifs).

## Installation (once published)

### Global install

```bash
npm install -g kiwifs
```

### npx (no install needed)

```bash
npx kiwifs serve --root ~/my-knowledge --port 3333
```

## Usage

### CLI

```bash
# Initialize a new knowledge base
kiwifs init ~/my-knowledge

# Start the server
kiwifs serve --root ~/my-knowledge --port 3333

# Mount a remote KiwiFS as a local folder (FUSE)
kiwifs mount --remote http://localhost:3333 ~/mounted-knowledge
```

## Protocols

KiwiFS supports multiple access protocols:

- **REST API** (`:3333`) — JSON API for CRUD operations
- **NFS** (`--nfs --nfs-port 2049`) — Native filesystem mount for Docker/K8s
- **S3 API** (`--s3 --s3-port 3334`) — S3-compatible API for aws cli, boto3, rclone
- **WebDAV** (`--webdav --webdav-port 3335`) — Windows/macOS network drive
- **FUSE** (`kiwifs mount`) — Mount remote KiwiFS as local folder

## Features

- **Files are the source of truth** — Plain markdown files, no database
- **Git-versioned** — Every change is a commit with full history
- **Full-text search** — SQLite FTS5 with BM25 ranking
- **Semantic search** — Vector embeddings (OpenAI, Ollama, Cohere, Bedrock, Vertex)
- **Wiki links** — `[[page-name]]` syntax with backlinks
- **Web UI** — Obsidian + Confluence fusion with graph view
- **Real-time events** — SSE stream for live updates
- **File watcher** — Detects external writes, auto-commits

## Configuration

Create `.kiwi/config.toml` in your knowledge root:

```toml
[server]
port = 3333
host = "0.0.0.0"

[search]
engine = "sqlite"  # or "grep"

  [search.vector]
  enabled = true

    [search.vector.embedder]
    provider = "openai"
    model = "text-embedding-3-small"
    api_key = "${OPENAI_API_KEY}"

    [search.vector.store]
    provider = "sqlite"  # or "qdrant", "pgvector", "pinecone", "weaviate"

[versioning]
strategy = "git"  # or "cow" or "none"
```

## Documentation

- [GitHub](https://github.com/amelia751/kiwifs)

## License

[BSL 1.1](https://github.com/amelia751/kiwifs/blob/main/LICENSE)
