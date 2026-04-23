# Contributing to KiwiFS

Thanks for your interest in contributing! KiwiFS is early-stage and we welcome all kinds of help — bug reports, feature requests, docs improvements, and code.

## Getting started

### Prerequisites

- Go 1.25+
- Node.js 20+ (for the web UI)
- Git

### Local development

```bash
git clone https://github.com/amelia751/kiwifs.git
cd kiwifs

# Build the frontend
cd ui && npm install && npm run build && cd ..

# Run the server
go run . serve --root ./knowledge --port 3333

# Run tests
go test ./... -race

# Build the binary
go build -o kiwifs .
```

### Project structure

```
kiwifs/
├── cmd/              CLI commands (serve, init, reindex, mount)
├── internal/
│   ├── api/          REST API handlers
│   ├── bootstrap/    Dependency wiring
│   ├── pipeline/     Write pipeline (git + index + SSE)
│   ├── search/       grep + SQLite FTS5
│   ├── storage/      Filesystem abstraction
│   ├── vectorstore/  Vector search backends
│   ├── versioning/   Git, copy-on-write, noop
│   ├── nfs/          NFS server
│   ├── s3/           S3-compatible API
│   ├── webdav/       WebDAV server
│   ├── fuse/         FUSE client
│   └── ...
├── ui/               React + TypeScript + shadcn/ui
└── main.go
```

## How to contribute

### Reporting bugs

Open a [GitHub issue](https://github.com/amelia751/kiwifs/issues/new?template=bug_report.md). Include:

- What you did
- What you expected
- What happened instead
- KiwiFS version (`kiwifs --version`), OS, Go version

### Suggesting features

Open a [GitHub issue](https://github.com/amelia751/kiwifs/issues/new?template=feature_request.md) or start a [Discussion](https://github.com/amelia751/kiwifs/discussions). Describe the use case, not just the solution.

### Submitting code

1. Fork the repo and create a branch from `main`.
2. Make your changes. Add tests if you're touching backend code.
3. Run `go test ./... -race` and `go vet ./...` — CI will check these too.
4. Open a pull request. Describe what you changed and why.

### Code style

- **Go**: standard `gofmt`. No linter config beyond `go vet`.
- **TypeScript**: Prettier defaults. Tailwind for styling.
- **Commits**: short summary line, present tense ("Add search endpoint", not "Added search endpoint").

### What we're looking for help with

Check the [issues labeled `good first issue`](https://github.com/amelia751/kiwifs/labels/good%20first%20issue) for starter tasks. Areas where help is especially welcome:

- **Testing** — more integration tests, especially for NFS/S3/WebDAV protocols
- **Documentation** — usage guides, examples, config reference
- **Frontend** — UI polish, accessibility, new components
- **Search** — improving FTS5 ranking, vector search UX

## License

By contributing, you agree that your contributions will be licensed under the [Business Source License 1.1](LICENSE). Contributions will convert to Apache 2.0 along with the rest of the codebase per the BSL terms.
