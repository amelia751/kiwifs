# KiwiFS — Active Todolist

What's left to build next, in priority order.

---

## 1. Codebase slim-down (`refactor/slim` branch)

Mechanical refactoring — no feature changes, no behavior changes.
Run `go test ./...` after each step to verify zero regressions.

### Step 1 — `cmd/init.go`: embed templates (~450 LOC removed)

The file is 80% markdown strings with ugly backtick concatenation.
Move all template content to real `.md` files under `cmd/templates/`
and load them via `//go:embed`.

1. Create directory structure:
   ```
   cmd/templates/
   ├── agent-knowledge/
   │   ├── SCHEMA.md
   │   ├── index.md
   │   ├── log.md
   │   └── concepts/.gitkeep
   ├── team-wiki/
   │   ├── index.md
   │   └── ...
   ├── runbook/
   │   └── ...
   ├── research/
   │   └── ...
   ├── gitignore.txt          (default .gitignore)
   └── config.toml            (default config)
   ```
2. Move each template string from `init.go` into its corresponding file
3. Add `//go:embed templates` in `init.go`
4. Replace inline strings with `fs.ReadFile(templates, "templates/agent-knowledge/SCHEMA.md")`
5. Delete all the `const decisionTemplate = ...` and `map[string]string{...}` blocks
6. **Test:** `go build ./... && go test ./...`

### Step 2 — Split `handlers.go` into domain files (~0 LOC removed, organization)

Split the 2,137-line monolith into files by domain.
Same package, same `Handlers` struct — just file boundaries.

1. `handlers_health.go` — Health, Healthz, Readyz, Metrics, SetBuildVersion
2. `handlers_file.go` — Tree, ReadFile, WriteFile, DeleteFile, BulkWrite, UploadAsset, ResolveLinks, buildTree, addPermalinks, detectContentType
3. `handlers_search.go` — Search, VerifiedSearch, SemanticSearch, StalePages, Contradictions, Meta, search types/helpers
4. `handlers_version.go` — Versions, Version, Diff, Blame
5. `handlers_events.go` — Events (SSE)
6. `handlers_graph.go` — Backlinks, Graph, computeGraph, extractFrontmatterTags, ToC
7. `handlers_content.go` — Templates, Comments, Theme, UIConfig, Janitor
8. `handlers_dataview.go` — Query, QueryAggregate, ViewRefresh
9. `handlers_share.go` — CreateShareLink, ListShareLinks, RevokeShareLink, PublicPage, PublicFile, PublicTree, buildPublicTree
10. **Test:** `go build ./... && go test ./...` (nothing should break — same package)

### Step 3 — Extract handler helpers (~150 LOC removed)

Now that files are split, extract repeated patterns:

1. **`bindJSON(c, &v) error`** — replaces the repeated `c.Bind` + `"invalid JSON body"` pattern (used in ~6 handlers)
2. **`requirePath(c) (string, error)`** — replaces the repeated `path := c.QueryParam("path"); if path == "" { return 400 }` pattern (~10 handlers)
3. **`readFileOr404(c, store, path) ([]byte, error)`** — replaces the repeated `store.Read` + `os.IsNotExist` → 404 pattern (~5 handlers)
4. **Merge `Search` / `VerifiedSearch` response tail** — both map `[]search.Result` → `[]searchResultEntry` with permalink injection. Extract `buildSearchEntries(results, publicURL)`.
5. **`modifiedAfterFilter`** — extract the RFC3339 parse + stat + filter loop shared by Search and SemanticSearch
6. **Test:** `go test ./internal/api/...`

### Step 4 — `mcpserver/client.go` HTTP helpers (~70 LOC removed)

Every `RemoteBackend` method does: `do(GET/POST)` → `readBody` → `json.Unmarshal`.

1. Add `getJSON(ctx, path, &out) error` and `postJSON(ctx, path, body, &out) error` methods
2. Rewrite `ReadFile`, `Tree`, `Search`, `SearchSemantic`, `QueryMeta`, `Versions`, `Backlinks`, `Health` to use them
3. Delete `contains` + `searchString` helpers in test file — use `strings.Contains` instead
4. **Test:** `go test ./internal/mcpserver/...`

### Step 5 — `search/sqlite.go` dedup (~80 LOC removed)

1. Extract `placeholders(n int) string` helper for the repeated `IN (?,?,?)` construction (used in 5-6 places)
2. Merge the two `Search` FTS query branches (pathPrefix vs no-prefix) into one query with conditional `AND`
3. Merge `softTrustBoost` / `hardTrustBoost` into one function with a coefficient parameter
4. Extract `scanMetaRow(rows) (MetaResult, error)` for the repeated path+frontmatter scan pattern
5. **Test:** `go test ./internal/search/...`

### Step 6 — Test helpers (~150 LOC removed)

1. **`handlers_test.go`**: extract `mustPutFile(t, s, path, body)` — the repeated PUT + assert 200 pattern appears ~15 times
2. **`mcpserver_test.go`**: extract `mustCallTool(t, handler, name, args) *mcp.CallToolResult` — same setup in every handler test
3. **`handlers_test.go`**: consolidate the 4 `NewServer(cfg, pipe, nil, cstore, nil, nil)` call sites into one shared `buildMinimalServer(t)`
4. **Test:** `go test ./internal/api/... ./internal/mcpserver/...`

### Step 7 — Comment trim (~200 LOC removed)

1. Remove section banners (`// ─── ... ───`) — ~20 lines
2. Remove comments that narrate the function name (e.g. `// Search performs a full-text search` above `func Search`)
3. Trim multi-paragraph rationale comments to 1-2 lines (keep the "why", drop the "what")
4. Remove `// TODO` comments for features that are already done
5. **Do NOT remove**: comments explaining non-obvious behavior, security rationale, or performance tradeoffs
6. **Test:** `go build ./... && go test ./...`

### Step 8 — Minor helpers (~50 LOC removed)

1. **`fuse.go`**: extract `httpErrno(status int) syscall.Errno` for repeated HTTP status → errno mapping
2. **`fuse.go`**: extract `childPath(parent, name string) string` for repeated path construction
3. **`pipeline.go`**: extract `commitAndTrack(ctx, path, actor, msg) error` for the repeated commit + log + track pattern
4. **Test:** `go test ./internal/fuse/... ./internal/pipeline/...`

**Expected total: ~1,150 LOC removed (conservative), codebase drops from ~29.8K → ~28.6K**

---

## 2. Dataview v0.3 — remaining functions

- [ ] `filter(field, predicate)` — requires lambda compilation
- [ ] `sort(field)`, `unique(field)`, `flat(field)` — complex
- [ ] `dur(str)` — human duration parsing (~40 lines)
- [ ] Task toggling endpoint

---

## 3. Permalinks — remaining

### Rename handling (deferred to v0.2+)

- [ ] When a file is moved/renamed via API, optionally update all
  `[[old-name]]` references in other files to `[[new-name]]`.
  Controlled by config: `[server] update_links_on_rename = true`

---

## 4. Data durability — remaining phases

### 4.1 Track `.kiwi/` user data in git

- [ ] After writing comment JSON, call `pipeline.CommitOnly` for `.kiwi/comments/{id}.json`
- [ ] After config change via API, call `CommitOnly` for `.kiwi/config.toml`
- [ ] Add exception for `.kiwi/config.toml` and `.kiwi/templates/` in watcher dot-dir skip
- [ ] Add `.kiwi/state/` to `.gitignore` in init templates

### 4.2 Atomic file writes

- [ ] `storage/local.go` `Write()` — write to `{abs}.kiwi.tmp` → `f.Sync()` → `os.Rename(tmp, abs)`

### 4.3 Uncommitted path tracking

- [ ] Wire `DrainUncommitted` call at startup
- [ ] Verify `.kiwi/state/uncommitted.log` path is created if missing

---

## 5. Pre-public launch

### 5.1 First release

- [ ] Scrub `18.209.226.85` from git history (appears in 1 commit)
- [ ] Cut `v0.1.0` tag → triggers release workflow
- [ ] Verify: GitHub release has linux/darwin x amd64/arm64 binaries
- [ ] Verify: `curl install.sh | sh` works from the raw GitHub URL

### 5.2 Distribution

- [ ] Docker Hub: create `kiwifs/kiwifs` org, add secrets, verify push
- [ ] npm: flip `private: false` in `npm/package.json`, `npm publish`
- [ ] Optional: `kiwifs.dev` domain for docs/install script

### 5.3 ONNX embedder

- [ ] `internal/embed/onnx.go` is a stub. Implement with CGO build tag,
  document sidecar pattern, or remove.
