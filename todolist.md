# KiwiFS — Active Todolist

What's left to build next, in priority order.

---

## 1. Dataview v0.3 — remaining functions

- [ ] `filter(field, predicate)` — requires lambda compilation.
- [ ] `sort(field)`, `unique(field)`, `flat(field)` — complex.
- [ ] `dur(str)` — human duration parsing (~40 lines).
- [ ] Task toggling endpoint.

---

## 2. Permalinks — remaining

### 2.1 LOW — Rename handling (deferred to v0.2+)

- [ ] When a file is moved/renamed via API, optionally update all
  `[[old-name]]` references in other files to `[[new-name]]`.
  Controlled by config: `[server] update_links_on_rename = true`
- [ ] Document as known limitation for v0.1

---

## 3. Data durability — remaining phases

Phases B + C (backup/restore CLI) are done. Three phases remain.

### 3.1 Track `.kiwi/` user data in git

Comments, config, and templates are user-created data stored under
`.kiwi/` but not committed to git. Host failure loses them silently.

- [ ] `internal/comments/comments.go` — after writing a comment JSON
  file, call `pipeline.CommitOnly(ctx, ".kiwi/comments/{id}.json",
  "kiwifs", "comment: add/edit/delete")`
- [ ] `internal/config/config.go` or API handler — after config change
  via API, call `CommitOnly` for `.kiwi/config.toml`
- [ ] `internal/watcher/watcher.go` — add exception for `.kiwi/config.toml`
  and `.kiwi/templates/` in the dot-dir skip logic
- [ ] Add `.kiwi/state/` to `.gitignore` in init templates (SQLite
  files must never be committed)

### 3.2 Atomic file writes

- [ ] `internal/storage/local.go` `Write()` — replace `os.WriteFile`
  with: write to `{abs}.kiwi.tmp` → `f.Sync()` → `os.Rename(tmp, abs)`

### 3.3 Uncommitted path tracking

- [ ] Wire `DrainUncommitted` call at process startup in
  `internal/bootstrap/bootstrap.go` or `cmd/serve.go`
- [ ] Verify `.kiwi/state/uncommitted.log` path is created if missing

---

## 4. Pre-public launch

### 4.1 First release

- [ ] Decide GitHub org — `go.mod` says `github.com/kiwifs/kiwifs`,
  docs say `github.com/amelia751/kiwifs`. Create org or update go.mod.
- [ ] Scrub `18.209.226.85` from git history (appears in 1 commit)
- [ ] Cut `v0.1.0` tag → triggers release workflow
- [ ] Verify: GitHub release has linux/darwin × amd64/arm64 binaries
- [ ] Verify: `curl install.sh | sh` works from the raw GitHub URL

### 4.2 Distribution

- [ ] Docker Hub: create `kiwifs/kiwifs` org, add secrets, verify push
- [ ] npm: flip `private: false` in `npm/package.json`, `npm publish`
- [ ] Optional: `kiwifs.dev` domain for docs/install script

### 4.3 ONNX embedder

- [ ] `internal/embed/onnx.go` is a stub. Implement with CGO build
  tag, document sidecar pattern, or remove. Low priority — OpenAI/
  Ollama/Cohere embedders all work.

---

## Done

<details>
<summary>Dataview — 7 review rounds, all fixed (merged to main)</summary>

**v0.1 core:** DQL Pratt parser, SQL compiler, executor with resource
limits, auto-indexer, renderer (table/list/json/count/distinct),
computed views + registry, 12 built-in functions, implicit metadata
fields, REST API, MCP tools, CLI commands, React UI. 4 rounds, 30 issues.

**v0.2 features (9):** Column aliases (`AS`), `WITHOUT ID`, computed
column expressions, 27 functions, `FROM #tag`, `GROUP BY` with rows,
`TASK` queries, `CALENDAR` parser + UI, multiple `WHERE`/`SORT` chaining.

**Round 5 — 8 bugs, 6 refactors, 14 tests:** matchTaskWhere safe default
+ all operators, evalTaskField tags/meta, CollectFields multi-sort,
RegenerateView end marker, Registry tag invalidation, _ext dynamic
extraction, regexreplace registered, removed dead IsColumn. Table-driven
refactors in functions/expr/compiler/lexer/executor. 38% LOC reduction.

**Round 6 — 4 issues:** regexp registered as custom SQLite function,
BetweenExpr in matchTaskWhere, removed dead writeFromAndFlatten call,
regex pattern caching via sync.Map.

**Round 7 (final) — 1 bug fixed, 21 edge tests added:** length() in
WHERE was using JSON path literal instead of json_extract — fixed.
Added comprehensive edge case tests: empty DB, null frontmatter, deep
nesting, special chars, regexp, regex_replace, task BETWEEN, task IS
NULL, WITHOUT ID + GROUP BY, chained WHERE + FROM, aliases, SQL
injection safety, default LIMIT, implicit meta fields, DISTINCT,
LIST/COUNT formats, multiple SORTs, function in WHERE, GROUP BY.

All pass. Build/vet/race clean.
</details>

<details>
<summary>Permalinks — core + 6 review rounds</summary>

Config (`public_url`, `KIWI_PUBLIC_URL`), `Permalink()` with URL
encoding, SPA deep linking (`/page/{path}`, pushState, popstate),
backward compat (`#/{path}`), API fields (tree/search/meta), headers
(`X-Permalink`), wiki link resolution (cached `Resolver` with dirty
flag), MCP tools (`resolve_links`, `include_permalinks`), REST
endpoints, Python client server-side resolution, shared `LinkResolver`
replacing duplicate logic, backend-agnostic `PublicURL()`, 404 UX.
Browser-tested 2026-04-24.
</details>

<details>
<summary>MCP Server — 4 rounds, 42 items fixed</summary>

Round 1 (18), Round 2 (9), Round 3 (4), Round 4 (11). Covers: remote
headers, tree recursion, search filtering, error types, pagination,
ETags, lazy schema, symlink guard, audit middleware, nil panics,
injection prevention, size caps, all test gaps.
</details>

<details>
<summary>JetRun Integration — wired and tested</summary>

KiwiFS deployed on EC2, binary on S3, turns.py refactored with merged
MCP config, prompt.py knowledge block, E2E test passed (agent used
kiwi_tree/read/write, git commit verified, search works).
</details>

<details>
<summary>Data durability — phases B + C done</summary>

Backup config + background git push sync, `kiwifs backup` CLI,
`kiwifs restore` CLI (git clone + auto-reindex).
</details>
