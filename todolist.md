# KiwiFS — Active Todolist

What's left to build next, in priority order.

---

## 1. Dataview v0.2 — DQL feature parity with Obsidian Dataview

Learned from scanning [obsidian-dataview](https://github.com/blacksmithgu/obsidian-dataview)
(8.8K stars, the industry standard). Items ordered by impact and
difficulty. **We already use goldmark with AST walking** — task
extraction is a natural extension, not a new dependency.

### Libraries to use (not build from scratch):

- **goldmark** `github.com/yuin/goldmark` — already in go.mod.
  Add `extension.TaskList` to extract `TaskCheckBox` nodes (checked/
  unchecked) via AST walk. Zero new dependencies.
- **Go stdlib `regexp`** — for `regextest()`, `regexmatch()`,
  `regexreplace()` functions. No dependency needed.
- **Go stdlib `time`** — for duration arithmetic. Obsidian Dataview's
  `dur("2 days")` maps to `time.ParseDuration` + custom parsing for
  day/week/month/year units.
- **No new parser library needed** — our Pratt parser already handles
  expressions. Column aliases (`AS`), `WITHOUT ID`, and computed
  column expressions are parser/compiler additions, not rewrites.

---

### 1.1 Column aliases (`AS`) — easy, high impact

Users want: `TABLE started AS "Start Date", file.folder AS Path`

**Files to change:**
- [x] **`query.go`** — add `Alias` field to a new `FieldSpec` struct:
  ```go
  type FieldSpec struct {
      Expr  string // field path or expression
      Alias string // "" means use Expr as header
  }
  ```
  Change `QueryPlan.Fields []string` → `QueryPlan.Fields []FieldSpec`.
- [x] **`parser.go`** — in `parseFieldList`, after scanning a field,
  check if next word is `AS`. If so, consume the alias (quoted string
  or bare word):
  ```go
  if strings.ToUpper(firstWord(rest)) == "AS" {
      rest = skipWord(rest) // skip AS
      alias, rest = scanAlias(rest) // handles "quoted" or bare
  }
  fields = append(fields, FieldSpec{Expr: field, Alias: alias})
  ```
- [x] **`compiler.go`** — `aliasFor()` should use `FieldSpec.Alias`
  when non-empty, falling back to the auto-generated alias.
- [x] **`executor.go`** — `execSelect` builds column list from
  `FieldSpec.Alias` or `FieldSpec.Expr`.
- [x] **`renderer.go`** — `renderTable` uses alias for column headers.
- [x] **`KiwiQuery.tsx`** — no change needed (uses server column names).
- [x] **Test:** `TestParseQuery_ColumnAlias` — parse
  `TABLE name AS "Full Name", status AS State` and verify aliases.
- [x] **Test:** `TestIntegration_ColumnAlias` — execute and verify
  column headers in result.

### 1.2 `WITHOUT ID` modifier — easy, high impact

Users want: `TABLE WITHOUT ID name, status` (omit `_path` column)
and `LIST WITHOUT ID` (omit file link from list output).

- [x] **`query.go`** — add `WithoutID bool` to `QueryPlan`.
- [x] **`parser.go`** — in `parseType`, after consuming TABLE/LIST,
  check for `WITHOUT` followed by `ID`:
  ```go
  if strings.ToUpper(firstWord(rest)) == "WITHOUT" {
      r2 := skipWord(rest)
      if strings.ToUpper(firstWord(r2)) == "ID" {
          plan.WithoutID = true
          rest = skipWord(r2)
      }
  }
  ```
- [x] **`compiler.go`** — in `compileSelect`, skip `file_meta.path`
  from SELECT when `plan.WithoutID` is true.
- [x] **`executor.go`** — in `execSelect`, skip `_path`/`path` from
  result columns and row building when `WithoutID`.
- [x] **`renderer.go`** — `renderTable`/`renderList` skip path column
  when `WithoutID`.
- [x] **Test:** `TestParseQuery_WithoutID` — parse
  `TABLE WITHOUT ID name` → `plan.WithoutID == true`.
- [x] **Test:** `TestIntegration_WithoutID` — execute, verify no
  `_path` column in output.

### 1.3 Computed expressions in column positions — medium

Users want: `TABLE days_since(last_active) AS "Days Idle", name`
— arbitrary expressions as SELECT columns, not just field names.

- [x] **`query.go`** — change `FieldSpec.Expr` from `string` to accept
  either a raw field path or a parsed `Expr` AST node. Add a
  `FieldSpec.Parsed Expr` field.
- [x] **`parser.go`** — in `parseFieldList`, instead of just `scanField`,
  try to parse each column as a full expression (stopping at `,` or
  clause keyword). Use `splitAtCommaOrClause` to find boundaries, then
  `ParseExpr` on each segment.
- [x] **`compiler.go`** — in `compileSelect`, when `FieldSpec.Parsed`
  is set, call `compileExpr(spec.Parsed)` to get the SQL fragment
  instead of `fieldToSQL(spec.Expr)`.
- [x] **`executor.go`** — column scanning logic already handles
  arbitrary SQL columns; just need to label them correctly.
- [x] **Test:** `TestParseQuery_ComputedColumn` — parse
  `TABLE days_since(last_active) AS "Idle"`.
- [x] **Test:** `TestIntegration_ComputedColumn` — execute, verify
  the computed value appears in results.

### 1.4 Expanded function library — medium, high value

Add the most impactful functions from Obsidian Dataview. Each is a
new entry in `funcRegistry` in `functions.go` + a `compileXxx` func.

**Aggregation functions** (compile to SQLite aggregates):
- [x] `sum(field)` → `SUM(json_extract(...))`
- [x] `average(field)` → `AVG(json_extract(...))`
- [x] `min(field)` → `MIN(json_extract(...))`
- [x] `max(field)` → `MAX(json_extract(...))`
  Note: these only make sense in GROUP BY or as the sole column in
  a COUNT-like query. Compiler must detect aggregate usage and wrap
  appropriately.

**Conditional/utility:**
- [x] `choice(cond, ifTrue, ifFalse)` →
  `CASE WHEN <cond> THEN <ifTrue> ELSE <ifFalse> END`
- [x] `typeof(field)` → `json_type(frontmatter, '$.field')`
- [x] `number(field)` → `CAST(<field> AS REAL)`
- [x] `string(field)` → `CAST(<field> AS TEXT)`

**String functions:**
- [x] `replace(str, old, new)` → `REPLACE(<str>, <old>, <new>)`
- [x] `substring(str, start, len)` → `SUBSTR(<str>, <start>, <len>)`
- [x] `split(str, sep)` — not directly in SQLite; implement as
  JSON array via `json_each` + recursive CTE, or return as-is and
  note limitation.
- [x] `join(list, sep)` → `GROUP_CONCAT(<field>, <sep>)`
- [x] `regextest(pattern, str)` → compile to Go-side post-filter
  (SQLite `REGEXP` requires a custom function registered via
  `regexp.MatchString`; register once at DB open in `search.NewSQLite`).
- [x] `regexreplace(str, pattern, replacement)` → Go-side post-process.

**List/array functions:**
- [ ] `filter(field, predicate)` — complex; defer to v0.3 unless
  there's demand. Requires lambda compilation.
- [ ] `sort(field)` → `json_group_array` with ORDER BY. Complex.
- [ ] `unique(field)` → `DISTINCT` subquery.
- [ ] `flat(field)` — essentially what FLATTEN does; document overlap.
- [x] `nonnull(field)` → `json_extract(...) IS NOT NULL` filter.

**Date/duration:**
- [ ] `dur(str)` — parse human durations ("2 days", "1 month 3 days")
  into seconds. Store as number. Implement in Go: custom parser
  (day=86400, week=604800, month=2592000, year=31536000). ~40 lines.
- [x] `dateformat(date, format)` → `strftime(<format>, <date>)`.
- [x] `striptime(date)` → `date(<date>)` (SQLite `date()` strips time).
- [x] `round(num, digits)` → `ROUND(<num>, <digits>)`.

For each function:
- [x] Add `compileXxx` to `functions.go`.
- [x] Register in `funcRegistry`.
- [x] Add test in `dataview_test.go`.

### 1.5 `FROM` by tag — medium

Obsidian Dataview: `FROM #game/moba OR #game/crpg`. Tags live in
frontmatter `tags` field or inline `#tag` in content. KiwiFS already
indexes frontmatter.

- [x] **`parser.go`** — in `parseFrom`, detect `#tag` syntax (starts
  with `#`). Parse as tag filter, not folder. Support `OR`, `AND`,
  negation (`-#tag`).
- [x] **`query.go`** — add `FromTags []TagFilter` alongside `From`.
  `TagFilter` has `Tag string`, `Negate bool`.
- [x] **`compiler.go`** — tag filter compiles to:
  ```sql
  EXISTS (SELECT 1 FROM json_each(file_meta.frontmatter, '$.tags')
         WHERE value = ?)
  ```
  For negation, wrap in `NOT EXISTS`.
- [x] **Test:** `TestParseQuery_FromTag` — parse `TABLE name FROM #game`.
- [x] **Test:** `TestIntegration_FromTag` — seed files with tags,
  query by tag, verify correct results.

### 1.6 `GROUP BY` with rows — medium, high impact

Currently GROUP BY only returns `{key, count}`. Obsidian Dataview
returns actual rows per group with field "swizzling" (`rows.file.link`).

- [x] **`executor.go`** — `execGroupBy` currently uses
  `SELECT grp, COUNT(*)`. Change to execute the full SELECT (with all
  fields), then group results in Go:
  1. Execute normal SELECT query (no GROUP BY in SQL).
  2. Iterate results, bucket into `map[string][]map[string]any`.
  3. Build `GroupResult` with `Rows` populated.
- [x] **`query.go`** — `GroupResult.Rows` already exists in the struct.
  Just populate it.
- [x] **`renderer.go`** — `renderGroupedTable` already handles
  `g.Rows`. Just ensure the data flows through.
- [x] **Test:** `TestIntegration_GroupByWithRows` — GROUP BY status,
  verify each group has its rows with field values.

### 1.7 TASK query type — hard, killer feature

This is the most complex addition. Requires extracting tasks from
markdown body, not just frontmatter.

**Phase A: Task indexing (pipeline)**
- [x] **`internal/markdown/markdown.go`** — add `Task` struct:
  ```go
  type Task struct {
      Text      string         `json:"text"`
      Completed bool           `json:"completed"`
      Line      int            `json:"line"`
      Tags      []string       `json:"tags,omitempty"`
      Due       string         `json:"due,omitempty"`
      Children  []Task         `json:"children,omitempty"`
      Meta      map[string]any `json:"meta,omitempty"`
  }
  ```
- [x] **`internal/markdown/markdown.go`** — add `Tasks()` function.
  Use goldmark with `extension.TaskList` (already in go.mod). Walk
  AST for `*extast.TaskCheckBox` nodes. Extract text from parent
  `*ast.ListItem`. Parse inline metadata (`[due:: 2026-05-01]`
  format, regex: `\[(\w+)::\s*([^\]]+)\]`). Build parent/child
  relationships via indentation level.
- [x] **`internal/search/sqlite.go`** — add `tasks` column to
  `file_meta` table (TEXT, JSON array). In `indexFile`, after parsing
  frontmatter, also parse tasks and store as JSON. Migration: `ALTER
  TABLE file_meta ADD COLUMN tasks TEXT NOT NULL DEFAULT '[]'`.

**Phase B: TASK query type (DQL)**
- [x] **`parser.go`** — add `TASK` as a query type in `parseType`.
  No column list needed (tasks have fixed schema).
- [x] **`compiler.go`** — TASK queries compile differently:
  ```sql
  SELECT path, tasks FROM file_meta
  WHERE json_array_length(tasks) > 0
  ```
  Then filter individual tasks in Go (WHERE on task-level fields
  like `completed`, `due`, `tags`).
- [x] **`executor.go`** — add `execTask` method. Parse the `tasks`
  JSON column, apply task-level filters, return flattened task list
  with parent file info.
- [x] **`renderer.go`** — add `renderTaskList` method. Output:
  ```
  - [ ] Buy groceries #shopping
  - [x] Send email
    - [ ] Follow up by Friday [due:: 2026-05-01]
  ```
- [x] **`query.go`** — add task-specific result fields or reuse
  `Rows` with task schema columns (`text`, `completed`, `due`,
  `line`, `path`).

**Phase C: Task toggling (optional, deferred)**
- [ ] POST endpoint to toggle a task's checked status by writing
  back to the original file. This is the only Dataview feature that
  modifies files. Consider carefully — adds write complexity.

- [x] **Tests:** `TestMarkdown_ExtractTasks`, `TestParseQuery_Task`,
  `TestIntegration_TaskQuery`, `TestIntegration_TaskWhereCompleted`.

### 1.8 CALENDAR query type — easy (UI-only)

This is purely a rendering concern — the query is a normal TABLE
query, just displayed as a calendar in the UI.

- [ ] **`KiwiQuery.tsx`** — detect `CALENDAR` format. Render a
  month grid where each result dot appears on its date field.
  Use a lightweight React calendar grid (no dependency — just a
  CSS grid of 7×5 cells with date numbers).
- [x] **`parser.go`** — add `CALENDAR` as a query type. Require
  exactly one field (the date field). Store as `plan.Type = "calendar"`.
- [x] **`renderer.go`** — server-side, CALENDAR renders as JSON
  (same as TABLE). The UI does the visual calendar layout.
- [x] **API** — no change needed; `format=json` returns rows, UI
  renders them as calendar.
- [x] **Test:** `TestParseQuery_Calendar`.

### 1.9 Multiple WHERE/SORT (chaining) — easy

Obsidian allows: `WHERE x > 1 WHERE y < 5 SORT z SORT w`.

- [x] **`parser.go`** — allow multiple WHERE clauses. Join them
  with AND: `plan.Where = &BinaryExpr{Left: existing, Op: OpAnd,
  Right: newExpr}`.
- [x] **`parser.go`** — for multiple SORT, build a sort chain. This
  is a deeper change — need `[]SortSpec` instead of single
  `Sort`+`Order` fields:
  ```go
  type SortSpec struct {
      Field string
      Order string // "asc" | "desc"
  }
  ```
  Change `QueryPlan.Sort`/`Order` → `QueryPlan.Sorts []SortSpec`.
- [x] **`compiler.go`** — `writeOrderBy` iterates `plan.Sorts`.
- [x] **Test:** `TestParseQuery_MultipleWhere`,
  `TestParseQuery_MultipleSort`.

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
<summary>Dataview — 4 review rounds, 30 issues found and fixed</summary>

**Core implementation:** DQL parser (Pratt expression parser + statement
parser), SQL compiler, executor with resource limits, auto-indexer,
renderer (table/list/json/count/distinct), computed views + registry,
built-in functions (contains, startsWith, endsWith, matches, length,
lower, upper, default, date, now, days_since), implicit metadata fields,
REST API, MCP tools, CLI commands, React UI component.

41 dataview tests + 25 handler tests pass. Build clean, vet clean,
race-free.

**Round 1 (12 issues):** FLATTEN column ambiguity, `_ext` SQL broken,
`max_scan_rows`/`query_timeout` not enforced, keyword field names,
empty `IN()`, `writeOrderBy` swallowed errors, MCP `Groups` dropped,
`AutoIndexer` lock contention, misleading `Total`, `isISODate` too
aggressive, no FLATTEN integration test, CLI missing config read.

**Round 2 (8 issues):** Non-functional SUM/AVG (removed), raw string
concatenation in aggregate handler, per-call executor allocation in MCP,
`RegenerateView` bypassed limits, UI showed "N of -1", unterminated
backtick silent, `kiwi-format` ignored, no handler-level tests.

**Round 3 (5 issues):** Registry lazy-regen unlimited executor
(`NewRegistry` now requires `*Executor`), `ValidWhereExpr` blocklist
(replaced with `ParseExpr`), dead `aggregateRequest` struct, dead `OPS`
constant, missing list-format view test.

**Round 4 (5 issues):** DISTINCT backtick support (uses `scanField`
now), `splitAtClause` missing FROM, dead `fetchLimit` variable,
`Execute` mutated plan in-place (shallow copy now).
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
