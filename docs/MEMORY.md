# Episodic and central memory

KiwiFS is file-first: agents write plain markdown, git versions everything, and optional headers record **lineage** (`derived-from` from `X-Actor` / `X-Provenance`). For agent *memory systems*, you usually want two layers on top of that:

1. **Episodic memory** — per-run, per-session, or time-bounded raw notes (high volume, append-friendly).
2. **Central (semantic) memory** — “what the product should believe” on entity and concept pages, possibly after human or LLM **consolidation** from episodes.

This document describes the conventions, configuration, and tooling KiwiFS provides for that split. It does **not** run an LLM for you: consolidation is your job (or your scheduler); KiwiFS gives you a **data model** and a **report** to see which episodes are not yet linked from consolidated pages.

---

## Two different provenance lists

| Frontmatter | Meaning |
|-------------|---------|
| `derived-from` | Injected on **write** from HTTP headers. Records which run / job **produced** this file. Same structure as before: `type` + `id` (+ `date`, `actor`, …). |
| `merged-from` | You (or a consolidation job) set this on a **downstream** page. It says: “this semantic or summary page was built **from** these episodes (or other units).” |

You can have both: an episode has `derived-from: run:…` from the API, and a `concepts/foo.md` page has `merged-from` listing the episode ids that were folded into the concept.

---

## `memory_kind` in frontmatter

Use `memory_kind` to classify a page. Recognised values include:

- `episodic` — raw run/session log material.
- `semantic` — durable, curated knowledge.
- `consolidation` — optional staging area (pending merge to semantic).
- `working` — optional scratch, high churn.

`memory_kind: semantic` or `memory_kind: consolidation` prevents a file from being classified as episodic even if it lives under the episodes path prefix (see below).

---

## Path convention: `episodes/`

By default, any markdown under the prefix **`episodes/`** (configurable) is treated as **episodic** when `memory_kind` is not set to `semantic` or `consolidation`. That lets you drop files into a folder without always setting `memory_kind`.

To change the prefix, set in `.kiwi/config.toml`:

```toml
[memory]
# Relative to the knowledge root. Default when unset: episodes/
episodes_path_prefix = "episodes/"
```

---

## Episodic identity: `episode_id` and `merged-from`

- Prefer **`episode_id`** in the frontmatter of episodic files. It is the primary key the tooling uses to see if a consolidated page cites that episode.
- If `episode_id` is missing, **`id`** is used as a fallback when the file is still classified as episodic.
- A **`merged-from`** entry can reference an episode by:
  - `type: episode` together with `id` set to the episodic file’s `episode_id`, or
  - `type: episode` together with `path` set to the episode’s path (e.g. `episodes/2026/run.md` relative to the knowledge root) when you have not yet assigned ids.

`merged-from` is a **sequence** in YAML, each item shaped like:

```yaml
merged-from:
  - type: episode
    id: run-7f3a
    date: "2026-04-27T12:00:00Z"
    note: "merged into concept auth from nightly job"
  - type: run
    id: run-249
```

**Compatibility:** an episodic file is considered “covered” by a `merged-from` on any other page if that page cites `type: episode` with a matching `id`, **or** cites `type: run`, `session`, `trace`, `event`, or `ingest` with the **same** id (so consolidators that key off run ids still match). Path-only `merged-from` items match `episode:path:` plus the normalised file path.

---

## `kiwifs memory report`

List episodic files and which ones are **not** referenced by any `merged-from` anywhere in the tree:

```bash
kiwifs memory report --root /path/to/knowledge
```

Options:

- `--json` / `-j` — machine-readable output (useful for CI and dashboards).
- `--episodes-prefix` — override `[memory] episodes_path_prefix` for a single run.

**What the report does *not* do:** it does not read `derived-from` to decide “merged”. Only **`merged-from`** (and the path / id rules above) counts toward coverage. The intent is to answer: “What episodic content still needs to be pulled into a central or semantic page?”

---

## REST API (`GET /api/kiwi/memory/report`)

When KiwiFS is running (`kiwifs serve`), remote clients and consolidation workers can fetch the same JSON as `kiwifs memory report --json`:

```bash
curl -s "http://localhost:3333/api/kiwi/memory/report"
curl -s "http://localhost:3333/api/kiwi/memory/report?episodes_prefix=raw/"
```

Optional query parameter **`episodes_prefix`** overrides `[memory] episodes_path_prefix` from `.kiwi/config.toml`. Response shape matches **`memory.Report`** (counts, `episodic_files`, `unmerged`, `warnings`).

---

## MCP (`kiwi_memory_report`)

The MCP server exposes **`kiwi_memory_report`** with optional **`episodes_prefix`**, returning a short human-readable summary (same inputs as the REST endpoint).

---

## Programmatic: `internal/memory` (Go)

- **`memory.InjectMergedFrom([]byte, []memory.MergedFromEntry)`** — idempotently appends to `merged-from` while preserving the markdown body, similar to how provenance injects `derived-from`.
- **`memory.Scan` / `memory.Options{ EpisodesPathPrefix: ... }`** — the same walk used by the CLI; embed custom consolidation workers that read/write the store and then call the report for metrics.

```go
out, err := memory.InjectMergedFrom(inputBytes, []memory.MergedFromEntry{
	{Type: "episode", ID: "ep-1", Note: "nightly merge"},
})
```

---

## Suggested operator workflow

1. Agents (or the API) write under `episodes/` with `X-Provenance` and optional `episode_id` in frontmatter.
2. A scheduled job or on-demand script reads unmerged episodes (`kiwifs memory report` or `memory.Scan` + JSON), calls your LLM or rules engine, and writes/updates `concepts/` (or `semantic/`) with **`merged-from`** set via the API, MCP, or `InjectMergedFrom`.
3. `kiwifs memory report` goes green (no unmerged rows) for your id strategy, or you use the unmerged list as a queue.

That keeps **git** as the audit trail, **files** as the source of truth, and **merged-from** as the explicit “this central page subsumes these episodes” edge from episodic to semantic memory.
