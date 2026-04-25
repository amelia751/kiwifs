# KiwiFS admin guide

This guide covers the operational knobs a self-host admin needs to run KiwiFS
for a team: API key management, quotas, trust promotion, backups, upgrades,
and incident response. Start with [README.md](../README.md) for a feature
tour — this doc assumes you already have the server up.

- [Configuration layout](#configuration-layout)
- [API keys](#api-keys)
- [OIDC / SSO](#oidc--sso)
- [Rotating keys without a restart](#rotating-keys-without-a-restart)
- [Per-space setup](#per-space-setup)
- [Quotas and rate limits](#quotas-and-rate-limits)
- [Trust promotion (verified / source-of-truth)](#trust-promotion-verified--source-of-truth)
- [Janitor tuning](#janitor-tuning)
- [Workflow reminders](#workflow-reminders)
- [Backups and restore](#backups-and-restore)
- [Health + metrics](#health--metrics)
- [Upgrades](#upgrades)
- [Incident response](#incident-response)

## Configuration layout

Every KiwiFS root owns a `.kiwi/` directory:

```
<root>/
  .kiwi/
    config.toml           # this file
    templates/            # decision / workflow templates
    state/                # SQLite indexes, WAL, vector cache (in .gitignore)
    comments/             # inline review comments (committed)
    themes/               # saved theme overrides
    uploads/              # binary assets referenced by pages
  index.md                # first page new users see
  ...                     # your markdown tree
```

The `state/` folder is rebuildable from files — `kiwifs reindex` will
regenerate everything in it if you nuke it.

## API keys

KiwiFS supports four auth modes. Pick one in `.kiwi/config.toml`:

```toml
[auth]
type = "none"            # dev mode — localhost only, open to anyone
# type = "apikey"        # single shared bearer token
# type = "perspace"      # a different token per space (multi-tenant)
# type = "oidc"          # verify JWTs from your IdP
```

### Single global key

```toml
[auth]
type    = "apikey"
api_key = "${KIWI_API_KEY}"      # set KIWI_API_KEY in the environment
```

Clients send `Authorization: Bearer <key>`. Use this mode for
single-team or single-tenant deployments.

### Per-space keys

For multi-tenant setups (one server, multiple independent knowledge
bases) give each space its own key:

```toml
[auth]
type = "perspace"

[[auth.api_keys]]
key   = "${KIWI_MARKETING_KEY}"
space = "marketing"
actor = "marketing-agent"

[[auth.api_keys]]
key   = "${KIWI_ENGINEERING_KEY}"
space = "engineering"
actor = "engineering-agent"
```

Requests to `/api/kiwi/marketing/...` must carry a marketing-scoped
bearer; engineering keys cannot read marketing pages (and vice versa).
The `actor` value is used for git attribution — commits show up as
"marketing-agent" rather than the bearer itself.

## OIDC / SSO

```toml
[auth]
type = "oidc"

[auth.oidc]
issuer    = "https://accounts.google.com"
client_id = "123.apps.googleusercontent.com"
```

KiwiFS verifies incoming JWTs against the issuer's JWKS. The verifier
is cached in memory — your identity provider does not get hit on every
request. Changing the issuer URL requires a full restart (the cache
lives inside the verifier value).

## Rotating keys without a restart

Editing keys in `.kiwi/config.toml` and then sending `SIGHUP` to the
server hot-swaps the live key set:

```bash
# macOS / Linux
kill -HUP "$(pgrep -f 'kiwifs serve')"

# Docker
docker kill --signal HUP kiwifs
```

On SIGHUP the server re-reads `.kiwi/config.toml`, filters keys per
space, and atomically replaces the active set — in-flight requests
finish on the old keys, new requests see the new keys. `OIDC` issuer
changes still require a restart (JWKS cache is not swappable).

The event shows up in the log as:

```
sighup: auth reloaded for 3 space(s)
```

## Per-space setup

```toml
[server]
port = 3333

[[spaces]]
name = "engineering"
root = "/data/engineering"

[[spaces]]
name = "marketing"
root = "/data/marketing"
```

Each space has its own git repo, its own search index, and its own
optional `.kiwi/config.toml` overlay. Space-local config wins over the
server-wide file for janitor, search, and UI settings. The default
space (from `[storage] root`) stays available under `/api/kiwi/...`
without a prefix, which keeps existing clients working.

## Quotas and rate limits

Body-size and request-rate limits are enforced in the HTTP middleware:

| Limit | Where | Default |
|---|---|---|
| Per-request body | `middleware.BodyLimit` | 32 MB |
| Per-client rate | `middleware.RateLimiter` | 100 req/s sustained, 200 burst |
| Binary asset size | `[assets] max_file_size` | 10 MB (image/PDF upload path) |

Override in config:

```toml
[assets]
max_file_size = "50MB"
allowed_types = ["image/png", "image/jpeg", "image/webp", "application/pdf"]
```

For larger uploads (videos, datasets) use the S3 gateway — it streams
to a temp file and atomically renames into the store, so you can PUT a
2 GB object without touching RAM:

```bash
aws s3 cp big.mp4 s3://knowledge/media/ --endpoint-url http://host:3334
```

## Trust promotion (verified / source-of-truth)

Every page has an optional trust level in frontmatter:

```yaml
---
trust: verified                   # suggestion | validated | verified | source-of-truth
verified-by: alice@example.com
verified-at: 2026-04-20
---
```

Admins promote/demote trust via the UI (page actions menu → "Set trust
level") or by editing the frontmatter directly. Every change is a git
commit with the actor's name, so there's an audit trail of who
verified what and when.

Search behaviour:

| Level | Default search (soft boost) | `search/verified` (hard filter) |
|---|---|---|
| `source-of-truth` | 1.6× | always included |
| `verified` | 1.3× | always included |
| `validated` | 1.1× | dropped |
| `suggestion` | 1× | dropped |

The soft boost is on by default; pass `boost=none` to turn it off for a
single request.

## Janitor tuning

```toml
[janitor]
interval     = "24h"   # how often to run; "0s" disables scheduled scans
stale_days   = 90      # threshold for "this page hasn't been touched"
startup_scan = true    # do one scan at boot so the panel isn't empty
```

Thresholds are per space — engineering docs rot faster than HR policies.
Each scheduled scan broadcasts an SSE `janitor.scan` event so the UI can
surface a badge without polling; results are cached and served by
`GET /api/kiwi/janitor`. Pass `?fresh=1` to force a re-scan on demand.

## Workflow reminders

Any page with `due-date`, `tasks`, or `approval` frontmatter becomes a
workflow page. The reminder scheduler scans those pages and fires:

- `workflow.reminder` SSE events (page-level due dates, overdue tasks,
  overdue approvals)
- `GET /api/kiwi/workflow/reminders` returns the current inbox
  (list of reminders + timestamp of the last sweep)

The scan cadence follows the janitor interval by default; tune it with
`[workflow] reminder_interval` if you need a different rhythm.

## Backups and restore

```toml
[backup]
remote   = "git@backup.example:kiwi/knowledge.git"
interval = "15m"                  # also honours KIWI_BACKUP_INTERVAL env
branch   = "main"                 # optional; defaults to current
```

KiwiFS runs a background `git push` on a ticker and then **verifies**
the push actually landed by comparing `git ls-remote` to the local
HEAD. Failures are logged but don't kill the loop — a silent backup
failure is the worst kind.

Restore on a fresh machine:

```bash
kiwifs restore --remote git@backup.example:kiwi/knowledge.git --root /data/knowledge
kiwifs serve  --root /data/knowledge
```

`restore` clones the remote, drops a scaffolded `.kiwi/config.toml` if
missing, and runs a full `kiwifs reindex` so the SQLite + vector
indexes line up with the restored tree.

## Health + metrics

Three unauthenticated probe endpoints are exposed for load balancers
and monitoring:

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | liveness — 200 while the process is alive |
| `GET /readyz`  | readiness — 200 when storage + search are ready (503 otherwise) |
| `GET /metrics` | Prometheus text format |

Sample `/metrics` output:

```
# HELP kiwifs_uptime_seconds Seconds since server start.
# TYPE kiwifs_uptime_seconds counter
kiwifs_uptime_seconds 1823

# HELP kiwifs_sse_subscribers Current SSE clients.
# TYPE kiwifs_sse_subscribers gauge
kiwifs_sse_subscribers 4

# HELP kiwifs_janitor_issues Open janitor issues from the latest scan.
# TYPE kiwifs_janitor_issues gauge
kiwifs_janitor_issues 12

# HELP kiwifs_workflow_reminders Active overdue reminders.
# TYPE kiwifs_workflow_reminders gauge
kiwifs_workflow_reminders 3
```

Probe loggers are suppressed so Prometheus / Kubernetes don't flood your
access log.

## Upgrades

1. `git pull && cd ui && npm run build && cd .. && go build -o kiwifs .`
   (or pull a new binary from the releases page).
2. Replace the old binary on disk — `kiwifs serve` uses an embedded UI,
   so there's no separate frontend deploy.
3. Either restart the process or (for auth-only changes) send `SIGHUP`.
4. Verify `/readyz` returns 200 and `/metrics` reports a fresh uptime.

KiwiFS state is rebuildable from `.md` files and git history, so
rollback is always an option: stop the server, restore the previous
binary, and run `kiwifs reindex` if the schema changed.

## Incident response

Common problems and fixes:

| Symptom | Likely cause | Fix |
|---|---|---|
| 401 everywhere after restart | expired bearer / OIDC JWKS rotated | re-auth the client; SIGHUP after editing `.kiwi/config.toml` |
| UI blank after theme edit | CSS variable typo | `rm .kiwi/themes/active.json` and reload |
| Janitor panel shows stale counts | scheduler disabled | set `[janitor] interval = "24h"` + SIGHUP |
| `GET /api/kiwi/search` returns 503 | search engine misconfigured | switch `[search] engine = "sqlite"` and reindex |
| 409 on every save | concurrent edit or stale ETag | reload and re-apply; see [FAQ](../FAQ.md) |
| Mount shows 0 bytes | FUSE client missing auth | pass `--api-key` or `KIWI_API_KEY` to `kiwifs mount` |

When in doubt, check:

```bash
curl -s localhost:3333/metrics | grep -E 'uptime|subscribers|janitor'
tail -100 /var/log/kiwifs/server.log
```

If `/readyz` stays 503, the storage root is unreachable — most
commonly a disk full or a permission error on `.kiwi/state/`.
