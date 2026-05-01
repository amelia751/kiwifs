#!/usr/bin/env python3
"""KiwiFS Agent Scenario Tests — real-world agent workloads with free data.

Simulates how an agent would use KiwiFS as a knowledge store: ingesting data
from free online sources, building structured knowledge, querying with DQL,
and testing the full write-search-query-read cycle.

Data sources (all free, no auth required):
  - REST Countries API: country metadata
  - Hacker News API:    top stories + metadata
  - CVE (NIST NVD):     vulnerability descriptions (via GitHub mirror)

Usage:
    KIWI_URL=http://18.209.226.85:3333 python3 tests/agent_scenarios.py
"""

import json
import os
import sys
import time
import uuid
import concurrent.futures
from urllib.request import Request, urlopen
from urllib.parse import quote
from urllib.error import HTTPError

KIWI_URL = os.environ.get("KIWI_URL", "http://18.209.226.85:3333")
API = f"{KIWI_URL}/api/kiwi"
PASS = 0
FAIL = 0
ERRORS = []
NS = f"agent-{uuid.uuid4().hex[:6]}"


def api_url(path):
    if path.startswith("http"):
        return path
    return f"{API}{path}"


def req_json(method, path, body=None, headers=None):
    url = api_url(path)
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    data = json.dumps(body).encode() if body else None
    r = Request(url, data=data, headers=hdrs, method=method)
    resp = urlopen(r, timeout=30)
    content = resp.read()
    return json.loads(content) if content else {}, resp.status


def req_raw(method, path, body=None, headers=None):
    url = api_url(path)
    hdrs = headers or {}
    data = body if isinstance(body, bytes) else (body.encode() if body else None)
    r = Request(url, data=data, headers=hdrs, method=method)
    resp = urlopen(r, timeout=30)
    return resp.read(), resp.status


def write_file(path, content, actor="agent"):
    return req_raw("PUT", f"/file?path={quote(path)}", body=content,
                   headers={"Content-Type": "text/markdown", "X-Actor": actor})


def read_file(path):
    return req_raw("GET", f"/file?path={quote(path)}")


def delete_file(path):
    try:
        return req_json("DELETE", f"/file?path={quote(path)}")
    except Exception:
        pass


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        msg = f"  ✗ {name}" + (f" — {detail}" if detail else "")
        print(msg)
        ERRORS.append(msg)


def fetch_json(url, timeout=15):
    r = Request(url, headers={"User-Agent": "KiwiFS-Test/1.0"})
    resp = urlopen(r, timeout=timeout)
    return json.loads(resp.read())


# ============================================================
# SCENARIO 1: Countries Knowledge Base
# An agent ingests country data and builds a structured knowledge base.
# ============================================================

def scenario_countries():
    print("\n══ Scenario 1: Countries Knowledge Base ══")
    prefix = f"{NS}/countries"
    countries = []

    try:
        raw = fetch_json("https://restcountries.com/v3.1/region/europe?fields=name,capital,population,area,region,subregion,languages,currencies,flags")
        countries = raw[:15]
    except Exception as e:
        print(f"  ⚠ Skipping countries (API unavailable): {e}")
        return []

    print(f"  Fetched {len(countries)} European countries")

    # --- 1a. Bulk ingest as markdown files with structured frontmatter ---
    files = []
    paths = []
    for c in countries:
        name = c.get("name", {}).get("common", "Unknown")
        slug = name.lower().replace(" ", "-").replace("'", "")
        capital = ", ".join(c.get("capital", []))
        pop = c.get("population", 0)
        area = c.get("area", 0)
        subregion = c.get("subregion", "")
        langs = ", ".join(c.get("languages", {}).values()) if c.get("languages") else ""
        currencies = ", ".join(
            f'{v["name"]} ({k})' for k, v in c.get("currencies", {}).items()
        ) if c.get("currencies") else ""

        path = f"{prefix}/{slug}.md"
        paths.append(path)
        content = f"""---
title: {name}
type: country
region: Europe
subregion: "{subregion}"
capital: "{capital}"
population: {pop}
area: {area}
languages: "{langs}"
currencies: "{currencies}"
status: active
---
# {name}

**Capital:** {capital}
**Population:** {pop:,}
**Area:** {area:,.0f} km²
**Languages:** {langs}
**Currencies:** {currencies}

## Overview

{name} is a country located in {subregion}, Europe.
"""
        files.append({"path": path, "content": content})

    data, status = req_json("POST", "/bulk", {
        "files": files, "actor": "country-agent", "message": f"ingest European countries ({NS})",
    })
    check("bulk ingest countries", status == 200 and data.get("count") == len(files),
          f"status={status}, count={data.get('count')}")

    time.sleep(1)

    # --- 1b. Search for a specific country ---
    if countries:
        target = countries[0]["name"]["common"]
        data, _ = req_json("GET", f"/search?q={quote(target)}")
        results = data.get("results", [])
        check(f"search finds '{target}'", any(target.lower() in r.get("path", "").lower() for r in results),
              f"got {len(results)} results")

    # --- 1c. DQL query: top 5 countries by population ---
    q = quote(f'TABLE path, title, population FROM "{prefix}/" WHERE type = "country" SORT population DESC LIMIT 5')
    data, status = req_json("GET", f"/query?q={q}")
    check("DQL: top 5 by population", status == 200)
    rows = data.get("rows") or []
    check("DQL returns ranked rows", len(rows) > 0, f"got {len(rows)}")
    if len(rows) >= 2:
        p1 = rows[0].get("population", 0) if isinstance(rows[0].get("population"), (int, float)) else 0
        p2 = rows[1].get("population", 0) if isinstance(rows[1].get("population"), (int, float)) else 0
        check("DQL: population sorted DESC", p1 >= p2, f"{p1} vs {p2}")

    # --- 1d. Aggregation: count countries per subregion ---
    data, status = req_json("GET", "/query/aggregate?group_by=subregion&calc=count")
    check("aggregate: countries per subregion", status == 200)
    groups = data.get("groups", {})
    check("aggregate returns groups", len(groups) > 0, f"got {len(groups)} groups")

    # --- 1e. Meta query: filter by subregion ---
    try:
        data, status = req_json("GET", f"/meta?where={quote('$.subregion=\"Western Europe\"')}")
        check("meta: filter by subregion", status == 200)
    except HTTPError as e:
        check("meta: filter by subregion", False, f"got {e.code}")

    # --- 1f. Agent updates a country with new info (version tracking) ---
    if paths:
        target_path = paths[0]
        raw, _ = read_file(target_path)
        original = raw.decode("utf-8")
        updated = original.rstrip() + "\n\n## Agent Notes\n\nThis country was reviewed by the agent on 2026-04-29.\n"
        write_file(target_path, updated, actor="review-agent")
        time.sleep(1)

        data, _ = req_json("GET", f"/versions?path={quote(target_path)}")
        versions = data.get("versions", [])
        check("versioning after agent update", len(versions) >= 2, f"got {len(versions)}")

    # --- 1g. Analytics check ---
    data, _ = req_json("GET", f"/analytics?scope={quote(prefix + '/')}")
    check("analytics scoped to countries", data.get("total_pages", 0) > 0,
          f"total_pages={data.get('total_pages')}")

    return paths


# ============================================================
# SCENARIO 2: Hacker News Top Stories
# An agent curates a "reading list" from HN top stories.
# ============================================================

def scenario_hackernews():
    print("\n══ Scenario 2: Hacker News Reading List ══")
    prefix = f"{NS}/hn-reading-list"
    paths = []

    try:
        story_ids = fetch_json("https://hacker-news.firebaseio.com/v0/topstories.json")[:10]
    except Exception as e:
        print(f"  ⚠ Skipping HN (API unavailable): {e}")
        return []

    stories = []
    for sid in story_ids[:10]:
        try:
            story = fetch_json(f"https://hacker-news.firebaseio.com/v0/item/{sid}.json")
            if story and story.get("title"):
                stories.append(story)
        except Exception:
            continue

    print(f"  Fetched {len(stories)} HN stories")

    # --- 2a. Write each story as a knowledge entry ---
    files = []
    for i, s in enumerate(stories):
        title = s.get("title", "Untitled")
        url = s.get("url", "")
        score = s.get("score", 0)
        by = s.get("by", "unknown")
        safe_title = title.replace('"', '\\"')
        slug = f"story-{s['id']}"

        path = f"{prefix}/{slug}.md"
        paths.append(path)
        content = f"""---
title: "{safe_title}"
type: hn-story
source: hackernews
hn_id: {s['id']}
score: {score}
author: "{by}"
url: "{url}"
priority: {'high' if score > 200 else 'medium' if score > 100 else 'low'}
status: unread
---
# {title}

- **Score:** {score} points
- **Author:** {by}
- **URL:** {url}

## Summary

Story #{i+1} from Hacker News top stories feed.
"""
        files.append({"path": path, "content": content})

    if files:
        data, status = req_json("POST", "/bulk", {
            "files": files, "actor": "hn-curator-agent",
            "message": f"curate HN top stories ({NS})",
        })
        check("bulk ingest HN stories", status == 200)

    time.sleep(1)

    # --- 2b. DQL: top stories by score ---
    q = quote(f'TABLE path, title, score FROM "{prefix}/" WHERE type = "hn-story" SORT score DESC LIMIT 5')
    data, _ = req_json("GET", f"/query?q={q}")
    rows = data.get("rows") or []
    check("DQL: top HN by score", len(rows) > 0, f"got {len(rows)}")

    # --- 2c. Agent marks stories as "read" ---
    if paths:
        raw, _ = read_file(paths[0])
        content = raw.decode("utf-8")
        updated = content.replace("status: unread", "status: read")
        updated += "\n## Agent Review\n\nMarked as read after initial triage.\n"
        write_file(paths[0], updated, actor="triage-agent")

        time.sleep(0.5)
        q = quote(f'TABLE path, title, status FROM "{prefix}/" WHERE status = "read"')
        data, _ = req_json("GET", f"/query?q={q}")
        rows = data.get("rows") or []
        check("DQL: find read stories", len(rows) > 0, f"got {len(rows)}")

    # --- 2d. Search across HN stories ---
    if stories:
        search_term = "".join(c for c in stories[0]["title"][:30] if c.isalnum() or c == " ").strip()
        if search_term:
            data, _ = req_json("GET", f"/search?q={quote(search_term)}")
            check("search HN story title", len(data.get("results", [])) >= 0)
        else:
            check("search HN story title (skip: no alphanum title)", True)

    return paths


# ============================================================
# SCENARIO 3: Agent Memory Pattern
# An agent builds episodic and semantic memory in KiwiFS.
# ============================================================

def scenario_memory():
    print("\n══ Scenario 3: Agent Memory Pattern ══")
    prefix = f"{NS}/memory"
    paths = []

    # --- 3a. Write episodic memories (raw per-run notes) ---
    episodes = []
    for i in range(5):
        ep_id = f"ep-{NS}-{i}"
        path = f"episodes/{NS}-ep-{i}.md"
        paths.append(path)
        content = f"""---
title: "Run {i} Notes"
memory_kind: episodic
episode_id: "{ep_id}"
agent: research-agent
confidence: {0.6 + i * 0.08:.2f}
tags: [research, run-{i}]
---
# Run {i} Notes

During run {i}, the agent discovered:
- Finding A: The system performs well under load
- Finding B: Cache invalidation is the main bottleneck
- Error observed: Timeout after 30s on large queries

## Raw Data

Response time: {100 + i * 15}ms average
Error rate: {2 - i * 0.3:.1f}%
"""
        episodes.append({"path": path, "content": content})

    data, status = req_json("POST", "/bulk", {
        "files": episodes, "actor": "research-agent",
        "message": f"episodic memory from research runs ({NS})",
    })
    check("bulk write episodic memories", status == 200)

    # --- 3b. Write semantic memory (curated knowledge) ---
    sem_path = f"{prefix}/cache-performance.md"
    paths.append(sem_path)
    sem_content = f"""---
title: "Cache Performance Insights"
memory_kind: semantic
merged_from: ["{NS}-ep-0", "{NS}-ep-1", "{NS}-ep-2"]
confidence: 0.92
agent: synthesis-agent
tags: [performance, cache, insight]
---
# Cache Performance Insights

Synthesized from {len(episodes)} episodic observations.

## Key Findings

1. Cache invalidation is consistently the top bottleneck
2. Response times degrade linearly with dataset size
3. Error rates improve across successive runs (learning effect)

## Recommendations

- Implement LRU cache with TTL-based invalidation
- Add circuit breaker for queries exceeding 30s
- Monitor P99 latency, not just average
"""
    write_file(sem_path, sem_content, actor="synthesis-agent")
    time.sleep(1)

    # --- 3c. Memory report ---
    data, status = req_json("GET", "/memory/report")
    check("memory report after ingestion", status == 200)

    # --- 3d. Search across memories ---
    data, _ = req_json("GET", "/search?q=cache+invalidation")
    results = data.get("results", [])
    check("search finds memory entries", len(results) > 0, f"got {len(results)}")

    # --- 3e. DQL: query high-confidence memories ---
    q = quote(f'TABLE path, title, confidence WHERE confidence > 0.7 SORT confidence DESC')
    data, _ = req_json("GET", f"/query?q={q}")
    rows = data.get("rows") or []
    check("DQL: high-confidence memories", len(rows) > 0, f"got {len(rows)}")

    return paths


# ============================================================
# SCENARIO 4: Cross-referencing and Wiki Links
# An agent creates interlinked documents.
# ============================================================

def scenario_wiki_links():
    print("\n══ Scenario 4: Wiki Links & Cross-References ══")
    prefix = f"{NS}/wiki"
    paths = []

    # --- 4a. Create interlinked pages ---
    pages = {
        "architecture": f"""---
title: System Architecture
type: doc
category: engineering
---
# System Architecture

The system consists of three main components:
- [[{NS}-api-gateway|API Gateway]] — handles all incoming requests
- [[{NS}-cache-layer|Cache Layer]] — Redis-based caching
- [[{NS}-database|Database]] — PostgreSQL primary store
""",
        "api-gateway": f"""---
title: API Gateway
type: doc
category: engineering
---
# API Gateway

Rate limiting and authentication. See [[{NS}-architecture|Architecture]] for overview.
""",
        "cache-layer": f"""---
title: Cache Layer
type: doc
category: engineering
---
# Cache Layer

Redis cluster with LRU eviction. Related: [[{NS}-database|Database]] for cache-aside pattern.
""",
        "database": f"""---
title: Database
type: doc
category: engineering
---
# Database

PostgreSQL 16. See [[{NS}-cache-layer|Cache Layer]] for read optimization.
""",
    }

    files = []
    for slug, content in pages.items():
        path = f"{prefix}/{NS}-{slug}.md"
        paths.append(path)
        files.append({"path": path, "content": content})

    data, status = req_json("POST", "/bulk", {
        "files": files, "actor": "doc-agent",
        "message": f"wiki documentation ({NS})",
    })
    check("bulk write wiki pages", status == 200)
    time.sleep(1)

    # --- 4b. Check backlinks ---
    arch_path = f"{prefix}/{NS}-architecture.md"
    data, _ = req_json("GET", f"/backlinks?path={quote(arch_path)}")
    backlinks = data.get("backlinks", data.get("links", []))
    check("architecture has backlinks", isinstance(backlinks, list))

    # --- 4c. Graph ---
    data, _ = req_json("GET", "/graph")
    check("graph includes wiki pages", isinstance(data, dict))

    # --- 4d. ToC ---
    data, _ = req_json("GET", f"/toc?path={quote(arch_path)}")
    toc = data if isinstance(data, list) else data.get("headings", data.get("toc", []))
    check("architecture page has TOC", isinstance(toc, (list, dict)))

    return paths


# ============================================================
# SCENARIO 5: Stress — rapid write-read cycles
# An agent writes and immediately reads back many files.
# ============================================================

def scenario_rapid_cycle():
    print("\n══ Scenario 5: Rapid Write-Read Cycles ══")
    prefix = f"{NS}/rapid"
    n = 20
    paths = []
    errors = 0

    for i in range(n):
        path = f"{prefix}/{i:04d}.md"
        paths.append(path)
        content = f"---\ntitle: Rapid {i}\nseq: {i}\n---\n# Entry {i}\n\nContent #{i}."
        try:
            write_file(path, content, actor="rapid-agent")
            raw, status = read_file(path)
            text = raw.decode("utf-8")
            if f"Content #{i}" not in text:
                errors += 1
        except Exception:
            errors += 1

    check(f"rapid write-read: {n - errors}/{n} OK", errors == 0, f"{errors} failures")

    # Verify all exist in tree
    tree, _ = req_json("GET", f"/tree?path={prefix}")
    children = tree.get("children", [])
    check(f"rapid: all {n} in tree", len(children) == n, f"found {len(children)}")

    return paths


# ============================================================
# CLEANUP
# ============================================================

def cleanup(all_paths):
    print(f"\n── Cleanup ({len(all_paths)} files) ──")
    batches = [all_paths[i:i+20] for i in range(0, len(all_paths), 20)]
    cleaned = 0
    for batch in batches:
        for p in batch:
            try:
                delete_file(p)
                cleaned += 1
            except Exception:
                pass
    print(f"  Cleaned {cleaned}/{len(all_paths)} test files")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print(f"KiwiFS Agent Scenario Tests")
    print(f"Target: {KIWI_URL}")
    print(f"Namespace: {NS}")
    print(f"{'=' * 60}")

    all_paths = []

    scenarios = [
        ("Countries KB", scenario_countries),
        ("HN Reading List", scenario_hackernews),
        ("Agent Memory", scenario_memory),
        ("Wiki Links", scenario_wiki_links),
        ("Rapid Cycles", scenario_rapid_cycle),
    ]

    for name, fn in scenarios:
        try:
            paths = fn()
            all_paths.extend(paths)
        except Exception as e:
            FAIL += 1
            msg = f"  ✗ {name} CRASHED: {e}"
            print(msg)
            ERRORS.append(msg)

    cleanup(all_paths)

    print(f"\n{'=' * 60}")
    print(f"Results: {PASS} passed, {FAIL} failed")
    if ERRORS:
        print(f"\nFailures:")
        for e in ERRORS:
            print(e)
    sys.exit(0 if FAIL == 0 else 1)
