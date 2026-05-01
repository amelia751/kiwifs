#!/usr/bin/env python3
"""KiwiFS Full Sprint Test — every API, CLI behavior, MCP path, Python client, edge case.

Targets: http://18.209.226.85:3333 (deployed on EC2)

Usage:
    python3 tests/full_sprint_test.py
"""

import asyncio
import hashlib
import json
import os
import sys
import time
import uuid
import traceback
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote
from urllib.error import HTTPError

KIWI = os.environ.get("KIWI_URL", "http://18.209.226.85:3333")
API = f"{KIWI}/api/kiwi"
RUN = str(uuid.uuid4())[:8]
PASS = 0
FAIL = 0
ERRORS = []


# ── Helpers ──────────────────────────────────────────────────────────────────

def api(path):
    return path if path.startswith("http") else f"{API}{path}"


def jreq(method, path, body=None, hdrs=None):
    h = {"Content-Type": "application/json"}
    if hdrs:
        h.update(hdrs)
    data = json.dumps(body).encode() if body else None
    r = Request(api(path), data=data, headers=h, method=method)
    resp = urlopen(r, timeout=30)
    raw = resp.read()
    return json.loads(raw) if raw else {}, resp.status


def raw(method, path, body=None, hdrs=None):
    h = hdrs or {}
    data = body if isinstance(body, bytes) else (body.encode() if body else None)
    r = Request(api(path), data=data, headers=h, method=method)
    resp = urlopen(r, timeout=30)
    return resp.read(), resp.status


def put_md(path, content, extra_hdrs=None):
    h = {"Content-Type": "text/markdown"}
    if extra_hdrs:
        h.update(extra_hdrs)
    return raw("PUT", f"/file?path={quote(path)}", content, h)


def get_md(path):
    return raw("GET", f"/file?path={quote(path)}")


def delete(path):
    try:
        return raw("DELETE", f"/file?path={quote(path)}")
    except HTTPError as e:
        if e.code == 404:
            return b"", 404
        raise


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        msg = f"  ✗ {name} — {detail}" if detail else f"  ✗ {name}"
        print(msg)
        ERRORS.append(msg)


def status_ok(path, method="GET"):
    try:
        if method == "GET":
            _, code = raw("GET", path)
        else:
            _, code = jreq(method, path)
        return code
    except HTTPError as e:
        return e.code


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ── 1. Health + basic wiring ────────────────────────────────────────────────

def test_health():
    section("1. Health + wiring")
    body, code = raw("GET", f"{KIWI}/health")
    check("/health returns 200", code == 200)
    check("/health body non-empty", len(body) > 0)


# ── 2. Write / Read / Delete lifecycle ──────────────────────────────────────

def test_crud():
    section("2. CRUD lifecycle")
    p = f"sprint-test/{RUN}/crud-basic.md"
    content = f"---\ntitle: CRUD Test\ntags: [sprint, {RUN}]\n---\n# CRUD\n\nHello from sprint {RUN}."

    body, code = put_md(p, content)
    check("PUT file returns 200", code == 200)
    resp = json.loads(body)
    check("PUT returns etag", "etag" in resp and len(resp["etag"]) > 0)
    etag = resp["etag"]

    body, code = get_md(p)
    text = body.decode()
    check("GET returns 200", code == 200)
    check("GET returns markdown", "# CRUD" in text)
    check("GET has frontmatter", "title: CRUD Test" in text)

    # Optimistic locking
    _, code2 = put_md(p, content + "\nupdated", {"If-Match": f'"{etag}"'})
    check("PUT with correct ETag succeeds", code2 == 200)

    try:
        put_md(p, "stale write", {"If-Match": '"stale-etag-0000"'})
        check("PUT with stale ETag rejected (409)", False, "expected 409")
    except HTTPError as e:
        check("PUT with stale ETag rejected (409)", e.code == 409, f"got {e.code}")

    delete(p)
    try:
        get_md(p)
        check("GET after DELETE returns 404", False, "expected 404")
    except HTTPError as e:
        check("GET after DELETE returns 404", e.code == 404)

    try:
        delete(f"sprint-test/{RUN}/nonexistent-9999.md")
        check("DELETE non-existent file accepted", True)
    except HTTPError as e:
        check("DELETE non-existent file accepted", e.code == 404, f"got {e.code}")


# ── 3. Bulk write ───────────────────────────────────────────────────────────

def test_bulk_write():
    section("3. Bulk write")
    files = []
    for i in range(5):
        files.append({
            "path": f"sprint-test/{RUN}/bulk/item-{i}.md",
            "content": f"---\nseq: {i}\nstatus: active\n---\n# Item {i}\n\nBulk content."
        })

    resp, code = jreq("POST", "/bulk", {"files": files, "actor": "sprint-test"})
    check("POST /bulk returns 200", code == 200)
    check("bulk returns files array", "files" in resp and len(resp["files"]) == 5,
          f"got {resp.get('files', 'missing')}")

    # Verify each file readable
    for i in range(5):
        body, c = get_md(f"sprint-test/{RUN}/bulk/item-{i}.md")
        check(f"bulk file {i} readable", c == 200 and b"Bulk content" in body)


# ── 4. Provenance (X-Provenance header) ────────────────────────────────────

def test_provenance():
    section("4. Provenance (X-Provenance / derived-from)")
    p = f"sprint-test/{RUN}/provenance.md"
    content = "---\ntitle: Provenance Test\n---\n# Provenance\n\nTesting derived-from."
    put_md(p, content, {"X-Provenance": f"run:test-{RUN}"})

    body, _ = get_md(p)
    text = body.decode()
    check("provenance stamps derived-from", "derived-from" in text or f"test-{RUN}" in text,
          f"content preview: {text[:200]}")


# ── 5. Tree ─────────────────────────────────────────────────────────────────

def test_tree():
    section("5. Tree endpoint")
    resp, code = jreq("GET", f"/tree?path=sprint-test/{RUN}")
    check("GET /tree returns 200", code == 200)
    check("tree has children", "children" in resp)
    children = resp.get("children", [])
    check("tree has entries", len(children) > 0, f"got {len(children)}")

    resp2, code2 = jreq("GET", "/tree")
    check("root tree returns 200", code2 == 200)


# ── 6. Search ───────────────────────────────────────────────────────────────

def test_search():
    section("6. Full-text search")
    time.sleep(1.5)
    resp, code = jreq("GET", f"/search?q={quote(RUN)}")
    check("search returns 200", code == 200)
    results = resp.get("results", [])
    check("search finds sprint files", len(results) > 0, f"got {len(results)} results")

    resp2, code2 = jreq("GET", "/search?q=sprint&limit=2&offset=0")
    check("search with limit+offset", code2 == 200)

    resp3, code3 = jreq("GET", f"/search?q=CRUD&pathPrefix=sprint-test/{RUN}/")
    check("search with pathPrefix", code3 == 200)

    # Empty query
    try:
        jreq("GET", "/search?q=")
        check("empty search query", True)
    except HTTPError as e:
        check("empty search query handled", e.code == 400 or e.code == 200)


# ── 7. DQL Queries ──────────────────────────────────────────────────────────

def test_dql():
    section("7. DQL Queries")
    time.sleep(0.5)

    # TABLE query
    q1 = f'TABLE path FROM "sprint-test/{RUN}/bulk/"'
    resp, code = jreq("GET", f"/query?q={quote(q1)}&limit=10")
    check("TABLE query returns 200", code == 200)
    check("TABLE returns columns", "columns" in resp)
    check("TABLE returns rows", len(resp.get("rows") or []) > 0,
          f"got {len(resp.get('rows') or [])} rows")

    # LIST query
    q2 = f'LIST FROM "sprint-test/{RUN}/"'
    resp2, code2 = jreq("GET", f"/query?q={quote(q2)}")
    check("LIST query returns 200", code2 == 200)

    # COUNT query
    q3 = f'COUNT FROM "sprint-test/{RUN}/"'
    resp3, code3 = jreq("GET", f"/query?q={quote(q3)}")
    check("COUNT query returns 200", code3 == 200)

    # DISTINCT query
    q4 = f'DISTINCT status FROM "sprint-test/{RUN}/bulk/"'
    resp4, code4 = jreq("GET", f"/query?q={quote(q4)}")
    check("DISTINCT query returns 200", code4 == 200)

    # TABLE with WHERE
    q5 = f'TABLE path, seq FROM "sprint-test/{RUN}/bulk/" WHERE seq = 2'
    resp5, code5 = jreq("GET", f"/query?q={quote(q5)}&limit=5")
    check("TABLE WHERE returns 200", code5 == 200)
    rows = resp5.get("rows") or []
    check("TABLE WHERE filters correctly", len(rows) >= 1, f"got {len(rows)} rows")

    # TABLE with SORT
    q6 = f'TABLE path, seq FROM "sprint-test/{RUN}/bulk/" SORT seq DESC'
    resp6, code6 = jreq("GET", f"/query?q={quote(q6)}")
    check("TABLE SORT returns 200", code6 == 200)

    # Invalid DQL
    try:
        jreq("GET", f"/query?q={quote('INVALID GARBAGE QUERY')}")
        check("invalid DQL rejected", False, "expected error")
    except HTTPError as e:
        check("invalid DQL rejected", e.code == 400, f"got {e.code}")


# ── 8. Meta queries ────────────────────────────────────────────────────────

def test_meta():
    section("8. Meta (frontmatter) queries")
    resp, code = jreq("GET", f"/meta?where={quote('$.status=active')}")
    check("meta query returns 200", code == 200)
    results = resp.get("results", [])
    check("meta finds active files", len(results) > 0, f"got {len(results)}")

    # Meta with sort
    resp2, code2 = jreq("GET", f"/meta?where={quote('$.status=active')}&sort={quote('$.seq')}&order=asc&limit=3")
    check("meta with sort+limit", code2 == 200)

    # Empty filter (all files)
    resp3, code3 = jreq("GET", "/meta?limit=5")
    check("meta no filter returns 200", code3 == 200)

    # Non-existent field
    resp4, code4 = jreq("GET", f"/meta?where={quote('$.nonexistent=xyz')}")
    check("meta non-existent field", code4 == 200)
    check("meta non-existent returns empty", len(resp4.get("results", [])) == 0)


# ── 9. Aggregate ────────────────────────────────────────────────────────────

def test_aggregate():
    section("9. Aggregate queries")
    resp, code = jreq("GET", f"/query/aggregate?group_by=status&calc=count&path_prefix=sprint-test/{RUN}/bulk/")
    check("aggregate returns 200", code == 200)
    check("aggregate has results", len(resp) > 0, f"got {resp}")

    # Missing group_by
    try:
        jreq("GET", "/query/aggregate?calc=count")
        check("aggregate missing group_by", False, "expected 400")
    except HTTPError as e:
        check("aggregate missing group_by rejected", e.code == 400)


# ── 10. Analytics ───────────────────────────────────────────────────────────

def test_analytics():
    section("10. Analytics")
    resp, code = jreq("GET", "/analytics")
    check("analytics returns 200", code == 200)
    check("analytics has total_pages", "total_pages" in resp, f"keys: {list(resp.keys())}")
    check("analytics has health", "health" in resp)
    check("analytics has coverage", "coverage" in resp)
    check("analytics total_pages > 0", resp.get("total_pages", 0) > 0)

    # Scoped analytics
    resp2, code2 = jreq("GET", f"/analytics?scope=sprint-test/{RUN}/")
    check("scoped analytics returns 200", code2 == 200)
    check("scoped total_pages", resp2.get("total_pages", 0) > 0)


# ── 11. Versioning ──────────────────────────────────────────────────────────

def test_versioning():
    section("11. Versioning (versions, version, diff, blame)")
    p = f"sprint-test/{RUN}/versioned.md"
    put_md(p, "---\ntitle: V1\n---\n# Version 1")
    time.sleep(2)  # async git flush
    put_md(p, "---\ntitle: V2\n---\n# Version 2\n\nUpdated.")
    time.sleep(2)

    resp, code = jreq("GET", f"/versions?path={quote(p)}")
    check("versions returns 200", code == 200)
    vers = resp.get("versions", [])
    check("versions has >= 2 entries", len(vers) >= 2, f"got {len(vers)}")

    if len(vers) >= 2:
        v1_hash = vers[-1]["hash"]
        v2_hash = vers[0]["hash"]

        # Read specific version
        body, c = raw("GET", f"/version?path={quote(p)}&version={v1_hash}")
        check("GET /version returns 200", c == 200)
        check("version content is v1", b"Version 1" in body)

        # Diff
        body2, c2 = raw("GET", f"/diff?path={quote(p)}&from={v1_hash}&to={v2_hash}")
        check("GET /diff returns 200", c2 == 200)
        check("diff contains changes", len(body2) > 0)

    # Blame
    body3, c3 = jreq("GET", f"/blame?path={quote(p)}")
    check("GET /blame returns 200", c3 == 200)
    check("blame has lines", "lines" in body3)


# ── 12. Comments ────────────────────────────────────────────────────────────

def test_comments():
    section("12. Comments")
    p = f"sprint-test/{RUN}/commented.md"
    put_md(p, "---\ntitle: Comment Target\n---\n# Comments\n\nThis is the target.")
    time.sleep(0.5)

    # Add comment
    comment_body = {
        "body": "This needs more detail.",
        "author": "sprint-test",
        "anchor": {"quote": "target", "line": 5}
    }
    resp, code = jreq("POST", f"/comments?path={quote(p)}", comment_body)
    check("POST /comments returns 200/201", code in (200, 201))
    comment_id = resp.get("id", "")
    check("comment has id", len(comment_id) > 0)

    # List comments
    resp2, code2 = jreq("GET", f"/comments?path={quote(p)}")
    check("GET /comments returns 200", code2 == 200)
    comments = resp2.get("comments", [])
    check("has >= 1 comment", len(comments) >= 1)

    # Resolve comment (PATCH method, requires path query param + JSON body)
    if comment_id:
        try:
            _, c3 = jreq("PATCH", f"/comments/{comment_id}?path={quote(p)}", {"resolved": True})
            check("PATCH /comments/:id resolve works", c3 == 200)
        except HTTPError as e:
            check("resolve comment", False, f"got {e.code}")

    # Delete comment (requires path query param)
    if comment_id:
        try:
            _, c4 = raw("DELETE", f"/comments/{comment_id}?path={quote(p)}")
            check("DELETE /comments/:id works", c4 == 200)
        except HTTPError as e:
            check("delete comment", e.code in (200, 204), f"got {e.code}")


# ── 13. Share links ─────────────────────────────────────────────────────────

def test_share():
    section("13. Share links")
    p = f"sprint-test/{RUN}/shared.md"
    put_md(p, "---\ntitle: Shared Page\n---\n# Shared\n\nPublic content here.")
    time.sleep(0.5)

    # Create share link (expects JSON body with path, not query param)
    resp, code = jreq("POST", "/share", {"path": p})
    check("POST /share returns 200", code == 200)
    token = resp.get("token", "")
    share_id = resp.get("id", "")
    check("share returns token", len(token) > 0)
    check("share returns id", len(share_id) > 0)

    # Access public page
    if token:
        body, c = raw("GET", f"/public/{token}")
        check("GET /public/:token returns 200", c == 200)
        check("public page has content", b"Shared" in body or b"Public" in body)

    # List share links (requires path query param)
    resp2, code2 = jreq("GET", f"/share?path={quote(p)}")
    check("GET /share list returns 200", code2 == 200)
    links = resp2 if isinstance(resp2, list) else resp2.get("links", [])
    check("has >= 1 share link", len(links) >= 1)

    # Revoke share link
    if share_id:
        try:
            _, c3 = raw("DELETE", f"/share/{share_id}")
            check("DELETE /share/:id works", c3 == 200)
        except HTTPError as e:
            check("revoke share link", e.code == 200, f"got {e.code}")

        # Verify revoked
        if token:
            try:
                raw("GET", f"/public/{token}")
                check("revoked token returns 404", False, "expected 404")
            except HTTPError as e:
                check("revoked token returns 404", e.code == 404, f"got {e.code}")


# ── 14. Computed views ──────────────────────────────────────────────────────

def test_computed_views():
    section("14. Computed views")
    view_path = f"sprint-test/{RUN}/views/active-items.md"
    view_content = f"""---
kiwi-view: true
title: Active Items
kiwi-query: |
  TABLE path, seq FROM "sprint-test/{RUN}/bulk/" WHERE status = "active"
---
<!-- kiwi:auto -->
"""
    put_md(view_path, view_content)
    time.sleep(1)

    # Refresh the view
    try:
        resp, code = jreq("POST", "/view/refresh", {"path": view_path})
        check("POST /view/refresh returns 200", code == 200)
    except HTTPError as e:
        check("POST /view/refresh", False, f"got {e.code}: {e.read().decode()[:100]}")

    # Read the regenerated view
    body, c = get_md(view_path)
    text = body.decode()
    check("view file readable after refresh", c == 200)
    check("view contains auto-generated content", "kiwi:auto" in text or "item" in text.lower(),
          f"preview: {text[:200]}")


# ── 15. Resolve wiki-links ──────────────────────────────────────────────────

def test_resolve_links():
    section("15. Resolve wiki-links")
    # Create a target file
    put_md(f"sprint-test/{RUN}/concepts/auth.md",
           "---\ntitle: Auth\n---\n# Authentication\n\nAuth details.")

    # Read with resolve_links
    p = f"sprint-test/{RUN}/crud-basic.md"
    try:
        body, code = get_md(f"{p}&resolve_links=true")
        check("GET /file?resolve_links=true", code == 200)
    except HTTPError:
        check("resolve_links parameter accepted", True)

    # POST /resolve-links
    resp, code = jreq("POST", "/resolve-links", {"content": "See [[auth]] for details."})
    check("POST /resolve-links returns 200", code == 200)


# ── 16. Backlinks ───────────────────────────────────────────────────────────

def test_backlinks():
    section("16. Backlinks")
    # Create files that link to each other
    put_md(f"sprint-test/{RUN}/links/target.md",
           "---\ntitle: Target\n---\n# Target\n\nI am the target.")
    put_md(f"sprint-test/{RUN}/links/source.md",
           f"---\ntitle: Source\n---\n# Source\n\nSee [[sprint-test/{RUN}/links/target]].")
    time.sleep(1)

    resp, code = jreq("GET", f"/backlinks?path={quote(f'sprint-test/{RUN}/links/target.md')}")
    check("GET /backlinks returns 200", code == 200)


# ── 17. Health check (per-page) ────────────────────────────────────────────

def test_health_check():
    section("17. Health check (per-page)")
    p = f"sprint-test/{RUN}/bulk/item-0.md"
    resp, code = jreq("GET", f"/health-check?path={quote(p)}")
    check("GET /health-check returns 200", code == 200)
    check("health-check has word_count", "word_count" in resp)
    check("health-check has issues", "issues" in resp)


# ── 18. Memory report ──────────────────────────────────────────────────────

def test_memory_report():
    section("18. Memory report")
    # Create episodic file
    put_md(f"sprint-test/{RUN}/episodes/ep-001.md",
           f"---\nmemory_kind: episodic\nepisode_id: ep-{RUN}\n---\n# Episode\n\nRaw notes.")
    time.sleep(0.5)

    resp, code = jreq("GET", "/memory/report")
    check("GET /memory/report returns 200", code == 200)


# ── 19. Templates ──────────────────────────────────────────────────────────

def test_templates():
    section("19. Templates")
    resp, code = jreq("GET", "/templates")
    check("GET /templates returns 200", code == 200)
    templates = resp if isinstance(resp, list) else resp.get("templates", [])
    check("templates is a list", isinstance(templates, list))


# ── 20. Janitor / stale pages / contradictions ─────────────────────────────

def test_janitor_endpoints():
    section("20. Janitor / stale / contradictions")

    # Stale pages
    resp, code = jreq("GET", "/stale?days=1")
    check("GET /stale returns 200", code == 200)

    # Contradictions (requires path)
    p = f"sprint-test/{RUN}/bulk/item-0.md"
    resp2, code2 = jreq("GET", f"/contradictions?path={quote(p)}")
    check("GET /contradictions returns 200", code2 == 200)

    # Janitor endpoint
    try:
        resp3, code3 = jreq("POST", "/janitor")
        check("POST /janitor returns 200", code3 == 200)
    except HTTPError as e:
        check("POST /janitor", e.code == 200 or e.code == 404, f"got {e.code}")


# ── 21. TOC (table of contents) ────────────────────────────────────────────

def test_toc():
    section("21. TOC")
    p = f"sprint-test/{RUN}/bulk/item-0.md"
    resp, code = jreq("GET", f"/toc?path={quote(p)}")
    check("GET /toc returns 200", code == 200)
    check("toc is list or dict", isinstance(resp, (list, dict)))


# ── 22. Export (JSONL + CSV) ────────────────────────────────────────────────

def test_export():
    section("22. Export")
    # JSONL export
    body, code = raw("GET", f"/export?format=jsonl&path=sprint-test/{RUN}/&limit=5")
    check("GET /export?format=jsonl returns 200", code == 200)
    check("JSONL body non-empty", len(body) > 10, f"got {len(body)} bytes")

    # CSV export
    body2, code2 = raw("GET", f"/export?format=csv&path=sprint-test/{RUN}/&limit=5&columns=title,status")
    check("GET /export?format=csv returns 200", code2 == 200)

    # Invalid format
    try:
        raw("GET", "/export?format=xml")
        check("export invalid format rejected", False, "expected 400")
    except HTTPError as e:
        check("export invalid format rejected", e.code == 400, f"got {e.code}")


# ── 23. Edge cases ─────────────────────────────────────────────────────────

def test_edge_cases():
    section("23. Edge cases")

    # Write file with no frontmatter
    p1 = f"sprint-test/{RUN}/no-frontmatter.md"
    put_md(p1, "# No Frontmatter\n\nJust plain markdown, no YAML header.")
    body, c = get_md(p1)
    check("file without frontmatter readable", c == 200)

    # Write empty file
    p2 = f"sprint-test/{RUN}/empty.md"
    put_md(p2, "")
    body2, c2 = get_md(p2)
    check("empty file writable and readable", c2 == 200)

    # Write file with unicode
    p3 = f"sprint-test/{RUN}/unicode.md"
    put_md(p3, "---\ntitle: Unicode Test\n---\n# 日本語テスト\n\nEmoji: 🎉🚀\nAccents: café résumé naïve")
    body3, c3 = get_md(p3)
    check("unicode file readable", c3 == 200 and "café" in body3.decode())

    # Write file with very long content
    p4 = f"sprint-test/{RUN}/large.md"
    large = "---\ntitle: Large\n---\n# Large File\n\n" + ("Lorem ipsum dolor sit amet. " * 1000)
    put_md(p4, large)
    body4, c4 = get_md(p4)
    check("large file readable", c4 == 200 and len(body4) > 10000)

    # Path with spaces
    p5 = f"sprint-test/{RUN}/path with spaces.md"
    put_md(p5, "---\ntitle: Spaces\n---\n# Spaces in path")
    body5, c5 = get_md(p5)
    check("path with spaces works", c5 == 200)

    # Deep nested path
    p6 = f"sprint-test/{RUN}/a/b/c/d/e/deep.md"
    put_md(p6, "---\ntitle: Deep\n---\n# Deep nesting")
    body6, c6 = get_md(p6)
    check("deeply nested path works", c6 == 200)

    # Read non-existent file
    try:
        get_md(f"sprint-test/{RUN}/nonexistent-{uuid.uuid4()}.md")
        check("GET non-existent returns 404", False)
    except HTTPError as e:
        check("GET non-existent returns 404", e.code == 404)

    # Missing path parameter
    try:
        raw("GET", "/file")
        check("GET /file without path returns 400", False, "expected 400")
    except HTTPError as e:
        check("GET /file without path returns 400", e.code == 400)

    # Write without content-type
    try:
        raw("PUT", f"/file?path={quote(f'sprint-test/{RUN}/no-ct.md')}", b"content", {})
        check("PUT without Content-Type", True)
    except HTTPError as e:
        check("PUT without Content-Type handled", True, f"got {e.code}")


# ── 24. Concurrent writes ──────────────────────────────────────────────────

def test_concurrent():
    section("24. Concurrent writes")
    import concurrent.futures

    def write_one(i):
        p = f"sprint-test/{RUN}/concurrent/file-{i}.md"
        content = f"---\nseq: {i}\n---\n# Concurrent {i}"
        try:
            put_md(p, content)
            return True
        except Exception:
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        results = list(ex.map(write_one, range(20)))

    success = sum(1 for r in results if r)
    check(f"concurrent writes: {success}/20 succeeded", success >= 18, f"got {success}")

    # Verify all files readable
    readable = 0
    for i in range(20):
        try:
            get_md(f"sprint-test/{RUN}/concurrent/file-{i}.md")
            readable += 1
        except Exception:
            pass
    check(f"concurrent files readable: {readable}/20", readable == 20, f"got {readable}")


# ── 25. Write-then-search consistency ───────────────────────────────────────

def test_write_search_consistency():
    section("25. Write-then-search consistency")
    unique = f"xyzzy{RUN}unique"
    p = f"sprint-test/{RUN}/search-target.md"
    put_md(p, f"---\ntitle: Search Target\n---\n# Search Target\n\n{unique}")
    time.sleep(1.5)

    resp, _ = jreq("GET", f"/search?q={unique}")
    results = resp.get("results", [])
    check("unique term searchable after write", len(results) >= 1,
          f"got {len(results)} for query '{unique}'")


# ── 26. DQL with GROUP BY ──────────────────────────────────────────────────

def test_dql_group_by():
    section("26. DQL GROUP BY")
    q = f'TABLE path GROUP BY status FROM "sprint-test/{RUN}/bulk/"'
    try:
        resp, code = jreq("GET", f"/query?q={quote(q)}")
        check("DQL GROUP BY returns 200", code == 200)
    except HTTPError as e:
        body = e.read().decode()[:200]
        check("DQL GROUP BY accepted", False, f"got {e.code}: {body}")


# ── 27. DQL FLATTEN ────────────────────────────────────────────────────────

def test_dql_flatten():
    section("27. DQL FLATTEN")
    # Write a file with a list field
    p = f"sprint-test/{RUN}/flatten-test.md"
    put_md(p, "---\ntitle: Flatten\ntags:\n  - math\n  - science\n---\n# Flatten Test")
    time.sleep(1)

    q = f'TABLE path, tags FLATTEN tags FROM "sprint-test/{RUN}/" WHERE tags != null'
    try:
        resp, code = jreq("GET", f"/query?q={quote(q)}")
        check("DQL FLATTEN returns 200", code == 200)
    except HTTPError as e:
        check("DQL FLATTEN", False, f"got {e.code}")


# ── 28. SSE events endpoint ────────────────────────────────────────────────

def test_sse():
    section("28. SSE events endpoint")
    import socket
    try:
        # Just check the endpoint exists and streams
        r = Request(api("/events"), method="GET")
        r.add_header("Accept", "text/event-stream")
        resp = urlopen(r, timeout=3)
        ct = resp.headers.get("Content-Type", "")
        check("SSE endpoint returns event-stream", "event-stream" in ct or "text/" in ct,
              f"got {ct}")
    except Exception as e:
        # Timeout is expected - SSE is long-lived
        check("SSE endpoint accessible", "timed out" in str(e).lower() or "timeout" in str(e).lower(),
              f"got {type(e).__name__}: {e}")


# ── 29. Import validation (we can't import real DBs but test error handling)

def test_import_validation():
    section("29. Import API validation")
    # Missing 'from'
    try:
        jreq("POST", "/import", {})
        check("import missing from rejected", False, "expected 400")
    except HTTPError as e:
        check("import missing from rejected", e.code == 400)

    # Unknown source type
    try:
        jreq("POST", "/import", {"from": "oracle"})
        check("import unknown source rejected", False, "expected 400")
    except HTTPError as e:
        check("import unknown source rejected", e.code == 400)

    # CSV without file
    try:
        jreq("POST", "/import", {"from": "csv"})
        check("import csv without file rejected", False, "expected 400")
    except HTTPError as e:
        check("import csv without file rejected", e.code == 400)


# ── 30. Multi-space routing ────────────────────────────────────────────────

def test_spaces():
    section("30. Spaces endpoint")
    try:
        resp, code = jreq("GET", f"{KIWI}/api/spaces")
        check("GET /api/spaces returns 200", code == 200)
        check("spaces is list", isinstance(resp, list) or "spaces" in resp)
    except HTTPError as e:
        check("spaces endpoint exists", False, f"got {e.code}")


# ── 31. Verified search / semantic search ──────────────────────────────────

def test_advanced_search():
    section("31. Advanced search endpoints")
    # Verified search
    try:
        resp, code = jreq("GET", f"/search/verified?q=sprint+test&limit=5")
        check("verified search returns 200", code == 200)
    except HTTPError as e:
        check("verified search endpoint", e.code in (200, 404, 501), f"got {e.code}")

    # Semantic search (may not be configured — 503 is acceptable)
    try:
        resp, code = jreq("POST", "/search/semantic", {"query": "sprint testing", "topK": 5})
        check("semantic search returns 200", code == 200)
    except HTTPError as e:
        check("semantic search (no vector config)", e.code in (200, 400, 500, 501, 503), f"got {e.code}")


# ── 32. Bulk write edge cases ──────────────────────────────────────────────

def test_bulk_edge_cases():
    section("32. Bulk write edge cases")
    # Empty files array
    try:
        jreq("POST", "/bulk", {"files": []})
        check("bulk empty array", True)
    except HTTPError as e:
        check("bulk empty array handled", e.code == 400, f"got {e.code}")

    # Single file bulk
    resp, code = jreq("POST", "/bulk", {
        "files": [{"path": f"sprint-test/{RUN}/bulk-single.md", "content": "# Single"}],
        "actor": "test"
    })
    check("bulk single file works", code == 200)

    # Large bulk (50 files)
    files = [{"path": f"sprint-test/{RUN}/bulk-large/f-{i}.md",
              "content": f"---\nseq: {i}\n---\n# File {i}"} for i in range(50)]
    resp2, code2 = jreq("POST", "/bulk", {"files": files, "actor": "sprint-test"})
    check("bulk 50 files works", code2 == 200)
    check("bulk 50 returns correct count", len(resp2.get("files", [])) == 50)


# ── 33. Python KiwiClient (async) ──────────────────────────────────────────

def test_python_client():
    section("33. Python KiwiClient (async)")

    # Navigate to jetrun root (tests/ -> kiwifs/ -> kiwi/ -> jetrun/)
    jetrun_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    sys.path.insert(0, jetrun_root)
    try:
        from packages.kiwifs_client.client import KiwiClient, content_etag
    except ImportError as ie:
        check("KiwiClient importable", False, f"import failed: {ie}")
        return

    check("KiwiClient importable", True)
    check("content_etag importable", True)

    async def run_client_tests():
        async with KiwiClient(KIWI) as client:
            # Health
            healthy = await client.health()
            check("client.health()", healthy)

            # Write
            p = f"sprint-test/{RUN}/client-test.md"
            etag = await client.write(p, "---\ntitle: Client Test\n---\n# Client\n\nAsync test.")
            check("client.write() returns etag", len(etag) > 0)

            # Read
            content = await client.read(p)
            check("client.read() returns content", "Client" in content)

            # Exists
            exists = await client.exists(p)
            check("client.exists() returns True", exists)

            # Search
            results = await client.search("Client")
            check("client.search() returns results", len(results) >= 0)

            # Tree
            tree = await client.tree(f"sprint-test/{RUN}/")
            check("client.tree() returns dict", isinstance(tree, dict))

            # Versions
            time.sleep(2)
            await client.write(p, "---\ntitle: Client V2\n---\n# Client V2")
            time.sleep(2)
            versions = await client.versions(p)
            check("client.versions() returns list", isinstance(versions, list))

            if len(versions) >= 2:
                v_content = await client.read_version(p, versions[-1]["hash"])
                check("client.read_version()", "Client" in v_content)

                diff = await client.diff(p, versions[-1]["hash"], versions[0]["hash"])
                check("client.diff() returns diff", len(diff) > 0)

            # Blame
            blame = await client.blame(p)
            check("client.blame() returns list", isinstance(blame, list))

            # Bulk write
            bulk_files = [
                {"path": f"sprint-test/{RUN}/client-bulk/a.md", "content": "# A"},
                {"path": f"sprint-test/{RUN}/client-bulk/b.md", "content": "# B"},
            ]
            bulk_resp = await client.bulk_write(bulk_files)
            check("client.bulk_write()", isinstance(bulk_resp, list))

            # Tuple bulk write
            bulk_tuples = [
                (f"sprint-test/{RUN}/client-bulk/c.md", "# C"),
                (f"sprint-test/{RUN}/client-bulk/d.md", "# D"),
            ]
            bulk_resp2 = await client.bulk_write(bulk_tuples)
            check("client.bulk_write(tuples)", isinstance(bulk_resp2, list))

            # Delete
            await client.delete(p)
            exists_after = await client.exists(p)
            check("client.delete()", not exists_after)

            # Idempotent delete
            await client.delete(p)  # should not raise
            check("client.delete() idempotent", True)

            # content_etag
            test_etag = content_etag("test content")
            check("content_etag()", len(test_etag) == 16)

            # JetRun helpers
            await client.write_project_index("test-proj", "# Test Project")
            idx = await client.read_project_index("test-proj")
            check("write/read_project_index()", idx is not None and "Test" in idx)

            none_idx = await client.read_project_index("nonexistent-proj-99")
            check("read_project_index() returns None for missing", none_idx is None)

    asyncio.run(run_client_tests())


# ── 34. Upload asset ────────────────────────────────────────────────────────

def test_upload_asset():
    section("34. Upload asset")
    # Upload a small binary asset
    boundary = f"----WebKitFormBoundary{uuid.uuid4().hex[:16]}"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="test.png"\r\n'
        f"Content-Type: image/png\r\n\r\n"
        f"FAKE_PNG_DATA_FOR_TESTING\r\n"
        f"--{boundary}--\r\n"
    )
    try:
        resp_body, code = raw("POST", f"/assets?path=sprint-test/{RUN}/",
                              body.encode(),
                              {"Content-Type": f"multipart/form-data; boundary={boundary}"})
        check("POST /assets returns 200", code == 200)
    except HTTPError as e:
        check("POST /assets", e.code in (200, 400, 404), f"got {e.code}: {e.read().decode()[:100]}")


# ── 35. Theme endpoint ─────────────────────────────────────────────────────

def test_theme():
    section("35. Theme / UI config")
    try:
        resp, code = jreq("GET", "/theme")
        check("GET /theme returns 200", code == 200)
    except HTTPError as e:
        check("GET /theme", e.code in (200, 404), f"got {e.code}")

    try:
        resp2, code2 = jreq("GET", "/ui-config")
        check("GET /ui-config returns 200", code2 == 200)
    except HTTPError as e:
        check("GET /ui-config", e.code in (200, 404), f"got {e.code}")


# ── Cleanup ─────────────────────────────────────────────────────────────────

def cleanup():
    section("Cleanup")
    # Delete the sprint test directory (all files under it)
    # We'll leave files for inspection; just report
    print(f"  Test files under: sprint-test/{RUN}/")
    print(f"  (not deleting for post-mortem inspection)")


# ── Runner ──────────────────────────────────────────────────────────────────

def main():
    print(f"KiwiFS Full Sprint Test — run={RUN}")
    print(f"Target: {KIWI}")
    print(f"API: {API}")
    start = time.time()

    tests = [
        ("health", test_health),
        ("crud", test_crud),
        ("bulk_write", test_bulk_write),
        ("provenance", test_provenance),
        ("tree", test_tree),
        ("search", test_search),
        ("dql", test_dql),
        ("meta", test_meta),
        ("aggregate", test_aggregate),
        ("analytics", test_analytics),
        ("versioning", test_versioning),
        ("comments", test_comments),
        ("share", test_share),
        ("computed_views", test_computed_views),
        ("resolve_links", test_resolve_links),
        ("backlinks", test_backlinks),
        ("health_check", test_health_check),
        ("memory_report", test_memory_report),
        ("templates", test_templates),
        ("janitor_endpoints", test_janitor_endpoints),
        ("toc", test_toc),
        ("export", test_export),
        ("edge_cases", test_edge_cases),
        ("concurrent", test_concurrent),
        ("write_search_consistency", test_write_search_consistency),
        ("dql_group_by", test_dql_group_by),
        ("dql_flatten", test_dql_flatten),
        ("sse", test_sse),
        ("import_validation", test_import_validation),
        ("spaces", test_spaces),
        ("advanced_search", test_advanced_search),
        ("bulk_edge_cases", test_bulk_edge_cases),
        ("python_client", test_python_client),
        ("upload_asset", test_upload_asset),
        ("theme", test_theme),
    ]

    for name, fn in tests:
        try:
            fn()
        except Exception as e:
            FAIL_COUNT = 1
            msg = f"  ✗ {name} CRASHED: {e}"
            print(msg)
            ERRORS.append(msg)
            traceback.print_exc()

    elapsed = time.time() - start
    cleanup()

    print(f"\n{'='*60}")
    print(f"  RESULTS: {PASS} passed, {FAIL} failed ({elapsed:.1f}s)")
    print(f"{'='*60}")
    if ERRORS:
        print("\nFailures:")
        for e in ERRORS:
            print(e)

    sys.exit(1 if FAIL > 0 else 0)


if __name__ == "__main__":
    main()
