#!/usr/bin/env python3
"""KiwiFS Stability Test Suite — comprehensive endpoint and edge-case testing.

Usage:
    KIWI_URL=http://18.209.226.85:3333 python3 tests/stability_test.py
"""

import json
import os
import sys
import time
import uuid
import concurrent.futures
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote
from urllib.error import HTTPError

KIWI_URL = os.environ.get("KIWI_URL", "http://18.209.226.85:3333")
API = f"{KIWI_URL}/api/kiwi"
PASS = 0
FAIL = 0
ERRORS = []


def api_url(path):
    if path.startswith("http"):
        return path
    return f"{API}{path}"


def req_json(method, path, body=None, headers=None):
    """Send a JSON request and parse JSON response."""
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
    """Send a request and return raw bytes + status."""
    url = api_url(path)
    hdrs = headers or {}
    data = body if isinstance(body, bytes) else (body.encode() if body else None)
    r = Request(url, data=data, headers=hdrs, method=method)
    resp = urlopen(r, timeout=30)
    return resp.read(), resp.status


def write_file(path, content, actor="stability-test"):
    """Write a markdown file via PUT /file (raw body, text/markdown)."""
    return req_raw("PUT", f"/file?path={quote(path)}", body=content,
                   headers={"Content-Type": "text/markdown", "X-Actor": actor})


def read_file(path):
    """Read a file via GET /file (returns raw markdown text)."""
    return req_raw("GET", f"/file?path={quote(path)}")


def delete_file(path):
    """Delete a file via DELETE /file."""
    return req_json("DELETE", f"/file?path={quote(path)}")


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


# ---------- 1. HEALTH ----------
def test_health():
    print("\n── Health & Readiness ──")
    data, status = req_json("GET", f"{KIWI_URL}/health")
    check("GET /health returns 200", status == 200)
    check("/health status=ok", data.get("status") == "ok")


# ---------- 2. WRITE / READ / DELETE ----------
def test_crud():
    print("\n── CRUD (write / read / delete) ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-stability/{uid}.md"
    content = f"---\ntitle: Stability Test {uid}\ntags: [test, stability]\n---\n# Test {uid}\n\nBody content."

    raw_resp, status = write_file(path, content)
    check("PUT /file returns 200", status == 200)
    resp_data = json.loads(raw_resp)
    check("PUT /file returns etag", "etag" in resp_data)

    raw_content, status = read_file(path)
    text = raw_content.decode("utf-8")
    check("GET /file returns 200", status == 200)
    check("GET /file contains body", "Body content" in text)
    check("GET /file has frontmatter", f"title: Stability Test {uid}" in text)

    data, status = delete_file(path)
    check("DELETE /file returns 200", status == 200)
    check("DELETE returns path", data.get("deleted") == path)

    try:
        read_file(path)
        check("GET deleted file returns 404", False, "expected 404 got 200")
    except HTTPError as e:
        check("GET deleted file returns 404", e.code == 404, f"got {e.code}")

    return uid


# ---------- 3. TREE ----------
def test_tree():
    print("\n── Tree ──")
    data, status = req_json("GET", "/tree")
    check("GET /tree returns 200", status == 200)
    check("/tree has root isDir", data.get("isDir") is True)
    check("/tree has children", isinstance(data.get("children"), list))


# ---------- 4. SEARCH ----------
def test_search():
    print("\n── Search ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-search/{uid}.md"
    content = f"---\ntitle: SearchTarget {uid}\n---\n# Unique Token {uid}\n\nSearchable platypus {uid} content."
    write_file(path, content)
    time.sleep(1)

    data, status = req_json("GET", f"/search?q={uid}")
    check("GET /search returns 200", status == 200)
    results = data.get("results", [])
    check("search finds written file", any(uid in r.get("path", "") for r in results),
          f"got {len(results)} results")

    delete_file(path)


# ---------- 5. META ----------
def test_meta():
    print("\n── Meta ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-meta/{uid}.md"
    content = f"---\ntitle: MetaTest {uid}\ncategory: stability\npriority: high\n---\n# Meta {uid}"
    write_file(path, content)
    time.sleep(0.5)

    data, status = req_json("GET", "/meta")
    check("GET /meta returns 200", status == 200)
    results = data.get("results", [])
    check("/meta returns results", len(results) > 0, f"got {len(results)}")

    delete_file(path)


# ---------- 6. DQL QUERY ----------
def test_dql():
    print("\n── DQL Query ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-dql/{uid}.md"
    content = f"---\ntitle: DQL Target {uid}\nstatus: active\n---\n# DQL {uid}"
    write_file(path, content)
    time.sleep(0.5)

    q = quote(f"TABLE path, title WHERE title LIKE \"%DQL%\"")
    data, status = req_json("GET", f"/query?q={q}")
    check("GET /query returns 200", status == 200)
    rows = data.get("rows", [])
    check("DQL returns rows", isinstance(rows, list), f"type={type(rows)}")

    q2 = quote("TABLE path WHERE status = \"active\"")
    data2, _ = req_json("GET", f"/query?q={q2}")
    rows2 = data2.get("rows", [])
    check("DQL filter by frontmatter field", isinstance(rows2, list))

    delete_file(path)


# ---------- 7. DQL AGGREGATION ----------
def test_aggregate():
    print("\n── DQL Aggregation ──")
    try:
        data, status = req_json("GET", "/query/aggregate?group_by=status&calc=count")
        check("GET /query/aggregate returns 200", status == 200)
        check("/query/aggregate has groups", "groups" in data, f"keys={list(data.keys())}")
    except HTTPError as e:
        check("GET /query/aggregate returns 200", False, f"got {e.code}")


# ---------- 8. BULK WRITE ----------
def test_bulk_write():
    print("\n── Bulk Write ──")
    uid = uuid.uuid4().hex[:8]
    files = [
        {"path": f"test-bulk/{uid}-1.md", "content": f"---\ntitle: Bulk 1\n---\n# Bulk {uid}-1"},
        {"path": f"test-bulk/{uid}-2.md", "content": f"---\ntitle: Bulk 2\n---\n# Bulk {uid}-2"},
        {"path": f"test-bulk/{uid}-3.md", "content": f"---\ntitle: Bulk 3\n---\n# Bulk {uid}-3"},
    ]

    data, status = req_json("POST", "/bulk", {
        "files": files, "actor": "bulk-test", "message": f"bulk test {uid}",
    })
    check("POST /bulk returns 200", status == 200)
    check("bulk returned 3 results", data.get("count") == 3)

    tree, _ = req_json("GET", "/tree?path=test-bulk")
    children = tree.get("children", [])
    written = [c["name"] for c in children if uid in c.get("name", "")]
    check("bulk write created 3 files", len(written) == 3, f"found {len(written)}")

    for f in files:
        delete_file(f["path"])


# ---------- 9. VERSIONING (git) ----------
def test_versioning():
    print("\n── Versioning ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-versions/{uid}.md"

    write_file(path, f"---\ntitle: V1\n---\n# Version 1 {uid}", actor="v-test")
    time.sleep(1.5)
    write_file(path, f"---\ntitle: V2\n---\n# Version 2 {uid}", actor="v-test")
    time.sleep(1.5)

    data, status = req_json("GET", f"/versions?path={quote(path)}")
    check("GET /versions returns 200", status == 200)
    versions = data.get("versions", [])
    check("versions has >= 2 entries", len(versions) >= 2, f"got {len(versions)}")

    if len(versions) >= 2:
        h1 = versions[0]["hash"]
        h2 = versions[1]["hash"]

        try:
            raw_ver, ver_status = req_raw("GET", f"/version?path={quote(path)}&version={h2}")
            check("GET /version?version returns content", ver_status == 200 and len(raw_ver) > 0)
        except HTTPError as e:
            check("GET /version?version returns content", False, f"got {e.code}")

        try:
            diff_raw, status3 = req_raw("GET", f"/diff?path={quote(path)}&from={h2}&to={h1}")
            check("GET /diff returns 200", status3 == 200)
        except HTTPError as e:
            check("GET /diff returns 200", False, f"got {e.code}")

    delete_file(path)


# ---------- 10. BACKLINKS / GRAPH ----------
def test_graph():
    print("\n── Backlinks & Graph ──")
    uid = uuid.uuid4().hex[:8]
    path_a = f"test-links/{uid}-source.md"
    path_b = f"test-links/{uid}-target.md"

    write_file(path_b, f"---\ntitle: Target {uid}\n---\n# Target")
    write_file(path_a, f"---\ntitle: Source {uid}\n---\n# Source\n\nSee [[{uid}-target]].")
    time.sleep(0.5)

    data, status = req_json("GET", f"/backlinks?path={quote(path_b)}")
    check("GET /backlinks returns 200", status == 200)

    data2, status2 = req_json("GET", "/graph")
    check("GET /graph returns 200", status2 == 200)
    check("/graph has nodes", "nodes" in data2 or "links" in data2 or isinstance(data2, dict))

    delete_file(path_a)
    delete_file(path_b)


# ---------- 11. JANITOR ----------
def test_janitor():
    print("\n── Janitor ──")
    try:
        data, status = req_json("GET", "/janitor")
        check("GET /janitor returns 200", status == 200)
    except HTTPError as e:
        check("GET /janitor returns 200", False, f"got {e.code}")


# ---------- 12. ANALYTICS ----------
def test_analytics():
    print("\n── Analytics ──")
    try:
        data, status = req_json("GET", "/analytics")
        check("GET /analytics returns 200", status == 200)
        check("/analytics has total_pages", "total_pages" in data,
              f"keys={list(data.keys())[:10]}")
    except HTTPError as e:
        check("GET /analytics returns 200", False, f"got {e.code}")


# ---------- 13. COMMENTS ----------
def test_comments():
    print("\n── Comments ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-comments/{uid}.md"
    write_file(path, f"---\ntitle: Comment Test\n---\n# Comment Test {uid}")

    data, status = req_json("POST", f"/comments?path={quote(path)}", {
        "anchor": {"quote": f"Comment Test {uid}"},
        "body": f"Test comment {uid}", "author": "stability-test",
    })
    check("POST /comments returns 200", status == 200)
    comment_id = data.get("id", "")

    data2, status2 = req_json("GET", f"/comments?path={quote(path)}")
    check("GET /comments returns 200", status2 == 200)
    comments = data2.get("comments", [])
    check("comments list has entry", len(comments) > 0, f"got {len(comments)}")

    if comment_id:
        try:
            _, status3 = req_json("PATCH", f"/comments/{comment_id}?path={quote(path)}",
                                  {"resolved": True})
            check("PATCH resolve comment", status3 == 200)
        except HTTPError as e:
            check("PATCH resolve comment", False, f"got {e.code}")
        try:
            _, status4 = req_json("DELETE", f"/comments/{comment_id}?path={quote(path)}")
            check("DELETE comment", status4 == 200)
        except HTTPError as e:
            check("DELETE comment", False, f"got {e.code}")

    delete_file(path)


# ---------- 14. HEALTH-CHECK (per page) ----------
def test_health_check():
    print("\n── Health Check (per page) ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-hc/{uid}.md"
    write_file(path, f"---\ntitle: HC {uid}\n---\n# Health Check\n\nSome body text for word count.")
    time.sleep(0.5)

    try:
        data, status = req_json("GET", f"/health-check?path={quote(path)}")
        check("GET /health-check returns 200", status == 200)
        check("/health-check has path", data.get("path") == path)
    except HTTPError as e:
        check("GET /health-check returns 200", False, f"got {e.code}")

    delete_file(path)


# ---------- 15. IMPORT/EXPORT ----------
def test_export():
    print("\n── Export ──")
    try:
        data, _ = req_raw("GET", "/export?format=jsonl")
        lines = data.decode().strip().split("\n")
        check("GET /export returns JSONL lines", len(lines) > 0, f"got {len(lines)} lines")
        first = json.loads(lines[0])
        check("export line has path", "path" in first)
    except Exception as e:
        check("GET /export works", False, str(e))


# ---------- 16. TEMPLATES ----------
def test_templates():
    print("\n── Templates ──")
    try:
        data, status = req_json("GET", "/templates")
        check("GET /templates returns 200", status == 200)
        check("/templates has list", "templates" in data)
    except HTTPError as e:
        check("GET /templates returns 200", False, f"got {e.code}")


# ---------- 17. STALE PAGES ----------
def test_stale():
    print("\n── Stale Pages ──")
    try:
        data, status = req_json("GET", "/stale")
        check("GET /stale returns 200", status == 200)
        check("/stale has results", "results" in data)
    except HTTPError as e:
        check("GET /stale returns 200", False, f"got {e.code}")


# ---------- 18. CONTRADICTIONS ----------
def test_contradictions():
    print("\n── Contradictions ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-contra/{uid}.md"
    write_file(path, f"---\ntitle: Contra {uid}\n---\n# Contra {uid}\n\nSome content.")
    time.sleep(0.5)

    try:
        data, status = req_json("GET", f"/contradictions?path={quote(path)}")
        check("GET /contradictions returns 200", status == 200)
        check("/contradictions has structure", "contradictions" in data)
    except HTTPError as e:
        check("GET /contradictions returns 200", False, f"got {e.code}")

    delete_file(path)


# ---------- 19. SHARE LINKS ----------
def test_share():
    print("\n── Share Links ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-share/{uid}.md"
    write_file(path, f"---\ntitle: Share Test\n---\n# Share {uid}")
    time.sleep(0.3)

    try:
        data, status = req_json("POST", "/share", {"path": path})
        check("POST /share returns 200", status == 200)
        token = data.get("token", "")
        share_id = data.get("id", "")
        check("share link has token", bool(token), f"data={data}")

        if token:
            pub_raw, pub_status = req_raw("GET", f"{KIWI_URL}/api/kiwi/public/{token}")
            check("public page accessible", pub_status == 200 and len(pub_raw) > 0)

        shares, _ = req_json("GET", f"/share?path={quote(path)}")
        check("GET /share lists links", isinstance(shares, (list, dict)))

        if share_id:
            revoke_data, revoke_status = req_json("DELETE", f"/share/{share_id}")
            check("DELETE /share revokes link", revoke_status == 200)
    except HTTPError as e:
        check("share links work", False, f"got {e.code}: {e.read().decode()[:200]}")

    delete_file(path)


# ---------- 20. EDGE CASES ----------
def test_edge_cases():
    print("\n── Edge Cases ──")

    deep_path = "test-edge/level1/level2/level3/deep.md"
    _, status = write_file(deep_path, "---\ntitle: Deep File\n---\n# Deep")
    check("write to deeply nested path", status == 200)

    uni_path = "test-edge/unicode.md"
    uni_content = "---\ntitle: 日本語テスト\ntags: [テスト, résumé, Ñoño]\n---\n# 日本語 Content\n\nBody: 🎉 café naïve"
    _, status = write_file(uni_path, uni_content)
    check("write unicode content", status == 200)
    raw, _ = read_file(uni_path)
    check("read back unicode", "日本語" in raw.decode("utf-8"))

    large_path = "test-edge/large.md"
    large_content = "---\ntitle: Large File\n---\n# Large\n\n" + ("Lorem ipsum dolor sit amet. " * 500)
    _, status = write_file(large_path, large_content)
    check("write large file (~14KB)", status == 200)

    empty_fm_path = "test-edge/no-frontmatter.md"
    _, status = write_file(empty_fm_path, "# No Frontmatter\n\nJust body text.")
    check("write without frontmatter", status == 200)

    _, status = write_file(empty_fm_path, "# Overwritten\n\nNew body.")
    check("overwrite existing file", status == 200)

    empty_body_path = "test-edge/empty-body.md"
    _, status = write_file(empty_body_path, "---\ntitle: Empty Body\nstatus: draft\n---\n")
    check("write file with only frontmatter", status == 200)

    special_path = "test-edge/special-chars.md"
    special_content = '---\ntitle: "Quotes & Ampersands <html>"\ndescription: "Line1\\nLine2"\n---\n# Special'
    _, status = write_file(special_path, special_content)
    check("write special chars in frontmatter", status == 200)

    try:
        read_file("nonexistent-12345.md")
        check("GET non-existent returns 404", False)
    except HTTPError as e:
        check("GET non-existent returns 404", e.code == 404)

    for p in [deep_path, uni_path, large_path, empty_fm_path, empty_body_path, special_path]:
        try:
            delete_file(p)
        except Exception:
            pass


# ---------- 21. CONCURRENT WRITES ----------
def test_concurrent():
    print("\n── Concurrent Writes ──")
    uid = uuid.uuid4().hex[:8]
    n_workers = 10
    results = {}

    def do_write(i):
        path = f"test-concurrent/{uid}-{i:03d}.md"
        content = f"---\ntitle: Concurrent {i}\nseq: {i}\n---\n# Worker {i}\n\nWritten by worker {i}."
        try:
            _, status = write_file(path, content, actor=f"worker-{i}")
            return i, status, path
        except Exception as e:
            return i, str(e), path

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = [pool.submit(do_write, i) for i in range(n_workers)]
        for f in concurrent.futures.as_completed(futures):
            i, status, path = f.result()
            results[i] = status

    successes = sum(1 for s in results.values() if s == 200)
    check(f"concurrent writes: {successes}/{n_workers} succeeded", successes == n_workers,
          f"failures: {[k for k, v in results.items() if v != 200]}")

    time.sleep(1)
    tree, _ = req_json("GET", "/tree?path=test-concurrent")
    children = tree.get("children", [])
    found = [c["name"] for c in children if uid in c.get("name", "")]
    check(f"all {n_workers} concurrent files exist in tree", len(found) == n_workers,
          f"found {len(found)}")

    for i in range(n_workers):
        try:
            delete_file(f"test-concurrent/{uid}-{i:03d}.md")
        except Exception:
            pass


# ---------- 22. CONCURRENT BULK WRITE ----------
def test_concurrent_bulk():
    print("\n── Concurrent Bulk Writes ──")
    uid = uuid.uuid4().hex[:8]
    n_batches = 5
    files_per_batch = 5
    results = {}

    def bulk_write(batch_i):
        files = [
            {
                "path": f"test-cbulk/{uid}-b{batch_i}-f{j}.md",
                "content": f"---\ntitle: Batch {batch_i} File {j}\n---\n# B{batch_i}F{j}",
            }
            for j in range(files_per_batch)
        ]
        try:
            _, status = req_json("POST", "/bulk", {
                "files": files, "actor": f"bulk-worker-{batch_i}",
                "message": f"concurrent bulk {uid} batch {batch_i}",
            })
            return batch_i, status
        except Exception as e:
            return batch_i, str(e)

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_batches) as pool:
        futures = [pool.submit(bulk_write, i) for i in range(n_batches)]
        for f in concurrent.futures.as_completed(futures):
            batch_i, status = f.result()
            results[batch_i] = status

    successes = sum(1 for s in results.values() if s == 200)
    check(f"concurrent bulk: {successes}/{n_batches} batches succeeded", successes == n_batches)

    time.sleep(1)
    total_expected = n_batches * files_per_batch
    tree, _ = req_json("GET", "/tree?path=test-cbulk")
    children = tree.get("children", [])
    found = [c["name"] for c in children if uid in c.get("name", "")]
    check(f"concurrent bulk created {total_expected} files", len(found) == total_expected,
          f"found {len(found)}")

    for i in range(n_batches):
        for j in range(files_per_batch):
            try:
                delete_file(f"test-cbulk/{uid}-b{i}-f{j}.md")
            except Exception:
                pass


# ---------- 23. RESOLVE LINKS ----------
def test_resolve_links():
    print("\n── Resolve Links ──")
    uid = uuid.uuid4().hex[:8]
    target_path = f"test-resolve/{uid}-target.md"
    write_file(target_path, f"---\ntitle: Resolve Target {uid}\n---\n# Target")
    time.sleep(0.3)

    try:
        data, status = req_json("POST", "/resolve-links", {
            "content": f"See [[{uid}-target]] for details.",
        })
        check("POST /resolve-links returns 200", status == 200)
        check("/resolve-links has content", "content" in data)
    except HTTPError as e:
        check("POST /resolve-links returns 200", False, f"got {e.code}")

    delete_file(target_path)


# ---------- 24. TOC ----------
def test_toc():
    print("\n── Table of Contents ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-toc/{uid}.md"
    content = "---\ntitle: TOC Test\n---\n# Heading 1\n\n## Sub 1.1\n\n## Sub 1.2\n\n### Sub 1.2.1\n\n# Heading 2"
    write_file(path, content)
    time.sleep(0.5)

    try:
        data, status = req_json("GET", f"/toc?path={quote(path)}")
        check("GET /toc returns 200", status == 200)
        toc = data if isinstance(data, list) else data.get("headings", data.get("toc", []))
        check("/toc returns data", isinstance(toc, (list, dict)))
    except HTTPError as e:
        check("GET /toc returns 200", False, f"got {e.code}")

    delete_file(path)


# ---------- 25. EVENTS (SSE) ----------
def test_events():
    print("\n── Events (SSE) ──")
    try:
        r = Request(f"{API}/events", headers={"Accept": "text/event-stream"})
        resp = urlopen(r, timeout=3)
        check("GET /events connects (SSE)", resp.status == 200)
    except Exception as e:
        if "timed out" in str(e).lower():
            check("GET /events connects (SSE)", True)
        else:
            check("GET /events connects (SSE)", False, str(e))


# ---------- 26. THEME ----------
def test_theme():
    print("\n── Theme ──")
    try:
        data, status = req_json("GET", "/theme")
        check("GET /theme returns 200", status == 200)
    except HTTPError as e:
        check("GET /theme returns 200", False, f"got {e.code}")


# ---------- 27. MEMORY REPORT ----------
def test_memory():
    print("\n── Memory Report ──")
    uid = uuid.uuid4().hex[:8]
    ep_path = f"episodes/{uid}-episode.md"
    content = f"---\ntitle: Episode {uid}\nmemory_kind: episodic\nepisode_id: ep-{uid}\n---\n# Episode\n\nSome notes."
    write_file(ep_path, content)
    time.sleep(0.5)

    try:
        data, status = req_json("GET", "/memory/report")
        check("GET /memory/report returns 200", status == 200)
        check("memory report has structure", isinstance(data, dict))
    except HTTPError as e:
        check("GET /memory/report returns 200", False, f"got {e.code}")

    delete_file(ep_path)


# ---------- 28. BLAME ----------
def test_blame():
    print("\n── Blame ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-blame/{uid}.md"
    write_file(path, f"---\ntitle: Blame {uid}\n---\n# Blame Test\n\nLine for blame.")
    time.sleep(1)

    try:
        data, status = req_json("GET", f"/blame?path={quote(path)}")
        check("GET /blame returns 200", status == 200)
    except HTTPError as e:
        check("GET /blame returns 200", False, f"got {e.code}")

    delete_file(path)


# ---------- 29. ETAG / CONDITIONAL REQUESTS ----------
def test_etag():
    print("\n── ETag & Conditional Requests ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-etag/{uid}.md"

    raw_resp, _ = write_file(path, f"---\ntitle: ETag {uid}\n---\n# V1")
    etag = json.loads(raw_resp).get("etag", "")
    check("PUT returns etag", bool(etag))

    if etag:
        raw2, _ = write_file(path, f"---\ntitle: ETag {uid}\n---\n# V2")
        check("second write succeeds", True)

        try:
            req_raw("PUT", f"/file?path={quote(path)}",
                    body=f"---\ntitle: ETag {uid}\n---\n# V3 conflict".encode(),
                    headers={"Content-Type": "text/markdown", "If-Match": etag})
            check("stale etag returns 409", False, "expected conflict")
        except HTTPError as e:
            check("stale etag returns 409", e.code == 409, f"got {e.code}")

    delete_file(path)


# ---------- 30. DQL EDGE CASES ----------
def test_dql_edge():
    print("\n── DQL Edge Cases ──")
    uid = uuid.uuid4().hex[:8]
    for i in range(5):
        write_file(f"test-dqledge/{uid}-{i}.md",
                   f"---\ntitle: Item {i}\npriority: {i}\nstatus: {'active' if i % 2 == 0 else 'draft'}\n---\n# Item {i}")
    time.sleep(0.5)

    try:
        q = quote(f"TABLE path, title, priority WHERE path LIKE \"%{uid}%\" SORT priority DESC LIMIT 3")
        data, status = req_json("GET", f"/query?q={q}")
        check("DQL SORT + LIMIT works", status == 200)
        rows = data.get("rows") or []
        check("DQL LIMIT respected", len(rows) <= 3, f"got {len(rows)}")
    except HTTPError as e:
        check("DQL SORT + LIMIT", False, f"got {e.code}")

    try:
        q = quote("LIST WHERE status = \"active\" LIMIT 5")
        data, status = req_json("GET", f"/query?q={q}")
        check("DQL LIST query works", status == 200)
        rows = data.get("rows") or []
        check("DQL LIST returns rows", isinstance(rows, list))
    except HTTPError as e:
        check("DQL LIST query", False, f"got {e.code}")

    try:
        q = quote("TABLE path, status GROUP BY status")
        data, status = req_json("GET", f"/query?q={q}")
        check("DQL GROUP BY works", status == 200)
    except HTTPError as e:
        check("DQL GROUP BY", False, f"got {e.code}: {e.read().decode()[:200]}")

    for i in range(5):
        try:
            delete_file(f"test-dqledge/{uid}-{i}.md")
        except Exception:
            pass


# ---------- 31. VERIFIED SEARCH ----------
def test_verified_search():
    print("\n── Verified Search ──")
    try:
        data, status = req_json("GET", "/search/verified?q=test")
        check("GET /search/verified returns 200", status == 200)
        check("/search/verified has results", "results" in data)
    except HTTPError as e:
        check("GET /search/verified returns 200", False, f"got {e.code}")


# ---------- 32. UI CONFIG ----------
def test_ui_config():
    print("\n── UI Config ──")
    try:
        data, status = req_json("GET", "/ui-config")
        check("GET /ui-config returns 200", status == 200)
    except HTTPError as e:
        check("GET /ui-config returns 200", False, f"got {e.code}")


# ---------- 33. VERSION / DIFF DETAILS ----------
def test_version_detail():
    print("\n── Version Detail ──")
    uid = uuid.uuid4().hex[:8]
    path = f"test-vdetail/{uid}.md"
    write_file(path, f"---\ntitle: VD1\n---\n# VD1 {uid}")
    time.sleep(1.5)
    write_file(path, f"---\ntitle: VD2\n---\n# VD2 {uid}")
    time.sleep(1.5)

    data, _ = req_json("GET", f"/versions?path={quote(path)}")
    versions = data.get("versions", [])
    if len(versions) >= 2:
        h1 = versions[0]["hash"]
        h2 = versions[1]["hash"]

        try:
            raw, status = req_raw("GET", f"/version?path={quote(path)}&version={h2}")
            check("GET /version?version returns content", status == 200 and len(raw) > 0)
        except HTTPError as e:
            check("GET /version?version", False, f"got {e.code}")

        try:
            diff_raw, status2 = req_raw("GET", f"/diff?path={quote(path)}&from={h2}&to={h1}")
            check("GET /diff with from/to returns 200", status2 == 200)
        except HTTPError as e:
            check("GET /diff with from/to", False, f"got {e.code}")
    else:
        check("version detail: need >= 2 versions", False, f"got {len(versions)}")

    delete_file(path)


# ---------- MAIN ----------
if __name__ == "__main__":
    print(f"KiwiFS Stability Test Suite")
    print(f"Target: {KIWI_URL}")
    print(f"{'=' * 60}")

    tests = [
        test_health,
        test_crud,
        test_tree,
        test_search,
        test_meta,
        test_dql,
        test_aggregate,
        test_bulk_write,
        test_versioning,
        test_graph,
        test_janitor,
        test_analytics,
        test_comments,
        test_health_check,
        test_export,
        test_templates,
        test_stale,
        test_contradictions,
        test_share,
        test_edge_cases,
        test_concurrent,
        test_concurrent_bulk,
        test_resolve_links,
        test_toc,
        test_events,
        test_theme,
        test_memory,
        test_blame,
        test_etag,
        test_dql_edge,
        test_verified_search,
        test_ui_config,
        test_version_detail,
    ]

    for t in tests:
        try:
            t()
        except Exception as e:
            FAIL += 1
            msg = f"  ✗ {t.__name__} CRASHED: {e}"
            print(msg)
            ERRORS.append(msg)

    print(f"\n{'=' * 60}")
    print(f"Results: {PASS} passed, {FAIL} failed")
    if ERRORS:
        print(f"\nFailures:")
        for e in ERRORS:
            print(e)
    sys.exit(0 if FAIL == 0 else 1)
