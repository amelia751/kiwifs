#!/usr/bin/env python3
"""KiwiFS Stress Tests — push concurrency and throughput limits.

Usage:
    KIWI_URL=http://18.209.226.85:3333 python3 tests/stress_test.py
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
NS = f"stress-{uuid.uuid4().hex[:6]}"


def api_url(path):
    return f"{API}{path}" if path.startswith("/") else path


def req_json(method, path, body=None, headers=None):
    url = api_url(path)
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    data = json.dumps(body).encode() if body else None
    r = Request(url, data=data, headers=hdrs, method=method)
    resp = urlopen(r, timeout=60)
    content = resp.read()
    return json.loads(content) if content else {}, resp.status


def req_raw(method, path, body=None, headers=None):
    url = api_url(path)
    hdrs = headers or {}
    data = body if isinstance(body, bytes) else (body.encode() if body else None)
    r = Request(url, data=data, headers=hdrs, method=method)
    resp = urlopen(r, timeout=60)
    return resp.read(), resp.status


def write_file(path, content, actor="stress"):
    return req_raw("PUT", f"/file?path={quote(path)}", body=content,
                   headers={"Content-Type": "text/markdown", "X-Actor": actor})


def delete_file(path):
    try:
        req_json("DELETE", f"/file?path={quote(path)}")
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


# ============================================================
# STRESS 1: High concurrent writes (30 parallel writers)
# ============================================================

def stress_concurrent_writes():
    print("\n── Stress: 30 Concurrent Writers ──")
    prefix = f"{NS}/cwrite"
    n = 30
    results = {}

    def do_write(i):
        path = f"{prefix}/{i:04d}.md"
        content = f"---\ntitle: Concurrent {i}\nworker: {i}\n---\n# Worker {i}\n\n{'x' * 200}"
        try:
            _, status = write_file(path, content, actor=f"w-{i}")
            return i, status
        except HTTPError as e:
            return i, e.code
        except Exception as e:
            return i, str(e)

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as pool:
        futures = [pool.submit(do_write, i) for i in range(n)]
        for f in concurrent.futures.as_completed(futures):
            i, status = f.result()
            results[i] = status
    elapsed = time.time() - t0

    successes = sum(1 for s in results.values() if s == 200)
    check(f"30 concurrent writes: {successes}/{n} in {elapsed:.1f}s", successes == n,
          f"failures: {[(k, v) for k, v in results.items() if v != 200][:5]}")

    # Cleanup
    for i in range(n):
        delete_file(f"{prefix}/{i:04d}.md")


# ============================================================
# STRESS 2: Bulk write batches in parallel
# ============================================================

def stress_parallel_bulk():
    print("\n── Stress: 10 Parallel Bulk Writes (10 files each) ──")
    prefix = f"{NS}/pbulk"
    n_batches = 10
    files_per = 10
    results = {}

    def bulk_write(batch_i):
        files = [
            {
                "path": f"{prefix}/b{batch_i:02d}-f{j:02d}.md",
                "content": f"---\ntitle: B{batch_i}F{j}\nbatch: {batch_i}\n---\n# B{batch_i}F{j}\n\n{'y' * 100}",
            }
            for j in range(files_per)
        ]
        try:
            _, status = req_json("POST", "/bulk", {
                "files": files, "actor": f"bulk-{batch_i}",
                "message": f"stress batch {batch_i}",
            })
            return batch_i, status
        except HTTPError as e:
            return batch_i, e.code
        except Exception as e:
            return batch_i, str(e)

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_batches) as pool:
        futures = [pool.submit(bulk_write, i) for i in range(n_batches)]
        for f in concurrent.futures.as_completed(futures):
            batch_i, status = f.result()
            results[batch_i] = status
    elapsed = time.time() - t0

    successes = sum(1 for s in results.values() if s == 200)
    total = n_batches * files_per
    check(f"parallel bulk: {successes}/{n_batches} batches ({total} files) in {elapsed:.1f}s",
          successes == n_batches)

    time.sleep(1)
    tree, _ = req_json("GET", f"/tree?path={prefix}")
    children = tree.get("children", [])
    check(f"parallel bulk: {total} files in tree", len(children) == total,
          f"found {len(children)}")

    # Cleanup
    for i in range(n_batches):
        for j in range(files_per):
            delete_file(f"{prefix}/b{i:02d}-f{j:02d}.md")


# ============================================================
# STRESS 3: Search under load
# ============================================================

def stress_search():
    print("\n── Stress: Search Under Load ──")
    prefix = f"{NS}/searchload"

    # Write seed data
    files = []
    for i in range(20):
        path = f"{prefix}/doc-{i:03d}.md"
        content = f"---\ntitle: Search Doc {i}\ncategory: stress-test\n---\n# Document {i}\n\nThis document discusses performance optimization and caching strategies for distributed systems."
        files.append({"path": path, "content": content})

    req_json("POST", "/bulk", {"files": files, "actor": "seed", "message": "search seed"})
    time.sleep(1)

    # Parallel searches
    n_searches = 20
    results = {}

    def do_search(i):
        queries = ["optimization", "caching", "distributed", "performance", "strategies"]
        q = queries[i % len(queries)]
        try:
            data, status = req_json("GET", f"/search?q={quote(q)}")
            return i, status, len(data.get("results", []))
        except Exception as e:
            return i, str(e), 0

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(do_search, i) for i in range(n_searches)]
        for f in concurrent.futures.as_completed(futures):
            i, status, count = f.result()
            results[i] = (status, count)
    elapsed = time.time() - t0

    successes = sum(1 for s, _ in results.values() if s == 200)
    check(f"parallel search: {successes}/{n_searches} in {elapsed:.1f}s", successes == n_searches)

    # Cleanup
    for i in range(20):
        delete_file(f"{prefix}/doc-{i:03d}.md")


# ============================================================
# STRESS 4: DQL under concurrent load
# ============================================================

def stress_dql():
    print("\n── Stress: DQL Under Concurrent Load ──")
    prefix = f"{NS}/dqlload"

    files = []
    for i in range(30):
        path = f"{prefix}/item-{i:03d}.md"
        content = f"""---
title: Item {i}
category: "{'alpha' if i % 3 == 0 else 'beta' if i % 3 == 1 else 'gamma'}"
score: {i * 10}
status: "{'active' if i % 2 == 0 else 'archived'}"
---
# Item {i}

Content for item {i}.
"""
        files.append({"path": path, "content": content})

    req_json("POST", "/bulk", {"files": files, "actor": "dql-seed", "message": "dql seed"})
    time.sleep(1)

    queries = [
        f'TABLE path, title, score FROM "{prefix}/" WHERE status = "active" SORT score DESC LIMIT 10',
        f'TABLE path, title, category FROM "{prefix}/" WHERE category = "alpha"',
        f'TABLE path, score FROM "{prefix}/" WHERE score > 100 SORT score ASC',
        f'LIST FROM "{prefix}/" WHERE status = "archived" LIMIT 5',
        f'TABLE path, title FROM "{prefix}/" LIMIT 15',
    ]

    n_queries = 15
    results = {}

    def do_query(i):
        q = queries[i % len(queries)]
        try:
            data, status = req_json("GET", f"/query?q={quote(q)}")
            return i, status
        except HTTPError as e:
            return i, e.code
        except Exception as e:
            return i, str(e)

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(do_query, i) for i in range(n_queries)]
        for f in concurrent.futures.as_completed(futures):
            i, status = f.result()
            results[i] = status
    elapsed = time.time() - t0

    successes = sum(1 for s in results.values() if s == 200)
    check(f"parallel DQL: {successes}/{n_queries} in {elapsed:.1f}s", successes == n_queries,
          f"failures: {[(k, v) for k, v in results.items() if v != 200][:5]}")

    # Cleanup
    for i in range(30):
        delete_file(f"{prefix}/item-{i:03d}.md")


# ============================================================
# STRESS 5: Write-delete rapid cycle (deletion stress)
# ============================================================

def stress_write_delete():
    print("\n── Stress: Rapid Write-Delete Cycles ──")
    prefix = f"{NS}/wdcycle"
    n = 20
    errors = 0

    t0 = time.time()
    for i in range(n):
        path = f"{prefix}/ephemeral-{i:03d}.md"
        try:
            write_file(path, f"---\ntitle: Ephemeral {i}\n---\n# Temp\n\nWill be deleted.")
            delete_file(path)
        except Exception:
            errors += 1
    elapsed = time.time() - t0

    check(f"write-delete cycles: {n - errors}/{n} in {elapsed:.1f}s", errors == 0,
          f"{errors} failures")

    # Verify nothing lingers
    try:
        tree, _ = req_json("GET", f"/tree?path={prefix}")
        children = tree.get("children", [])
        check("write-delete: clean tree", len(children) == 0, f"found {len(children)} lingering files")
    except HTTPError as e:
        if e.code == 404:
            check("write-delete: clean tree", True)
        else:
            check("write-delete: clean tree", False, f"got {e.code}")


# ============================================================
# STRESS 6: Mixed read-write-search concurrent load
# ============================================================

def stress_mixed():
    print("\n── Stress: Mixed Read/Write/Search Concurrent ──")
    prefix = f"{NS}/mixed"

    # Seed some data
    for i in range(10):
        write_file(f"{prefix}/base-{i:02d}.md",
                   f"---\ntitle: Base {i}\ntype: mixed-test\n---\n# Base {i}\n\nKnowledge base entry.")

    time.sleep(0.5)
    n_ops = 30
    results = {}

    def mixed_op(i):
        try:
            op = i % 3
            if op == 0:
                write_file(f"{prefix}/dynamic-{i:03d}.md",
                           f"---\ntitle: Dynamic {i}\n---\n# Dyn {i}")
                return i, "write", 200
            elif op == 1:
                data, status = req_json("GET", "/search?q=knowledge")
                return i, "search", status
            else:
                data, status = req_json("GET", f"/query?q={quote(f'TABLE path FROM \"{prefix}/\"')}")
                return i, "dql", status
        except HTTPError as e:
            return i, "error", e.code
        except Exception as e:
            return i, "error", str(e)

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as pool:
        futures = [pool.submit(mixed_op, i) for i in range(n_ops)]
        for f in concurrent.futures.as_completed(futures):
            i, op_type, status = f.result()
            results[i] = (op_type, status)
    elapsed = time.time() - t0

    successes = sum(1 for _, s in results.values() if s == 200)
    check(f"mixed load: {successes}/{n_ops} in {elapsed:.1f}s", successes == n_ops,
          f"failures: {[(k, v) for k, v in results.items() if v[1] != 200][:5]}")

    # Cleanup
    for i in range(10):
        delete_file(f"{prefix}/base-{i:02d}.md")
    for i in range(n_ops):
        if i % 3 == 0:
            delete_file(f"{prefix}/dynamic-{i:03d}.md")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print(f"KiwiFS Stress Tests")
    print(f"Target: {KIWI_URL}")
    print(f"Namespace: {NS}")
    print(f"{'=' * 60}")

    tests = [
        stress_concurrent_writes,
        stress_parallel_bulk,
        stress_search,
        stress_dql,
        stress_write_delete,
        stress_mixed,
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
