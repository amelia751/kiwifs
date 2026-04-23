import { useEffect, useState } from "react";
import { api, type MetaFilter, type MetaResult } from "@/lib/api";

// KiwiQuery renders a live table of file_meta rows matching a YAML-like query.
//
// Usage inside a markdown page:
//
// ```kiwi-query
// from: runs/
// where: $.status = published
// where: $.priority = high
// sort: $.priority
// order: desc
// limit: 20
// columns: path, status, priority
// ```
//
// `from:` is a path prefix filter applied client-side (the /meta API doesn't
// know about directories). Everything else is forwarded to the endpoint.

type QueryBlock = {
  from?: string;
  where: MetaFilter[];
  sort?: string;
  order?: "asc" | "desc";
  limit?: number;
  columns?: string[];
};

type Props = {
  source: string;
  onNavigate?: (path: string) => void;
};

// The set of operators we forward verbatim. Keep in sync with the server's
// allowlist — anything else makes the backend return 400, which we surface
// in the error banner.
const OPS = ["!=", "<=", ">=", "<>", "=", "<", ">"];

function parseQuery(source: string): { block: QueryBlock; error?: string } {
  const block: QueryBlock = { where: [] };
  for (const rawLine of source.split("\n")) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const colon = line.indexOf(":");
    if (colon < 0) {
      return { block, error: `bad line (missing colon): ${line}` };
    }
    const key = line.slice(0, colon).trim().toLowerCase();
    const val = line.slice(colon + 1).trim();

    switch (key) {
      case "from":
        block.from = val;
        break;
      case "where": {
        const op = OPS.find((o) => val.includes(o));
        if (!op) return { block, error: `where needs operator: ${val}` };
        const i = val.indexOf(op);
        block.where.push({
          field: val.slice(0, i).trim(),
          op,
          value: val.slice(i + op.length).trim(),
        });
        break;
      }
      case "sort":
        block.sort = val;
        break;
      case "order":
        if (val !== "asc" && val !== "desc") {
          return { block, error: `order must be asc|desc, got ${val}` };
        }
        block.order = val;
        break;
      case "limit": {
        const n = parseInt(val, 10);
        if (!Number.isFinite(n)) return { block, error: `bad limit: ${val}` };
        block.limit = n;
        break;
      }
      case "columns":
        block.columns = val.split(",").map((c) => c.trim()).filter(Boolean);
        break;
      default:
        return { block, error: `unknown key: ${key}` };
    }
  }
  return { block };
}

// columnValue pulls a value out of either the path or a JSON-path into
// frontmatter ("$.status", "$.derived-from[*].id", …). The array syntax
// only returns the first matching value — good enough for a table cell.
function columnValue(row: MetaResult, col: string): string {
  if (col === "path") return row.path;
  if (!col.startsWith("$.")) return "";
  const remainder = col.slice(2);
  // Tokenise into (key) / ([*]) / (.subkey) fragments so a.b[*].c walks right.
  let cur: unknown = row.frontmatter;
  let i = 0;
  while (i < remainder.length) {
    if (remainder.startsWith("[*]", i)) {
      if (!Array.isArray(cur) || cur.length === 0) return "";
      cur = cur[0];
      i += 3;
      continue;
    }
    if (remainder[i] === ".") {
      i++;
      continue;
    }
    // Key up to the next special char.
    let j = i;
    while (j < remainder.length && remainder[j] !== "." && remainder[j] !== "[") {
      j++;
    }
    const key = remainder.slice(i, j);
    if (cur && typeof cur === "object" && !Array.isArray(cur)) {
      cur = (cur as Record<string, unknown>)[key];
    } else {
      return "";
    }
    i = j;
  }
  if (cur == null) return "";
  if (typeof cur === "object") return JSON.stringify(cur);
  return String(cur);
}

export function KiwiQuery({ source, onNavigate }: Props) {
  const [results, setResults] = useState<MetaResult[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const { block, error: parseError } = parseQuery(source);

  useEffect(() => {
    if (parseError) {
      setError(parseError);
      setResults(null);
      return;
    }
    let cancelled = false;
    setError(null);
    setResults(null);
    api
      .meta({
        where: block.where,
        sort: block.sort,
        order: block.order,
        limit: block.limit,
      })
      .then((resp) => {
        if (cancelled) return;
        let rows = resp.results;
        if (block.from) {
          // Normalise the prefix so both "runs" and "runs/" match the same set.
          const prefix = block.from.replace(/\/+$/, "") + "/";
          rows = rows.filter(
            (r) => r.path === block.from || r.path.startsWith(prefix),
          );
        }
        setResults(rows);
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
    // Re-run when the serialised query changes; don't depend on the parsed
    // block object identity (a fresh object every render would loop forever).
  }, [source]); // eslint-disable-line react-hooks/exhaustive-deps

  const columns = block.columns?.length ? block.columns : ["path", "$.status"];

  if (error) {
    return (
      <div className="kiwi-query-error rounded border border-red-300 bg-red-50 p-2 text-sm text-red-800">
        <div className="font-semibold">kiwi-query error</div>
        <div className="font-mono">{error}</div>
      </div>
    );
  }
  if (results == null) {
    return (
      <div className="kiwi-query-loading text-muted-foreground text-sm">
        Loading query…
      </div>
    );
  }
  if (results.length === 0) {
    return (
      <div className="kiwi-query-empty text-muted-foreground text-sm">
        No results.
      </div>
    );
  }
  return (
    <div className="kiwi-query overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b text-left">
            {columns.map((c) => (
              <th key={c} className="px-2 py-1 font-medium">
                {c.startsWith("$.") ? c.slice(2) : c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {results.map((row) => (
            <tr key={row.path} className="border-b last:border-b-0">
              {columns.map((c) => {
                const v = columnValue(row, c);
                if (c === "path" && onNavigate) {
                  return (
                    <td key={c} className="px-2 py-1">
                      <a
                        href={`#${row.path}`}
                        className="wiki-link"
                        onClick={(e) => {
                          e.preventDefault();
                          onNavigate(row.path);
                        }}
                      >
                        {v}
                      </a>
                    </td>
                  );
                }
                return (
                  <td key={c} className="px-2 py-1">
                    {v}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
