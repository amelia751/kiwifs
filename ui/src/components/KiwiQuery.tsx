import { useEffect, useState, useCallback } from "react";
import { api, type QueryResponse } from "@/lib/api";

// KiwiQuery renders a live table from a DQL (Dataview Query Language) query.
//
// Accepts both the legacy YAML-like format and raw DQL:
//
// ```kiwi-query
// TABLE name, status FROM "students/" WHERE status = "active" SORT name ASC
// ```
//
// Legacy format (still supported):
// ```kiwi-query
// from: runs/
// where: $.status = published
// sort: $.priority
// order: desc
// limit: 20
// columns: path, status, priority
// ```

type Props = {
  source: string;
  isComputedView?: boolean;
  onNavigate?: (path: string) => void;
};

const LEGACY_KEYS = new Set(["from", "where", "sort", "order", "limit", "columns"]);

function isLegacyFormat(source: string): boolean {
  const lines = source.trim().split("\n");
  return lines.every((line) => {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) return true;
    const colon = trimmed.indexOf(":");
    if (colon < 0) return false;
    const key = trimmed.slice(0, colon).trim().toLowerCase();
    return LEGACY_KEYS.has(key);
  });
}

function legacyToDQL(source: string): string {
  let from = "";
  const wheres: string[] = [];
  let sort = "";
  let order = "";
  let limit = "";
  let columns: string[] = [];

  for (const rawLine of source.split("\n")) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const colon = line.indexOf(":");
    if (colon < 0) continue;
    const key = line.slice(0, colon).trim().toLowerCase();
    const val = line.slice(colon + 1).trim();

    switch (key) {
      case "from":
        from = val;
        break;
      case "where":
        wheres.push(val.replace(/^\$\./, ""));
        break;
      case "sort":
        sort = val.replace(/^\$\./, "");
        break;
      case "order":
        order = val;
        break;
      case "limit":
        limit = val;
        break;
      case "columns":
        columns = val
          .split(",")
          .map((c) => c.trim().replace(/^\$\./, ""))
          .filter(Boolean);
        break;
    }
  }

  const parts = ["TABLE"];
  if (columns.length > 0) {
    parts.push(columns.join(", "));
  }
  if (from) {
    parts.push(`FROM "${from}"`);
  }
  if (wheres.length > 0) {
    parts.push("WHERE " + wheres.join(" AND "));
  }
  if (sort) {
    parts.push(`SORT ${sort}${order ? " " + order.toUpperCase() : ""}`);
  }
  if (limit) {
    parts.push(`LIMIT ${limit}`);
  }
  return parts.join(" ");
}

function formatHeader(col: string): string {
  let s = col.replace(/^_/, "").replace(/\./g, " ").replace(/_/g, " ");
  return s
    .split(" ")
    .map((w) => (w.length > 0 ? w[0].toUpperCase() + w.slice(1) : w))
    .join(" ");
}

function formatCell(v: unknown): string {
  if (v == null) return "—";
  if (typeof v === "boolean") return v ? "true" : "false";
  if (typeof v === "number") {
    if (v === Math.floor(v)) return String(v);
    return v.toFixed(2);
  }
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

export function KiwiQuery({ source, isComputedView, onNavigate }: Props) {
  const [data, setData] = useState<QueryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const dql = isLegacyFormat(source) ? legacyToDQL(source) : source.trim();

  useEffect(() => {
    let cancelled = false;
    setError(null);
    setData(null);
    api
      .query(dql)
      .then((resp) => {
        if (!cancelled) setData(resp);
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [dql]);

  const copyDQL = useCallback(() => {
    navigator.clipboard.writeText(dql).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [dql]);

  if (error) {
    return (
      <div className="kiwi-query-error rounded border border-red-300 bg-red-50 p-2 text-sm text-red-800">
        <div className="font-semibold">kiwi-query error</div>
        <div className="font-mono">{error}</div>
      </div>
    );
  }
  if (data == null) {
    return (
      <div className="kiwi-query-loading text-muted-foreground text-sm">
        Loading query…
      </div>
    );
  }

  if (isComputedView) {
    return (
      <div className="kiwi-query">
        <div className="mb-2 rounded bg-blue-50 px-2 py-1 text-xs text-blue-700 border border-blue-200">
          This page is auto-generated from a query. Edit the query in the frontmatter.
        </div>
        {renderResult(data, dql, onNavigate, copyDQL, copied)}
      </div>
    );
  }

  return (
    <div className="kiwi-query">
      {renderResult(data, dql, onNavigate, copyDQL, copied)}
    </div>
  );
}

function isCalendarQuery(dql: string): boolean {
  return /^\s*CALENDAR\b/i.test(dql);
}

function renderResult(
  data: QueryResponse,
  dql: string,
  onNavigate?: (path: string) => void,
  onCopy?: () => void,
  copied?: boolean,
) {
  if (isCalendarQuery(dql)) {
    return renderCalendar(data, onNavigate, onCopy, copied);
  }

  if (data.groups && data.groups.length > 0) {
    return renderGroups(data, onCopy, copied);
  }

  if (data.rows.length === 0) {
    return (
      <div className="kiwi-query-empty text-muted-foreground text-sm">
        No results.
      </div>
    );
  }

  const cols = data.columns ?? [];

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b text-left">
            {cols.map((c) => (
              <th key={c} className="px-2 py-1 font-medium">
                {formatHeader(c)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.rows.map((row, i) => (
            <tr key={i} className="border-b last:border-b-0">
              {cols.map((c) => {
                const v = formatCell(row[c]);
                if ((c === "_path" || c === "path") && onNavigate) {
                  return (
                    <td key={c} className="px-2 py-1">
                      <a
                        href={`#${row[c]}`}
                        className="wiki-link"
                        onClick={(e) => {
                          e.preventDefault();
                          onNavigate(String(row[c]));
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
      {data.has_more && (
        <div className="text-muted-foreground mt-1 text-xs">
          Showing {data.rows.length}+ results
        </div>
      )}
      {onCopy && (
        <button
          onClick={onCopy}
          className="text-muted-foreground hover:text-foreground mt-1 text-xs underline"
        >
          {copied ? "Copied!" : "Copy as DQL"}
        </button>
      )}
    </div>
  );
}

type CalendarEntry = { path: string; date: string };

function parseCalendarData(data: QueryResponse): {
  dateField: string;
  entries: CalendarEntry[];
  byDate: Map<string, CalendarEntry[]>;
} {
  const cols = data.columns ?? [];
  const dateField = cols.find((c) => c !== "_path" && c !== "path") ?? cols[0] ?? "date";

  const entries: CalendarEntry[] = [];
  const byDate = new Map<string, CalendarEntry[]>();

  for (const row of data.rows) {
    const raw = row[dateField];
    if (raw == null) continue;
    const dateStr = String(raw).slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) continue;
    const entry: CalendarEntry = {
      path: String(row["_path"] ?? row["path"] ?? ""),
      date: dateStr,
    };
    entries.push(entry);
    const existing = byDate.get(dateStr);
    if (existing) existing.push(entry);
    else byDate.set(dateStr, [entry]);
  }

  return { dateField, entries, byDate };
}

function getMonthsFromEntries(entries: CalendarEntry[]): string[] {
  const months = new Set<string>();
  for (const e of entries) {
    months.add(e.date.slice(0, 7));
  }
  const sorted = [...months].sort();
  if (sorted.length === 0) {
    const now = new Date();
    sorted.push(`${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`);
  }
  return sorted;
}

const WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

const MONTH_NAMES = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

function renderMonthGrid(
  yearMonth: string,
  byDate: Map<string, CalendarEntry[]>,
  onNavigate?: (path: string) => void,
) {
  const [y, m] = yearMonth.split("-").map(Number);
  const firstDay = new Date(y, m - 1, 1);
  const daysInMonth = new Date(y, m, 0).getDate();
  // Monday=0 ... Sunday=6
  let startDow = firstDay.getDay() - 1;
  if (startDow < 0) startDow = 6;

  const cells: (number | null)[] = [];
  for (let i = 0; i < startDow; i++) cells.push(null);
  for (let d = 1; d <= daysInMonth; d++) cells.push(d);
  while (cells.length % 7 !== 0) cells.push(null);

  return (
    <div key={yearMonth} className="mb-4">
      <div className="mb-1 text-sm font-semibold">
        {MONTH_NAMES[m - 1]} {y}
      </div>
      <div
        className="grid gap-px"
        style={{ gridTemplateColumns: "repeat(7, 1fr)" }}
      >
        {WEEKDAYS.map((d) => (
          <div
            key={d}
            className="text-muted-foreground py-1 text-center text-xs font-medium"
          >
            {d}
          </div>
        ))}
        {cells.map((day, i) => {
          if (day == null) {
            return <div key={`empty-${i}`} className="p-1" />;
          }
          const dateStr = `${yearMonth}-${String(day).padStart(2, "0")}`;
          const hits = byDate.get(dateStr);
          const count = hits?.length ?? 0;
          const isToday =
            dateStr ===
            new Date().toISOString().slice(0, 10);

          return (
            <div
              key={dateStr}
              className={[
                "relative rounded p-1 text-center text-xs",
                count > 0
                  ? "bg-primary/20 font-medium cursor-pointer hover:bg-primary/40"
                  : "",
                isToday ? "ring-1 ring-primary" : "",
              ]
                .filter(Boolean)
                .join(" ")}
              title={
                count > 0
                  ? hits!.map((e) => e.path).join(", ")
                  : undefined
              }
              onClick={
                count === 1 && onNavigate
                  ? () => onNavigate(hits![0].path)
                  : undefined
              }
            >
              {day}
              {count > 1 && (
                <span className="text-muted-foreground ml-0.5 text-[9px]">
                  ({count})
                </span>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function renderCalendar(
  data: QueryResponse,
  onNavigate?: (path: string) => void,
  onCopy?: () => void,
  copied?: boolean,
) {
  const { entries, byDate } = parseCalendarData(data);

  if (entries.length === 0) {
    return (
      <div className="kiwi-query-empty text-muted-foreground text-sm">
        No results with valid dates.
      </div>
    );
  }

  const months = getMonthsFromEntries(entries);

  return (
    <div className="overflow-x-auto">
      <div className="inline-flex gap-4">
        {months.map((ym) => renderMonthGrid(ym, byDate, onNavigate))}
      </div>
      <div className="text-muted-foreground mt-1 text-xs">
        {entries.length} entry{entries.length !== 1 ? "ies" : "y"} across{" "}
        {byDate.size} day{byDate.size !== 1 ? "s" : ""}
      </div>
      {onCopy && (
        <button
          onClick={onCopy}
          className="text-muted-foreground hover:text-foreground mt-1 text-xs underline"
        >
          {copied ? "Copied!" : "Copy as DQL"}
        </button>
      )}
    </div>
  );
}

function renderGroups(
  data: QueryResponse,
  onCopy?: () => void,
  copied?: boolean,
) {
  return (
    <div>
      {data.groups!.map((g) => (
        <div key={g.key} className="mb-3">
          <div className="text-sm font-semibold">
            {g.key || "(empty)"}{" "}
            <span className="text-muted-foreground font-normal">
              ({g.count})
            </span>
          </div>
          <div className="ml-2 mt-1">
            <div
              className="bg-primary/20 rounded"
              style={{
                height: "16px",
                width: `${Math.min(100, (g.count / Math.max(...data.groups!.map((x) => x.count))) * 100)}%`,
                minWidth: "4px",
              }}
            />
          </div>
        </div>
      ))}
      {onCopy && (
        <button
          onClick={onCopy}
          className="text-muted-foreground hover:text-foreground mt-1 text-xs underline"
        >
          {copied ? "Copied!" : "Copy as DQL"}
        </button>
      )}
    </div>
  );
}
