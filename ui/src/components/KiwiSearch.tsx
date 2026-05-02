import { useEffect, useRef, useState } from "react";
import { Calendar, Clock, File, Filter, FolderOpen, Sparkles, X } from "lucide-react";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { api, type MetaFilter, type SearchResult, type SemanticResult, type TreeEntry } from "@/lib/api";
import { titleize } from "@/lib/paths";
import { cn } from "@/lib/cn";

const RECENT_KEY = "kiwi:recent-searches";
const MAX_RECENT = 8;

type Props = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSelect: (path: string) => void;
  tree: TreeEntry | null;
  initialQuery?: string;
  /** Hide the Full-text / Semantic mode chips (defaults to false). */
  hideModeToggle?: boolean;
};

type Mode = "fts" | "semantic";

type Hit = {
  path: string;
  snippet?: string;
  score?: number;
  kind: Mode;
};

function topDirs(tree: TreeEntry | null): string[] {
  if (!tree?.children) return [];
  return tree.children
    .filter((c) => c.isDir)
    .map((c) => c.name)
    .sort();
}

function loadRecentSearches(): string[] {
  try {
    const raw = localStorage.getItem(RECENT_KEY);
    if (!raw) return [];
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.filter((s: unknown) => typeof s === "string" && s.trim()) : [];
  } catch {
    return [];
  }
}

function saveRecentSearch(q: string) {
  const trimmed = q.trim();
  if (!trimmed) return;
  const prev = loadRecentSearches().filter((s) => s !== trimmed);
  const next = [trimmed, ...prev].slice(0, MAX_RECENT);
  localStorage.setItem(RECENT_KEY, JSON.stringify(next));
}

function clearRecentSearches() {
  localStorage.removeItem(RECENT_KEY);
}

export function KiwiSearch({ open, onOpenChange, onSelect, tree, initialQuery, hideModeToggle }: Props) {
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState<Mode>("fts");
  const [hits, setHits] = useState<Hit[]>([]);
  const [loading, setLoading] = useState(false);
  const [unavailable, setUnavailable] = useState(false);
  const [dirFilter, setDirFilter] = useState("");
  const [dateFilter, setDateFilter] = useState("");
  const [recents, setRecents] = useState<string[]>([]);
  const debounce = useRef<number | null>(null);

  const dirs = topDirs(tree);

  useEffect(() => {
    if (open) {
      setRecents(loadRecentSearches());
      if (initialQuery) setQuery(initialQuery);
    } else {
      setQuery("");
      setHits([]);
      setDirFilter("");
      setDateFilter("");
    }
  }, [open, initialQuery]);

  const filtered = dirFilter
    ? hits.filter((h) => h.path.startsWith(dirFilter + "/"))
    : hits;

  useEffect(() => {
    if (debounce.current) window.clearTimeout(debounce.current);
    if (!query.trim()) {
      setHits([]);
      setLoading(false);
      setUnavailable(false);
      return;
    }
    setLoading(true);
    debounce.current = window.setTimeout(() => {
      const modifiedAfter = dateFilterToISO(dateFilter);
      const { text: textQuery, filters: metaFilters } = parseFieldFilters(query);

      const metaPromise = metaFilters.length > 0
        ? api.meta({ where: metaFilters, limit: 200 }).then((r) =>
            new Set(r.results.map((x) => x.path))
          ).catch(() => null as Set<string> | null)
        : Promise.resolve(null as Set<string> | null);

      if (mode === "fts") {
        const searchQ = textQuery.trim();
        if (!searchQ && metaFilters.length === 0) {
          setHits([]);
          setLoading(false);
          return;
        }
        const ftsPromise = searchQ
          ? api.search(searchQ, modifiedAfter ? { modifiedAfter } : undefined)
          : Promise.resolve(null);
        Promise.all([ftsPromise, metaPromise]).then(([ftsRes, metaPaths]) => {
          let results: Hit[] = [];
          if (ftsRes) {
            results = ftsRes.results.map((x: SearchResult) => ({
              path: x.path,
              snippet: x.snippet,
              score: x.score,
              kind: "fts" as Mode,
            }));
          }
          if (metaPaths) {
            if (results.length > 0) {
              results = results.filter((h) => metaPaths.has(h.path));
            } else {
              results = Array.from(metaPaths).map((p) => ({
                path: p,
                kind: "fts" as Mode,
              }));
            }
          }
          setHits(results);
          setUnavailable(false);
        }).catch(() => setHits([]))
          .finally(() => setLoading(false));
      } else {
        api
          .semanticSearch(textQuery || query, 15, 0, modifiedAfter ? { modifiedAfter } : undefined)
          .then((r) => {
            const best = new Map<string, SemanticResult>();
            for (const hit of r.results) {
              const prev = best.get(hit.path);
              if (!prev || hit.score > prev.score) best.set(hit.path, hit);
            }
            return metaPromise.then((metaPaths) => {
              let results = Array.from(best.values()).map((x) => ({
                path: x.path,
                snippet: x.snippet,
                score: x.score,
                kind: "semantic" as Mode,
              }));
              if (metaPaths) {
                results = results.filter((h) => metaPaths.has(h.path));
              }
              setHits(results);
              setUnavailable(false);
            });
          })
          .catch((e) => {
            setHits([]);
            setUnavailable(String(e).includes("503"));
          })
          .finally(() => setLoading(false));
      }
    }, 150);
    return () => {
      if (debounce.current) window.clearTimeout(debounce.current);
    };
  }, [query, mode, dateFilter]);

  function handleSelect(path: string) {
    if (query.trim()) saveRecentSearch(query.trim());
    onSelect(path);
    onOpenChange(false);
  }

  function handleRecentClick(q: string) {
    setQuery(q);
  }

  return (
    <CommandDialog
      open={open}
      onOpenChange={onOpenChange}
      commandProps={{ shouldFilter: false }}
    >
      <CommandInput
        placeholder={
          mode === "fts"
            ? "Full-text search…"
            : "Semantic search (meaning, not keywords)…"
        }
        value={query}
        onValueChange={setQuery}
      />
      <div className="flex items-center gap-1 px-3 py-2 border-b border-border text-xs flex-wrap">
        {!hideModeToggle && (
          <>
            <ModeChip
              active={mode === "fts"}
              onClick={() => setMode("fts")}
              label="Full-text"
            />
            <ModeChip
              active={mode === "semantic"}
              onClick={() => setMode("semantic")}
              label="Semantic"
              icon={<Sparkles className="h-3 w-3" />}
            />
          </>
        )}
        {dirs.length > 0 && (
          <>
            <span className="w-px h-4 bg-border mx-1" />
            <FolderOpen className="h-3 w-3 text-muted-foreground" />
            {dirFilter ? (
              <button
                type="button"
                onClick={() => setDirFilter("")}
                className="inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs bg-secondary text-secondary-foreground border-secondary"
              >
                {dirFilter}
                <X className="h-2.5 w-2.5" />
              </button>
            ) : (
              dirs.map((d) => (
                <ModeChip
                  key={d}
                  active={false}
                  onClick={() => setDirFilter(d)}
                  label={d}
                />
              ))
            )}
          </>
        )}
        <span className="w-px h-4 bg-border mx-1" />
        <Calendar className="h-3 w-3 text-muted-foreground" />
        {dateFilter ? (
          <button
            type="button"
            onClick={() => setDateFilter("")}
            className="inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs bg-secondary text-secondary-foreground border-secondary"
          >
            {dateFilter}
            <X className="h-2.5 w-2.5" />
          </button>
        ) : (
          <>
            <ModeChip active={false} onClick={() => setDateFilter("7d")} label="7 days" />
            <ModeChip active={false} onClick={() => setDateFilter("30d")} label="30 days" />
            <ModeChip active={false} onClick={() => setDateFilter("90d")} label="90 days" />
          </>
        )}
        {parseFieldFilters(query).filters.length > 0 && (
          <>
            <span className="w-px h-4 bg-border mx-1" />
            <Filter className="h-3 w-3 text-muted-foreground" />
            {parseFieldFilters(query).filters.map((f, i) => (
              <span
                key={i}
                className="inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs bg-primary/10 text-primary border-primary/30"
              >
                {f.field.slice(2)}={f.value}
              </span>
            ))}
          </>
        )}
      </div>
      <CommandList>
        {unavailable && (
          <div className="px-3 py-4 text-xs text-muted-foreground">
            Semantic search isn't enabled on this server. Toggle back to
            full-text, or set <code className="font-mono">search.vector</code>{" "}
            in the kiwifs config.
          </div>
        )}
        {!query.trim() && recents.length > 0 && (
          <CommandGroup heading="Recent searches">
            {recents.map((q) => (
              <CommandItem
                key={q}
                value={"recent:" + q}
                onSelect={() => handleRecentClick(q)}
              >
                <Clock className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                <span className="text-sm truncate">{q}</span>
              </CommandItem>
            ))}
            <CommandItem
              value="__clear_recent__"
              onSelect={() => {
                clearRecentSearches();
                setRecents([]);
              }}
            >
              <X className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
              <span className="text-sm text-muted-foreground">Clear recent searches</span>
            </CommandItem>
          </CommandGroup>
        )}
        {query && filtered.length === 0 && !loading && !unavailable ? (
          <CommandEmpty>
            <div className="text-center py-6">
              <p className="text-sm text-muted-foreground">No results found.</p>
              <p className="text-xs text-muted-foreground mt-1">Try broader terms, check spelling, or switch to {mode === "fts" ? "semantic" : "full-text"} search.</p>
            </div>
          </CommandEmpty>
        ) : null}
        {filtered.map((r) => (
          <CommandItem
            key={r.kind + ":" + r.path}
            value={r.path}
            onSelect={() => handleSelect(r.path)}
          >
            <File className="h-4 w-4 text-muted-foreground mt-0.5 shrink-0" />
            <div className="min-w-0 flex-1">
              <div className="text-sm truncate">{titleize(r.path)}</div>
              <div className="text-xs text-muted-foreground truncate">
                {r.path}
              </div>
              {r.snippet && (
                <div
                  className="kiwi-search-snippet text-xs text-muted-foreground mt-0.5 line-clamp-2"
                  dangerouslySetInnerHTML={{
                    __html: r.kind === "semantic" ? highlightTerms(r.snippet, query) : r.snippet,
                  }}
                />
              )}
            </div>
          </CommandItem>
        ))}
      </CommandList>
      <div className="text-[11px] text-muted-foreground px-3 py-2 border-t border-border flex justify-between">
        <span>↑↓ navigate · enter to open · esc to close · <code className="font-mono">field:value</code> to filter</span>
        <span>
          {loading
            ? "Searching…"
            : query.trim()
              ? (filtered.length + " result" + (filtered.length === 1 ? "" : "s") +
                (dirFilter && filtered.length !== hits.length
                  ? " in " + dirFilter
                  : ""))
              : ""}
        </span>
      </div>
    </CommandDialog>
  );
}

function parseFieldFilters(q: string): { text: string; filters: MetaFilter[] } {
  const filters: MetaFilter[] = [];
  const textParts: string[] = [];
  for (const token of q.split(/\s+/)) {
    const colonIdx = token.indexOf(":");
    if (colonIdx > 0 && colonIdx < token.length - 1) {
      const field = token.slice(0, colonIdx);
      const value = token.slice(colonIdx + 1);
      if (/^[a-zA-Z][a-zA-Z0-9_-]*$/.test(field)) {
        filters.push({ field: `$.${field}`, op: "=", value });
        continue;
      }
    }
    textParts.push(token);
  }
  return { text: textParts.join(" "), filters };
}

function dateFilterToISO(filter: string): string | undefined {
  if (!filter) return undefined;
  const days = filter === "7d" ? 7 : filter === "30d" ? 30 : filter === "90d" ? 90 : 0;
  if (days === 0) return undefined;
  const d = new Date(Date.now() - days * 86400_000);
  return d.toISOString();
}

function highlightTerms(text: string, query: string): string {
  const words = query.trim().split(/\s+/).filter(Boolean);
  if (words.length === 0) return escapeHtml(text);
  const escaped = words.map((w) => w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const re = new RegExp(`(${escaped.join("|")})`, "gi");
  return escapeHtml(text).replace(re, "<mark>$1</mark>");
}

function escapeHtml(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function ModeChip({
  active,
  onClick,
  label,
  icon,
}: {
  active: boolean;
  onClick: () => void;
  label: string;
  icon?: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={cn(
        "inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 text-xs transition-colors",
        active
          ? "bg-primary text-primary-foreground border-primary"
          : "bg-transparent text-muted-foreground border-border hover:text-foreground hover:border-foreground/40"
      )}
    >
      {icon}
      {label}
    </button>
  );
}
