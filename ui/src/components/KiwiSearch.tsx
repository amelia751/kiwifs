import { useEffect, useRef, useState } from "react";
import { FileText, Sparkles } from "lucide-react";
import {
  CommandDialog,
  CommandEmpty,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { api, type SearchResult, type SemanticResult } from "@/lib/api";
import { titleize } from "@/lib/paths";
import { cn } from "@/lib/cn";

type Props = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSelect: (path: string) => void;
};

type Mode = "fts" | "semantic";

type Hit = {
  path: string;
  snippet?: string;
  score?: number;
  kind: Mode;
};

export function KiwiSearch({ open, onOpenChange, onSelect }: Props) {
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState<Mode>("fts");
  const [hits, setHits] = useState<Hit[]>([]);
  const [loading, setLoading] = useState(false);
  const [unavailable, setUnavailable] = useState(false);
  const debounce = useRef<number | null>(null);

  useEffect(() => {
    if (!open) {
      setQuery("");
      setHits([]);
    }
  }, [open]);

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
      if (mode === "fts") {
        api
          .search(query)
          .then((r) => {
            setHits(
              r.results.map((x: SearchResult) => ({
                path: x.path,
                snippet: x.snippet,
                score: x.score,
                kind: "fts",
              }))
            );
            setUnavailable(false);
          })
          .catch(() => setHits([]))
          .finally(() => setLoading(false));
      } else {
        api
          .semanticSearch(query, 15, 0)
          .then((r) => {
            // Collapse multiple chunks per path to the best-scoring hit.
            const best = new Map<string, SemanticResult>();
            for (const hit of r.results) {
              const prev = best.get(hit.path);
              if (!prev || hit.score > prev.score) best.set(hit.path, hit);
            }
            setHits(
              Array.from(best.values()).map((x) => ({
                path: x.path,
                snippet: x.snippet,
                score: x.score,
                kind: "semantic",
              }))
            );
            setUnavailable(false);
          })
          .catch((e) => {
            setHits([]);
            // 503 → vector search is disabled server-side. Surface it rather
            // than silently showing "No results" for users who don't know why.
            setUnavailable(String(e).includes("503"));
          })
          .finally(() => setLoading(false));
      }
    }, 150);
    return () => {
      if (debounce.current) window.clearTimeout(debounce.current);
    };
  }, [query, mode]);

  return (
    <CommandDialog
      open={open}
      onOpenChange={onOpenChange}
      // We do our own ranking on the server; cmdk's filtering would hide results.
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
      <div className="flex items-center gap-1 px-3 py-2 border-b border-border text-xs">
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
      </div>
      <CommandList>
        {unavailable && (
          <div className="px-3 py-4 text-xs text-muted-foreground">
            Semantic search isn't enabled on this server. Toggle back to
            full-text, or set <code className="font-mono">search.vector</code>{" "}
            in the kiwifs config.
          </div>
        )}
        {query && hits.length === 0 && !loading && !unavailable ? (
          <CommandEmpty>No results.</CommandEmpty>
        ) : null}
        {hits.map((r) => (
          <CommandItem
            key={`${r.kind}:${r.path}`}
            value={r.path}
            onSelect={() => {
              onSelect(r.path);
              onOpenChange(false);
            }}
          >
            <FileText className="h-4 w-4 text-muted-foreground mt-0.5 shrink-0" />
            <div className="min-w-0 flex-1">
              <div className="text-sm truncate">{titleize(r.path)}</div>
              <div className="text-xs text-muted-foreground truncate">
                {r.path}
              </div>
              {r.snippet && (
                <div
                  className="kiwi-search-snippet text-xs text-muted-foreground mt-0.5 line-clamp-2"
                  // FTS snippets carry <mark>…</mark>; semantic snippets are
                  // plain text. dangerouslySetInnerHTML works for both since
                  // the plain case is just a string with no HTML to parse.
                  dangerouslySetInnerHTML={{ __html: r.snippet }}
                />
              )}
            </div>
          </CommandItem>
        ))}
      </CommandList>
      <div className="text-[11px] text-muted-foreground px-3 py-2 border-t border-border flex justify-between">
        <span>↑↓ navigate · enter to open · esc to close</span>
        <span>
          {loading
            ? "Searching…"
            : `${hits.length} result${hits.length === 1 ? "" : "s"}`}
        </span>
      </div>
    </CommandDialog>
  );
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
