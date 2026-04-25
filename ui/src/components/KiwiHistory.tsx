import { useEffect, useMemo, useState } from "react";
import ReactDiffViewer, { DiffMethod } from "react-diff-viewer-continued";
import { formatDistanceToNow, parseISO } from "date-fns";
import { GitBranch, History, RotateCcw, User, X } from "lucide-react";
import { api, type BlameLine, type Version } from "@/lib/api";
import { titleize } from "@/lib/paths";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";

type ViewMode = "diff" | "blame";

type Props = {
  path: string;
  onClose: () => void;
  onRestored?: () => void;
};

// Parse git date strings liberally — git uses multiple formats depending on
// the backend (ISO 8601 from go-git, RFC 2822 from the shell fallback).
function parseDate(d: string): Date | null {
  if (!d) return null;
  try {
    const iso = parseISO(d);
    if (!isNaN(iso.getTime())) return iso;
  } catch {
    /* fall through */
  }
  const fallback = new Date(d);
  return isNaN(fallback.getTime()) ? null : fallback;
}

function relative(d: string): string {
  const parsed = parseDate(d);
  if (!parsed) return d;
  return formatDistanceToNow(parsed, { addSuffix: true });
}

export function KiwiHistory({ path, onClose, onRestored }: Props) {
  const [versions, setVersions] = useState<Version[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedHash, setSelectedHash] = useState<string | null>(null);
  const [isDark, setIsDark] = useState<boolean>(() =>
    typeof document !== "undefined" &&
    document.documentElement.classList.contains("dark")
  );
  const [oldContent, setOldContent] = useState<string>("");
  const [newContent, setNewContent] = useState<string>("");
  const [contentError, setContentError] = useState<string | null>(null);
  const [contentLoading, setContentLoading] = useState(false);
  const [viewMode, setViewMode] = useState<ViewMode>("diff");
  const [blameLines, setBlameLines] = useState<BlameLine[] | null>(null);
  const [blameLoading, setBlameLoading] = useState(false);
  const [restoring, setRestoring] = useState(false);
  const [confirmRestore, setConfirmRestore] = useState(false);

  useEffect(() => {
    const obs = new MutationObserver(() =>
      setIsDark(document.documentElement.classList.contains("dark"))
    );
    obs.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class"],
    });
    return () => obs.disconnect();
  }, []);

  useEffect(() => {
    let cancelled = false;
    setVersions(null);
    setError(null);
    setSelectedHash(null);
    api
      .versions(path)
      .then((r) => {
        if (cancelled) return;
        setVersions(r.versions);
        if (r.versions.length > 0) setSelectedHash(r.versions[0].hash);
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [path]);

  const parentHash = useMemo(() => {
    if (!versions || !selectedHash) return null;
    const idx = versions.findIndex((v) => v.hash === selectedHash);
    if (idx < 0 || idx >= versions.length - 1) return null;
    return versions[idx + 1].hash;
  }, [versions, selectedHash]);

  useEffect(() => {
    if (!selectedHash) return;
    let cancelled = false;
    setContentLoading(true);
    setContentError(null);

    const loadNew = api.readVersion(path, selectedHash);
    const loadOld = parentHash
      ? api.readVersion(path, parentHash)
      : Promise.resolve("");

    Promise.all([loadOld, loadNew])
      .then(([oldTxt, newTxt]) => {
        if (cancelled) return;
        setOldContent(oldTxt);
        setNewContent(newTxt);
      })
      .catch((e) => {
        if (!cancelled) setContentError(String(e));
      })
      .finally(() => {
        if (!cancelled) setContentLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [path, selectedHash, parentHash]);

  useEffect(() => {
    if (viewMode !== "blame") return;
    let cancelled = false;
    setBlameLoading(true);
    api.blame(path).then((r) => {
      if (!cancelled) setBlameLines(r.lines);
    }).catch(() => {
      if (!cancelled) setBlameLines([]);
    }).finally(() => {
      if (!cancelled) setBlameLoading(false);
    });
    return () => { cancelled = true; };
  }, [path, viewMode]);

  async function restoreVersion() {
    if (!selectedHash) return;
    setRestoring(true);
    try {
      // Grab the current ETag first so the restore goes through optimistic
      // locking like any normal edit — otherwise a concurrent writer's changes
      // would be silently wiped out by the restore.
      const current = await api.readFile(path).catch(() => ({ etag: null }));
      const content = await api.readVersion(path, selectedHash);
      await api.writeFile(path, content, current.etag || undefined);
      setConfirmRestore(false);
      onRestored?.();
      onClose();
    } catch (e) {
      const msg = String(e);
      if (msg.includes("409")) {
        setContentError(
          "This page changed since you opened history. Close and reopen the " +
            "panel to see the latest version, then retry the restore.",
        );
      } else {
        setContentError(msg);
      }
    } finally {
      setRestoring(false);
    }
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-8 py-3 border-b border-border">
        <div className="flex items-center gap-2 min-w-0">
          <History className="h-4 w-4 text-muted-foreground shrink-0" />
          <div className="text-sm font-medium truncate">
            {titleize(path)}
          </div>
          <span className="text-xs text-muted-foreground font-mono truncate">
            {path}
          </span>
          <Separator orientation="vertical" className="h-4 mx-1" />
          <div className="flex items-center gap-1">
            <Button
              variant={viewMode === "diff" ? "secondary" : "ghost"}
              size="sm"
              className="h-7 text-xs"
              onClick={() => setViewMode("diff")}
            >
              Diff
            </Button>
            <Button
              variant={viewMode === "blame" ? "secondary" : "ghost"}
              size="sm"
              className="h-7 text-xs"
              onClick={() => setViewMode("blame")}
            >
              <GitBranch className="h-3 w-3" /> Blame
            </Button>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {selectedHash && versions && versions.length > 0 && selectedHash !== versions[0].hash && (
            confirmRestore ? (
              <div className="flex items-center gap-1">
                <span className="text-xs text-muted-foreground">Restore {selectedHash.slice(0, 7)}?</span>
                <Button size="sm" className="h-7 text-xs" onClick={restoreVersion} disabled={restoring}>
                  {restoring ? "Restoring…" : "Yes"}
                </Button>
                <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={() => setConfirmRestore(false)}>
                  No
                </Button>
              </div>
            ) : (
              <Button variant="outline" size="sm" onClick={() => setConfirmRestore(true)}>
                <RotateCcw className="h-3.5 w-3.5" /> Restore
              </Button>
            )
          )}
          <Button variant="outline" size="sm" onClick={onClose}>
            <X className="h-3.5 w-3.5" /> Close
          </Button>
        </div>
      </div>

      <div className="flex-1 min-h-0 flex">
        <aside className="w-80 shrink-0 border-r border-border flex flex-col">
          <div className="px-4 py-2 text-xs uppercase tracking-wide text-muted-foreground border-b border-border">
            Versions
          </div>
          <ScrollArea className="flex-1 kiwi-scroll">
            {error && (
              <div className="p-4 text-xs text-destructive font-mono">
                {error}
              </div>
            )}
            {!versions && !error && (
              <div className="p-4 text-xs text-muted-foreground">Loading…</div>
            )}
            {versions && versions.length === 0 && (
              <div className="p-4 text-xs text-muted-foreground">
                No version history.
              </div>
            )}
            {versions && versions.length > 0 && (
              <ul className="p-2 space-y-0.5">
                {versions.map((v) => {
                  const active = v.hash === selectedHash;
                  return (
                    <li key={v.hash}>
                      <button
                        type="button"
                        onClick={() => setSelectedHash(v.hash)}
                        className={
                          "w-full text-left rounded-md px-3 py-2 text-xs transition-colors " +
                          (active
                            ? "bg-accent text-accent-foreground"
                            : "hover:bg-accent/60")
                        }
                      >
                        <div className="font-medium text-sm truncate text-foreground">
                          {v.message || "(no message)"}
                        </div>
                        <div className="mt-1 flex items-center gap-2 text-muted-foreground">
                          <User className="h-3 w-3" />
                          <span className="truncate">{v.author || "unknown"}</span>
                          <span className="ml-auto font-mono text-[10px]">
                            {v.hash.slice(0, 7)}
                          </span>
                        </div>
                        <div className="mt-0.5 text-[11px] text-muted-foreground">
                          {relative(v.date)}
                        </div>
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
          </ScrollArea>
        </aside>

        <main className="flex-1 min-w-0 overflow-auto kiwi-scroll">
          {viewMode === "blame" ? (
            blameLoading ? (
              <div className="p-6 text-sm text-muted-foreground">Loading blame…</div>
            ) : blameLines && blameLines.length > 0 ? (
              <div className="p-4">
                <table className="w-full text-xs font-mono">
                  <tbody>
                    {blameLines.map((bl) => (
                      <tr key={bl.line} className="hover:bg-accent/50">
                        <td className="text-muted-foreground text-right pr-3 py-0.5 select-none w-10">
                          {bl.line}
                        </td>
                        <td className="text-muted-foreground pr-3 py-0.5 truncate max-w-[120px]" title={`${bl.author} · ${bl.hash.slice(0, 7)}`}>
                          {bl.author.split(/\s/)[0]}
                        </td>
                        <td className="text-muted-foreground pr-3 py-0.5 w-16">
                          {bl.hash.slice(0, 7)}
                        </td>
                        <td className="text-muted-foreground pr-3 py-0.5 w-24 text-[10px]">
                          {relative(bl.date)}
                        </td>
                        <td className="py-0.5 whitespace-pre">{bl.text}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="grid place-items-center h-full text-sm text-muted-foreground">
                No blame data available.
              </div>
            )
          ) : !selectedHash ? (
            <div className="grid place-items-center h-full text-sm text-muted-foreground">
              Select a version to view its diff.
            </div>
          ) : contentError ? (
            <div className="p-6 text-sm text-destructive font-mono">
              {contentError}
            </div>
          ) : contentLoading ? (
            <div className="p-6 text-sm text-muted-foreground">Loading diff…</div>
          ) : (
            <div className="p-4">
              <div className="mb-3 px-2 text-xs text-muted-foreground flex items-center gap-2">
                <span className="font-mono">
                  {parentHash ? parentHash.slice(0, 7) : "—"}
                </span>
                <span>→</span>
                <span className="font-mono">{selectedHash.slice(0, 7)}</span>
                {!parentHash && (
                  <span className="ml-2">(initial commit)</span>
                )}
              </div>
              <Separator className="mb-3" />
              <div className="text-sm">
                <ReactDiffViewer
                  oldValue={oldContent}
                  newValue={newContent}
                  splitView
                  useDarkTheme={isDark}
                  compareMethod={DiffMethod.WORDS}
                  leftTitle={parentHash ? `Parent ${parentHash.slice(0, 7)}` : "Empty"}
                  rightTitle={`This version ${selectedHash.slice(0, 7)}`}
                />
              </div>
            </div>
          )}
        </main>
      </div>
    </div>
  );
}
