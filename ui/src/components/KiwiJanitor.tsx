import { useEffect, useState } from "react";
import {
  AlertCircle,
  AlertTriangle,
  CheckCircle2,
  Info,
  Loader2,
  Search,
  Stethoscope,
} from "lucide-react";
import { api, type JanitorIssue, type JanitorResult } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { cn } from "@/lib/cn";

type Props = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onNavigate: (path: string) => void;
};

type ChipDef = {
  label: string;
  value: string;
  matches: (kind: string) => boolean;
};

const FILTER_CHIPS: readonly ChipDef[] = [
  { label: "All", value: "all", matches: () => true },
  { label: "Stale", value: "stale", matches: (k) => k === "stale" || k === "no-review-date" },
  { label: "Orphans", value: "orphan", matches: (k) => k === "orphan" },
  { label: "Duplicates", value: "duplicate", matches: (k) => k === "duplicate" },
  {
    label: "Missing Metadata",
    value: "missing-metadata",
    matches: (k) => k === "missing-owner" || k === "missing-status",
  },
  { label: "Broken Links", value: "broken-link", matches: (k) => k === "broken-link" },
  {
    label: "Contradictions",
    value: "contradiction",
    matches: (k) => k === "contradiction",
  },
  {
    label: "Empty",
    value: "empty-page",
    matches: (k) => k === "empty-page",
  },
  {
    label: "Decisions",
    value: "decision-found",
    matches: (k) => k === "decision-found",
  },
];

function severityIcon(severity: JanitorIssue["severity"]) {
  switch (severity) {
    case "error":
      return <AlertCircle className="h-4 w-4 text-red-500 shrink-0" />;
    case "warning":
      return <AlertTriangle className="h-4 w-4 text-amber-500 shrink-0" />;
    default:
      return <Info className="h-4 w-4 text-blue-500 shrink-0" />;
  }
}

export function KiwiJanitor({ open, onOpenChange, onNavigate }: Props) {
  const [scanning, setScanning] = useState(false);
  const [result, setResult] = useState<JanitorResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("all");

  useEffect(() => {
    if (!open) return;
    setScanning(true);
    setError(null);
    api
      .janitorScan()
      .then((r) => setResult(r))
      .catch((e) => setError(String(e)))
      .finally(() => setScanning(false));
  }, [open]);

  const activeChip =
    FILTER_CHIPS.find((c) => c.value === filter) ?? FILTER_CHIPS[0];
  const filtered = result ? result.issues.filter((i) => activeChip.matches(i.kind)) : [];

  const healthPct =
    result && result.scanned > 0
      ? Math.round((result.healthy / result.scanned) * 100)
      : 0;

  const healthColor =
    healthPct >= 80
      ? "text-green-600 dark:text-green-400"
      : healthPct >= 50
        ? "text-amber-600 dark:text-amber-400"
        : "text-red-600 dark:text-red-400";

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl max-h-[85vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Stethoscope className="h-5 w-5" />
            Knowledge Health
          </DialogTitle>
        </DialogHeader>

        {scanning && (
          <div className="flex flex-col items-center justify-center py-12 gap-3 text-muted-foreground">
            <Loader2 className="h-8 w-8 animate-spin" />
            <span className="text-sm">Scanning knowledge base…</span>
          </div>
        )}

        {error && (
          <div className="text-sm text-destructive font-mono py-4">{error}</div>
        )}

        {result && !scanning && (
          <div className="flex flex-col gap-4 overflow-hidden">
            {/* Summary bar */}
            <div className="flex items-center gap-4 rounded-lg border border-border p-4">
              <div className={cn("text-3xl font-bold tabular-nums", healthColor)}>
                {healthPct}%
              </div>
              <div className="flex-1 min-w-0">
                <div className="h-2 rounded-full bg-muted overflow-hidden">
                  <div
                    className={cn(
                      "h-full rounded-full transition-all",
                      healthPct >= 80
                        ? "bg-green-500"
                        : healthPct >= 50
                          ? "bg-amber-500"
                          : "bg-red-500",
                    )}
                    style={{ width: `${healthPct}%` }}
                  />
                </div>
                <div className="flex gap-4 mt-2 text-xs text-muted-foreground">
                  <span>{result.scanned} scanned</span>
                  <span>{result.healthy} healthy</span>
                  <span>{result.issues.length} issues</span>
                </div>
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={() => {
                  setScanning(true);
                  setError(null);
                  api
                    .janitorScan()
                    .then((r) => setResult(r))
                    .catch((e) => setError(String(e)))
                    .finally(() => setScanning(false));
                }}
              >
                Re-scan
              </Button>
            </div>

            {/* Filter chips */}
            <div className="flex items-center gap-1.5 flex-wrap">
              {FILTER_CHIPS.map((chip) => {
                const count =
                  chip.value === "all"
                    ? result.issues.length
                    : result.issues.filter((i) => chip.matches(i.kind)).length;
                if (chip.value !== "all" && count === 0) return null;
                return (
                  <button
                    key={chip.value}
                    type="button"
                    onClick={() => setFilter(chip.value)}
                    className={cn(
                      "inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 text-xs transition-colors",
                      filter === chip.value
                        ? "bg-primary text-primary-foreground border-primary"
                        : "bg-transparent text-muted-foreground border-border hover:text-foreground hover:border-foreground/40",
                    )}
                  >
                    {chip.label}
                    <span className="ml-0.5 opacity-70">{count}</span>
                  </button>
                );
              })}
            </div>

            {/* Issue list */}
            <div className="flex-1 overflow-auto space-y-2 min-h-0">
              {filtered.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-8 gap-2 text-muted-foreground">
                  <CheckCircle2 className="h-8 w-8 text-green-500" />
                  <span className="text-sm">
                    {filter === "all"
                      ? "No issues found!"
                      : "No issues of this type."}
                  </span>
                </div>
              ) : (
                filtered.map((issue, idx) => (
                  <div
                    key={idx}
                    className="rounded-lg border border-border p-3 space-y-1.5"
                  >
                    <div className="flex items-start gap-2">
                      {severityIcon(issue.severity)}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 flex-wrap">
                          <Badge variant="outline" className="text-[10px] px-1.5 py-0">
                            {issue.kind}
                          </Badge>
                          <button
                            type="button"
                            onClick={() => onNavigate(issue.path)}
                            className="text-sm font-mono text-primary hover:underline truncate"
                          >
                            {issue.path}
                          </button>
                        </div>
                        <p className="text-sm text-foreground mt-1">
                          {issue.message}
                        </p>
                        {issue.related && issue.related.length > 0 && (
                          <div className="flex items-center gap-1.5 mt-1 flex-wrap">
                            <Search className="h-3 w-3 text-muted-foreground shrink-0" />
                            {issue.related.map((r) => (
                              <button
                                key={r}
                                type="button"
                                onClick={() => onNavigate(r)}
                                className="text-xs text-primary hover:underline font-mono"
                              >
                                {r}
                              </button>
                            ))}
                          </div>
                        )}
                        {issue.suggestion && (
                          <p className="text-xs text-muted-foreground mt-1 italic">
                            Suggestion: {issue.suggestion}
                          </p>
                        )}
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
