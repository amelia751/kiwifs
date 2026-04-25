import { useEffect, useMemo, useState } from "react";
import {
  AlertTriangle,
  CheckCircle,
  Clock,
  Shield,
  User,
  XCircle,
} from "lucide-react";
import { api } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/cn";

type Props = {
  path?: string;
  frontmatter: Record<string, any>;
  onOwnerClick?: (owner: string) => void;
  onNavigate?: (path: string) => void;
};

function formatDate(d: string | undefined): string | null {
  if (!d) return null;
  try {
    return new Date(d).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  } catch {
    return String(d);
  }
}

function isPast(d: string | undefined): boolean {
  if (!d) return false;
  try {
    return new Date(d).getTime() < Date.now();
  } catch {
    return false;
  }
}

export function KiwiTrustBadge({
  path,
  frontmatter,
  onOwnerClick,
  onNavigate,
}: Props) {
  const owner = frontmatter.owner as string | undefined;
  const status = frontmatter.status as string | undefined;
  const confidence =
    typeof frontmatter.confidence === "number"
      ? frontmatter.confidence
      : undefined;
  const sourceOfTruth = frontmatter["source-of-truth"] === true;
  const reviewed = frontmatter["reviewed"] as string | undefined;
  const nextReview = frontmatter["next-review"] as string | undefined;
  const deprecatedReason = frontmatter["deprecated-reason"] as
    | string
    | undefined;

  const stale = useMemo(() => isPast(nextReview), [nextReview]);
  const isDeprecated = status?.toLowerCase() === "deprecated";

  // Ask the backend for pages that share tags/topics and disagree with this
  // one. We only query when the page is a Source of Truth — otherwise the
  // set of potential conflicts is too noisy to be useful.
  const [conflicts, setConflicts] = useState<string[]>([]);
  useEffect(() => {
    if (!path || !sourceOfTruth) {
      setConflicts([]);
      return;
    }
    let cancelled = false;
    api
      .contradictions(path)
      .then((r) => {
        if (cancelled) return;
        setConflicts(
          Array.isArray(r?.contradictions) ? r.contradictions.slice(0, 5) : [],
        );
      })
      .catch(() => {
        if (!cancelled) setConflicts([]);
      });
    return () => {
      cancelled = true;
    };
  }, [path, sourceOfTruth]);

  const hasAnyMeta =
    owner || status || confidence != null || sourceOfTruth || reviewed;

  if (!hasAnyMeta && !stale && !isDeprecated && conflicts.length === 0)
    return null;

  const statusColor =
    status?.toLowerCase() === "verified"
      ? "border-green-500/50 bg-green-500/10 text-green-700 dark:text-green-400"
      : status?.toLowerCase() === "draft"
        ? "border-gray-400/50 bg-gray-400/10 text-gray-600 dark:text-gray-400"
        : status?.toLowerCase() === "outdated"
          ? "border-amber-500/50 bg-amber-500/10 text-amber-700 dark:text-amber-400"
          : status?.toLowerCase() === "deprecated"
            ? "border-red-500/50 bg-red-500/10 text-red-700 dark:text-red-400"
            : "";

  const confidenceColor =
    confidence != null
      ? confidence < 0.3
        ? "bg-red-500"
        : confidence < 0.7
          ? "bg-amber-500"
          : "bg-green-500"
      : "";

  return (
    <div className="space-y-2">
      {/* Warnings */}
      {isDeprecated && (
        <div className="flex items-center gap-2 rounded-md border border-red-500/30 bg-red-500/5 px-4 py-2.5 text-sm text-red-700 dark:text-red-400">
          <XCircle className="h-4 w-4 shrink-0" />
          <span>
            This page is deprecated.
            {deprecatedReason ? ` ${deprecatedReason}` : ""}
          </span>
        </div>
      )}
      {stale && !isDeprecated && (
        <div className="flex items-center gap-2 rounded-md border border-amber-500/30 bg-amber-500/5 px-4 py-2.5 text-sm text-amber-700 dark:text-amber-400">
          <AlertTriangle className="h-4 w-4 shrink-0" />
          <span>
            This page has not been reviewed since {formatDate(nextReview)}.
            It may contain outdated information.
          </span>
        </div>
      )}
      {conflicts.length > 0 && (
        <div className="flex items-start gap-2 rounded-md border border-amber-500/30 bg-amber-500/5 px-4 py-2.5 text-sm text-amber-700 dark:text-amber-400">
          <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
          <div className="flex-1 min-w-0">
            <div>
              This source-of-truth page overlaps with {conflicts.length} other
              page{conflicts.length === 1 ? "" : "s"} on the same topic:
            </div>
            <div className="flex flex-wrap gap-2 mt-1">
              {conflicts.map((p) => (
                <button
                  key={p}
                  type="button"
                  onClick={() => onNavigate?.(p)}
                  className="font-mono text-xs underline decoration-dotted hover:decoration-solid"
                >
                  {p}
                </button>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Inline badges */}
      <div className="flex items-center gap-2 flex-wrap">
        {owner && (
          <Tooltip>
            <TooltipTrigger asChild>
              <Badge
                variant="outline"
                className={cn(
                  "gap-1 text-xs",
                  onOwnerClick && "cursor-pointer hover:bg-accent",
                )}
                onClick={() => onOwnerClick?.(owner)}
              >
                <User className="h-3 w-3" />
                {owner}
              </Badge>
            </TooltipTrigger>
            <TooltipContent>Page owner</TooltipContent>
          </Tooltip>
        )}
        {status && (
          <Tooltip>
            <TooltipTrigger asChild>
              <Badge variant="outline" className={cn("gap-1 text-xs", statusColor)}>
                {status.toLowerCase() === "verified" ? (
                  <CheckCircle className="h-3 w-3" />
                ) : status.toLowerCase() === "deprecated" ? (
                  <XCircle className="h-3 w-3" />
                ) : (
                  <Clock className="h-3 w-3" />
                )}
                {status}
              </Badge>
            </TooltipTrigger>
            <TooltipContent>Verification status</TooltipContent>
          </Tooltip>
        )}
        {sourceOfTruth && (
          <Tooltip>
            <TooltipTrigger asChild>
              <Badge
                variant="outline"
                className="gap-1 text-xs border-amber-500/50 bg-amber-500/10 text-amber-700 dark:text-amber-400"
              >
                <Shield className="h-3 w-3" />
                Source of Truth
              </Badge>
            </TooltipTrigger>
            <TooltipContent>This page is the authoritative source</TooltipContent>
          </Tooltip>
        )}
        {confidence != null && (
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center gap-1.5">
                <span className="text-xs text-muted-foreground">
                  Confidence
                </span>
                <div className="h-1.5 w-16 rounded-full bg-muted overflow-hidden">
                  <div
                    className={cn("h-full rounded-full transition-all", confidenceColor)}
                    style={{ width: `${Math.round(confidence * 100)}%` }}
                  />
                </div>
                <span className="text-xs text-muted-foreground">
                  {Math.round(confidence * 100)}%
                </span>
              </div>
            </TooltipTrigger>
            <TooltipContent>
              Content confidence: {Math.round(confidence * 100)}%
            </TooltipContent>
          </Tooltip>
        )}
        {reviewed && (
          <span className="text-xs text-muted-foreground flex items-center gap-1">
            <Clock className="h-3 w-3" />
            Reviewed: {formatDate(reviewed)}
          </span>
        )}
        {nextReview && !stale && (
          <span className="text-xs text-muted-foreground flex items-center gap-1">
            <Clock className="h-3 w-3" />
            Next review: {formatDate(nextReview)}
          </span>
        )}
      </div>
    </div>
  );
}
