import {
  AlertTriangle,
  ArrowRight,
  Calendar,
  CheckCircle2,
  FileText,
  Scale,
  User,
  XCircle,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/cn";

type Props = {
  path: string;
  frontmatter: Record<string, any>;
  content: string;
  onNavigate: (path: string) => void;
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

const STATUS_CONFIG: Record<string, { color: string; icon: React.ReactNode }> = {
  active: {
    color: "border-green-500/50 bg-green-500/10 text-green-700 dark:text-green-400",
    icon: <CheckCircle2 className="h-3.5 w-3.5" />,
  },
  superseded: {
    color: "border-amber-500/50 bg-amber-500/10 text-amber-700 dark:text-amber-400",
    icon: <ArrowRight className="h-3.5 w-3.5" />,
  },
  reversed: {
    color: "border-red-500/50 bg-red-500/10 text-red-700 dark:text-red-400",
    icon: <XCircle className="h-3.5 w-3.5" />,
  },
};

export function KiwiDecisionLog({
  path: _path,
  frontmatter,
  content: _content,
  onNavigate,
}: Props) {
  void _path;
  void _content;
  const status = (frontmatter["decision-status"] ?? frontmatter.status ?? "active") as string;
  const owner = frontmatter.owner as string | undefined;
  const date = frontmatter.date as string | undefined;
  const decision = frontmatter.decision as string | undefined;
  const alternatives = (frontmatter.alternatives ?? []) as Array<{
    option: string;
    pros?: string[];
    cons?: string[];
  }>;
  const impact = frontmatter.impact as string | undefined;
  const reversalConditions = frontmatter["reversal-conditions"] as
    | string
    | undefined;
  const linkedDocs = (frontmatter["linked-docs"] ?? frontmatter.links ?? []) as string[];
  const timeline = (frontmatter.timeline ?? []) as Array<{
    date: string;
    event: string;
  }>;

  const cfg = STATUS_CONFIG[status.toLowerCase()] ?? STATUS_CONFIG.active;

  return (
    <div className="space-y-4">
      {/* Decision header */}
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center gap-2 flex-wrap">
            <Scale className="h-5 w-5 text-primary" />
            <CardTitle className="text-base">Decision Record</CardTitle>
            <Badge
              variant="outline"
              className={cn("gap-1 text-xs ml-auto", cfg.color)}
            >
              {cfg.icon}
              {status}
            </Badge>
          </div>
          <div className="flex items-center gap-3 text-xs text-muted-foreground mt-2">
            {owner && (
              <span className="flex items-center gap-1">
                <User className="h-3 w-3" />
                {owner}
              </span>
            )}
            {date && (
              <span className="flex items-center gap-1">
                <Calendar className="h-3 w-3" />
                {formatDate(date)}
              </span>
            )}
          </div>
        </CardHeader>
        {decision && (
          <CardContent className="pt-0">
            <blockquote className="border-l-4 border-primary pl-4 py-2 text-sm italic bg-muted/50 rounded-r-md pr-4">
              {decision}
            </blockquote>
          </CardContent>
        )}
      </Card>

      {/* Alternatives */}
      {alternatives.length > 0 && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm">Alternatives Considered</CardTitle>
          </CardHeader>
          <CardContent className="pt-0 space-y-3">
            {alternatives.map((alt, idx) => (
              <div
                key={idx}
                className="rounded-md border border-border p-3 space-y-2"
              >
                <div className="font-medium text-sm">{alt.option}</div>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  {alt.pros && alt.pros.length > 0 && (
                    <div>
                      <div className="text-green-600 dark:text-green-400 font-medium mb-1">
                        Pros
                      </div>
                      <ul className="space-y-0.5 text-muted-foreground">
                        {alt.pros.map((p, i) => (
                          <li key={i} className="flex items-start gap-1">
                            <span className="text-green-500 mt-0.5">+</span>
                            {p}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                  {alt.cons && alt.cons.length > 0 && (
                    <div>
                      <div className="text-red-600 dark:text-red-400 font-medium mb-1">
                        Cons
                      </div>
                      <ul className="space-y-0.5 text-muted-foreground">
                        {alt.cons.map((c, i) => (
                          <li key={i} className="flex items-start gap-1">
                            <span className="text-red-500 mt-0.5">-</span>
                            {c}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      )}

      {/* Impact */}
      {impact && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm">Impact</CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <p className="text-sm text-muted-foreground">{impact}</p>
          </CardContent>
        </Card>
      )}

      {/* Reversal Conditions */}
      {reversalConditions && (
        <Card className="border-amber-500/30">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm flex items-center gap-2">
              <AlertTriangle className="h-4 w-4 text-amber-500" />
              Reversal Conditions
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <p className="text-sm text-amber-700 dark:text-amber-400">
              {reversalConditions}
            </p>
          </CardContent>
        </Card>
      )}

      {/* Linked Docs */}
      {linkedDocs.length > 0 && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm">Linked Documents</CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <div className="flex flex-wrap gap-2">
              {linkedDocs.map((doc) => (
                <button
                  key={doc}
                  type="button"
                  onClick={() => onNavigate(doc)}
                  className="inline-flex items-center gap-1 text-sm text-primary hover:underline"
                >
                  <FileText className="h-3.5 w-3.5" />
                  {doc}
                </button>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Timeline */}
      {timeline.length > 0 && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm">Timeline</CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <div className="space-y-2">
              {timeline.map((entry, idx) => (
                <div key={idx} className="flex items-start gap-3">
                  <div className="flex flex-col items-center">
                    <div className="h-2 w-2 rounded-full bg-primary mt-1.5" />
                    {idx < timeline.length - 1 && (
                      <div className="w-px flex-1 bg-border" />
                    )}
                  </div>
                  <div className="pb-3">
                    <div className="text-xs text-muted-foreground">
                      {formatDate(entry.date)}
                    </div>
                    <div className="text-sm">{entry.event}</div>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
