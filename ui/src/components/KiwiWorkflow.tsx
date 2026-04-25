import { useEffect, useState } from "react";
import {
  CheckCircle2,
  Circle,
  Clock,
  Loader2,
  ShieldAlert,
  ShieldCheck,
  User,
} from "lucide-react";
import { api, type WorkflowMeta, type WorkflowTask } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/cn";

type Props = {
  path: string;
  frontmatter: Record<string, any>;
  onRefresh: () => void;
};

const STATUS_STYLE: Record<string, string> = {
  todo: "border-gray-400/50 bg-gray-400/10 text-gray-600 dark:text-gray-400",
  "in-progress":
    "border-blue-500/50 bg-blue-500/10 text-blue-700 dark:text-blue-400",
  done: "border-green-500/50 bg-green-500/10 text-green-700 dark:text-green-400",
  blocked:
    "border-red-500/50 bg-red-500/10 text-red-700 dark:text-red-400",
};

function isOverdue(date?: string): boolean {
  if (!date) return false;
  try {
    return new Date(date).getTime() < Date.now();
  } catch {
    return false;
  }
}

function formatShortDate(d: string): string {
  try {
    return new Date(d).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
    });
  } catch {
    return d;
  }
}

export function KiwiWorkflow({ path, frontmatter: _fm, onRefresh }: Props) {
  void _fm;
  const [workflow, setWorkflow] = useState<WorkflowMeta | null>(null);
  const [updating, setUpdating] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setError(null);
    api
      .getWorkflow(path)
      .then((w) => {
        if (!cancelled) setWorkflow(w);
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [path]);

  async function toggleTask(task: WorkflowTask) {
    const newStatus = task.status === "done" ? "todo" : "done";
    setUpdating(task.id);
    try {
      await api.updateTask(path, task.id, newStatus);
      onRefresh();
      const w = await api.getWorkflow(path);
      setWorkflow(w);
    } catch (e) {
      setError(String(e));
    } finally {
      setUpdating(null);
    }
  }

  async function handleApproval(status: "approved" | "rejected") {
    setUpdating("approval");
    try {
      await api.updateApproval(path, status);
      onRefresh();
      const w = await api.getWorkflow(path);
      setWorkflow(w);
    } catch (e) {
      setError(String(e));
    } finally {
      setUpdating(null);
    }
  }

  if (error && !workflow) return null;
  if (!workflow) return null;

  const progressPct = Math.round(workflow.progress * 100);

  return (
    <Card className="mt-6">
      <CardHeader className="pb-3">
        <CardTitle className="text-sm font-medium flex items-center justify-between">
          <span>Workflow</span>
          <span className="text-xs text-muted-foreground tabular-nums">
            {progressPct}% complete
          </span>
        </CardTitle>
        <div className="h-1.5 rounded-full bg-muted overflow-hidden">
          <div
            className={cn(
              "h-full rounded-full transition-all",
              progressPct >= 100
                ? "bg-green-500"
                : progressPct >= 50
                  ? "bg-blue-500"
                  : "bg-amber-500",
            )}
            style={{ width: `${progressPct}%` }}
          />
        </div>
      </CardHeader>
      <CardContent className="space-y-1.5 pt-0">
        {/* Tasks */}
        {workflow.tasks.map((task) => (
          <div
            key={task.id}
            className="flex items-center gap-2 py-1.5 group"
          >
            <button
              type="button"
              disabled={updating === task.id}
              onClick={() => toggleTask(task)}
              className="shrink-0 text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
            >
              {updating === task.id ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : task.status === "done" ? (
                <CheckCircle2 className="h-4 w-4 text-green-500" />
              ) : (
                <Circle className="h-4 w-4" />
              )}
            </button>
            <span
              className={cn(
                "flex-1 text-sm min-w-0 truncate",
                task.status === "done" && "line-through text-muted-foreground",
              )}
            >
              {task.title}
            </span>
            {task.assignee && (
              <Badge variant="outline" className="text-[10px] px-1.5 py-0 gap-1 shrink-0">
                <User className="h-2.5 w-2.5" />
                {task.assignee}
              </Badge>
            )}
            {task.dueDate && (
              <span
                className={cn(
                  "text-[10px] shrink-0 flex items-center gap-0.5",
                  isOverdue(task.dueDate) && task.status !== "done"
                    ? "text-red-500"
                    : "text-muted-foreground",
                )}
              >
                <Clock className="h-2.5 w-2.5" />
                {formatShortDate(task.dueDate)}
              </span>
            )}
            <Badge
              variant="outline"
              className={cn(
                "text-[10px] px-1.5 py-0 shrink-0",
                STATUS_STYLE[task.status] || "",
              )}
            >
              {task.status}
            </Badge>
          </div>
        ))}

        {/* Approval */}
        {workflow.approval && (
          <div className="border-t border-border pt-3 mt-3 space-y-2">
            <div className="flex items-center gap-2">
              <span className="text-xs font-medium text-muted-foreground">
                Approval
              </span>
              <Badge
                variant="outline"
                className={cn(
                  "text-[10px] px-1.5 py-0",
                  workflow.approval.status === "approved"
                    ? "border-green-500/50 bg-green-500/10 text-green-700 dark:text-green-400"
                    : workflow.approval.status === "rejected"
                      ? "border-red-500/50 bg-red-500/10 text-red-700 dark:text-red-400"
                      : "border-amber-500/50 bg-amber-500/10 text-amber-700 dark:text-amber-400",
                )}
              >
                {workflow.approval.status}
              </Badge>
              {workflow.approval.approver && (
                <span className="text-xs text-muted-foreground">
                  by {workflow.approval.approver}
                </span>
              )}
              {workflow.approval.date && (
                <span className="text-xs text-muted-foreground">
                  on {formatShortDate(workflow.approval.date)}
                </span>
              )}
            </div>
            {workflow.approval.comment && (
              <p className="text-xs text-muted-foreground italic">
                "{workflow.approval.comment}"
              </p>
            )}
            {workflow.approval.status === "pending" && (
              <div className="flex items-center gap-2">
                <Button
                  size="sm"
                  variant="outline"
                  className="gap-1 text-green-600 hover:text-green-700 hover:bg-green-50 dark:text-green-400 dark:hover:bg-green-950"
                  disabled={updating === "approval"}
                  onClick={() => handleApproval("approved")}
                >
                  <ShieldCheck className="h-3.5 w-3.5" />
                  Approve
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  className="gap-1 text-red-600 hover:text-red-700 hover:bg-red-50 dark:text-red-400 dark:hover:bg-red-950"
                  disabled={updating === "approval"}
                  onClick={() => handleApproval("rejected")}
                >
                  <ShieldAlert className="h-3.5 w-3.5" />
                  Reject
                </Button>
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
