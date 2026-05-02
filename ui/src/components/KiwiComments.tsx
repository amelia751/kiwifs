import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { CheckCircle, Circle, Plus, Trash2 } from "lucide-react";
import { api, type Comment, type CommentAnchor } from "@/lib/api";
import {
  anchorFromSelection,
  clearWraps,
  wrapAnchor,
} from "@/lib/highlights";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import {
  Popover,
  PopoverAnchor,
  PopoverContent,
} from "@/components/ui/popover";

import { Textarea } from "@/components/ui/textarea";

type Props = {
  path: string;
  containerRef: React.RefObject<HTMLElement | null>;
  // Changes whenever the rendered markdown is replaced so we can re-apply
  // highlight spans on top of the fresh DOM.
  renderKey: unknown;
  refreshKey?: number;
};

type PopoverState =
  | { mode: "closed" }
  | { mode: "add"; anchor: CommentAnchor; rect: DOMRect }
  | { mode: "view"; id: string; rect: DOMRect };

// Virtual-element factory for Radix Popover anchored at an arbitrary viewport
// rect. PopoverAnchor accepts `virtualRef` shape via Slot, but the simplest
// portable approach is to render an absolute-positioned empty div as the anchor.
function rectAnchorStyle(rect: DOMRect): React.CSSProperties {
  return {
    position: "fixed",
    left: rect.left,
    top: rect.top,
    width: rect.width,
    height: rect.height,
    pointerEvents: "none",
  };
}

export function KiwiComments({ path, containerRef, renderKey, refreshKey }: Props) {
  const [comments, setComments] = useState<Comment[]>([]);
  const [popover, setPopover] = useState<PopoverState>({ mode: "closed" });
  const [draftBody, setDraftBody] = useState("");
  const [selectionBtn, setSelectionBtn] = useState<{ rect: DOMRect } | null>(
    null,
  );
  const draftRef = useRef<HTMLTextAreaElement>(null);

  // Fetch comments for this page. Re-fetches when an SSE event arrives
  // (refreshKey bump) so externally-added comments show up live.
  useEffect(() => {
    let cancelled = false;
    api
      .listComments(path)
      .then((r) => {
        if (!cancelled) setComments(r.comments);
      })
      .catch(() => {
        if (!cancelled) setComments([]);
      });
    return () => {
      cancelled = true;
    };
  }, [path, refreshKey]);

  const openView = useCallback((id: string, rect: DOMRect) => {
    setPopover({ mode: "view", id, rect });
    setSelectionBtn(null);
  }, []);

  // Apply highlight spans after every render of the markdown container.
  // useLayoutEffect so the wraps are in place before the browser paints.
  useLayoutEffect(() => {
    const root = containerRef.current as HTMLElement | null;
    if (!root) return;
    clearWraps(root);
    comments.forEach((c) => wrapAnchor(root, c.anchor, c.id, openView));
    return () => {
      if (root) clearWraps(root);
    };
  }, [comments, containerRef, renderKey, openView]);

  // Float an "Add comment" button near any non-empty selection inside the
  // prose container.
  useEffect(() => {
    const root = containerRef.current as HTMLElement | null;
    if (!root) return;
    const handler = () => {
      const sel = window.getSelection();
      if (!sel || sel.rangeCount === 0 || sel.isCollapsed) {
        setSelectionBtn(null);
        return;
      }
      const range = sel.getRangeAt(0);
      if (!root.contains(range.commonAncestorContainer)) {
        setSelectionBtn(null);
        return;
      }
      if (!sel.toString().trim()) {
        setSelectionBtn(null);
        return;
      }
      const rect = range.getBoundingClientRect();
      setSelectionBtn({ rect });
    };
    document.addEventListener("selectionchange", handler);
    return () => document.removeEventListener("selectionchange", handler);
  }, [containerRef]);

  // Focus the textarea whenever the add popover opens.
  useEffect(() => {
    if (popover.mode === "add") {
      setTimeout(() => draftRef.current?.focus(), 0);
    }
  }, [popover.mode]);

  function startAdd() {
    const root = containerRef.current as HTMLElement | null;
    if (!root) return;
    const anchor = anchorFromSelection(root);
    if (!anchor) return;
    const sel = window.getSelection();
    const rect =
      sel && sel.rangeCount
        ? sel.getRangeAt(0).getBoundingClientRect()
        : root.getBoundingClientRect();
    setPopover({ mode: "add", anchor, rect });
    setDraftBody("");
    setSelectionBtn(null);
    window.getSelection()?.removeAllRanges();
  }

  async function submit() {
    if (popover.mode !== "add") return;
    const body = draftBody.trim();
    if (!body) return;
    try {
      const created = await api.addComment(path, popover.anchor, body);
      setComments((xs) => [...xs, created]);
      setPopover({ mode: "closed" });
      setDraftBody("");
    } catch (err) {
      // Keep the popover open on error so the user can retry.
      console.error("add comment failed", err);
    }
  }

  async function toggleResolve(id: string) {
    const c = comments.find((x) => x.id === id);
    if (!c) return;
    try {
      const updated = await api.resolveComment(path, id, !c.resolved);
      setComments((xs) => xs.map((x) => (x.id === id ? updated : x)));
    } catch (err) {
      console.error("resolve comment failed", err);
    }
  }

  async function remove(id: string) {
    try {
      await api.deleteComment(path, id);
      setComments((xs) => xs.filter((c) => c.id !== id));
      setPopover({ mode: "closed" });
    } catch (err) {
      console.error("delete comment failed", err);
    }
  }

  const activeView =
    popover.mode === "view"
      ? comments.find((c) => c.id === popover.id)
      : undefined;

  return (
    <>
      {/* Floating "Add comment" affordance near the live selection */}
      {selectionBtn && popover.mode === "closed" ? (
        <div
          className="fixed z-50"
          style={{
            left: Math.max(
              8,
              selectionBtn.rect.left + selectionBtn.rect.width / 2 - 60,
            ),
            top: Math.max(8, selectionBtn.rect.top - 36),
          }}
        >
          <Button
            size="sm"
            className="h-7 px-2.5 text-xs shadow-md"
            onMouseDown={(e) => e.preventDefault()}
            onClick={startAdd}
          >
            <Plus className="h-3.5 w-3.5" /> Comment
          </Button>
        </div>
      ) : null}

      {/* Add popover, anchored at the selection rect */}
      {popover.mode === "add" ? (
        <Popover
          open
          onOpenChange={(o) => {
            if (!o) setPopover({ mode: "closed" });
          }}
        >
          <PopoverAnchor asChild>
            <div style={rectAnchorStyle(popover.rect)} />
          </PopoverAnchor>
          <PopoverContent
            side="bottom"
            align="center"
            className="w-80"
            onOpenAutoFocus={(e) => e.preventDefault()}
          >
            <div className="mb-2 text-xs text-muted-foreground">
              Commenting on:{" "}
              <span className="rounded bg-muted px-1.5 py-0.5 font-mono">
                {truncate(popover.anchor.quote, 80)}
              </span>
            </div>
            <Textarea
              ref={draftRef}
              value={draftBody}
              onChange={(e) => setDraftBody(e.target.value)}
              placeholder="Add a comment…"
              rows={3}
              className="resize-none text-sm"
              onKeyDown={(e) => {
                if (e.key === "Escape") setPopover({ mode: "closed" });
                if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) submit();
              }}
            />
            <div className="mt-2 flex items-center justify-end gap-2">
              <Button
                variant="ghost"
                size="sm"
                className="text-foreground"
                onClick={() => setPopover({ mode: "closed" })}
              >
                Cancel
              </Button>
              <Button
                size="sm"
                onClick={submit}
                disabled={!draftBody.trim()}
              >
                Save
              </Button>
            </div>
          </PopoverContent>
        </Popover>
      ) : null}

      {/* View existing comment, anchored at the highlight rect */}
      {popover.mode === "view" && activeView ? (
        <Popover
          open
          onOpenChange={(o) => {
            if (!o) setPopover({ mode: "closed" });
          }}
        >
          <PopoverAnchor asChild>
            <div style={rectAnchorStyle(popover.rect)} />
          </PopoverAnchor>
          <PopoverContent side="bottom" align="center" className="w-80">
            <div className="mb-1 text-xs text-muted-foreground flex items-center gap-1.5">
              <span>{activeView.author} · {formatDate(activeView.createdAt)}</span>
              {activeView.resolved && (
                <span className="text-green-700 dark:text-green-400 flex items-center gap-0.5">
                  <CheckCircle className="h-3 w-3" /> Resolved
                </span>
              )}
            </div>
            <div className="whitespace-pre-wrap text-sm">{activeView.body}</div>
            <div className="mt-3 flex items-center justify-end gap-1">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => toggleResolve(activeView.id)}
              >
                {activeView.resolved ? (
                  <><Circle className="h-3.5 w-3.5" /> Unresolve</>
                ) : (
                  <><CheckCircle className="h-3.5 w-3.5" /> Resolve</>
                )}
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => remove(activeView.id)}
                className="text-destructive hover:text-destructive"
              >
                <Trash2 className="h-3.5 w-3.5" /> Delete
              </Button>
            </div>
          </PopoverContent>
        </Popover>
      ) : null}

      {/* Footer list — always visible so comments are discoverable. */}
      <CommentsList
        comments={comments}
        onFocus={(c) => {
          const root = containerRef.current as HTMLElement | null;
          if (!root) return;
          const span = root.querySelector<HTMLElement>(
            `span[data-comment-id="${c.id}"]`,
          );
          if (span) {
            span.scrollIntoView({ behavior: "smooth", block: "center" });
            openView(c.id, span.getBoundingClientRect());
          }
        }}
        onDelete={remove}
        onToggleResolve={toggleResolve}
      />
    </>
  );
}

function CommentsList({
  comments,
  onFocus,
  onDelete,
  onToggleResolve,
}: {
  comments: Comment[];
  onFocus: (c: Comment) => void;
  onDelete: (id: string) => void;
  onToggleResolve: (id: string) => void;
}) {
  if (comments.length === 0) {
    return (
      <div className="text-sm text-muted-foreground">
        Select text in the page to add an inline comment.
      </div>
    );
  }
  return (
    <div className="text-sm">
      <ul className="space-y-3">
        {comments.map((c) => (
          <li key={c.id}>
            <Card className={"p-3" + (c.resolved ? " opacity-60" : "")}>
              <div className="flex items-start gap-2">
                <button
                  type="button"
                  onClick={() => onFocus(c)}
                  className="flex-1 text-left"
                >
                  <div className="text-xs text-muted-foreground mb-1 flex items-center gap-1.5">
                    <span>{c.author} · {formatDate(c.createdAt)}</span>
                    {c.resolved && (
                      <span className="text-green-700 dark:text-green-400 flex items-center gap-0.5">
                        <CheckCircle className="h-3 w-3" /> Resolved
                      </span>
                    )}
                  </div>
                  <div className="text-xs italic text-muted-foreground mb-1.5">
                    "{truncate(c.anchor.quote, 120)}"
                  </div>
                  <div className={"whitespace-pre-wrap" + (c.resolved ? " line-through" : "")}>{c.body}</div>
                </button>
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={() => onToggleResolve(c.id)}
                  title={c.resolved ? "Unresolve" : "Resolve"}
                  className="h-7 w-7 text-muted-foreground"
                >
                  {c.resolved ? <Circle className="h-3.5 w-3.5" /> : <CheckCircle className="h-3.5 w-3.5" />}
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={() => onDelete(c.id)}
                  title="Delete"
                  className="h-7 w-7 text-muted-foreground hover:text-destructive"
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </Button>
              </div>
            </Card>
          </li>
        ))}
      </ul>
    </div>
  );
}

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + "…" : s;
}

function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}
