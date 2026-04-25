import { useEffect, useMemo, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import remarkMath from "remark-math";
import rehypeSlug from "rehype-slug";
import rehypeRaw from "rehype-raw";
import rehypeKatex from "rehype-katex";
import rehypeAutolinkHeadings from "rehype-autolink-headings";
import matter from "gray-matter";
import Zoom from "react-medium-image-zoom";
import "react-medium-image-zoom/dist/styles.css";
import { AlertTriangle, Calendar, ChevronDown, ChevronRight, Edit, FileAxis3D, History as HistoryIcon, Link2, MessageSquareQuote, Pin, Star, Tag, User } from "lucide-react";
import { api, type TreeEntry } from "@/lib/api";
import { titleize } from "@/lib/paths";
import { KiwiBreadcrumb } from "./KiwiBreadcrumb";
import { KiwiToC } from "./KiwiToC";
import { KiwiBacklinks } from "./KiwiBacklinks";
import { KiwiComments } from "./KiwiComments";
import { KiwiQuery } from "./KiwiQuery";
import { KiwiTrustBadge } from "./KiwiTrustBadge";
import { KiwiPageMeta } from "./KiwiPageMeta";
import { KiwiWorkflow } from "./KiwiWorkflow";
import { KiwiDecisionLog } from "./KiwiDecisionLog";
import { PageActions } from "./PageActions";
import { ShikiCode } from "./ShikiCode";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { buildResolver, remarkWikiLinks } from "@/lib/wikiLinks";
import { friendlyError } from "@/lib/friendlyError";

type Props = {
  path: string;
  tree: TreeEntry | null;
  onNavigate: (path: string) => void;
  onEdit: () => void;
  onHistory?: () => void;
  onToggleStar?: () => void;
  isStarred?: boolean;
  onTogglePin?: () => void;
  isPinned?: boolean;
  onDeleted?: () => void;
  onDuplicated?: (newPath: string) => void;
  onMoved?: (newPath: string) => void;
  onTagClick?: (tag: string) => void;
  refreshKey?: number;
  onRefresh?: () => void;
};

const CALLOUT_PREFIXES: Array<{ emoji: string; cls: string }> = [
  { emoji: "ℹ️", cls: "kiwi-callout-info" },
  { emoji: "⚠️", cls: "kiwi-callout-warn" },
  { emoji: "🛑", cls: "kiwi-callout-error" },
];

function splitCallout(text: string): { emoji: string; cls: string; rest: string } | null {
  const trimmed = text.trimStart();
  for (const p of CALLOUT_PREFIXES) {
    if (trimmed.startsWith(p.emoji)) {
      return { emoji: p.emoji, cls: p.cls, rest: trimmed.slice(p.emoji.length).trimStart() };
    }
  }
  return null;
}

export function KiwiPage({ path, tree, onNavigate, onEdit, onHistory, onToggleStar, isStarred, onTogglePin, isPinned, onDeleted, onDuplicated, onMoved, onTagClick, refreshKey, onRefresh }: Props) {
  const [content, setContent] = useState<string | null>(null);
  const [lastModified, setLastModified] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [commentCount, setCommentCount] = useState(0);
  const [lastAuthor, setLastAuthor] = useState<string | null>(null);
  const [versionError, setVersionError] = useState(false);
  const [commentError, setCommentError] = useState(false);
  const proseRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let cancelled = false;
    setContent(null);
    setError(null);
    setLastModified(null);
    api
      .readFile(path)
      .then((r) => {
        if (!cancelled) {
          setContent(r.content);
          setLastModified(r.lastModified);
        }
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => { cancelled = true; };
  }, [path, refreshKey]);

  useEffect(() => {
    let cancelled = false;
    setVersionError(false);
    api.versions(path).then((r) => {
      if (cancelled || !r.versions.length) return;
      setLastAuthor(r.versions[0].author);
    }).catch(() => { if (!cancelled) setVersionError(true); });
    return () => { cancelled = true; };
  }, [path]);

  useEffect(() => {
    let cancelled = false;
    setCommentError(false);
    api.listComments(path).then((r) => {
      if (!cancelled) setCommentCount(r.comments.length);
    }).catch(() => { if (!cancelled) setCommentError(true); });
    return () => { cancelled = true; };
  }, [path, refreshKey]);

  const resolver = useMemo(() => buildResolver(tree), [tree]);

  const parsed = useMemo(() => {
    if (content == null) return { body: "", meta: {} as Record<string, unknown> };
    try {
      const m = matter(content);
      let body = m.content;
      if (typeof m.data?.title === "string") {
        const h1Match = body.match(/^\s*#\s+(.+)\n?/);
        if (h1Match && h1Match[1].trim() === m.data.title.trim()) {
          body = body.replace(/^\s*#\s+.+\n?/, "");
        }
      }
      return { body, meta: (m.data || {}) as Record<string, unknown> };
    } catch {
      return { body: content, meta: {} };
    }
  }, [content]);

  const badges = useMemo(() => frontmatterBadges(parsed.meta), [parsed.meta]);
  const frontmatterTitle = typeof parsed.meta.title === "string" ? parsed.meta.title : null;
  const statusBadge = badges.find((b) => b.key === "status");
  const tagBadges = badges.filter((b) => b.key === "tags");
  const otherBadges = badges.filter((b) => b.key !== "status" && b.key !== "tags");

  if (error) {
    const friendly = friendlyError(error);
    return (
      <div className="flex flex-col h-full">
        <StickyBreadcrumb path={path} onNavigate={onNavigate} />
        <div className="p-8 max-w-lg space-y-2">
          <div className="text-lg font-semibold">{friendly.title}</div>
          <div className="text-sm text-muted-foreground">{friendly.detail}</div>
          {friendly.originalMessage && (
            <details className="text-xs text-muted-foreground mt-3">
              <summary className="cursor-pointer hover:text-foreground">
                Technical details
              </summary>
              <pre className="mt-1 font-mono whitespace-pre-wrap">
                {friendly.originalMessage}
              </pre>
            </details>
          )}
        </div>
      </div>
    );
  }
  if (content === null) {
    return (
      <div className="flex flex-col h-full">
        <StickyBreadcrumb path={path} onNavigate={onNavigate} />
        <div className="p-8 text-sm text-muted-foreground">Loading…</div>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      {/* ── Sticky breadcrumb bar ── */}
      <StickyBreadcrumb path={path} onNavigate={onNavigate} />

      {/* ── Scrollable content ── */}
      <div className="flex-1 overflow-auto kiwi-scroll">
        <div className="max-w-6xl mx-auto px-8 py-6">
          {/* ── Page header zone ── */}
          <div className="mb-6">
            <div className="flex items-start justify-between gap-4">
              <div className="min-w-0">
                <h1 className="text-2xl font-bold tracking-tight text-foreground leading-tight">
                  {frontmatterTitle || titleize(path)}
                </h1>
                {statusBadge && (
                  <Badge
                    variant="outline"
                    className={"mt-2 " + statusColor(statusBadge.value)}
                  >
                    {statusBadge.value}
                  </Badge>
                )}
              </div>
              <div className="flex items-center gap-2 shrink-0 pt-1">
                {onTogglePin && (
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={onTogglePin}
                    className="h-8 w-8"
                    aria-label={isPinned ? "Unpin page" : "Pin page"}
                    aria-pressed={isPinned ? true : false}
                    title={isPinned ? "Unpin page" : "Pin page"}
                  >
                    <Pin className={"h-4 w-4" + (isPinned ? " fill-current text-primary" : "")} />
                  </Button>
                )}
                {onToggleStar && (
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={onToggleStar}
                    className="h-8 w-8"
                    aria-label={isStarred ? "Unstar page" : "Star page"}
                    aria-pressed={isStarred ? true : false}
                    title={isStarred ? "Unstar page" : "Star page"}
                  >
                    <Star className={"h-4 w-4" + (isStarred ? " fill-amber-500 text-amber-500" : "")} />
                  </Button>
                )}
                {onHistory && (
                  <Button variant="outline" size="sm" onClick={onHistory}>
                    <HistoryIcon className="h-3.5 w-3.5" /> History
                  </Button>
                )}
                <Button variant="default" size="sm" onClick={onEdit}>
                  <Edit className="h-3.5 w-3.5" /> Edit
                </Button>
                <PageActions
                  path={path}
                  onDeleted={onDeleted}
                  onDuplicated={onDuplicated}
                  onMoved={onMoved}
                />
              </div>
            </div>

            {/* ── Metadata bar ── */}
            <div className="flex items-center gap-3 mt-3 text-xs text-muted-foreground flex-wrap">
              {lastAuthor && (
                <span className="flex items-center gap-1">
                  <User className="h-3 w-3" />
                  {lastAuthor}
                </span>
              )}
              {lastModified && (
                <span className="flex items-center gap-1">
                  <Calendar className="h-3 w-3" />
                  Last modified {formatRelative(lastModified)}
                </span>
              )}
              {commentCount > 0 && (
                <span className="flex items-center gap-1">
                  <MessageSquareQuote className="h-3 w-3" />
                  {commentCount} comment{commentCount === 1 ? "" : "s"}
                </span>
              )}
              {(versionError || commentError) && (
                <span className="flex items-center gap-1 text-amber-600 dark:text-amber-400" title={
                  [versionError && "version history", commentError && "comments"].filter(Boolean).join(" and ") + " unavailable"
                }>
                  <AlertTriangle className="h-3 w-3" />
                  Some metadata unavailable
                </span>
              )}
            </div>

            {/* ── Tags ── */}
            {tagBadges.length > 0 && (
              <div className="flex flex-wrap gap-1.5 mt-3">
                {tagBadges.map((b) => (
                  <Badge
                    key={b.value}
                    variant="secondary"
                    className="cursor-pointer hover:bg-primary/20 transition-colors gap-1"
                    onClick={() => onTagClick?.(b.value)}
                  >
                    <Tag className="h-3 w-3" />
                    {b.value}
                  </Badge>
                ))}
              </div>
            )}

            {/* ── Other labels ── */}
            {otherBadges.length > 0 && (
              <div className="flex flex-wrap gap-1.5 mt-2">
                {otherBadges.map((b) => (
                  <Badge
                    key={b.key + ":" + b.value}
                    variant="outline"
                  >
                    <span className="text-muted-foreground mr-1">{b.key}:</span>
                    <span>{b.value}</span>
                  </Badge>
                ))}
              </div>
            )}

            {/* ── Trust badge + metadata editor ── */}
            <div className="mt-3 space-y-3">
              <KiwiTrustBadge
                path={path}
                frontmatter={parsed.meta}
                onNavigate={onNavigate}
              />
              <MetadataPanel
                path={path}
                frontmatter={parsed.meta}
                onSaved={onRefresh}
              />
            </div>
          </div>

          {/* ── Content zone + ToC ── */}
          <div className="flex gap-6">
            <article className="min-w-0 flex-1">
              {/* proseRef anchors text-selection features (highlights, comments)
                  so it must attach to the real content surface in both render
                  branches — previously only the non-decision branch set it,
                  which silently broke comments on decision pages. */}
              <div ref={proseRef} className="kiwi-prose">
              {parsed.meta.type === "decision" ? (
                <KiwiDecisionLog
                  path={path}
                  frontmatter={parsed.meta}
                  content={parsed.body}
                  onNavigate={onNavigate}
                />
              ) : (
                <>
                  <ReactMarkdown
                    remarkPlugins={[remarkGfm, remarkMath, [remarkWikiLinks, { resolver }]]}
                    rehypePlugins={[
                      rehypeRaw,
                      rehypeKatex,
                      rehypeSlug,
                      [rehypeAutolinkHeadings, { behavior: "wrap" }],
                    ]}
                    components={{
                      a: ({ href, children, ...rest }) => {
                        const h = href ?? "";
                        if (h.startsWith("kiwi:")) {
                          const target = h.slice("kiwi:".length);
                          return (
                            <a
                              href={`#${target}`}
                              onClick={(e) => {
                                e.preventDefault();
                                onNavigate(target);
                              }}
                              className="wiki-link"
                              {...(rest as any)}
                            >
                              {children}
                            </a>
                          );
                        }
                        if (h.startsWith("kiwi-missing:")) {
                          const target = h.slice("kiwi-missing:".length);
                          return (
                            <a
                              href="#"
                              onClick={(e) => {
                                e.preventDefault();
                                onNavigate(`${target}.md`);
                              }}
                              title={`Missing: ${target} — click to create`}
                              className="wiki-link-missing"
                              {...(rest as any)}
                            >
                              {children}
                            </a>
                          );
                        }
                        return (
                          <a
                            href={h}
                            target={h.startsWith("http") ? "_blank" : undefined}
                            rel={h.startsWith("http") ? "noreferrer" : undefined}
                            {...(rest as any)}
                          >
                            {children}
                          </a>
                        );
                      },
                      code: ({ className, children, ...rest }: any) => {
                        const match = /language-([A-Za-z0-9_-]+)/.exec(className || "");
                        const lang = match ? match[1] : undefined;
                        const raw = String(children).replace(/\n$/, "");
                        if (lang === "kiwi-query") {
                          return <KiwiQuery source={raw} onNavigate={onNavigate} />;
                        }
                        if (!lang || !raw.includes("\n")) {
                          return <code className={className} {...rest}>{children}</code>;
                        }
                        return <ShikiCode code={raw} lang={lang} />;
                      },
                      pre: ({ children }) => <>{children}</>,
                      img: ({ src, alt, ...rest }) => (
                        <Zoom wrapElement="span" classDialog="kiwi-zoom-dialog" zoomMargin={32}>
                          <img src={src as string} alt={alt as string} {...(rest as any)} />
                        </Zoom>
                      ),
                      p: ({ children, ...rest }) => {
                        const arr = Array.isArray(children) ? children : [children];
                        const first = arr[0];
                        if (typeof first === "string") {
                          const hit = splitCallout(first);
                          if (hit) {
                            const rest2 = [hit.rest, ...arr.slice(1)];
                            return (
                              <div className={`kiwi-callout ${hit.cls}`}>
                                <span className="mr-1.5">{hit.emoji}</span>
                                {rest2}
                              </div>
                            );
                          }
                        }
                        return <p {...(rest as any)}>{children}</p>;
                      },
                    }}
                  >
                    {parsed.body}
                  </ReactMarkdown>
                </>
              )}
              </div>

              {/* ── Workflow ── */}
              {Array.isArray(parsed.meta.tasks) && (
                <KiwiWorkflow
                  path={path}
                  frontmatter={parsed.meta as Record<string, any>}
                  onRefresh={() => onRefresh?.()}
                />
              )}

              {/* ── Footer zone: fixed order, collapsible ── */}
              <div className="mt-12 space-y-2">
                <CollapsibleFooterSection
                  icon={<MessageSquareQuote className="h-4 w-4" />}
                  title="Comments"
                  storageKey="footer-comments"
                  defaultOpen={commentCount > 0}
                >
                  <KiwiComments
                    path={path}
                    containerRef={proseRef}
                    renderKey={content}
                    refreshKey={refreshKey}
                  />
                </CollapsibleFooterSection>

                <CollapsibleFooterSection
                  icon={<Link2 className="h-4 w-4" />}
                  title="Backlinks"
                  storageKey="footer-backlinks"
                >
                  <KiwiBacklinks path={path} onNavigate={onNavigate} refreshKey={refreshKey} />
                </CollapsibleFooterSection>
              </div>

              {/* ── File info ── */}
              <div className="border-t border-border mt-8 pt-4 pb-2">
                <div className="text-xs text-muted-foreground flex items-center gap-3">
                  <FileAxis3D className="h-3.5 w-3.5" />
                  <code className="font-mono">{path}</code>
                </div>
              </div>
            </article>
            <KiwiToC markdown={parsed.body} containerRef={proseRef} />
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Sticky Breadcrumb ── */

function StickyBreadcrumb({ path, onNavigate }: { path: string; onNavigate: (p: string) => void }) {
  return (
    <div className="sticky top-0 z-10 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80 border-b border-border shrink-0">
      <div className="px-8 py-2 max-w-6xl mx-auto">
        <KiwiBreadcrumb path={path} onNavigate={onNavigate} />
      </div>
    </div>
  );
}

/* ── Collapsible footer section ── */

function CollapsibleFooterSection({
  icon,
  title,
  children,
  storageKey,
  defaultOpen,
}: {
  icon: React.ReactNode;
  title: string;
  children: React.ReactNode;
  storageKey: string;
  defaultOpen?: boolean;
}) {
  const [collapsed, setCollapsed] = useState(() => {
    try {
      const stored = localStorage.getItem(`kiwifs-${storageKey}`);
      if (stored !== null) return stored === "1";
    } catch {}
    return !defaultOpen;
  });

  return (
    <div className="border border-border rounded-lg">
      <button
        type="button"
        onClick={() => {
          const next = !collapsed;
          setCollapsed(next);
          try { localStorage.setItem(`kiwifs-${storageKey}`, next ? "1" : "0"); } catch {}
        }}
        className="flex items-center gap-2 w-full px-4 py-2.5 text-sm font-medium text-muted-foreground hover:text-foreground transition-colors"
      >
        {icon}
        <span className="flex-1 text-left">{title}</span>
        {collapsed
          ? <ChevronRight className="h-4 w-4" />
          : <ChevronDown className="h-4 w-4" />}
      </button>
      {!collapsed && <div className="px-4 pb-4">{children}</div>}
    </div>
  );
}

/* ── Helpers ── */

function formatRelative(httpDate: string): string {
  try {
    const d = new Date(httpDate);
    const now = Date.now();
    const diff = now - d.getTime();
    if (diff < 60_000) return "just now";
    if (diff < 3600_000) return `${Math.floor(diff / 60_000)}m ago`;
    if (diff < 86400_000) return `${Math.floor(diff / 3600_000)}h ago`;
    if (diff < 604800_000) return `${Math.floor(diff / 86400_000)}d ago`;
    return d.toLocaleDateString();
  } catch {
    return httpDate;
  }
}

function statusColor(value: string): string {
  const v = value.toLowerCase().replace(/[^a-z]/g, "");
  if (["done", "complete", "completed", "live", "published"].includes(v))
    return "border-green-500/50 bg-green-500/10 text-green-700 dark:text-green-400";
  if (["inprogress", "wip", "active", "started"].includes(v))
    return "border-blue-500/50 bg-blue-500/10 text-blue-700 dark:text-blue-400";
  if (["draft", "todo", "planned"].includes(v))
    return "border-amber-500/50 bg-amber-500/10 text-amber-700 dark:text-amber-400";
  if (["blocked", "stuck", "cancelled", "deprecated"].includes(v))
    return "border-red-500/50 bg-red-500/10 text-red-700 dark:text-red-400";
  return "";
}

function frontmatterBadges(
  meta: Record<string, unknown>
): Array<{ key: string; value: string }> {
  const out: Array<{ key: string; value: string }> = [];
  for (const [key, raw] of Object.entries(meta)) {
    if (key === "title") continue;
    if (raw == null) continue;
    if (Array.isArray(raw)) {
      for (const item of raw) {
        if (item == null) continue;
        out.push({ key, value: String(item) });
      }
      continue;
    }
    if (typeof raw === "object") {
      out.push({ key, value: JSON.stringify(raw) });
      continue;
    }
    out.push({ key, value: String(raw) });
  }
  return out;
}

// MetadataPanel is a collapsible wrapper that reveals the full trust-metadata
// editor for a page. Collapsed by default to keep the reading surface clean.
function MetadataPanel({
  path,
  frontmatter,
  onSaved,
}: {
  path: string;
  frontmatter: Record<string, any>;
  onSaved?: () => void;
}) {
  const [open, setOpen] = useState(false);

  return (
    <div className="rounded-lg border border-border">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-2 w-full px-4 py-2 text-xs text-muted-foreground hover:text-foreground transition-colors"
      >
        {open ? (
          <ChevronDown className="h-3.5 w-3.5" />
        ) : (
          <ChevronRight className="h-3.5 w-3.5" />
        )}
        Page metadata
        <span className="ml-auto text-[10px] uppercase tracking-wide">
          owner · status · review dates
        </span>
      </button>
      {open && (
        <div className="border-t border-border p-1">
          <KiwiPageMeta
            path={path}
            frontmatter={frontmatter}
            onSaved={onSaved}
          />
        </div>
      )}
    </div>
  );
}
