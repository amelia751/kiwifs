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
import { AlertTriangle, Calendar, CheckSquare, ChevronDown, ChevronRight, Edit, FileAxis3D, FileQuestion, History as HistoryIcon, Link2, List, MessageSquareQuote, Pin, Plus, Star, Tag, Type, User } from "lucide-react";
import { api, type TreeEntry } from "@/lib/api";
import { titleize } from "@/lib/paths";
import { KiwiBreadcrumb } from "./KiwiBreadcrumb";
import { KiwiToC } from "./KiwiToC";
import { KiwiBacklinks } from "./KiwiBacklinks";
import { KiwiComments } from "./KiwiComments";
import { KiwiQuery } from "./KiwiQuery";
import { PageActions } from "./PageActions";
import { ShikiCode } from "./ShikiCode";
import { ExcalidrawMarkdownPreview, isExcalidrawMarkdown } from "./ExcalidrawMarkdownPreview";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { buildResolver, remarkWikiLinks } from "@/lib/wikiLinks";

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
};

type FrontmatterProperty = {
  key: string;
  value: unknown;
  kind: "text" | "list" | "date" | "boolean" | "object";
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

function parseMarkdownPage(content: string): { body: string; meta: Record<string, unknown> } {
  const fallback = splitFrontmatterBlock(content);

  try {
    const m = matter(content);
    const parsedMeta = (m.data || {}) as Record<string, unknown>;
    const meta = Object.keys(parsedMeta).length > 0
      ? parsedMeta
      : fallback
        ? parseSimpleFrontmatter(fallback.raw)
        : {};
    const body = stripDuplicateTitle(fallback && m.content === content ? fallback.body : m.content, meta);
    return { body, meta };
  } catch {
    if (fallback) {
      const meta = parseSimpleFrontmatter(fallback.raw);
      return { body: stripDuplicateTitle(fallback.body, meta), meta };
    }
    return { body: content, meta: {} };
  }
}

function splitFrontmatterBlock(content: string): { raw: string; body: string } | null {
  const withoutBom = content.replace(/^\uFEFF/, "");
  if (!withoutBom.startsWith("---\n") && !withoutBom.startsWith("---\r\n")) return null;

  const rest = withoutBom.replace(/^---[ \t]*\r?\n/, "");
  const match = rest.match(/\r?\n---[ \t]*(?:\r?\n|$)/);
  if (!match || match.index == null) return null;

  const raw = rest.slice(0, match.index);
  const body = rest.slice(match.index + match[0].length);
  return { raw, body };
}

function parseSimpleFrontmatter(raw: string): Record<string, unknown> {
  const meta: Record<string, unknown> = {};
  let listKey: string | null = null;

  for (const line of raw.split(/\r?\n/)) {
    const listItem = line.match(/^\s+-\s+(.*)$/);
    if (listKey && listItem) {
      const current = meta[listKey];
      if (Array.isArray(current)) current.push(parseFrontmatterScalar(listItem[1]));
      continue;
    }

    const entry = line.match(/^([A-Za-z0-9_-]+):(?:\s*(.*))?$/);
    if (!entry) continue;

    const [, key, rawValue = ""] = entry;
    const value = rawValue.trim();
    if (value === "") {
      meta[key] = [];
      listKey = key;
      continue;
    }

    meta[key] = parseFrontmatterScalar(value);
    listKey = null;
  }

  return meta;
}

function parseFrontmatterScalar(value: string): unknown {
  const trimmed = value.trim();
  if ((trimmed.startsWith('"') && trimmed.endsWith('"')) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
    return trimmed.slice(1, -1);
  }
  if (trimmed === "true") return true;
  if (trimmed === "false") return false;
  if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) return Number(trimmed);
  return trimmed;
}

function stripDuplicateTitle(body: string, meta: Record<string, unknown>): string {
  if (typeof meta.title !== "string") return body;
  const h1Match = body.match(/^\s*#\s+(.+)\n?/);
  if (h1Match && h1Match[1].trim() === meta.title.trim()) {
    return body.replace(/^\s*#\s+.+\n?/, "");
  }
  return body;
}

export function KiwiPage({ path, tree, onNavigate, onEdit, onHistory, onToggleStar, isStarred, onTogglePin, isPinned, onDeleted, onDuplicated, onMoved, onTagClick, refreshKey }: Props) {
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
    return parseMarkdownPage(content);
  }, [content]);

  const properties = useMemo(() => frontmatterProperties(parsed.meta), [parsed.meta]);
  const badges = useMemo(() => frontmatterBadges(parsed.meta), [parsed.meta]);
  const frontmatterTitle = typeof parsed.meta.title === "string" ? parsed.meta.title : null;
  const statusBadge = badges.find((b) => b.key === "status");
  const tagBadges = badges.filter((b) => b.key === "tags");

  if (error) {
    const is404 = error.startsWith("Error: 404") || error.includes("404");
    return (
      <div className="flex flex-col h-full">
        <StickyBreadcrumb path={path} onNavigate={onNavigate} />
        {is404 ? (
          <div className="flex-1 grid place-items-center">
            <div className="text-center max-w-md px-8">
              <FileQuestion className="h-12 w-12 mx-auto mb-4 text-muted-foreground/50" />
              <h2 className="text-lg font-semibold text-foreground mb-1">Page not found</h2>
              <p className="text-sm text-muted-foreground mb-1">
                <code className="font-mono text-xs bg-muted px-1.5 py-0.5 rounded">{path}</code>
              </p>
              <p className="text-sm text-muted-foreground mb-6">
                This page may have been moved, renamed, or deleted.
              </p>
              <div className="flex flex-col gap-2 items-center">
                <Button size="sm" onClick={() => onNavigate("")} className="gap-2">
                  Go to index
                </Button>
                <Button variant="outline" size="sm" onClick={() => onNavigate(path)} className="gap-2">
                  <Plus className="h-3.5 w-3.5" /> Create this page
                </Button>
              </div>
            </div>
          </div>
        ) : (
          <div className="p-8 text-sm text-destructive font-mono">{error}</div>
        )}
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
                  <Button variant="ghost" size="icon" onClick={onTogglePin} className="h-8 w-8">
                    <Pin className={"h-4 w-4" + (isPinned ? " fill-current text-primary" : "")} />
                  </Button>
                )}
                {onToggleStar && (
                  <Button variant="ghost" size="icon" onClick={onToggleStar} className="h-8 w-8">
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

            {/* ── Properties ── */}
            {properties.length > 0 && (
              <FrontmatterProperties
                properties={properties}
                onTagClick={onTagClick}
              />
            )}
          </div>

          {/* ── Content zone + ToC ── */}
          <div className="flex gap-6">
            <article className="min-w-0 flex-1">
              {isExcalidrawMarkdown(content, parsed.meta) ? (
                <ExcalidrawMarkdownPreview markdown={content} title={frontmatterTitle || titleize(path)} />
              ) : (
              <div ref={proseRef} className="kiwi-prose">
                <ReactMarkdown
                  remarkPlugins={[remarkGfm, remarkMath, [remarkWikiLinks, { resolver }]]}
                  rehypePlugins={[
                    rehypeRaw,
                    rehypeKatex,
                    rehypeSlug,
                    [rehypeAutolinkHeadings, { behavior: "wrap" }],
                  ]}
                  components={{
                    a: ({ href, children, node: _node, ...rest }) => {
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
                    code: ({ className, children, node: _node, ...rest }: any) => {
                      const match = /language-([A-Za-z0-9_-]+)/.exec(className || "");
                      const lang = match ? match[1] : undefined;
                      const raw = String(children).replace(/\n$/, "");
                      if (lang === "kiwi-query") {
                        return <KiwiQuery source={raw} onNavigate={onNavigate} isComputedView={parsed.meta?.["kiwi-view"] === true} />;
                      }
                      if (!lang || !raw.includes("\n")) {
                        return <code className={className} {...rest}>{children}</code>;
                      }
                      return <ShikiCode code={raw} lang={lang} />;
                    },
                    pre: ({ children }) => <>{children}</>,
                    img: ({ src, alt, node: _node, ...rest }) => (
                      <Zoom wrapElement="span" classDialog="kiwi-zoom-dialog" zoomMargin={32}>
                        <img src={src as string} alt={alt as string} {...(rest as any)} />
                      </Zoom>
                    ),
                    p: ({ children, node: _node, ...rest }) => {
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
              </div>
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
            {!isExcalidrawMarkdown(content, parsed.meta) && <KiwiToC markdown={parsed.body} containerRef={proseRef} />}
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Frontmatter properties ── */

function FrontmatterProperties({
  properties,
  onTagClick,
}: {
  properties: FrontmatterProperty[];
  onTagClick?: (tag: string) => void;
}) {
  return (
    <section className="mt-6 border-t border-border/70 pt-4" aria-label="Properties">
      <div className="mb-2 text-sm font-semibold text-foreground">Properties</div>
      <div className="space-y-1.5 text-sm">
        {properties.map((property) => (
          <div
            key={property.key}
            className="grid grid-cols-[minmax(8rem,12rem)_1fr] gap-4 rounded-md px-2 py-1.5 hover:bg-muted/40"
          >
            <div className="flex min-w-0 items-center gap-2 text-muted-foreground">
              <PropertyIcon kind={property.kind} />
              <span className="truncate font-medium">{property.key}</span>
            </div>
            <div className="min-w-0 text-foreground/90">
              <PropertyValue property={property} onTagClick={onTagClick} />
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function PropertyIcon({ kind }: { kind: FrontmatterProperty["kind"] }) {
  const className = "h-4 w-4 shrink-0";
  if (kind === "date") return <Calendar className={className} />;
  if (kind === "boolean") return <CheckSquare className={className} />;
  if (kind === "list" || kind === "object") return <List className={className} />;
  return <Type className={className} />;
}

function PropertyValue({
  property,
  onTagClick,
}: {
  property: FrontmatterProperty;
  onTagClick?: (tag: string) => void;
}) {
  const { key, value } = property;

  return <SemanticFrontmatterValue propertyKey={key} value={value} onTagClick={onTagClick} />;
}

function SemanticFrontmatterValue({
  propertyKey,
  value,
  onTagClick,
}: {
  propertyKey: string;
  value: unknown;
  onTagClick?: (tag: string) => void;
}) {
  if (Array.isArray(value)) {
    if (value.length === 0) return <span className="text-muted-foreground">[]</span>;

    return (
      <ul className="m-0 flex list-none flex-wrap gap-1.5 p-0" aria-label={`${propertyKey} values`}>
        {value.map((item, index) => (
          <li key={`${propertyKey}-${index}`} className="min-w-0">
            <SemanticFrontmatterValue propertyKey={propertyKey} value={item} onTagClick={onTagClick} />
          </li>
        ))}
      </ul>
    );
  }

  if (isPlainObject(value)) {
    const entries = Object.entries(value).filter(([, nestedValue]) => nestedValue != null);
    if (entries.length === 0) return <span className="text-muted-foreground">{`{}`}</span>;

    return (
      <dl className="m-0 space-y-1 rounded-md border border-border/60 p-2">
        {entries.map(([nestedKey, nestedValue]) => (
          <div key={nestedKey} className="grid grid-cols-[minmax(6rem,10rem)_1fr] gap-2">
            <dt className="min-w-0 truncate text-muted-foreground">{nestedKey}</dt>
            <dd className="m-0 min-w-0">
              <SemanticFrontmatterValue propertyKey={nestedKey} value={nestedValue} onTagClick={onTagClick} />
            </dd>
          </div>
        ))}
      </dl>
    );
  }

  if (typeof value === "boolean") {
    return (
      <label className="inline-flex items-center gap-2">
        <input type="checkbox" checked={value} readOnly aria-label={String(value)} className="h-4 w-4" />
        <span className="text-muted-foreground">{String(value)}</span>
      </label>
    );
  }

  const text = formatFrontmatterValue(value);
  const isLong = text.length > 80;
  const isTag = propertyKey === "tags" && typeof value === "string";

  if (isTag) {
    return (
      <button
        type="button"
        className="rounded-full border border-border bg-muted/60 px-2 py-0.5 text-xs hover:bg-primary/20"
        onClick={() => onTagClick?.(text)}
      >
        {text}
      </button>
    );
  }

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return <time dateTime={value.toISOString()}>{text}</time>;
  }

  if (isDateLikeString(value)) {
    return <time dateTime={text}>{text}</time>;
  }

  return (
    <span className={isLong ? "block whitespace-pre-wrap break-words leading-relaxed" : "break-words"}>
      {text}
    </span>
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

function frontmatterProperties(meta: Record<string, unknown>): FrontmatterProperty[] {
  return Object.entries(meta)
    .filter(([, value]) => value != null)
    .map(([key, value]) => ({
      key,
      value,
      kind: frontmatterKind(value),
    }));
}

function frontmatterKind(value: unknown): FrontmatterProperty["kind"] {
  if (typeof value === "boolean") return "boolean";
  if (value instanceof Date || isDateLikeString(value)) return "date";
  if (Array.isArray(value)) return "list";
  if (isPlainObject(value)) return "object";
  return "text";
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value != null && !Array.isArray(value) && !(value instanceof Date);
}

function isDateLikeString(value: unknown): boolean {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2})?/.test(value);
}

function formatFrontmatterValue(value: unknown): string {
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? "" : value.toLocaleDateString();
  }
  if (value == null) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (Array.isArray(value)) return value.map(formatFrontmatterValue).join(", ");
  if (typeof value === "object") return JSON.stringify(value, null, 2);
  return String(value);
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
        out.push({ key, value: formatFrontmatterValue(item) });
      }
      continue;
    }
    if (typeof raw === "object" && !(raw instanceof Date)) {
      out.push({ key, value: JSON.stringify(raw) });
      continue;
    }
    out.push({ key, value: formatFrontmatterValue(raw) });
  }
  return out;
}
