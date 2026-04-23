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
import { Edit, FileText, History as HistoryIcon, Link2 } from "lucide-react";
import { api, type TreeEntry } from "@/lib/api";
import { titleize } from "@/lib/paths";
import { KiwiBreadcrumb } from "./KiwiBreadcrumb";
import { KiwiToC } from "./KiwiToC";
import { KiwiBacklinks } from "./KiwiBacklinks";
import { KiwiComments } from "./KiwiComments";
import { KiwiQuery } from "./KiwiQuery";
import { ShikiCode } from "./ShikiCode";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { buildResolver, remarkWikiLinks } from "@/lib/wikiLinks";

type Props = {
  path: string;
  tree: TreeEntry | null;
  onNavigate: (path: string) => void;
  onEdit: () => void;
  onHistory?: () => void;
  refreshKey?: number;
};

// Callout prefixes recognised inline — slash-menu insertions drop these
// emoji leaders into plain paragraphs, so we detect them at render time and
// wrap the paragraph in the matching styled div instead of parsing markdown.
const CALLOUT_PREFIXES: Array<{ emoji: string; cls: string }> = [
  { emoji: "ℹ️", cls: "kiwi-callout-info" },
  { emoji: "⚠️", cls: "kiwi-callout-warn" },
  { emoji: "🛑", cls: "kiwi-callout-error" },
];

function splitCallout(text: string): { emoji: string; cls: string; rest: string } | null {
  const trimmed = text.trimStart();
  for (const p of CALLOUT_PREFIXES) {
    if (trimmed.startsWith(p.emoji)) {
      return {
        emoji: p.emoji,
        cls: p.cls,
        rest: trimmed.slice(p.emoji.length).trimStart(),
      };
    }
  }
  return null;
}

export function KiwiPage({ path, tree, onNavigate, onEdit, onHistory, refreshKey }: Props) {
  const [content, setContent] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const proseRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let cancelled = false;
    setContent(null);
    setError(null);
    api
      .readFile(path)
      .then((r) => {
        if (!cancelled) setContent(r.content);
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [path, refreshKey]);

  const resolver = useMemo(() => buildResolver(tree), [tree]);

  // Parse YAML frontmatter off the top so the markdown renderer never sees
  // it — otherwise the `---` delimiters round-trip as thematic breaks and
  // the YAML body as a paragraph.
  const parsed = useMemo(() => {
    if (content == null) return { body: "", meta: {} as Record<string, unknown> };
    try {
      const m = matter(content);
      return { body: m.content, meta: (m.data || {}) as Record<string, unknown> };
    } catch {
      return { body: content, meta: {} };
    }
  }, [content]);

  const badges = useMemo(() => frontmatterBadges(parsed.meta), [parsed.meta]);
  const frontmatterTitle = typeof parsed.meta.title === "string" ? parsed.meta.title : null;

  if (error) {
    return (
      <div className="p-8">
        <KiwiBreadcrumb path={path} onNavigate={onNavigate} />
        <div className="mt-6 text-sm text-destructive font-mono">{error}</div>
      </div>
    );
  }
  if (content === null) {
    return (
      <div className="p-8 text-sm text-muted-foreground">
        <KiwiBreadcrumb path={path} onNavigate={onNavigate} />
        <div className="mt-6">Loading…</div>
      </div>
    );
  }

  return (
    <div className="flex gap-6 px-8 py-6 max-w-[100rem] mx-auto">
      <article className="min-w-0 flex-1">
        <div className="flex items-center justify-between gap-4 mb-4">
          <KiwiBreadcrumb path={path} onNavigate={onNavigate} />
          <div className="flex items-center gap-2">
            {onHistory && (
              <Button variant="outline" size="sm" onClick={onHistory}>
                <HistoryIcon className="h-3.5 w-3.5" /> History
              </Button>
            )}
            <Button variant="outline" size="sm" onClick={onEdit}>
              <Edit className="h-3.5 w-3.5" /> Edit
            </Button>
          </div>
        </div>
        <div ref={proseRef} className="kiwi-prose">
          <h1>{frontmatterTitle || titleize(path)}</h1>
          {badges.length > 0 && (
            <div className="kiwi-frontmatter">
              {badges.map((b) => (
                <Badge key={b.key} variant="outline">
                  <span className="text-muted-foreground mr-1">{b.key}:</span>
                  <span>{b.value}</span>
                </Badge>
              ))}
            </div>
          )}
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
                // react-markdown's default code regex only matches [A-Za-z0-9_],
                // so hyphenated languages like `kiwi-query` would otherwise be
                // swallowed — match them explicitly here.
                const match = /language-([A-Za-z0-9_-]+)/.exec(className || "");
                const lang = match ? match[1] : undefined;
                const raw = String(children).replace(/\n$/, "");
                if (lang === "kiwi-query") {
                  return <KiwiQuery source={raw} onNavigate={onNavigate} />;
                }
                if (!lang || !raw.includes("\n")) {
                  return (
                    <code className={className} {...rest}>
                      {children}
                    </code>
                  );
                }
                return <ShikiCode code={raw} lang={lang} />;
              },
              pre: ({ children }) => <>{children}</>,
              img: ({ src, alt, ...rest }) => (
                <Zoom
                  wrapElement="span"
                  classDialog="kiwi-zoom-dialog"
                  zoomMargin={32}
                >
                  <img
                    src={src as string}
                    alt={alt as string}
                    {...(rest as any)}
                  />
                </Zoom>
              ),
              p: ({ children, ...rest }) => {
                // Detect a leading text node shaped like "ℹ️ …" / "⚠️ …" /
                // "🛑 …" and swap the paragraph for a styled callout block.
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
        <KiwiComments
          path={path}
          containerRef={proseRef}
          renderKey={content}
          refreshKey={refreshKey}
        />
        <KiwiBacklinks path={path} onNavigate={onNavigate} refreshKey={refreshKey} />
        <Separator className="mt-12 mb-4" />
        <div className="text-xs text-muted-foreground flex items-center gap-3 pb-2">
          <FileText className="h-3.5 w-3.5" />
          <code className="font-mono">{path}</code>
          <span className="ml-auto flex items-center gap-1">
            <Link2 className="h-3.5 w-3.5" />
            Markdown source
          </span>
        </div>
      </article>
      <KiwiToC markdown={parsed.body} containerRef={proseRef} />
    </div>
  );
}

function frontmatterBadges(
  meta: Record<string, unknown>
): Array<{ key: string; value: string }> {
  const out: Array<{ key: string; value: string }> = [];
  for (const [key, raw] of Object.entries(meta)) {
    // Skip the title — rendered as the page heading instead of a badge.
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
