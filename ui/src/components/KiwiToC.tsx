import { useEffect, useMemo, useRef, useState } from "react";
import GithubSlugger from "github-slugger";
import { cn } from "@/lib/cn";
import { Button } from "@/components/ui/button";

type Heading = { id: string; text: string; depth: number };

// Parse headings directly from markdown. Doing this on the source (rather than
// walking the rendered DOM) avoids a race where the ToC mounts before
// react-markdown finishes painting.
function parseHeadings(md: string): Heading[] {
  const lines = md.split("\n");
  const out: Heading[] = [];
  const slugger = new GithubSlugger();
  let inFence = false;
  for (const line of lines) {
    if (/^```/.test(line)) {
      inFence = !inFence;
      continue;
    }
    if (inFence) continue;
    const m = /^(#{1,6})\s+(.+?)\s*#*\s*$/.exec(line);
    if (!m) continue;
    const depth = m[1].length;
    if (depth < 2 || depth > 4) continue; // h1 is the page title; skip h5/h6
    const text = m[2].replace(/\[([^\]]+)\]\([^)]+\)/g, "$1").trim();
    const id = slugger.slug(text);
    if (!id) continue;
    out.push({ id, text, depth });
  }
  return out;
}

type Props = {
  markdown: string;
  containerRef: React.RefObject<HTMLElement>;
};

export function KiwiToC({ markdown, containerRef }: Props) {
  const headings = useMemo(() => parseHeadings(markdown), [markdown]);
  const [active, setActive] = useState<string | null>(null);
  const [collapsed, setCollapsed] = useState(false);
  const ids = useRef<string[]>([]);

  ids.current = headings.map((h) => h.id);

  useEffect(() => {
    if (!containerRef.current || headings.length === 0) return;
    const root = containerRef.current;
    const targets = headings
      .map((h) => root.querySelector<HTMLElement>(`#${CSS.escape(h.id)}`))
      .filter((el): el is HTMLElement => !!el);
    if (targets.length === 0) {
      setActive(headings[0]?.id ?? null);
      return;
    }

    const visible = new Map<string, number>();
    const observer = new IntersectionObserver(
      (entries) => {
        for (const e of entries) {
          if (e.isIntersecting) visible.set(e.target.id, e.intersectionRatio);
          else visible.delete(e.target.id);
        }
        if (visible.size === 0) return;
        // Pick the highest (first) heading currently in view.
        let best: string | null = null;
        for (const id of ids.current) {
          if (visible.has(id)) {
            best = id;
            break;
          }
        }
        if (best) setActive(best);
      },
      { rootMargin: "-80px 0px -60% 0px", threshold: [0, 1] },
    );
    targets.forEach((t) => observer.observe(t));
    setActive(headings[0].id);
    return () => observer.disconnect();
  }, [markdown, headings, containerRef]);

  if (headings.length === 0) return null;

  return (
    <aside className="hidden xl:block w-64 shrink-0">
      <div className="sticky top-4 pr-2 text-sm">
        <div className="flex items-center justify-between mb-2 px-2">
          <span className="text-xs uppercase tracking-wider text-muted-foreground">
            On this page
          </span>
          <Button
            variant="ghost"
            size="sm"
            className="h-6 px-2 text-xs text-muted-foreground"
            onClick={() => setCollapsed((c) => !c)}
          >
            {collapsed ? "Show" : "Hide"}
          </Button>
        </div>
        {!collapsed && (
          <nav className="border-l border-border">
            {headings.map((h) => (
              <a
                key={h.id}
                href={`#${h.id}`}
                onClick={(e) => {
                  e.preventDefault();
                  const el = document.getElementById(h.id);
                  if (el) {
                    el.scrollIntoView({ behavior: "smooth", block: "start" });
                    history.replaceState(null, "", `#${h.id}`);
                  }
                }}
                className={cn(
                  "block py-1 pr-2 border-l-2 -ml-px truncate transition-colors hover:text-foreground",
                  h.depth === 2 && "pl-3",
                  h.depth === 3 && "pl-6",
                  h.depth === 4 && "pl-9",
                  active === h.id
                    ? "text-foreground border-primary"
                    : "text-muted-foreground border-transparent",
                )}
              >
                {h.text}
              </a>
            ))}
          </nav>
        )}
      </div>
    </aside>
  );
}
