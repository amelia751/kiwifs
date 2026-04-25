// Knowledge graph view: nodes are markdown pages, edges are [[wiki-link]]
// references between them. Powered by Sigma.js (WebGL) + Graphology with a
// ForceAtlas2 layout; Louvain community detection drives the per-cluster
// palette.
//
// Target resolution: the server returns raw [[target]] strings from the link
// index. We reuse `buildResolver()` — the same fuzzy resolver the markdown
// renderer uses for in-page wiki links — so the graph shows the same shape a
// reader sees when clicking links.

import { useEffect, useMemo, useRef, useState } from "react";
import Graph from "graphology";
import circular from "graphology-layout/circular";
import forceAtlas2 from "graphology-layout-forceatlas2";
import louvain from "graphology-communities-louvain";
import {
  SigmaContainer,
  useLoadGraph,
  useRegisterEvents,
  useSetSettings,
  useSigma,
} from "@react-sigma/core";
import { ArrowLeft, Loader2, Search as SearchIcon, Tag } from "lucide-react";
import "@react-sigma/core/lib/style.css";
import { api, type GraphResponse, type TreeEntry } from "@/lib/api";
import { buildResolver } from "@/lib/wikiLinks";
import { titleize } from "@/lib/paths";
import { cn } from "@/lib/cn";
import {
  colorForGraphCommunity,
  readKiwiGraphTheme,
  type KiwiGraphTheme,
} from "@/lib/kiwiGraphTheme";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

type Props = {
  tree: TreeEntry | null;
  activePath?: string | null;
  // refreshKey lets the parent force a re-fetch after writes so renames and
  // new pages appear in the graph without a full page reload.
  refreshKey?: number;
  onNavigate: (path: string) => void;
  onClose: () => void;
};

function topDir(path: string): string {
  const i = path.indexOf("/");
  return i < 0 ? "(root)" : path.slice(0, i);
}

type Built = {
  graph: Graph;
  dirs: string[];
  tags: string[];
  theme: KiwiGraphTheme;
};

// Build the Graphology instance from the server response plus the file tree.
// Returns null while resolution is still possible but yields no edges — we
// still render an empty graph so the user sees "no links yet" rather than a
// blank screen.
function buildGraph(
  resp: GraphResponse,
  tree: TreeEntry | null,
  theme: KiwiGraphTheme,
): Built {
  const g = new Graph({ type: "undirected", multi: false });
  const resolver = buildResolver(tree);

  const tagSet = new Set<string>();
  for (const n of resp.nodes) {
    g.addNode(n.path, {
      label: titleize(n.path),
      path: n.path,
      dir: topDir(n.path),
      tags: n.tags || [],
      size: 4,
      color: theme.defaultNode,
    });
    if (n.tags) n.tags.forEach((t) => tagSet.add(t));
  }

  const dirSet = new Set<string>();
  for (const n of resp.nodes) dirSet.add(topDir(n.path));

  for (const e of resp.edges) {
    if (!g.hasNode(e.source)) continue;
    const resolved = resolver(e.target);
    if (!resolved || !g.hasNode(resolved)) continue;
    if (resolved === e.source) continue; // skip self-loops
    const edgeKey = g.hasEdge(e.source, resolved)
      ? g.edge(e.source, resolved)
      : g.addEdge(e.source, resolved, { size: 0.8, color: theme.edge });
    void edgeKey;
  }

  // Node size ∝ degree (backlinks + outgoing), clamped so hubs don't dwarf leaves.
  g.forEachNode((node, attrs) => {
    const deg = g.degree(node);
    g.setNodeAttribute(node, "size", Math.max(6, Math.min(22, 6 + Math.sqrt(deg) * 2.5)));
    void attrs;
  });

  // Community detection + colors. Louvain needs at least one edge; otherwise
  // every node gets the fallback color.
  if (g.size > 0) {
    louvain.assign(g, { nodeCommunityAttribute: "community" });
    g.forEachNode((node) => {
      const c = g.getNodeAttribute(node, "community") as number | undefined;
      const idx = typeof c === "number" ? c : 0;
      g.setNodeAttribute(
        node,
        "color",
        colorForGraphCommunity(idx, theme),
      );
    });
  }

  // Initial seed positions — ForceAtlas2 explodes if all nodes share
  // (0,0). The circular layout gives every node a distinct starting
  // point even in tiny graphs.
  circular.assign(g, { scale: 100 });
  // ForceAtlas2 needs ≥3 nodes *and* at least one edge to converge.
  // With 1-2 nodes or a fully disconnected graph it produces NaN
  // positions, which Sigma renders as a blank canvas. Keep the
  // circular seed in those cases — we still want to visualise the
  // pages, we just skip the physics step that can't run.
  if (g.order >= 3 && g.size >= 1) {
    forceAtlas2.assign(g, {
      iterations: 200,
      settings: {
        gravity: 1,
        scalingRatio: 10,
        slowDown: 2,
        barnesHutOptimize: g.order > 200,
        strongGravityMode: false,
      },
    });
  }

  return { graph: g, dirs: Array.from(dirSet).sort(), tags: Array.from(tagSet).sort(), theme };
}

export function KiwiGraph({ tree, activePath, refreshKey, onNavigate, onClose }: Props) {
  const [resp, setResp] = useState<GraphResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [dirFilter, setDirFilter] = useState<string>("");
  const [tagFilter, setTagFilter] = useState<string>("");
  const [query, setQuery] = useState<string>("");
  const [hovered, setHovered] = useState<string | null>(null);
  const [htmlClassEpoch, setHtmlClassEpoch] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setError(null);
    api
      .graph()
      .then((r) => {
        if (!cancelled) setResp(r);
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [refreshKey]);

  useEffect(() => {
    const obs = new MutationObserver(() =>
      setHtmlClassEpoch((n: number) => n + 1),
    );
    obs.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class"],
    });
    return () => obs.disconnect();
  }, []);

  const built = useMemo<Built | null>(() => {
    if (!resp) return null;
    return buildGraph(resp, tree, readKiwiGraphTheme());
  }, [resp, tree, htmlClassEpoch]);

  return (
    <div className="h-full w-full flex flex-col relative">
      <div className="flex items-center gap-3 px-6 py-3 border-b border-border bg-card">
        <Button variant="outline" size="sm" onClick={onClose}>
          <ArrowLeft className="h-3.5 w-3.5" /> Back
        </Button>
        <div className="font-semibold text-sm">Knowledge graph</div>
        <div className="text-xs text-muted-foreground">
          {built
            ? `${built.graph.order} pages · ${built.graph.size} links`
            : null}
        </div>
        <div className="ml-auto flex items-center gap-2">
          <div className="relative">
            <SearchIcon className="h-3.5 w-3.5 absolute left-2 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none" />
            <Input
              type="text"
              placeholder="Highlight…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="h-8 pl-7 w-48 text-sm"
            />
          </div>
          {built && built.dirs.length > 1 && (
            <Select
              value={dirFilter || "__all__"}
              onValueChange={(v) => setDirFilter(v === "__all__" ? "" : v)}
            >
              <SelectTrigger className="h-8 w-44 text-sm">
                <SelectValue placeholder="All folders" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__all__">All folders</SelectItem>
                {built.dirs.map((d) => (
                  <SelectItem key={d} value={d}>
                    {d}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          )}
          {built && built.tags.length > 0 && (
            <Select
              value={tagFilter || "__all__"}
              onValueChange={(v) => setTagFilter(v === "__all__" ? "" : v)}
            >
              <SelectTrigger className="h-8 w-44 text-sm">
                <Tag className="h-3 w-3 mr-1" />
                <SelectValue placeholder="All tags" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__all__">All tags</SelectItem>
                {built.tags.map((t) => (
                  <SelectItem key={t} value={t}>
                    {t}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          )}
        </div>
      </div>

      <div className="flex-1 relative">
        {error && (
          <div className="absolute inset-0 grid place-items-center text-sm text-destructive font-mono">
            {error}
          </div>
        )}
        {!error && !built && (
          <div className="absolute inset-0 grid place-items-center text-sm text-muted-foreground">
            <div className="flex items-center gap-2">
              <Loader2 className="h-4 w-4 animate-spin" /> Building graph…
            </div>
          </div>
        )}
        {built && built.graph.order === 0 && (
          <div className="absolute inset-0 grid place-items-center text-sm text-muted-foreground">
            <div className="max-w-md text-center space-y-2 px-6">
              <div className="text-base font-medium text-foreground">
                No pages yet
              </div>
              <p>
                The graph view shows how your pages connect. Create a few
                markdown files and link between them with{" "}
                <code className="font-mono text-xs px-1 py-0.5 rounded bg-muted">
                  [[page-name]]
                </code>{" "}
                to see your knowledge graph.
              </p>
            </div>
          </div>
        )}
        {built && built.graph.order > 0 && built.graph.size === 0 && (
          <div className="absolute inset-0 grid place-items-center text-sm text-muted-foreground pointer-events-none">
            <div className="max-w-md text-center space-y-2 px-6 bg-card/80 backdrop-blur rounded-lg py-4 border border-border/60">
              <div className="text-base font-medium text-foreground">
                {built.graph.order} page{built.graph.order === 1 ? "" : "s"}, no links yet
              </div>
              <p>
                Add{" "}
                <code className="font-mono text-xs px-1 py-0.5 rounded bg-muted">
                  [[page-name]]
                </code>{" "}
                wikilinks between your pages to see them cluster by topic.
              </p>
            </div>
          </div>
        )}
        {built && built.graph.order > 0 && (
          <SigmaContainer
            key={`${resp ? resp.nodes.length : 0}-${htmlClassEpoch}`}
            graph={built.graph as any}
            className="!bg-background"
            style={{ height: "100%", width: "100%" }}
            settings={{
              renderLabels: true,
              labelColor: { attribute: "color" },
              labelSize: 12,
              labelWeight: "500",
              labelDensity: 0.7,
              labelGridCellSize: 80,
              defaultEdgeColor: built.theme.edge,
              zIndex: true,
            }}
            >
            <GraphInteractions
              onNavigate={onNavigate}
              hovered={hovered}
              setHovered={setHovered}
              query={query.trim().toLowerCase()}
              dirFilter={dirFilter}
              tagFilter={tagFilter}
              activePath={activePath || undefined}
              colors={built.theme}
            />
          </SigmaContainer>
        )}
        {hovered && built && built.graph.hasNode(hovered) && (
          <Card className="absolute bottom-3 left-3 px-3 py-2 text-xs pointer-events-none">
            <div className="font-medium">
              {built.graph.getNodeAttribute(hovered, "label") as string}
            </div>
            <div className="text-muted-foreground font-mono">{hovered}</div>
            <div className="text-muted-foreground mt-1">
              {built.graph.degree(hovered)} connection
              {built.graph.degree(hovered) === 1 ? "" : "s"}
            </div>
          </Card>
        )}
        {built && built.graph.order > 0 && (
          <GraphLegend graph={built.graph} theme={built.theme} />
        )}
        <div
          className={cn(
            "absolute bottom-3 right-3 text-[10px] text-muted-foreground font-mono",
            "pointer-events-none",
          )}
        >
          drag to pan · scroll to zoom
        </div>
      </div>
    </div>
  );
}

// GraphInteractions runs inside <SigmaContainer> so it can use the sigma
// hooks. It owns: click→navigate, hover highlighting, query-based dimming,
// and the directory filter (applied via per-node hidden attribute so the
// force layout stays stable when the filter toggles).
function GraphInteractions({
  onNavigate,
  hovered,
  setHovered,
  query,
  dirFilter,
  tagFilter,
  activePath,
  colors,
}: {
  onNavigate: (path: string) => void;
  hovered: string | null;
  setHovered: (s: string | null) => void;
  query: string;
  dirFilter: string;
  tagFilter: string;
  activePath?: string;
  colors: KiwiGraphTheme;
}) {
  const sigma = useSigma();
  const registerEvents = useRegisterEvents();
  const setSettings = useSetSettings();
  const loadGraph = useLoadGraph();
  const loadedRef = useRef(false);

  // One-time graph handoff: the container already receives the graph, but
  // calling loadGraph makes re-render behaviour explicit and survives HMR.
  useEffect(() => {
    if (loadedRef.current) return;
    loadedRef.current = true;
    const g = sigma.getGraph();
    if (g) loadGraph(g as any);
  }, [loadGraph, sigma]);

  useEffect(() => {
    registerEvents({
      clickNode: (e) => {
        const node = e.node;
        onNavigate(node);
      },
      enterNode: (e) => setHovered(e.node),
      leaveNode: () => setHovered(null),
    });
  }, [registerEvents, onNavigate, setHovered]);

  // Reducers run on every render — they decide how each node/edge is drawn
  // this frame based on current hover/search/filter state. Fast path first:
  // when nothing's selected, return the attrs untouched.
  useEffect(() => {
    const graph = sigma.getGraph();
    const neighbors = new Set<string>();
    if (hovered && graph.hasNode(hovered)) {
      neighbors.add(hovered);
      graph.forEachNeighbor(hovered, (n: string) => neighbors.add(n));
    }

    setSettings({
      nodeReducer: (node, data) => {
        const out: any = { ...data };
        const path = (data as any).path as string;
        const dir = (data as any).dir as string;
        const label = ((data as any).label as string) || "";
        const tags = ((data as any).tags as string[]) || [];
        const tagOut = tagFilter && !tags.includes(tagFilter);
        const filteredOut = (dirFilter && dir !== dirFilter) || tagOut;
        const queryMatch = query
          ? path.toLowerCase().includes(query) || label.toLowerCase().includes(query)
          : true;
        if (filteredOut) {
          out.hidden = true;
          return out;
        }
        if (activePath && node === activePath) {
          out.size = Math.max((out.size as number) || 6, 10);
          out.zIndex = 3;
          out.forceLabel = true;
          out.borderColor = "#ffffff";
          out.borderSize = 2;
        }
        if (hovered) {
          if (!neighbors.has(node)) {
            if (node !== activePath) {
              out.color = colors.nodeDim;
              out.label = "";
              out.zIndex = 0;
            }
          } else {
            out.zIndex = 2;
            out.forceLabel = true;
          }
        } else if (query) {
          if (!queryMatch) {
            out.color = colors.nodeDim;
            out.label = "";
          } else {
            out.forceLabel = true;
            out.zIndex = 2;
          }
        }
        return out;
      },
      edgeReducer: (edge, data) => {
        const out: any = { ...data };
        const g = sigma.getGraph();
        const [s, t] = g.extremities(edge);
        if (dirFilter) {
          const sDir = g.getNodeAttribute(s, "dir") as string;
          const tDir = g.getNodeAttribute(t, "dir") as string;
          if (sDir !== dirFilter && tDir !== dirFilter) {
            out.hidden = true;
            return out;
          }
        }
        if (tagFilter) {
          const sTags = (g.getNodeAttribute(s, "tags") as string[]) || [];
          const tTags = (g.getNodeAttribute(t, "tags") as string[]) || [];
          if (!sTags.includes(tagFilter) && !tTags.includes(tagFilter)) {
            out.hidden = true;
            return out;
          }
        }
        if (hovered) {
          if (s !== hovered && t !== hovered) {
            out.color = colors.edgeGhost;
            out.size = 0.3;
          } else {
            out.color = colors.edgeStrong;
            out.size = 1.5;
            out.zIndex = 1;
          }
        }
        return out;
      },
    });

    sigma.refresh();
  }, [sigma, setSettings, hovered, query, dirFilter, tagFilter, activePath, colors]);

  return null;
}

function GraphLegend({ graph, theme }: { graph: Graph; theme: KiwiGraphTheme }) {
  const communities = new Map<number, { color: string; count: number; dirs: Map<string, number> }>();
  graph.forEachNode((_, attrs) => {
    const c = (attrs as any).community as number | undefined;
    if (c == null) return;
    const dir = (attrs as any).dir as string || "(root)";
    const existing = communities.get(c);
    if (existing) {
      existing.count++;
      existing.dirs.set(dir, (existing.dirs.get(dir) || 0) + 1);
    } else {
      const dirs = new Map<string, number>();
      dirs.set(dir, 1);
      communities.set(c, {
        color: colorForGraphCommunity(c, theme),
        count: 1,
        dirs,
      });
    }
  });
  if (communities.size <= 1) return null;

  const sorted = Array.from(communities.entries()).sort((a, b) => b[1].count - a[1].count);

  return (
    <Card className="absolute top-3 right-3 px-3 py-2 text-xs">
      <div className="text-muted-foreground mb-1.5 font-medium">Communities</div>
      <div className="space-y-1">
        {sorted.map(([idx, { color, count, dirs }]) => {
          const topDir = Array.from(dirs.entries()).sort((a, b) => b[1] - a[1])[0]?.[0] || "unknown";
          return (
            <div key={idx} className="flex items-center gap-2">
              <span
                className="h-2.5 w-2.5 rounded-full shrink-0"
                style={{ background: color }}
              />
              <span className="text-muted-foreground">
                {topDir} ({count})
              </span>
            </div>
          );
        })}
      </div>
    </Card>
  );
}
