import { lazy, Suspense, useCallback, useEffect, useRef, useState } from "react";
import {
  Activity,
  Check,
  ChevronDown,
  ChevronRight,
  Clock,
  File,
  FileAxis3D,
  History,
  Moon,
  Network,
  Palette,
  PanelLeftClose,
  PanelLeftOpen,
  Pin,
  Plus,
  Search as SearchIcon,
  Star,
  Sun,
} from "lucide-react";
import { KiwiTree } from "./components/KiwiTree";
import { KiwiPage } from "./components/KiwiPage";
import { KiwiSearch } from "./components/KiwiSearch";
import { KiwiToasts } from "./components/KiwiToasts";
import { KiwiFirstRunTour } from "./components/KiwiFirstRunTour";
import { NewPageDialog } from "./components/NewPageDialog";
import { KeyboardShortcuts } from "./components/KeyboardShortcuts";
import { SpaceSelector } from "./components/SpaceSelector";

// Heavy panels open on demand; code-splitting them trims ~1.5 MB off the
// initial bundle and first paint.
const KiwiEditor = lazy(() => import("./components/KiwiEditor").then((m) => ({ default: m.KiwiEditor })));
const KiwiGraph = lazy(() => import("./components/KiwiGraph").then((m) => ({ default: m.KiwiGraph })));
const KiwiHistory = lazy(() => import("./components/KiwiHistory").then((m) => ({ default: m.KiwiHistory })));
const KiwiThemeEditor = lazy(() => import("./components/KiwiThemeEditor").then((m) => ({ default: m.KiwiThemeEditor })));
const KiwiJanitor = lazy(() => import("./components/KiwiJanitor").then((m) => ({ default: m.KiwiJanitor })));

function LazyPanelFallback({ label }: { label: string }) {
  return (
    <div className="p-6 text-xs text-muted-foreground" role="status">
      Loading {label}…
    </div>
  );
}
import { useRecentPages } from "./hooks/useRecentPages";
import { useStarredPages } from "./hooks/useStarredPages";
import { usePinnedPages } from "./hooks/usePinnedPages";
import { titleize } from "./lib/paths";
import { Button } from "./components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "./components/ui/popover";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "./components/ui/tooltip";
import { api, getCurrentSpace, setCurrentSpace, sseUrl, type TreeEntry } from "./lib/api";
import { useTheme } from "./hooks/useTheme";
import { isMarkdown } from "./lib/paths";

export default function App() {
  const [tree, setTree] = useState<TreeEntry | null>(null);
  const [activePath, setActivePath] = useState<string | null>(null);
  const [editing, setEditing] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState<string | undefined>();
  const [newOpen, setNewOpen] = useState(false);
  const [newFolder, setNewFolder] = useState<string | undefined>();
  const [graphOpen, setGraphOpen] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [shortcutsOpen, setShortcutsOpen] = useState(false);
  const [themeEditorOpen, setThemeEditorOpen] = useState(false);
  const [janitorOpen, setJanitorOpen] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(() => {
    try {
      // On phones / narrow tablets we start closed so the editor gets
      // the full viewport width; users can still open the drawer.
      if (typeof window !== "undefined" && window.matchMedia("(max-width: 900px)").matches) {
        return false;
      }
      return localStorage.getItem("kiwifs-sidebar") !== "collapsed";
    } catch { return true; }
  });
  const [isMobile, setIsMobile] = useState(() => {
    try {
      return typeof window !== "undefined" && window.matchMedia("(max-width: 900px)").matches;
    } catch { return false; }
  });
  useEffect(() => {
    if (typeof window === "undefined") return;
    const mql = window.matchMedia("(max-width: 900px)");
    const apply = () => setIsMobile(mql.matches);
    apply();
    mql.addEventListener?.("change", apply);
    return () => mql.removeEventListener?.("change", apply);
  }, []);
  const [sidebarWidth, setSidebarWidth] = useState(() => {
    try {
      const saved = localStorage.getItem("kiwifs-sidebar-width");
      return saved ? Math.max(200, Math.min(480, parseInt(saved, 10))) : 272;
    } catch { return 272; }
  });
  const resizing = useRef(false);
  const { theme, toggleTheme, preset, setPreset, presets: themePresets } = useTheme();
  const [themeLocked, setThemeLocked] = useState(false);
  const currentSpace = getCurrentSpace() || "default";
  const { recent, recordVisit } = useRecentPages(currentSpace);
  const { starred, toggle: toggleStar, isStarred } = useStarredPages(currentSpace);
  const { pinned, toggle: togglePin, isPinned } = usePinnedPages(currentSpace);
  const editorRef = useRef<{ save: () => Promise<void> } | null>(null);
  const stateRef = useRef({ editing, activePath, graphOpen, historyOpen });
  stateRef.current = { editing, activePath, graphOpen, historyOpen };

  useEffect(() => {
    api
      .tree("/")
      .then((t) => setTree(t))
      .catch(() => setTree(null));
  }, [refreshKey]);

  useEffect(() => {
    if (!tree || activePath) return;
    const firstMd = firstMarkdown(tree);
    if (firstMd) setActivePath(firstMd);
  }, [tree, activePath]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const mod = e.metaKey || e.ctrlKey;
      const key = e.key.toLowerCase();
      if (mod && key === "k") {
        e.preventDefault();
        setSearchOpen((v) => !v);
      } else if (mod && key === "n") {
        e.preventDefault();
        setNewFolder(undefined);
        setNewOpen(true);
      } else if (mod && key === "e") {
        const { activePath, graphOpen, historyOpen } = stateRef.current;
        if (!activePath || graphOpen || historyOpen) return;
        e.preventDefault();
        setEditing((v) => !v);
      } else if (mod && key === "s") {
        if (!stateRef.current.editing) return;
        e.preventDefault();
        editorRef.current?.save().catch(() => {});
      } else if (mod && (key === "/" || key === "?")) {
        e.preventDefault();
        setShortcutsOpen((v) => !v);
      } else if (e.key === "Escape") {
        setSearchOpen(false);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const [spaceKey, setSpaceKey] = useState(0);

  useEffect(() => {
    api.getUIConfig().then((c) => setThemeLocked(c.themeLocked)).catch(() => {});
  }, [spaceKey]);

  const handleSpaceSwitch = useCallback(() => {
    setActivePath(null);
    setEditing(false);
    setGraphOpen(false);
    setHistoryOpen(false);
    setSpaceKey((k) => k + 1);
    setRefreshKey((k) => k + 1);
  }, []);

  useEffect(() => {
    const es = new EventSource(sseUrl());
    const bump = () => setRefreshKey((k) => k + 1);
    const events = ["write", "delete", "bulk", "comment.add", "comment.delete"];
    events.forEach((name) => es.addEventListener(name, bump));
    es.onerror = () => {};
    return () => {
      events.forEach((name) => es.removeEventListener(name, bump));
      es.close();
    };
  }, [spaceKey]);

  useEffect(() => {
    const hash = window.location.hash.replace(/^#\/?/, "");
    if (!hash) return;
    const parts = hash.split("/");
    api.listSpaces().then((res) => {
      const names = new Set(res.spaces.map((s) => s.name));
      if (parts.length > 1 && names.has(parts[0])) {
        const space = parts[0];
        const path = parts.slice(1).join("/");
        setCurrentSpace(space === "default" ? null : space);
        if (path) setActivePath(path);
        setSpaceKey((k) => k + 1);
        setRefreshKey((k) => k + 1);
      } else {
        setActivePath(hash);
      }
    }).catch(() => {
      setActivePath(hash);
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!activePath) return;
    const space = getCurrentSpace();
    const frag = space && space !== "default"
      ? `#/${space}/${activePath}`
      : `#/${activePath}`;
    if (window.location.hash !== frag) {
      window.history.replaceState(null, "", frag);
    }
  }, [activePath, spaceKey]);

  function navigate(path: string) {
    if (!path) {
      const firstMd = tree ? firstMarkdown(tree) : null;
      if (firstMd) setActivePath(firstMd);
      return;
    }
    if (!isMarkdown(path)) {
      const folder = findFolder(tree, path);
      const target = folder ? firstMarkdown(folder) : `${path}/index.md`;
      if (target) {
        setActivePath(target);
        setEditing(false);
        recordVisit(target);
      }
      return;
    }
    setActivePath(path);
    setEditing(false);
    setGraphOpen(false);
    setHistoryOpen(false);
    recordVisit(path);
    if (isMobile) setSidebarOpen(false);
  }

  const toggleSidebar = useCallback((open: boolean) => {
    setSidebarOpen(open);
    try { localStorage.setItem("kiwifs-sidebar", open ? "open" : "collapsed"); } catch {}
  }, []);

  return (
    <TooltipProvider delayDuration={250}>
      <KiwiToasts />
      <KiwiFirstRunTour />
      <div className="h-full flex flex-col bg-background text-foreground">
        {/* ── Header: full-width app bar ── */}
        <header className="h-12 shrink-0 border-b border-border bg-card flex items-center px-3 gap-2">
          {/* Left zone: sidebar toggle + logo + space */}
          <div className="flex items-center gap-2 min-w-0">
            <ToolbarButton
              onClick={() => toggleSidebar(!sidebarOpen)}
              label={sidebarOpen ? "Collapse sidebar" : "Expand sidebar"}
            >
              {sidebarOpen
                ? <PanelLeftClose className="h-4 w-4" />
                : <PanelLeftOpen className="h-4 w-4" />}
            </ToolbarButton>
            <div className="flex items-center gap-2">
              <div className="h-7 w-7 rounded-md bg-primary text-primary-foreground grid place-items-center font-bold text-sm shrink-0">
                K
              </div>
              <span className="font-semibold text-sm hidden sm:inline">KiwiFS</span>
            </div>
          </div>

          {/* Center zone: search bar */}
          <div className="flex-1 flex justify-center px-4">
            <button
              type="button"
              onClick={() => setSearchOpen(true)}
              className="flex items-center gap-2 px-3 py-1.5 rounded-md border border-border bg-background hover:bg-accent text-muted-foreground text-sm transition-colors w-full max-w-md"
            >
              <SearchIcon className="h-3.5 w-3.5 shrink-0" />
              <span className="flex-1 text-left truncate">Search pages…</span>
              <kbd className="text-[10px] bg-muted px-1.5 py-0.5 rounded font-mono hidden sm:inline">
                {navigator.platform?.includes("Mac") ? "⌘" : "Ctrl+"}K
              </kbd>
            </button>
          </div>

          {/* Right zone: actions */}
          <div className="flex items-center gap-0.5">
            <ToolbarButton onClick={() => { setNewFolder(undefined); setNewOpen(true); }} label="New page (⌘N)">
              <Plus className="h-4 w-4" />
            </ToolbarButton>
            <ToolbarButton onClick={() => setJanitorOpen(true)} label="Knowledge health">
              <Activity className="h-4 w-4" />
            </ToolbarButton>
            <ToolbarButton onClick={() => setGraphOpen((v) => !v)} label="Knowledge graph">
              <Network className="h-4 w-4" />
            </ToolbarButton>
            <ToolbarButton
              onClick={() => activePath && setHistoryOpen((v) => !v)}
              label="Version history"
            >
              <History className="h-4 w-4" />
            </ToolbarButton>
            {themeLocked ? (
              <ToolbarButton onClick={toggleTheme} label={theme === "dark" ? "Light mode" : "Dark mode"}>
                {theme === "dark" ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
              </ToolbarButton>
            ) : (
              <Popover>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <PopoverTrigger asChild>
                      <Button variant="ghost" size="icon" className="h-8 w-8" aria-label="Theme">
                        <Palette className="h-4 w-4" />
                      </Button>
                    </PopoverTrigger>
                  </TooltipTrigger>
                  <TooltipContent side="bottom">Theme</TooltipContent>
                </Tooltip>
                <PopoverContent align="end" className="w-48 p-1">
                  {themePresets.map((p) => {
                    const swatchColor = theme === "dark"
                      ? (p.dark.primary || p.light.primary || "0 0% 50%")
                      : (p.light.primary || "0 0% 50%");
                    return (
                      <button
                        key={p.name}
                        onClick={() => setPreset(p.name)}
                        className="flex items-center gap-2 w-full rounded-sm px-2 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground"
                      >
                        <span
                          className="h-4 w-4 rounded-full shrink-0 border border-border ring-1 ring-inset ring-white/20"
                          style={{ background: `hsl(${swatchColor})` }}
                        />
                        <span className="flex-1 text-left">{p.name}</span>
                        {preset === p.name && <Check className="h-3.5 w-3.5 text-primary" />}
                      </button>
                    );
                  })}
                  <div className="h-px bg-border my-1" />
                  <button
                    onClick={() => setThemeEditorOpen(true)}
                    className="flex items-center gap-2 w-full rounded-sm px-2 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground text-muted-foreground"
                  >
                    Customize…
                  </button>
                  <div className="h-px bg-border my-1" />
                  <button
                    onClick={toggleTheme}
                    className="flex items-center gap-2 w-full rounded-sm px-2 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground"
                  >
                    {theme === "dark" ? <Sun className="h-3.5 w-3.5" /> : <Moon className="h-3.5 w-3.5" />}
                    <span className="flex-1 text-left">
                      {theme === "dark" ? "Light mode" : "Dark mode"}
                    </span>
                  </button>
                </PopoverContent>
              </Popover>
            )}
          </div>
        </header>

        {/* ── Body: sidebar + content ── */}
        <div className="flex-1 flex overflow-hidden relative">
          {/* Mobile scrim: tap anywhere to close the drawer. */}
          {isMobile && sidebarOpen && (
            <div
              className="absolute inset-0 z-30 bg-background/60 backdrop-blur-sm"
              onClick={() => toggleSidebar(false)}
              aria-hidden="true"
            />
          )}
          {/* Sidebar */}
          <aside
            className={
              (isMobile
                ? "absolute inset-y-0 left-0 z-40 border-r border-border bg-card flex flex-col overflow-hidden shadow-xl"
                : "shrink-0 border-r border-border bg-card flex flex-col overflow-hidden") +
              (resizing.current ? "" : " transition-[width] duration-200")
            }
            style={{ width: sidebarOpen ? (isMobile ? Math.min(sidebarWidth, 320) : sidebarWidth) : 0 }}
            aria-hidden={!sidebarOpen}
          >
            <div className="flex flex-col h-full" style={{ minWidth: isMobile ? Math.min(sidebarWidth, 320) : sidebarWidth }}>
              {/* Space selector */}
              <SpaceSelector onSwitch={handleSpaceSwitch} />

              {/* Sidebar sections */}
              <div className="flex-1 overflow-auto kiwi-scroll">
                {starred.length > 0 && (
                  <SidebarSection icon={<Star className="h-3.5 w-3.5" />} title="Starred" storageKey="starred">
                    {starred.map((p) => (
                      <SidebarPageItem
                        key={p}
                        path={p}
                        active={activePath === p}
                        onSelect={navigate}
                        trailing={
                          <button
                            type="button"
                            onClick={(e) => { e.stopPropagation(); toggleStar(p); }}
                            className="opacity-0 group-hover:opacity-100 text-amber-500"
                          >
                            <Star className="h-3 w-3 fill-current" />
                          </button>
                        }
                      />
                    ))}
                  </SidebarSection>
                )}
                {pinned.length > 0 && (
                  <SidebarSection icon={<Pin className="h-3.5 w-3.5" />} title="Pinned" storageKey="pinned">
                    {pinned.map((p) => (
                      <SidebarPageItem
                        key={p}
                        path={p}
                        active={activePath === p}
                        onSelect={navigate}
                        trailing={
                          <button
                            type="button"
                            onClick={(e) => { e.stopPropagation(); togglePin(p); }}
                            className="opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-foreground"
                          >
                            <Pin className="h-3 w-3 fill-current" />
                          </button>
                        }
                      />
                    ))}
                  </SidebarSection>
                )}
                {recent.length > 0 && (
                  <SidebarSection icon={<Clock className="h-3.5 w-3.5" />} title="Recent" storageKey="recent">
                    {recent.slice(0, 5).map((r) => (
                      <SidebarPageItem
                        key={r.path}
                        path={r.path}
                        active={activePath === r.path}
                        onSelect={navigate}
                      />
                    ))}
                  </SidebarSection>
                )}
                <SidebarSection icon={<FileAxis3D className="h-3.5 w-3.5" />} title="Pages" storageKey="pages" defaultOpen>
                  <KiwiTree
                    activePath={activePath}
                    onSelect={navigate}
                    refreshKey={refreshKey}
                    onCreateChild={(folder) => {
                      setNewFolder(folder);
                      setNewOpen(true);
                    }}
                    onDeleted={() => {
                      setActivePath(null);
                      setRefreshKey((k) => k + 1);
                    }}
                    onDuplicated={(p) => {
                      setRefreshKey((k) => k + 1);
                      navigate(p);
                    }}
                    onMoved={(p) => {
                      setRefreshKey((k) => k + 1);
                      navigate(p);
                    }}
                  />
                </SidebarSection>
              </div>
            </div>
          </aside>

          {/* Sidebar resize handle */}
          {sidebarOpen && (
            <div
              className="w-1 cursor-col-resize hover:bg-primary/30 active:bg-primary/50 transition-colors shrink-0 relative z-10"
              onMouseDown={(e) => {
                e.preventDefault();
                resizing.current = true;
                const startX = e.clientX;
                const startW = sidebarWidth;
                let latestW = startW;
                const onMove = (ev: MouseEvent) => {
                  latestW = Math.max(200, Math.min(480, startW + ev.clientX - startX));
                  setSidebarWidth(latestW);
                };
                const onUp = () => {
                  resizing.current = false;
                  document.removeEventListener("mousemove", onMove);
                  document.removeEventListener("mouseup", onUp);
                  try { localStorage.setItem("kiwifs-sidebar-width", String(latestW)); } catch {}
                };
                document.addEventListener("mousemove", onMove);
                document.addEventListener("mouseup", onUp);
              }}
            />
          )}

          {/* Main content area */}
          <main className="flex-1 overflow-auto kiwi-scroll relative">
            {themeEditorOpen ? (
              <Suspense fallback={<LazyPanelFallback label="theme editor" />}>
                <KiwiThemeEditor
                  onClose={() => setThemeEditorOpen(false)}
                  onPresetReset={() => setPreset(preset)}
                />
              </Suspense>
            ) : graphOpen ? (
              <Suspense fallback={<LazyPanelFallback label="graph" />}>
                <KiwiGraph
                  tree={tree}
                  activePath={activePath}
                  refreshKey={refreshKey}
                  onNavigate={(p) => {
                    setGraphOpen(false);
                    navigate(p);
                  }}
                  onClose={() => setGraphOpen(false)}
                />
              </Suspense>
            ) : historyOpen && activePath ? (
              <Suspense fallback={<LazyPanelFallback label="history" />}>
                <KiwiHistory
                  path={activePath}
                  onClose={() => setHistoryOpen(false)}
                  onRestored={() => setRefreshKey((k) => k + 1)}
                />
              </Suspense>
            ) : editing && activePath ? (
              <Suspense fallback={<LazyPanelFallback label="editor" />}>
                <KiwiEditor
                  path={activePath}
                  tree={tree}
                  saveRef={editorRef}
                  onClose={() => setEditing(false)}
                  onNavigate={navigate}
                  onSaved={() => {
                    setEditing(false);
                    setRefreshKey((k) => k + 1);
                  }}
                />
              </Suspense>
            ) : activePath ? (
              <KiwiPage
                path={activePath}
                tree={tree}
                onNavigate={navigate}
                onEdit={() => setEditing(true)}
                onHistory={() => setHistoryOpen(true)}
                onToggleStar={() => toggleStar(activePath)}
                isStarred={isStarred(activePath)}
                onTogglePin={() => togglePin(activePath)}
                isPinned={isPinned(activePath)}
                onDeleted={() => {
                  setActivePath(null);
                  setRefreshKey((k) => k + 1);
                }}
                onDuplicated={(p) => {
                  setRefreshKey((k) => k + 1);
                  navigate(p);
                }}
                onMoved={(p) => {
                  setRefreshKey((k) => k + 1);
                  navigate(p);
                }}
                onTagClick={(tag) => {
                  setSearchQuery(`tag:${tag}`);
                  setSearchOpen(true);
                }}
                refreshKey={refreshKey}
                onRefresh={() => setRefreshKey((k) => k + 1)}
              />
            ) : (
              <WelcomeScreen
                onNewPage={() => { setNewFolder(undefined); setNewOpen(true); }}
                onSearch={() => setSearchOpen(true)}
                onGraph={() => setGraphOpen(true)}
              />
            )}
          </main>
        </div>
      </div>

      {/* Modals */}
      <KiwiSearch
        open={searchOpen}
        onOpenChange={(open) => {
          setSearchOpen(open);
          if (!open) setSearchQuery(undefined);
        }}
        onSelect={(p) => navigate(p)}
        tree={tree}
        initialQuery={searchQuery}
      />
      <NewPageDialog
        open={newOpen}
        onOpenChange={setNewOpen}
        defaultFolder={newFolder}
        onCreated={(p) => {
          setNewOpen(false);
          setRefreshKey((k) => k + 1);
          setActivePath(p);
          setEditing(true);
        }}
      />
      <KeyboardShortcuts
        open={shortcutsOpen}
        onOpenChange={setShortcutsOpen}
      />
      {janitorOpen && (
        <Suspense fallback={null}>
          <KiwiJanitor
            open={janitorOpen}
            onOpenChange={setJanitorOpen}
            onNavigate={(p) => {
              setJanitorOpen(false);
              navigate(p);
            }}
          />
        </Suspense>
      )}
    </TooltipProvider>
  );
}

/* ── Welcome Screen ── */

function WelcomeScreen({
  onNewPage,
  onSearch,
  onGraph,
}: {
  onNewPage: () => void;
  onSearch: () => void;
  onGraph: () => void;
}) {
  return (
    <div className="grid place-items-center h-full text-muted-foreground">
      <div className="text-center max-w-md">
        <div className="h-16 w-16 mx-auto mb-4 rounded-2xl bg-primary text-primary-foreground grid place-items-center font-bold text-3xl">
          K
        </div>
        <div className="text-2xl font-semibold mb-2 text-foreground">
          Welcome to KiwiFS
        </div>
        <div className="text-sm mb-6">
          Your knowledge base is ready. Get started by creating a page or exploring existing content.
        </div>
        <div className="flex flex-col gap-2 items-center">
          <Button onClick={onNewPage} className="gap-2">
            <Plus className="h-4 w-4" />
            Create your first page
          </Button>
          <Button variant="outline" onClick={onSearch} className="gap-2">
            <SearchIcon className="h-4 w-4" />
            Search pages
            <kbd className="ml-1 text-[10px] bg-muted px-1.5 py-0.5 rounded font-mono">
              {navigator.platform?.includes("Mac") ? "⌘" : "Ctrl+"}K
            </kbd>
          </Button>
          <Button variant="ghost" onClick={onGraph} className="gap-2 text-muted-foreground">
            <Network className="h-4 w-4" />
            View knowledge graph
          </Button>
        </div>
        <div className="mt-8 text-xs space-y-1">
          <div><kbd className="bg-muted px-1.5 py-0.5 rounded font-mono">⌘N</kbd> New page</div>
          <div><kbd className="bg-muted px-1.5 py-0.5 rounded font-mono">⌘E</kbd> Toggle editor</div>
          <div><kbd className="bg-muted px-1.5 py-0.5 rounded font-mono">⌘/</kbd> Keyboard shortcuts</div>
        </div>
      </div>
    </div>
  );
}

/* ── Toolbar Button ── */

function ToolbarButton({
  children,
  label,
  onClick,
}: {
  children: React.ReactNode;
  label: string;
  onClick: () => void;
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button
          variant="ghost"
          size="icon"
          className="h-8 w-8"
          aria-label={label}
          onClick={onClick}
        >
          {children}
        </Button>
      </TooltipTrigger>
      <TooltipContent side="bottom">{label}</TooltipContent>
    </Tooltip>
  );
}

/* ── Sidebar Section ── */

function SidebarSection({
  icon,
  title,
  children,
  storageKey,
  defaultOpen,
}: {
  icon: React.ReactNode;
  title: string;
  children: React.ReactNode;
  storageKey?: string;
  defaultOpen?: boolean;
}) {
  const [collapsed, setCollapsed] = useState(() => {
    if (!storageKey) return false;
    try {
      const stored = localStorage.getItem(`kiwifs-section-${storageKey}`);
      if (stored !== null) return stored === "1";
    } catch {}
    return !defaultOpen;
  });
  return (
    <div className="border-b border-border/50 last:border-b-0">
      <button
        type="button"
        onClick={() => {
          const next = !collapsed;
          setCollapsed(next);
          if (storageKey) {
            try { localStorage.setItem(`kiwifs-section-${storageKey}`, next ? "1" : "0"); } catch {}
          }
        }}
        className="flex items-center gap-1.5 px-3 py-2 text-xs text-muted-foreground uppercase tracking-wider w-full text-left hover:text-foreground hover:bg-accent/50 transition-colors"
      >
        {icon}
        <span className="flex-1">{title}</span>
        {collapsed
          ? <ChevronRight className="h-3 w-3" />
          : <ChevronDown className="h-3 w-3" />}
      </button>
      {!collapsed && <div className="pb-2">{children}</div>}
    </div>
  );
}

/* ── Sidebar Page Item ── */

function SidebarPageItem({
  path,
  active,
  onSelect,
  trailing,
}: {
  path: string;
  active: boolean;
  onSelect: (path: string) => void;
  trailing?: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={() => onSelect(path)}
      className={
        "group w-full flex items-center gap-1.5 px-3 py-1 text-left text-sm transition-colors " +
        "hover:bg-accent hover:text-accent-foreground " +
        (active ? "bg-accent text-accent-foreground font-medium" : "")
      }
    >
      <File className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
      <span className="truncate flex-1">{titleize(path)}</span>
      {trailing}
    </button>
  );
}

/* ── Helpers ── */

function findFolder(t: TreeEntry | null, path: string): TreeEntry | null {
  if (!t) return null;
  const clean = path.replace(/\/+$/, "");
  for (const c of t.children || []) {
    const cp = c.path.replace(/\/+$/, "");
    if (c.isDir && cp === clean) return c;
    if (c.isDir && clean.startsWith(cp + "/")) {
      const inner = findFolder(c, path);
      if (inner) return inner;
    }
  }
  return null;
}

function firstMarkdown(t: TreeEntry): string | null {
  const children = t.children || [];
  const idx = children.find(
    (c) => !c.isDir && c.name.toLowerCase() === "index.md",
  );
  if (idx) return idx.path;
  for (const c of children) {
    if (!c.isDir && c.path.toLowerCase().endsWith(".md")) return c.path;
  }
  for (const c of children) {
    if (c.isDir) {
      const r = firstMarkdown(c);
      if (r) return r;
    }
  }
  return null;
}
