import { useCallback, useEffect, useRef, useState } from "react";
import { Check, History, Keyboard, Moon, Network, Palette, Plus, Search as SearchIcon, Sun } from "lucide-react";
import { KiwiTree } from "./components/KiwiTree";
import { KiwiPage } from "./components/KiwiPage";
import { KiwiEditor } from "./components/KiwiEditor";
import { KiwiSearch } from "./components/KiwiSearch";
import { KiwiGraph } from "./components/KiwiGraph";
import { KiwiHistory } from "./components/KiwiHistory";
import { KiwiThemeEditor } from "./components/KiwiThemeEditor";
import { NewPageDialog } from "./components/NewPageDialog";
import { KeyboardShortcuts } from "./components/KeyboardShortcuts";
import { SpaceSelector } from "./components/SpaceSelector";
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
  const [newOpen, setNewOpen] = useState(false);
  const [graphOpen, setGraphOpen] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [shortcutsOpen, setShortcutsOpen] = useState(false);
  const [themeEditorOpen, setThemeEditorOpen] = useState(false);
  const { theme, toggleTheme, preset, setPreset, presets: themePresets } = useTheme();
  // Keep the latest values accessible to the global keydown listener without
  // retearing it on every state change — otherwise editors remount on each key.
  const editorRef = useRef<{ save: () => Promise<void> } | null>(null);
  const stateRef = useRef({ editing, activePath, graphOpen, historyOpen });
  stateRef.current = { editing, activePath, graphOpen, historyOpen };

  // Load tree; keep a copy in state so the wiki-link resolver can use it.
  useEffect(() => {
    api
      .tree("/")
      .then((t) => setTree(t))
      .catch(() => setTree(null));
  }, [refreshKey]);

  // Pick first .md file on first load as a reasonable starting point.
  useEffect(() => {
    if (!tree || activePath) return;
    const firstMd = firstMarkdown(tree);
    if (firstMd) setActivePath(firstMd);
  }, [tree, activePath]);

  // Global keyboard: Cmd/Ctrl+K for search, Cmd/Ctrl+N for new page,
  // Cmd/Ctrl+E to toggle edit, Cmd/Ctrl+S to save while editing.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const mod = e.metaKey || e.ctrlKey;
      const key = e.key.toLowerCase();
      if (mod && key === "k") {
        e.preventDefault();
        setSearchOpen((v) => !v);
      } else if (mod && key === "n") {
        e.preventDefault();
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

  // spaceKey bumps on space switch so all data-fetching effects re-run
  const [spaceKey, setSpaceKey] = useState(0);
  const handleSpaceSwitch = useCallback(() => {
    setActivePath(null);
    setEditing(false);
    setGraphOpen(false);
    setHistoryOpen(false);
    setSpaceKey((k) => k + 1);
    setRefreshKey((k) => k + 1);
  }, []);

  // Live updates: subscribe to the server's SSE feed. Any write/delete/bulk
  // or comment change bumps refreshKey, which cascades into tree reloads and
  // page re-fetches so "agents write, humans see live" actually holds.
  // Re-subscribes on space switch so events are scoped to the active space.
  useEffect(() => {
    const es = new EventSource(sseUrl());
    const bump = () => setRefreshKey((k) => k + 1);
    const events = [
      "write",
      "delete",
      "bulk",
      "comment.add",
      "comment.delete",
    ];
    events.forEach((name) => es.addEventListener(name, bump));
    es.onerror = () => {
      // Browsers auto-reconnect; swallow transient errors silently.
    };
    return () => {
      events.forEach((name) => es.removeEventListener(name, bump));
      es.close();
    };
  }, [spaceKey]);

  // URL sync: encode space + page in hash for bookmarkable URLs.
  // Format: #/{space}/{path} or #/{path} for the default space.
  useEffect(() => {
    const hash = window.location.hash.replace(/^#\/?/, "");
    if (!hash) return;
    const parts = hash.split("/");
    // Try the first segment as a space name by peeking at available spaces.
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

  // Push state to hash on navigation.
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
    // Breadcrumb root → first markdown file in tree.
    if (!path) {
      const firstMd = tree ? firstMarkdown(tree) : null;
      if (firstMd) setActivePath(firstMd);
      return;
    }
    if (!isMarkdown(path)) {
      const idx = `${path}/index.md`;
      setActivePath(idx);
      setEditing(false);
      return;
    }
    setActivePath(path);
    setEditing(false);
  }

  return (
    <TooltipProvider delayDuration={250}>
      <div className="h-full flex bg-background text-foreground">
        <aside className="w-72 shrink-0 border-r border-border bg-card flex flex-col">
          <header className="p-3 border-b border-border flex items-center gap-2">
            <div className="h-7 w-7 rounded-md bg-primary text-primary-foreground grid place-items-center font-bold text-sm">
              K
            </div>
            <div className="font-semibold text-sm">KiwiFS</div>
            <div className="ml-auto flex items-center gap-0.5">
              <ToolbarButton onClick={() => setNewOpen(true)} label="New page (⌘N)">
                <Plus className="h-4 w-4" />
              </ToolbarButton>
              <ToolbarButton onClick={() => setSearchOpen(true)} label="Search (⌘K)">
                <SearchIcon className="h-4 w-4" />
              </ToolbarButton>
              <ToolbarButton onClick={() => setGraphOpen(true)} label="Knowledge graph">
                <Network className="h-4 w-4" />
              </ToolbarButton>
              <ToolbarButton
                onClick={() => activePath && setHistoryOpen(true)}
                label="Version history"
              >
                <History className="h-4 w-4" />
              </ToolbarButton>
              <Popover>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <PopoverTrigger asChild>
                      <Button variant="ghost" size="icon" aria-label="Theme preset">
                        <Palette className="h-4 w-4" />
                      </Button>
                    </PopoverTrigger>
                  </TooltipTrigger>
                  <TooltipContent side="bottom">Theme preset</TooltipContent>
                </Tooltip>
                <PopoverContent align="end" className="w-48 p-1">
                  {themePresets.map((p) => (
                    <button
                      key={p.name}
                      onClick={() => setPreset(p.name)}
                      className="flex items-center gap-2 w-full rounded-sm px-2 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground"
                    >
                      <span
                        className="h-3 w-3 rounded-full shrink-0 border border-border"
                        style={{ background: `hsl(${p.light.primary || "0 0% 50%"})` }}
                      />
                      <span className="flex-1 text-left">{p.name}</span>
                      {preset === p.name && <Check className="h-3.5 w-3.5 text-primary" />}
                    </button>
                  ))}
                  <div className="h-px bg-border my-1" />
                  <button
                    onClick={() => setThemeEditorOpen(true)}
                    className="flex items-center gap-2 w-full rounded-sm px-2 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground text-muted-foreground"
                  >
                    Customize...
                  </button>
                </PopoverContent>
              </Popover>
              <ToolbarButton onClick={() => setShortcutsOpen(true)} label="Keyboard shortcuts (⌘?)">
                <Keyboard className="h-4 w-4" />
              </ToolbarButton>
              <ToolbarButton onClick={toggleTheme} label="Toggle theme">
                {theme === "dark" ? (
                  <Sun className="h-4 w-4" />
                ) : (
                  <Moon className="h-4 w-4" />
                )}
              </ToolbarButton>
            </div>
          </header>
          <SpaceSelector onSwitch={handleSpaceSwitch} />
          <div className="flex-1 overflow-auto kiwi-scroll">
            <KiwiTree
              activePath={activePath}
              onSelect={navigate}
              refreshKey={refreshKey}
            />
          </div>
        </aside>
        <main className="flex-1 overflow-auto kiwi-scroll">
          {themeEditorOpen ? (
            <KiwiThemeEditor
              onClose={() => setThemeEditorOpen(false)}
              onPresetReset={() => setPreset(preset)}
            />
          ) : graphOpen ? (
            <KiwiGraph
              tree={tree}
              onNavigate={(p) => {
                setGraphOpen(false);
                navigate(p);
              }}
              onClose={() => setGraphOpen(false)}
            />
          ) : historyOpen && activePath ? (
            <KiwiHistory
              path={activePath}
              onClose={() => setHistoryOpen(false)}
              onRestored={() => setRefreshKey((k) => k + 1)}
            />
          ) : editing && activePath ? (
            <KiwiEditor
              path={activePath}
              saveRef={editorRef}
              onClose={() => setEditing(false)}
              onSaved={() => {
                setEditing(false);
                setRefreshKey((k) => k + 1);
              }}
            />
          ) : activePath ? (
            <KiwiPage
              path={activePath}
              tree={tree}
              onNavigate={navigate}
              onEdit={() => setEditing(true)}
              onHistory={() => setHistoryOpen(true)}
              refreshKey={refreshKey}
            />
          ) : (
            <div className="grid place-items-center h-full text-muted-foreground">
              <div className="text-center">
                <div className="text-2xl font-semibold mb-2 text-foreground">
                  KiwiFS
                </div>
                <div className="text-sm">
                  Open a page from the sidebar or press ⌘K to search.
                </div>
              </div>
            </div>
          )}
        </main>
        <KiwiSearch
          open={searchOpen}
          onOpenChange={setSearchOpen}
          onSelect={(p) => navigate(p)}
        />
        <NewPageDialog
          open={newOpen}
          onOpenChange={setNewOpen}
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
      </div>
    </TooltipProvider>
  );
}

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

function firstMarkdown(t: TreeEntry): string | null {
  if (!t.isDir && t.path.toLowerCase().endsWith(".md")) return t.path;
  for (const c of t.children || []) {
    const r = firstMarkdown(c);
    if (r) return r;
  }
  return null;
}
