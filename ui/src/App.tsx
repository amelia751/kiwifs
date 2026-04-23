import { useEffect, useRef, useState } from "react";
import { History, Moon, Network, Plus, Search as SearchIcon, Sun } from "lucide-react";
import { KiwiTree } from "./components/KiwiTree";
import { KiwiPage } from "./components/KiwiPage";
import { KiwiEditor } from "./components/KiwiEditor";
import { KiwiSearch } from "./components/KiwiSearch";
import { KiwiGraph } from "./components/KiwiGraph";
import { KiwiHistory } from "./components/KiwiHistory";
import { NewPageDialog } from "./components/NewPageDialog";
import { Button } from "./components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "./components/ui/tooltip";
import { api, type TreeEntry } from "./lib/api";
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
  const [theme, toggleTheme] = useTheme();
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
      } else if (e.key === "Escape") {
        setSearchOpen(false);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // Live updates: subscribe to the server's SSE feed. Any write/delete/bulk
  // or comment change bumps refreshKey, which cascades into tree reloads and
  // page re-fetches so "agents write, humans see live" actually holds.
  useEffect(() => {
    const es = new EventSource("/api/kiwi/events");
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
  }, []);

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
              <ToolbarButton onClick={toggleTheme} label="Toggle theme">
                {theme === "dark" ? (
                  <Sun className="h-4 w-4" />
                ) : (
                  <Moon className="h-4 w-4" />
                )}
              </ToolbarButton>
            </div>
          </header>
          <div className="flex-1 overflow-auto kiwi-scroll">
            <KiwiTree
              activePath={activePath}
              onSelect={navigate}
              refreshKey={refreshKey}
            />
          </div>
        </aside>
        <main className="flex-1 overflow-auto kiwi-scroll">
          {graphOpen ? (
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
