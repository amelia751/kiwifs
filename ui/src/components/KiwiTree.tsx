import { useEffect, useRef, useState } from "react";
import { getCurrentSpace } from "../lib/api";
import {
  ChevronRight,
  Copy,
  File,
  FileAxis3D,
  FileImage,
  FileVideo,
  FileAudio,
  FileCode,
  FileArchive,
  Folder,
  FolderOpen,
  Move,
  Plus,
  Trash2,
} from "lucide-react";
import { cn } from "@/lib/cn";
import { api, type TreeEntry } from "@/lib/api";
import { notifyError } from "@/lib/notify";
import { isMarkdown, stem, stripTrailingSlash } from "@/lib/paths";
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuTrigger,
} from "@/components/ui/context-menu";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

type Props = {
  activePath: string | null;
  onSelect: (path: string) => void;
  refreshKey?: number;
  onCreateChild?: (folder: string) => void;
  onDeleted?: () => void;
  onDuplicated?: (newPath: string) => void;
  onMoved?: (newPath: string) => void;
};

type PromptDialog = {
  title: string;
  description: string;
  value: string;
  onConfirm: (value: string) => void;
};

type ConfirmDialog = {
  title: string;
  description: string;
  destructive?: boolean;
  onConfirm: () => void;
};

export function KiwiTree({ activePath, onSelect, refreshKey, onCreateChild, onDeleted, onDuplicated, onMoved }: Props) {
  const [root, setRoot] = useState<TreeEntry | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set([""]));
  const [dropTarget, setDropTarget] = useState<string | null>(null);
  const dragPath = useRef<string | null>(null);
  const [dupOpen, setDupOpen] = useState(false);
  const [dupSource, setDupSource] = useState("");
  const [dupTarget, setDupTarget] = useState("");
  const [dupBusy, setDupBusy] = useState(false);

  const [promptDialog, setPromptDialog] = useState<PromptDialog | null>(null);
  const [promptValue, setPromptValue] = useState("");
  const [alertMessage, setAlertMessage] = useState<string | null>(null);
  const [confirmDialog, setConfirmDialog] = useState<ConfirmDialog | null>(null);

  function openPromptDialog(d: PromptDialog) {
    setPromptValue(d.value);
    setPromptDialog(d);
  }

  function openDupDialog(srcPath: string) {
    setDupSource(srcPath);
    setDupTarget(srcPath.replace(/\.md$/i, "-copy.md"));
    setDupOpen(true);
  }

  function handleDuplicate() {
    let target = dupTarget.trim();
    if (!target) return;
    if (!target.endsWith(".md")) target += ".md";
    setDupBusy(true);
    api.readFile(dupSource).then(({ content }) =>
      api.writeFile(target, content).then(() => {
        setDupOpen(false);
        onDuplicated?.(target);
      })
    ).catch((e) => notifyError(`Failed to duplicate ${dupSource}`, e)).finally(() => setDupBusy(false));
  }

  useEffect(() => {
    api
      .tree("/")
      .then((t) => {
        setRoot(t);
        setError(null);
      })
      .catch((e) => setError(String(e)));
  }, [refreshKey]);

  if (error) {
    return (
      <div className="p-3 text-sm text-destructive font-mono">
        Tree error: {error}
      </div>
    );
  }
  if (!root) {
    return <div className="p-3 text-sm text-muted-foreground">Loading…</div>;
  }

  const toggle = (p: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(p)) next.delete(p);
      else next.add(p);
      return next;
    });
  };

  return (
    <div
      className="p-2 text-sm"
      onDragOver={(e) => {
        e.preventDefault();
        e.dataTransfer.dropEffect = "move";
      }}
      onDrop={(e) => {
        e.preventDefault();
        setDropTarget(null);
        const src = dragPath.current;
        if (!src || !src.includes("/")) return;
        const fileName = src.split("/").pop()!;
        const rootChildren = root?.children || [];
        if (rootChildren.some((c) => c.name === fileName)) {
          setAlertMessage(`A file named "${fileName}" already exists at root.`);
          return;
        }
        api.readFile(src).then(({ content }) =>
          api.writeFile(fileName, content).then(() =>
            api.deleteFile(src).then(() => onMoved?.(fileName))
          )
        ).catch((err) => notifyError(`Failed to move ${src}`, err));
      }}
    >
      {(root.children || []).map((child) => (
        <Node
          key={child.path}
          entry={child}
          depth={0}
          activePath={activePath}
          expanded={expanded}
          onToggle={toggle}
          onSelect={onSelect}
          onCreateChild={onCreateChild}
          onDeleted={onDeleted}
          openDupDialog={openDupDialog}
          onMoved={onMoved}
          dragPath={dragPath}
          dropTarget={dropTarget}
          setDropTarget={setDropTarget}
          openPromptDialog={openPromptDialog}
          openConfirmDialog={setConfirmDialog}
        />
      ))}

      <Dialog open={dupOpen} onOpenChange={setDupOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Duplicate page</DialogTitle>
            <DialogDescription>Enter the path for the new copy.</DialogDescription>
          </DialogHeader>
          <div className="grid gap-2">
            <Label htmlFor="tree-dup-path">New path</Label>
            <Input
              id="tree-dup-path"
              autoFocus
              value={dupTarget}
              onChange={(e) => setDupTarget(e.target.value)}
              className="font-mono"
              onKeyDown={(e) => { if (e.key === "Enter") handleDuplicate(); }}
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDupOpen(false)}>Cancel</Button>
            <Button onClick={handleDuplicate} disabled={dupBusy || !dupTarget.trim()}>
              {dupBusy ? "Duplicating..." : "Duplicate"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={!!promptDialog} onOpenChange={(open) => { if (!open) setPromptDialog(null); }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>{promptDialog?.title}</DialogTitle>
            <DialogDescription>{promptDialog?.description}</DialogDescription>
          </DialogHeader>
          <div className="grid gap-2">
            <Input
              autoFocus
              value={promptValue}
              onChange={(e) => setPromptValue(e.target.value)}
              className="font-mono"
              onKeyDown={(e) => {
                if (e.key === "Enter" && promptValue.trim() && promptDialog) {
                  promptDialog.onConfirm(promptValue.trim());
                  setPromptDialog(null);
                }
              }}
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setPromptDialog(null)}>Cancel</Button>
            <Button
              onClick={() => {
                if (promptValue.trim() && promptDialog) {
                  promptDialog.onConfirm(promptValue.trim());
                  setPromptDialog(null);
                }
              }}
              disabled={!promptValue.trim()}
            >
              Confirm
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={!!alertMessage} onOpenChange={(open) => { if (!open) setAlertMessage(null); }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Conflict</DialogTitle>
            <DialogDescription>{alertMessage}</DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button onClick={() => setAlertMessage(null)}>OK</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={!!confirmDialog} onOpenChange={(open) => { if (!open) setConfirmDialog(null); }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>{confirmDialog?.title}</DialogTitle>
            <DialogDescription>{confirmDialog?.description}</DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmDialog(null)}>Cancel</Button>
            <Button
              variant={confirmDialog?.destructive ? "destructive" : "default"}
              onClick={() => {
                confirmDialog?.onConfirm();
                setConfirmDialog(null);
              }}
            >
              Confirm
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

function Node({
  entry,
  depth,
  activePath,
  expanded,
  onToggle,
  onSelect,
  onCreateChild,
  onDeleted,
  openDupDialog,
  onMoved,
  dragPath,
  dropTarget,
  setDropTarget,
  openPromptDialog,
  openConfirmDialog,
}: {
  entry: TreeEntry;
  depth: number;
  activePath: string | null;
  expanded: Set<string>;
  onToggle: (p: string) => void;
  onSelect: (p: string) => void;
  onCreateChild?: (folder: string) => void;
  onDeleted?: () => void;
  openDupDialog?: (srcPath: string) => void;
  onMoved?: (newPath: string) => void;
  dragPath: React.MutableRefObject<string | null>;
  dropTarget: string | null;
  setDropTarget: (path: string | null) => void;
  openPromptDialog: (d: PromptDialog) => void;
  openConfirmDialog: (d: ConfirmDialog) => void;
}) {
  const path = stripTrailingSlash(entry.path);
  const isOpen = expanded.has(path);
  const isActive = activePath === path;

  if (entry.isDir) {
    return (
      <div>
        <ContextMenu>
          <ContextMenuTrigger asChild>
            <div
              className={cn(
                "group flex items-center gap-1.5 px-2 py-1 rounded-md transition-colors",
                "text-foreground/90 hover:bg-accent hover:text-accent-foreground",
                dropTarget === path && "ring-2 ring-primary bg-primary/10",
              )}
              style={{ paddingLeft: 8 + depth * 12 }}
              onDragOver={(e) => {
                e.preventDefault();
                e.dataTransfer.dropEffect = "move";
                setDropTarget(path);
              }}
              onDragLeave={() => {
                if (dropTarget === path) setDropTarget(null);
              }}
              onDrop={(e) => {
                e.preventDefault();
                setDropTarget(null);
                const src = dragPath.current;
                if (!src || src === path) return;
                const fileName = src.split("/").pop()!;
                const dest = `${path}/${fileName}`;
                if (src === dest) return;
                api.readFile(src).then(({ content }) =>
                  api.writeFile(dest, content).then(() =>
                    api.deleteFile(src).then(() => onMoved?.(dest))
                  )
                ).catch((err) => notifyError(`Failed to move ${src}`, err));
              }}
            >
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  onToggle(path);
                }}
                className="shrink-0 p-0.5"
              >
                <ChevronRight
                  className={cn(
                    "h-3.5 w-3.5 text-muted-foreground transition-transform",
                    isOpen && "rotate-90",
                  )}
                />
              </button>
              <button
                type="button"
                onClick={() => {
                  if (!isOpen) onToggle(path);
                  onSelect(path);
                }}
                className="flex items-center gap-1.5 flex-1 min-w-0 text-left"
              >
                {isOpen ? (
                  <FolderOpen className="h-4 w-4 text-primary shrink-0" />
                ) : (
                  <Folder className="h-4 w-4 text-muted-foreground shrink-0" />
                )}
                <span className="truncate">{entry.name}</span>
              </button>
              {onCreateChild && (
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    onCreateChild(path);
                  }}
                  className="opacity-0 group-hover:opacity-100 h-5 w-5 shrink-0 grid place-items-center rounded hover:bg-background/50 text-muted-foreground hover:text-foreground transition-opacity"
                  title={`New page in ${entry.name}`}
                >
                  <Plus className="h-3 w-3" />
                </button>
              )}
            </div>
          </ContextMenuTrigger>
          <ContextMenuContent>
            <ContextMenuItem onClick={() => onCreateChild?.(path)}>
              <Plus className="h-3.5 w-3.5" />
              New page in {entry.name}
            </ContextMenuItem>
            <ContextMenuItem onClick={() => onSelect(path)}>
              <File className="h-3.5 w-3.5" />
              Open folder
            </ContextMenuItem>
            <ContextMenuSeparator />
            <ContextMenuItem
              onClick={() => {
                openPromptDialog({
                  title: "Rename folder",
                  description: `Rename "${entry.name}" to:`,
                  value: entry.name,
                  onConfirm: (newName) => {
                    if (newName === entry.name) return;
                    const parentDir = path.includes("/") ? path.slice(0, path.lastIndexOf("/")) : "";
                    const newFolder = parentDir ? `${parentDir}/${newName}` : newName;
                    moveFolder(path, newFolder, entry).then(() => onMoved?.(newFolder)).catch((err) => notifyError(`Failed to rename folder ${path}`, err));
                  },
                });
              }}
            >
              <Move className="h-3.5 w-3.5" />
              Rename
            </ContextMenuItem>
            <ContextMenuItem
              onClick={() => {
                openPromptDialog({
                  title: "Move folder",
                  description: "Enter the new path for this folder:",
                  value: path,
                  onConfirm: (newPath) => {
                    if (newPath === path) return;
                    moveFolder(path, newPath.replace(/\/+$/, ""), entry).then(() => onMoved?.(newPath)).catch((err) => notifyError(`Failed to move folder ${path}`, err));
                  },
                });
              }}
            >
              <Move className="h-3.5 w-3.5" />
              Move
            </ContextMenuItem>
            <ContextMenuSeparator />
            <ContextMenuItem
              className="text-destructive focus:text-destructive"
              onClick={() => {
                const files = collectFiles(entry);
                openConfirmDialog({
                  title: "Delete folder",
                  description: `Delete folder "${entry.name}" and its ${files.length} file(s)?`,
                  destructive: true,
                  onConfirm: () => {
                    Promise.all(files.map((f) => api.deleteFile(f)))
                      .then(() => onDeleted?.())
                      .catch((err) => notifyError(`Failed to delete one or more files in "${path}"`, err));
                  },
                });
              }}
            >
              <Trash2 className="h-3.5 w-3.5" />
              Delete folder
            </ContextMenuItem>
          </ContextMenuContent>
        </ContextMenu>
        {isOpen && entry.children && (
          <div>
            {entry.children.map((c) => (
              <Node
                key={c.path}
                entry={c}
                depth={depth + 1}
                activePath={activePath}
                expanded={expanded}
                onToggle={onToggle}
                onSelect={onSelect}
                onCreateChild={onCreateChild}
                onDeleted={onDeleted}
                openDupDialog={openDupDialog}
                onMoved={onMoved}
                dragPath={dragPath}
                dropTarget={dropTarget}
                setDropTarget={setDropTarget}
                openPromptDialog={openPromptDialog}
                openConfirmDialog={openConfirmDialog}
              />
            ))}
          </div>
        )}
      </div>
    );
  }

  if (!isMarkdown(path)) {
    return (
      <a
        href={`/api/kiwi${getCurrentSpace() && getCurrentSpace() !== "default" ? "/" + getCurrentSpace() : ""}/file?path=${encodeURIComponent(path)}`}
        target="_blank"
        rel="noreferrer"
        className={cn(
          "w-full flex items-center gap-1.5 px-2 py-1 rounded-md text-left transition-colors",
          "hover:bg-accent hover:text-accent-foreground",
        )}
        style={{ paddingLeft: 8 + depth * 12 + 14 }}
      >
        <AssetIcon name={entry.name} />
        <span className="truncate">{entry.name}</span>
      </a>
    );
  }

  return (
    <ContextMenu>
      <ContextMenuTrigger asChild>
        <button
          type="button"
          onClick={() => onSelect(path)}
          draggable
          onDragStart={(e) => {
            dragPath.current = path;
            e.dataTransfer.effectAllowed = "move";
            e.dataTransfer.setData("text/plain", path);
          }}
          onDragEnd={() => {
            dragPath.current = null;
            setDropTarget(null);
          }}
          className={cn(
            "w-full flex items-center gap-1.5 px-2 py-1 rounded-md text-left transition-colors",
            "hover:bg-accent hover:text-accent-foreground",
            isActive && "bg-accent text-accent-foreground font-medium",
          )}
          style={{ paddingLeft: 8 + depth * 12 + 14 }}
        >
          <File className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
          <span className="truncate">{stem(entry.name)}</span>
        </button>
      </ContextMenuTrigger>
      <ContextMenuContent>
        <ContextMenuItem onClick={() => onSelect(path)}>
          <File className="h-3.5 w-3.5" />
          Open
        </ContextMenuItem>
        <ContextMenuSeparator />
        <ContextMenuItem onClick={() => openDupDialog?.(path)}>
          <Copy className="h-3.5 w-3.5" />
          Duplicate
        </ContextMenuItem>
        <ContextMenuItem
          onClick={() => {
            openPromptDialog({
              title: "Move / Rename",
              description: "Enter the new path:",
              value: path,
              onConfirm: (newPath) => {
                if (newPath === path) return;
                const finalPath = newPath.endsWith(".md") ? newPath : newPath + ".md";
                api.readFile(path).then(({ content }) =>
                  api.writeFile(finalPath, content).then(() =>
                    api.deleteFile(path).then(() => onMoved?.(finalPath))
                  )
                ).catch((err) => notifyError(`Failed to rename ${path}`, err));
              },
            });
          }}
        >
          <Move className="h-3.5 w-3.5" />
          Move / Rename
        </ContextMenuItem>
        <ContextMenuSeparator />
        <ContextMenuItem
          className="text-destructive focus:text-destructive"
          onClick={() => {
            openConfirmDialog({
              title: "Delete page",
              description: `Delete "${stem(entry.name)}"?`,
              destructive: true,
              onConfirm: () => {
                api
                  .deleteFile(path)
                  .then(() => onDeleted?.())
                  .catch(() => {});
              },
            });
          }}
        >
          <Trash2 className="h-3.5 w-3.5" />
          Delete
        </ContextMenuItem>
      </ContextMenuContent>
    </ContextMenu>
  );
}

function collectFiles(entry: TreeEntry): string[] {
  const out: string[] = [];
  for (const c of entry.children || []) {
    if (c.isDir) out.push(...collectFiles(c));
    else out.push(c.path);
  }
  return out;
}

async function moveFolder(oldPath: string, newPath: string, entry: TreeEntry): Promise<void> {
  const files = collectFiles(entry);
  for (const f of files) {
    const rel = f.slice(oldPath.length);
    const target = newPath + rel;
    const { content } = await api.readFile(f);
    await api.writeFile(target, content);
    await api.deleteFile(f);
  }
}

function AssetIcon({ name }: { name: string }) {
  const ext = name.toLowerCase().split(".").pop() || "";
  const cls = "h-3.5 w-3.5 text-muted-foreground shrink-0";
  if (["png", "jpg", "jpeg", "gif", "webp", "svg", "bmp", "ico"].includes(ext))
    return <FileImage className={cls} />;
  if (["mp4", "mov", "webm", "mkv", "avi"].includes(ext))
    return <FileVideo className={cls} />;
  if (["mp3", "wav", "flac", "ogg", "m4a"].includes(ext))
    return <FileAudio className={cls} />;
  if (["zip", "tar", "gz", "tgz", "7z", "rar"].includes(ext))
    return <FileArchive className={cls} />;
  if (["js", "ts", "tsx", "jsx", "py", "go", "rs", "json", "yaml", "yml", "toml"].includes(ext))
    return <FileCode className={cls} />;
  return <FileAxis3D className={cls} />;
}
