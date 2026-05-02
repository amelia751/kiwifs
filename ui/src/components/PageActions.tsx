import { useState } from "react";
import { Copy, Download, MoreHorizontal, Move, Printer, Trash2 } from "lucide-react";
import { api } from "@/lib/api";
import { stem } from "@/lib/paths";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

type Props = {
  path: string;
  onDeleted?: () => void;
  onDuplicated?: (newPath: string) => void;
  onMoved?: (newPath: string) => void;
};

export function PageActions({ path, onDeleted, onDuplicated, onMoved }: Props) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [moveOpen, setMoveOpen] = useState(false);
  const [dupOpen, setDupOpen] = useState(false);
  const [newPath, setNewPath] = useState("");
  const [dupPath, setDupPath] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleDelete() {
    setBusy(true);
    setError(null);
    try {
      await api.deleteFile(path);
      setDeleteOpen(false);
      onDeleted?.();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  async function handleDuplicate() {
    let target = dupPath.trim();
    if (!target) return;
    if (!target.endsWith(".md")) target += ".md";
    setBusy(true);
    setError(null);
    try {
      const { content } = await api.readFile(path);
      await api.writeFile(target, content);
      setDupOpen(false);
      onDuplicated?.(target);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  async function handleMove() {
    let target = newPath.trim();
    if (!target) return;
    if (!target.endsWith(".md")) target += ".md";
    setBusy(true);
    setError(null);
    try {
      const { content } = await api.readFile(path);
      await api.writeFile(target, content);
      await api.deleteFile(path);
      setMoveOpen(false);
      onMoved?.(target);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  function handleExport() {
    api.readFile(path).then(({ content }) => {
      const blob = new Blob([content], { type: "text/markdown" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = stem(path) + ".md";
      a.click();
      URL.revokeObjectURL(url);
      setMenuOpen(false);
    }).catch(() => {});
  }

  return (
    <>
      <Popover open={menuOpen} onOpenChange={setMenuOpen}>
        <PopoverTrigger asChild>
          <Button variant="ghost" size="icon" className="h-8 w-8" aria-label="More actions">
            <MoreHorizontal className="h-4 w-4" />
          </Button>
        </PopoverTrigger>
        <PopoverContent align="end" className="w-48 p-1">
          <MenuButton
            icon={<Copy className="h-3.5 w-3.5" />}
            label="Duplicate"
            onClick={() => {
              setMenuOpen(false);
              setDupPath(path.replace(/\.md$/i, "-copy.md"));
              setDupOpen(true);
              setError(null);
            }}
          />
          <MenuButton
            icon={<Move className="h-3.5 w-3.5" />}
            label="Move / Rename"
            onClick={() => {
              setMenuOpen(false);
              setNewPath(path);
              setMoveOpen(true);
              setError(null);
            }}
          />
          <MenuButton
            icon={<Download className="h-3.5 w-3.5" />}
            label="Export as Markdown"
            onClick={handleExport}
          />
          <MenuButton
            icon={<Printer className="h-3.5 w-3.5" />}
            label="Print / Save as PDF"
            onClick={() => { setMenuOpen(false); window.print(); }}
          />
          <div className="h-px bg-border my-1" />
          <MenuButton
            icon={<Trash2 className="h-3.5 w-3.5" />}
            label="Delete"
            onClick={() => {
              setMenuOpen(false);
              setDeleteOpen(true);
              setError(null);
            }}
            destructive
          />
        </PopoverContent>
      </Popover>

      <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Delete page</DialogTitle>
            <DialogDescription>
              This will permanently delete <code className="font-mono">{path}</code>. This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          {error && <div className="text-sm text-destructive font-mono">{error}</div>}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteOpen(false)}>
              Cancel
            </Button>
            <Button variant="destructive" onClick={handleDelete} disabled={busy}>
              {busy ? "Deleting..." : "Delete"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={dupOpen} onOpenChange={setDupOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Duplicate page</DialogTitle>
            <DialogDescription>
              Enter the path for the new copy.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-2">
            <Label htmlFor="dup-path">New path</Label>
            <Input
              id="dup-path"
              autoFocus
              value={dupPath}
              onChange={(e) => setDupPath(e.target.value)}
              className="font-mono"
              onKeyDown={(e) => {
                if (e.key === "Enter") handleDuplicate();
              }}
            />
          </div>
          {error && <div className="text-sm text-destructive font-mono">{error}</div>}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDupOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleDuplicate} disabled={busy || !dupPath.trim()}>
              {busy ? "Duplicating..." : "Duplicate"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={moveOpen} onOpenChange={setMoveOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Move / Rename</DialogTitle>
            <DialogDescription>
              Enter the new path for this page.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-2">
            <Label htmlFor="move-path">New path</Label>
            <Input
              id="move-path"
              autoFocus
              value={newPath}
              onChange={(e) => setNewPath(e.target.value)}
              className="font-mono"
              onKeyDown={(e) => {
                if (e.key === "Enter") handleMove();
              }}
            />
          </div>
          {error && <div className="text-sm text-destructive font-mono">{error}</div>}
          <DialogFooter>
            <Button variant="outline" onClick={() => setMoveOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleMove} disabled={busy || !newPath.trim()}>
              {busy ? "Moving..." : "Move"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

function MenuButton({
  icon,
  label,
  onClick,
  destructive,
  disabled,
}: {
  icon: React.ReactNode;
  label: string;
  onClick: () => void;
  destructive?: boolean;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={
        "flex items-center gap-2 w-full rounded-sm px-2 py-1.5 text-sm transition-colors " +
        "hover:bg-accent hover:text-accent-foreground disabled:opacity-50 " +
        (destructive ? "text-destructive" : "")
      }
    >
      {icon}
      {label}
    </button>
  );
}
