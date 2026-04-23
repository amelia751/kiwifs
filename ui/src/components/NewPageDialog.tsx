import { useEffect, useState } from "react";
import { File, Sparkles } from "lucide-react";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
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
import { cn } from "@/lib/cn";

type Template = { name: string; path: string };

type Props = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreated: (path: string) => void;
  defaultFolder?: string;
};

export function NewPageDialog({ open, onOpenChange, onCreated, defaultFolder }: Props) {
  const [path, setPath] = useState("");
  const [templates, setTemplates] = useState<Template[]>([]);
  const [selected, setSelected] = useState<string>("");
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) {
      setPath("");
      setSelected("");
      setError(null);
      return;
    }
    if (defaultFolder) {
      const prefix = defaultFolder.endsWith("/") ? defaultFolder : defaultFolder + "/";
      setPath(prefix);
    }
    api
      .listTemplates()
      .then((r) => setTemplates(r.templates || []))
      .catch(() => setTemplates([]));
  }, [open]);

  async function create() {
    setError(null);
    let p = path.trim();
    if (!p) {
      setError("Path is required.");
      return;
    }
    if (!p.endsWith(".md")) p += ".md";
    setCreating(true);
    try {
      let content = `# ${titleFromPath(p)}\n\n`;
      if (selected) {
        try {
          const tmpl = await api.readTemplate(selected);
          content = tmpl.content;
        } catch (e) {
          console.warn("template fetch failed", e);
        }
      }
      await api.writeFile(p, content);
      onCreated(p);
    } catch (e) {
      setError(String(e));
    } finally {
      setCreating(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Sparkles className="h-4 w-4 text-primary" />
            New page
          </DialogTitle>
          <DialogDescription>
            Create a markdown file at any path under the knowledge root.
          </DialogDescription>
        </DialogHeader>

        <div className="grid gap-4">
          <div className="grid gap-1.5">
            <Label htmlFor="new-page-path">Path</Label>
            <Input
              id="new-page-path"
              autoFocus
              value={path}
              onChange={(e) => setPath(e.target.value)}
              placeholder="concepts/new-topic.md"
              className="font-mono"
              onKeyDown={(e) => {
                if (e.key === "Enter") create();
              }}
            />
          </div>

          <div className="grid gap-1.5">
            <Label>Template</Label>
            <div className="flex flex-col max-h-48 overflow-auto kiwi-scroll border border-border rounded-md">
              <TemplateRow
                label="Blank page"
                active={selected === ""}
                onClick={() => setSelected("")}
              />
              {templates.map((t) => (
                <TemplateRow
                  key={t.name}
                  label={t.name}
                  active={selected === t.name}
                  onClick={() => setSelected(t.name)}
                />
              ))}
            </div>
          </div>

          {error && (
            <div className="text-sm text-destructive font-mono">{error}</div>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={create} disabled={creating}>
            {creating ? "Creating…" : "Create"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function TemplateRow({
  label,
  active,
  onClick,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "flex items-center gap-2 px-3 py-2 text-sm text-left transition-colors",
        "hover:bg-accent hover:text-accent-foreground",
        active && "bg-accent text-accent-foreground font-medium",
      )}
    >
      <File className="h-3.5 w-3.5 text-muted-foreground" />
      <span>{label}</span>
    </button>
  );
}

function titleFromPath(p: string): string {
  const base = p.split("/").pop() || p;
  const stem = base.replace(/\.md$/i, "").replace(/[-_]+/g, " ");
  return stem
    .split(/\s+/)
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : ""))
    .join(" ");
}
