import { useEffect, useState } from "react";
import { File, FileText, Loader2 } from "lucide-react";
import { api } from "@/lib/api";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

type Template = { name: string; path: string };

type Props = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSelect: (content: string) => void;
};

export function TemplatePicker({ open, onOpenChange, onSelect }: Props) {
  const [templates, setTemplates] = useState<Template[]>([]);
  const [loading, setLoading] = useState(true);
  const [fetching, setFetching] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setLoading(true);
    api
      .listTemplates()
      .then((r) => setTemplates(r.templates))
      .catch(() => setTemplates([]))
      .finally(() => setLoading(false));
  }, [open]);

  function handleSelect(name: string) {
    if (name === "__blank__") {
      onSelect("");
      onOpenChange(false);
      return;
    }
    setFetching(name);
    api
      .readTemplate(name)
      .then((r) => {
        onSelect(r.content);
        onOpenChange(false);
      })
      .catch(() => {})
      .finally(() => setFetching(null));
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Choose a template</DialogTitle>
        </DialogHeader>
        <div className="space-y-1">
          <button
            type="button"
            onClick={() => handleSelect("__blank__")}
            className="w-full flex items-center gap-3 px-3 py-2.5 rounded-md text-left hover:bg-accent hover:text-accent-foreground transition-colors"
          >
            <File className="h-5 w-5 text-muted-foreground shrink-0" />
            <div>
              <div className="text-sm font-medium">Blank page</div>
              <div className="text-xs text-muted-foreground">Start from scratch</div>
            </div>
          </button>
          {loading ? (
            <div className="flex items-center gap-2 px-3 py-4 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Loading templates...
            </div>
          ) : (
            templates.map((t) => (
              <button
                key={t.name}
                type="button"
                onClick={() => handleSelect(t.name)}
                disabled={fetching === t.name}
                className="w-full flex items-center gap-3 px-3 py-2.5 rounded-md text-left hover:bg-accent hover:text-accent-foreground transition-colors disabled:opacity-50"
              >
                <FileText className="h-5 w-5 text-primary shrink-0" />
                <div>
                  <div className="text-sm font-medium">{t.name}</div>
                  <div className="text-xs text-muted-foreground truncate">{t.path}</div>
                </div>
                {fetching === t.name && <Loader2 className="h-4 w-4 animate-spin ml-auto" />}
              </button>
            ))
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
