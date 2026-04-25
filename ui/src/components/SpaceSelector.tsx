import { useCallback, useEffect, useState } from "react";
import { Layers, Plus } from "lucide-react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from "./ui/select";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "./ui/dialog";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import {
  api,
  getCurrentSpace,
  setCurrentSpace,
  type SpaceMeta,
} from "../lib/api";

const NEW_SPACE_SENTINEL = "__new__";

export function SpaceSelector({
  onSwitch,
}: {
  onSwitch: () => void;
}) {
  const [spaces, setSpaces] = useState<SpaceMeta[]>([]);
  const [value, setValue] = useState(getCurrentSpace() || "default");
  const [loaded, setLoaded] = useState(false);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [newName, setNewName] = useState("");
  const [creating, setCreating] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

  const load = useCallback(() => {
    setLoadError(null);
    api
      .listSpaces()
      .then((res) => {
        setSpaces(res.spaces);
        setLoaded(true);
        if (!getCurrentSpace() && res.spaces.length > 0) {
          setValue(res.spaces[0].name);
        }
      })
      .catch((e) => {
        setLoaded(true);
        setLoadError(e instanceof Error ? e.message : String(e));
      });
  }, []);

  useEffect(load, [load]);

  const handleChange = useCallback(
    (name: string) => {
      if (name === NEW_SPACE_SENTINEL) {
        setDialogOpen(true);
        return;
      }
      setValue(name);
      setCurrentSpace(name === "default" ? null : name);
      onSwitch();
    },
    [onSwitch]
  );

  const handleCreate = useCallback(async () => {
    if (!newName.trim()) return;
    setCreating(true);
    try {
      await api.createSpace(newName.trim(), "");
      setDialogOpen(false);
      setNewName("");
      load();
      setValue(newName.trim());
      setCurrentSpace(newName.trim());
      onSwitch();
    } catch (e) {
      setErrorMsg(e instanceof Error ? e.message : "Failed to create space");
    } finally {
      setCreating(false);
    }
  }, [newName, onSwitch, load]);

  if (!loaded) return null;

  if (loadError) {
    return (
      <div className="px-3 py-2 border-b border-border text-[11px] text-amber-600 dark:text-amber-400 flex items-center gap-1.5">
        <Layers className="h-3 w-3" />
        <span className="truncate" title={loadError}>Spaces unavailable</span>
        <button
          type="button"
          onClick={load}
          className="ml-auto underline underline-offset-2 hover:text-foreground"
        >
          retry
        </button>
      </div>
    );
  }

  return (
    <>
      <div className="px-3 py-2 border-b border-border">
        <Select value={value} onValueChange={handleChange}>
          <SelectTrigger className="h-8 text-xs">
            <Layers className="h-3.5 w-3.5 mr-1.5 shrink-0 opacity-60" />
            <SelectValue placeholder="Space" />
          </SelectTrigger>
          <SelectContent>
            {spaces.map((s) => (
              <SelectItem key={s.name} value={s.name}>
                <div className="flex items-center justify-between w-full gap-3">
                  <span>{s.name}</span>
                  <span className="text-muted-foreground text-[11px] tabular-nums">
                    {s.fileCount} files
                    {s.lastModified && (
                      <> · {new Date(s.lastModified).toLocaleDateString()}</>
                    )}
                  </span>
                </div>
              </SelectItem>
            ))}
            <SelectSeparator />
            <SelectItem value={NEW_SPACE_SENTINEL}>
              <div className="flex items-center gap-1.5 text-muted-foreground">
                <Plus className="h-3.5 w-3.5" />
                <span>New Space</span>
              </div>
            </SelectItem>
          </SelectContent>
        </Select>
      </div>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="sm:max-w-sm">
          <DialogHeader>
            <DialogTitle>Create New Space</DialogTitle>
          </DialogHeader>
          <div className="grid gap-3 py-2">
            <div className="grid gap-1.5">
              <Label htmlFor="space-name">Space name</Label>
              <Input
                id="space-name"
                placeholder="e.g. team-docs"
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleCreate()}
                autoFocus
              />
            </div>
          </div>
          <div className="flex justify-end gap-2">
            <Button
              variant="ghost"
              onClick={() => setDialogOpen(false)}
            >
              Cancel
            </Button>
            <Button
              onClick={handleCreate}
              disabled={!newName.trim() || creating}
            >
              {creating ? "Creating..." : "Create"}
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={errorMsg !== null} onOpenChange={(open) => { if (!open) setErrorMsg(null); }}>
        <DialogContent className="sm:max-w-sm">
          <DialogHeader>
            <DialogTitle>Error</DialogTitle>
          </DialogHeader>
          <p className="text-sm text-destructive">{errorMsg}</p>
          <div className="flex justify-end">
            <Button variant="ghost" onClick={() => setErrorMsg(null)}>
              OK
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}
