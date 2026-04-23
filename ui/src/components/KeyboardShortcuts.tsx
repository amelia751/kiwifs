import { Keyboard } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

type Props = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
};

const MAC = navigator.platform.includes("Mac");
const MOD = MAC ? "⌘" : "Ctrl+";

const shortcuts: { section: string; items: { keys: string; label: string }[] }[] = [
  {
    section: "Navigation",
    items: [
      { keys: `${MOD}K`, label: "Search" },
      { keys: `${MOD}N`, label: "New page" },
      { keys: `${MOD}E`, label: "Toggle editor" },
      { keys: `${MOD}?`, label: "Keyboard shortcuts" },
    ],
  },
  {
    section: "Editor",
    items: [
      { keys: `${MOD}S`, label: "Save (also auto-saves after 2s)" },
      { keys: "/", label: "Slash commands (in editor)" },
      { keys: "Esc", label: "Close overlay / cancel" },
    ],
  },
];

export function KeyboardShortcuts({ open, onOpenChange }: Props) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Keyboard className="h-4 w-4" />
            Keyboard shortcuts
          </DialogTitle>
        </DialogHeader>
        <div className="space-y-4">
          {shortcuts.map((s) => (
            <div key={s.section}>
              <div className="text-xs uppercase tracking-wider text-muted-foreground mb-2">
                {s.section}
              </div>
              <div className="space-y-1.5">
                {s.items.map((item) => (
                  <div
                    key={item.keys}
                    className="flex items-center justify-between text-sm"
                  >
                    <span>{item.label}</span>
                    <kbd className="px-2 py-0.5 rounded border border-border bg-muted font-mono text-xs text-muted-foreground">
                      {item.keys}
                    </kbd>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </DialogContent>
    </Dialog>
  );
}
