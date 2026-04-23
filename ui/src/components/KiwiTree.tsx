import { useEffect, useState } from "react";
import { getCurrentSpace } from "../lib/api";
import {
  ChevronRight,
  FileText,
  File,
  FileImage,
  FileVideo,
  FileAudio,
  FileCode,
  FileArchive,
  Folder,
  FolderOpen,
} from "lucide-react";
import { cn } from "@/lib/cn";
import { api, type TreeEntry } from "@/lib/api";
import { isMarkdown, stripTrailingSlash } from "@/lib/paths";

type Props = {
  activePath: string | null;
  onSelect: (path: string) => void;
  refreshKey?: number;
};

// Lightweight custom tree: react-complex-tree brings styling + keyboard handling
// we don't need at this scale, and writing ~100 lines keeps the bundle tiny.
export function KiwiTree({ activePath, onSelect, refreshKey }: Props) {
  const [root, setRoot] = useState<TreeEntry | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set([""]));

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
    <div className="p-2 text-sm">
      {(root.children || []).map((child) => (
        <Node
          key={child.path}
          entry={child}
          depth={0}
          activePath={activePath}
          expanded={expanded}
          onToggle={toggle}
          onSelect={onSelect}
        />
      ))}
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
}: {
  entry: TreeEntry;
  depth: number;
  activePath: string | null;
  expanded: Set<string>;
  onToggle: (p: string) => void;
  onSelect: (p: string) => void;
}) {
  const path = stripTrailingSlash(entry.path);
  const isOpen = expanded.has(path);
  const isActive = activePath === path;

  if (entry.isDir) {
    return (
      <div>
        <button
          type="button"
          onClick={() => onToggle(path)}
          className={cn(
            "group w-full flex items-center gap-1.5 px-2 py-1 rounded-md text-left transition-colors",
            "text-foreground/90 hover:bg-accent hover:text-accent-foreground",
          )}
          style={{ paddingLeft: 8 + depth * 12 }}
        >
          <ChevronRight
            className={cn(
              "h-3.5 w-3.5 text-muted-foreground shrink-0 transition-transform",
              isOpen && "rotate-90",
            )}
          />
          {isOpen ? (
            <FolderOpen className="h-4 w-4 text-primary shrink-0" />
          ) : (
            <Folder className="h-4 w-4 text-muted-foreground shrink-0" />
          )}
          <span className="truncate">{entry.name}</span>
        </button>
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
              />
            ))}
          </div>
        )}
      </div>
    );
  }

  // Non-markdown entries are assets (images, PDFs, etc.) — they open as raw
  // downloads/previews rather than Kiwi pages. A plain `<a>` lets the browser
  // decide (inline for images, attachment for unknown MIMEs) and avoids
  // wiring a second navigation path through the React tree.
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
    <button
      type="button"
      onClick={() => onSelect(path)}
      className={cn(
        "w-full flex items-center gap-1.5 px-2 py-1 rounded-md text-left transition-colors",
        "hover:bg-accent hover:text-accent-foreground",
        isActive && "bg-accent text-accent-foreground font-medium",
      )}
      style={{ paddingLeft: 8 + depth * 12 + 14 }}
    >
      <FileText className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
      <span className="truncate">{entry.name}</span>
    </button>
  );
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
  return <File className={cls} />;
}
