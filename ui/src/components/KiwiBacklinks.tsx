import { useEffect, useState } from "react";
import { File } from "lucide-react";
import { api, type BacklinkEntry } from "@/lib/api";
import { titleize } from "@/lib/paths";

type Props = {
  path: string;
  onNavigate: (path: string) => void;
  refreshKey?: number;
};

export function KiwiBacklinks({ path, onNavigate, refreshKey }: Props) {
  const [links, setLinks] = useState<BacklinkEntry[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLinks(null);
    setError(null);
    api
      .backlinks(path)
      .then((r) => {
        if (!cancelled) setLinks(r.backlinks);
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => { cancelled = true; };
  }, [path, refreshKey]);

  if (error) {
    return <div className="text-sm text-muted-foreground">Backlinks unavailable.</div>;
  }
  if (!links || links.length === 0) {
    return <div className="text-sm text-muted-foreground">No pages link to this page.</div>;
  }

  return (
    <ul className="space-y-1">
      {links.map((l) => (
        <li key={l.path}>
          <button
            type="button"
            onClick={() => onNavigate(l.path)}
            className="flex items-center gap-2 w-full text-left rounded-md px-2 py-1 -mx-2 transition-colors hover:bg-accent hover:text-accent-foreground text-sm"
          >
            <File className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
            <span className="truncate">{titleize(l.path)}</span>
            <span className="text-xs text-muted-foreground ml-auto truncate">{l.path}</span>
          </button>
        </li>
      ))}
    </ul>
  );
}
