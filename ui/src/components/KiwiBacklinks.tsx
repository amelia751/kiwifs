import { useEffect, useState } from "react";
import { FileText, Link2 } from "lucide-react";
import { api, type BacklinkEntry } from "@/lib/api";
import { titleize } from "@/lib/paths";
import { Separator } from "@/components/ui/separator";

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
    return () => {
      cancelled = true;
    };
  }, [path, refreshKey]);

  if (error) {
    return (
      <div className="mt-10 text-sm text-muted-foreground">
        Backlinks unavailable.
      </div>
    );
  }
  if (!links) return null;
  if (links.length === 0) {
    return (
      <div className="mt-12">
        <Separator className="mb-4" />
        <div className="text-sm text-muted-foreground">
          <div className="flex items-center gap-2 mb-1">
            <Link2 className="h-4 w-4" />
            <span className="font-medium text-foreground">Backlinks</span>
          </div>
          <div>No pages link here yet.</div>
        </div>
      </div>
    );
  }

  return (
    <div className="mt-12">
      <Separator className="mb-4" />
      <div className="text-sm">
        <div className="flex items-center gap-2 mb-3">
          <Link2 className="h-4 w-4" />
          <span className="font-medium">
            Linked from {links.length} page{links.length === 1 ? "" : "s"}
          </span>
        </div>
        <ul className="space-y-1">
          {links.map((l) => (
            <li key={l.path}>
              <button
                type="button"
                onClick={() => onNavigate(l.path)}
                className="flex items-center gap-2 w-full text-left rounded-md px-2 py-1 -mx-2 transition-colors hover:bg-accent hover:text-accent-foreground"
              >
                <FileText className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                <span className="truncate">{titleize(l.path)}</span>
                <span className="text-xs text-muted-foreground ml-auto truncate">
                  {l.path}
                </span>
              </button>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
