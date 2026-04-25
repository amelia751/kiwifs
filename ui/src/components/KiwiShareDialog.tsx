import { useEffect, useState } from "react";
import {
  Check,
  Copy,
  Eye,
  Link2,
  Loader2,
  Plus,
  Trash2,
} from "lucide-react";
import { api, type ShareLink } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

type Props = {
  path: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
};

const EXPIRY_OPTIONS = [
  { label: "1 hour", value: "1h" },
  { label: "24 hours", value: "24h" },
  { label: "7 days", value: "7d" },
  { label: "30 days", value: "30d" },
  { label: "Never", value: "never" },
] as const;

// formatExpiry renders a friendly countdown. The UI re-runs this every
// ~15 s via a state-bumping effect so "2h left" visibly ticks down to
// "1h 59m left" → … → "Expired" without the user reloading.
function formatExpiry(expiresAt?: string, now: number = Date.now()): string {
  if (!expiresAt) return "Never";
  try {
    const d = new Date(expiresAt);
    const diff = d.getTime() - now;
    if (diff <= 0) return "Expired";
    if (diff < 60_000) return `${Math.ceil(diff / 1000)}s left`;
    if (diff < 3600_000) return `${Math.ceil(diff / 60_000)}m left`;
    if (diff < 86400_000) {
      const h = Math.floor(diff / 3600_000);
      const m = Math.floor((diff % 3600_000) / 60_000);
      return m > 0 ? `${h}h ${m}m left` : `${h}h left`;
    }
    const days = Math.floor(diff / 86400_000);
    const hours = Math.floor((diff % 86400_000) / 3600_000);
    return hours > 0 ? `${days}d ${hours}h left` : `${days}d left`;
  } catch {
    return expiresAt;
  }
}

function isExpired(expiresAt?: string, now: number = Date.now()): boolean {
  if (!expiresAt) return false;
  const d = new Date(expiresAt);
  return d.getTime() <= now;
}

export function KiwiShareDialog({ path, open, onOpenChange }: Props) {
  const [links, setLinks] = useState<ShareLink[]>([]);
  const [creating, setCreating] = useState(false);
  const [newExpiry, setNewExpiry] = useState("7d");
  const [newPassword, setNewPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [now, setNow] = useState(Date.now());

  async function reload() {
    setLoading(true);
    setError(null);
    try {
      const r = await api.listShareLinks(path);
      setLinks(Array.isArray(r) ? r : []);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!open) return;
    reload();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, path]);

  // Tick the "now" clock so the countdown updates. Every 15 s is enough
  // for minute-level granularity without waking the tab excessively.
  useEffect(() => {
    if (!open) return;
    const id = setInterval(() => setNow(Date.now()), 15_000);
    return () => clearInterval(id);
  }, [open]);

  // Auto-reload when any link in the list has just crossed its expiry
  // boundary — the server will have already flipped them to "revoked"
  // so the UI should stop showing stale green badges.
  useEffect(() => {
    if (!open) return;
    const expiredId = links.find(
      (l) => l.expiresAt && isExpired(l.expiresAt, now),
    )?.id;
    if (expiredId) {
      void reload();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [now, open]);

  async function handleCreate() {
    setCreating(true);
    setError(null);
    try {
      const link = await api.createShareLink(
        path,
        newExpiry === "never" ? undefined : newExpiry,
        newPassword || undefined,
      );
      setLinks((prev) => [link, ...prev]);
      setNewPassword("");
    } catch (e) {
      setError(String(e));
    } finally {
      setCreating(false);
    }
  }

  async function handleRevoke(id: string) {
    try {
      await api.revokeShareLink(id);
      setLinks((prev) => prev.filter((l) => l.id !== id));
    } catch (e) {
      setError(String(e));
    }
  }

  function handleCopy(link: ShareLink) {
    const url = api.publicShareUrl(link.token);
    // navigator.clipboard is only available on secure origins (https/localhost);
    // fall back to a hidden <textarea> + execCommand so self-hosted deploys
    // served over plain HTTP still let users grab the URL.
    const markCopied = () => {
      setCopied(link.id);
      setTimeout(() => setCopied(null), 2000);
    };
    const fallback = () => {
      try {
        const ta = document.createElement("textarea");
        ta.value = url;
        ta.style.position = "fixed";
        ta.style.opacity = "0";
        document.body.appendChild(ta);
        ta.select();
        const ok = document.execCommand("copy");
        document.body.removeChild(ta);
        if (ok) markCopied();
        else setError("Couldn't copy automatically — URL: " + url);
      } catch {
        setError("Couldn't copy automatically — URL: " + url);
      }
    };
    if (navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(url).then(markCopied).catch(fallback);
    } else {
      fallback();
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Link2 className="h-5 w-5" />
            Share Page
          </DialogTitle>
        </DialogHeader>

        {/* Create section */}
        <div className="space-y-3 pb-3 border-b border-border">
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1.5">
              <Label className="text-xs">Expires</Label>
              <Select value={newExpiry} onValueChange={setNewExpiry}>
                <SelectTrigger className="h-8 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {EXPIRY_OPTIONS.map((o) => (
                    <SelectItem key={o.value} value={o.value}>
                      {o.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1.5">
              <Label className="text-xs">Password (optional)</Label>
              <Input
                type="password"
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
                placeholder="••••"
                className="h-8 text-xs"
              />
            </div>
          </div>
          <Button
            size="sm"
            className="w-full gap-1"
            onClick={handleCreate}
            disabled={creating}
          >
            {creating ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <Plus className="h-3.5 w-3.5" />
            )}
            Create Link
          </Button>
        </div>

        {error && (
          <div className="text-sm text-destructive font-mono">{error}</div>
        )}

        {/* Existing links */}
        <div className="space-y-2 max-h-64 overflow-auto">
          {loading ? (
            <div className="flex justify-center py-6">
              <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
            </div>
          ) : links.length === 0 ? (
            <div className="text-sm text-muted-foreground text-center py-6">
              No share links yet
            </div>
          ) : (
            links.map((link) => (
              <div
                key={link.id}
                className="flex items-center gap-2 rounded-lg border border-border px-3 py-2"
              >
                <div className="flex-1 min-w-0">
                  <div className="text-xs font-mono truncate">
                    {link.token.slice(0, 12)}…
                  </div>
                  <div className="flex items-center gap-2 mt-0.5 text-[10px] text-muted-foreground">
                    <span
                      className={
                        isExpired(link.expiresAt, now)
                          ? "text-destructive"
                          : ""
                      }
                    >
                      {formatExpiry(link.expiresAt, now)}
                    </span>
                    <span className="flex items-center gap-0.5">
                      <Eye className="h-2.5 w-2.5" />
                      {link.viewCount}
                    </span>
                    {link.password && (
                      <Badge
                        variant="outline"
                        className="text-[9px] px-1 py-0"
                      >
                        password
                      </Badge>
                    )}
                  </div>
                </div>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7 shrink-0"
                  onClick={() => handleCopy(link)}
                  aria-label="Copy share link"
                  title="Copy share link"
                >
                  {copied === link.id ? (
                    <Check className="h-3.5 w-3.5 text-green-500" />
                  ) : (
                    <Copy className="h-3.5 w-3.5" />
                  )}
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7 shrink-0 text-destructive"
                  onClick={() => handleRevoke(link.id)}
                  aria-label="Revoke share link"
                  title="Revoke share link"
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </Button>
              </div>
            ))
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
