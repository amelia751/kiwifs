import { useCallback, useEffect, useMemo, useState } from "react";
import { Check, Loader2 } from "lucide-react";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

// EDITABLE_FIELDS defines exactly which keys this component can touch. Anything
// else in the frontmatter is preserved by the backend's PATCH /meta merge.
const EDITABLE_FIELDS = [
  "owner",
  "status",
  "visibility",
  "confidence",
  "source-of-truth",
  "reviewed",
  "next-review",
  "tags",
] as const;

type EditableKey = (typeof EDITABLE_FIELDS)[number];
type Draft = Partial<Record<EditableKey, unknown>>;

type Props = {
  path: string;
  frontmatter: Record<string, any>;
  onSaved?: () => void;
};

export function KiwiPageMeta({ path, frontmatter, onSaved }: Props) {
  const initial = useMemo<Draft>(() => {
    const out: Draft = {};
    for (const key of EDITABLE_FIELDS) {
      if (frontmatter[key] !== undefined) out[key] = frontmatter[key];
    }
    return out;
  }, [frontmatter]);

  const [draft, setDraft] = useState<Draft>(initial);
  const [saving, setSaving] = useState(false);
  const [savedAt, setSavedAt] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Reset when navigating to a different page.
  useEffect(() => {
    setDraft(initial);
    setError(null);
    setSavedAt(null);
  }, [initial]);

  const set = useCallback(
    (key: EditableKey, value: unknown) =>
      setDraft((d) => ({ ...d, [key]: value })),
    [],
  );

  const frontmatterView = { ...frontmatter, ...draft };

  const diff = useMemo<Record<string, unknown>>(() => {
    const out: Record<string, unknown> = {};
    for (const key of EDITABLE_FIELDS) {
      const next = draft[key];
      const prev = frontmatter[key];
      if (JSON.stringify(next) !== JSON.stringify(prev)) {
        out[key] = next === "" || next === undefined ? null : next;
      }
    }
    return out;
  }, [draft, frontmatter]);

  const dirty = Object.keys(diff).length > 0;

  async function save() {
    if (!dirty) return;
    setSaving(true);
    setError(null);
    try {
      await api.updateMeta(path, diff);
      setSavedAt(Date.now());
      onSaved?.();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  }

  const confidenceRaw = frontmatterView.confidence;
  const confidence =
    typeof confidenceRaw === "number" ? Math.round(confidenceRaw * 100) : 50;

  const tagsValue = Array.isArray(frontmatterView.tags)
    ? (frontmatterView.tags as unknown[]).join(", ")
    : typeof frontmatterView.tags === "string"
      ? (frontmatterView.tags as string)
      : "";

  const justSaved = savedAt != null && Date.now() - savedAt < 2500;

  return (
    <div className="rounded-lg border border-border p-4 space-y-3">
      <div className="grid grid-cols-2 gap-x-4 gap-y-3">
        {/* Owner */}
        <div className="space-y-1">
          <Label className="text-xs">Owner</Label>
          <Input
            value={(frontmatterView.owner as string) ?? ""}
            onChange={(e) => set("owner", e.target.value)}
            placeholder="owner@company.com"
            className="h-8 text-xs"
          />
        </div>

        {/* Status */}
        <div className="space-y-1">
          <Label className="text-xs">Status</Label>
          <Select
            value={(frontmatterView.status as string) ?? "draft"}
            onValueChange={(v) => set("status", v)}
          >
            <SelectTrigger className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="draft">Draft</SelectItem>
              <SelectItem value="verified">Verified</SelectItem>
              <SelectItem value="outdated">Outdated</SelectItem>
              <SelectItem value="deprecated">Deprecated</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Visibility */}
        <div className="space-y-1">
          <Label className="text-xs">Visibility</Label>
          <Select
            value={(frontmatterView.visibility as string) ?? "internal"}
            onValueChange={(v) => set("visibility", v)}
          >
            <SelectTrigger className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="internal">Internal</SelectItem>
              <SelectItem value="public">Public</SelectItem>
              <SelectItem value="private">Private</SelectItem>
              <SelectItem value="password">Password</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Confidence */}
        <div className="space-y-1">
          <Label className="text-xs">
            Confidence{" "}
            <span className="text-muted-foreground">({confidence}%)</span>
          </Label>
          <input
            type="range"
            min={0}
            max={100}
            value={confidence}
            onChange={(e) => set("confidence", Number(e.target.value) / 100)}
            className="w-full h-8 accent-primary"
          />
        </div>

        {/* Source of truth */}
        <div className="space-y-1">
          <Label className="text-xs">Source of Truth</Label>
          <div className="flex items-center h-8">
            <button
              type="button"
              role="switch"
              aria-checked={!!frontmatterView["source-of-truth"]}
              onClick={() =>
                set("source-of-truth", !frontmatterView["source-of-truth"])
              }
              className={
                "relative inline-flex h-5 w-9 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors " +
                (frontmatterView["source-of-truth"]
                  ? "bg-primary"
                  : "bg-muted")
              }
            >
              <span
                className={
                  "pointer-events-none block h-4 w-4 rounded-full bg-background shadow-sm transition-transform " +
                  (frontmatterView["source-of-truth"]
                    ? "translate-x-4"
                    : "translate-x-0")
                }
              />
            </button>
          </div>
        </div>

        {/* Reviewed date */}
        <div className="space-y-1">
          <Label className="text-xs">Reviewed</Label>
          <Input
            type="date"
            value={(frontmatterView.reviewed as string) ?? ""}
            onChange={(e) => set("reviewed", e.target.value)}
            className="h-8 text-xs"
          />
        </div>

        {/* Next review */}
        <div className="space-y-1">
          <Label className="text-xs">Next Review</Label>
          <Input
            type="date"
            value={(frontmatterView["next-review"] as string) ?? ""}
            onChange={(e) => set("next-review", e.target.value)}
            className="h-8 text-xs"
          />
        </div>

        {/* Tags */}
        <div className="space-y-1 col-span-2">
          <Label className="text-xs">Tags (comma-separated)</Label>
          <Input
            value={tagsValue}
            onChange={(e) =>
              set(
                "tags",
                e.target.value
                  .split(",")
                  .map((t) => t.trim())
                  .filter(Boolean),
              )
            }
            placeholder="api, docs, architecture"
            className="h-8 text-xs"
          />
        </div>
      </div>

      {error && <div className="text-xs font-mono text-destructive">{error}</div>}

      <div className="flex items-center justify-end gap-2 pt-1">
        {justSaved && !dirty && (
          <span className="text-xs text-green-600 dark:text-green-400 flex items-center gap-1">
            <Check className="h-3.5 w-3.5" /> Saved
          </span>
        )}
        <Button
          size="sm"
          onClick={save}
          disabled={!dirty || saving}
          className="gap-1"
        >
          {saving ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : null}
          Save metadata
        </Button>
      </div>
    </div>
  );
}
