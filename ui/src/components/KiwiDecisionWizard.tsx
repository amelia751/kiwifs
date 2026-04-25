// Decision wizard: guided UI for creating a new decision page or
// converting an existing note into a decision. The critique pointed out
// that decision frontmatter is complex enough that "copy this template
// and hand-edit YAML" is a real friction point — the wizard collects
// the required fields, assembles the frontmatter on the client, and
// writes the page through the normal PUT /file pipeline so it picks up
// git history, search indexing, and SSE broadcasts for free.
import { useMemo, useState } from "react";
import { Loader2, Plus, Trash2, Wand2 } from "lucide-react";
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
import { Textarea } from "@/components/ui/textarea";
import { api } from "@/lib/api";

type Alternative = {
  option: string;
  pros: string;
  cons: string;
};

type Props = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  // When set, the wizard pre-fills from an existing page's body so the
  // user is "converting this note into a decision" instead of starting
  // blank. Supplying the path also makes save() write back to the same
  // file; otherwise a new path under decisions/<slug>.md is created.
  sourcePath?: string;
  sourceBody?: string;
  defaultOwner?: string;
  onSaved?: (path: string) => void;
};

function slugify(s: string): string {
  return s
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .trim()
    .replace(/\s+/g, "-")
    .slice(0, 64);
}

function today(): string {
  return new Date().toISOString().slice(0, 10);
}

function ninetyDaysOut(): string {
  const d = new Date();
  d.setDate(d.getDate() + 90);
  return d.toISOString().slice(0, 10);
}

function toYamlList(items: string[], indent = "  "): string {
  if (items.length === 0) return "[]";
  return "\n" + items.map((v) => `${indent}- ${JSON.stringify(v)}`).join("\n");
}

function buildDecisionMarkdown(args: {
  title: string;
  decision: string;
  context: string;
  consequences: string;
  owner: string;
  alternatives: Alternative[];
  impact: string;
  reversal: string;
  confidence: number;
  sourceOfTruth: boolean;
  linkedDocs: string[];
  tags: string[];
}): string {
  const altBlock = args.alternatives
    .filter((a) => a.option.trim() !== "")
    .map((a) => {
      const pros = a.pros
        .split(/\r?\n/)
        .map((s) => s.trim())
        .filter(Boolean);
      const cons = a.cons
        .split(/\r?\n/)
        .map((s) => s.trim())
        .filter(Boolean);
      return (
        `  - option: ${JSON.stringify(a.option)}\n` +
        `    pros:${toYamlList(pros, "      ")}\n` +
        `    cons:${toYamlList(cons, "      ")}`
      );
    })
    .join("\n");

  const linked = args.linkedDocs
    .map((s) => s.trim())
    .filter(Boolean)
    .map((p) => `  - ${p}`)
    .join("\n");

  const tags = args.tags.map((t) => t.trim()).filter(Boolean);

  const fm = [
    "---",
    `title: ${JSON.stringify(args.title)}`,
    "type: decision",
    "status: active",
    `owner: ${args.owner}`,
    `decision: ${JSON.stringify(args.decision)}`,
    `date: ${today()}`,
    `confidence: ${args.confidence}`,
    `source-of-truth: ${args.sourceOfTruth}`,
    `reviewed: ${today()}`,
    `next-review: ${ninetyDaysOut()}`,
    altBlock ? `alternatives:\n${altBlock}` : "alternatives: []",
    args.impact ? `impact: ${JSON.stringify(args.impact)}` : null,
    args.reversal ? `reversal-conditions: ${JSON.stringify(args.reversal)}` : null,
    linked ? `linked-docs:\n${linked}` : null,
    tags.length ? `tags: [${tags.map((t) => JSON.stringify(t)).join(", ")}]` : "tags: [decision]",
    "---",
    "",
    `# ${args.title}`,
    "",
    "## Context",
    args.context || "_Why was this decision needed?_",
    "",
    "## Decision",
    args.decision,
    "",
    "## Consequences",
    args.consequences || "_What happens as a result?_",
    "",
  ];
  return fm.filter((x) => x !== null).join("\n");
}

export function KiwiDecisionWizard({
  open,
  onOpenChange,
  sourcePath,
  sourceBody,
  defaultOwner,
  onSaved,
}: Props) {
  const isConvert = Boolean(sourcePath && sourceBody);
  const [title, setTitle] = useState("");
  const [decision, setDecision] = useState("");
  const [context, setContext] = useState(sourceBody || "");
  const [consequences, setConsequences] = useState("");
  const [owner, setOwner] = useState(defaultOwner || "");
  const [impact, setImpact] = useState("");
  const [reversal, setReversal] = useState("");
  const [confidence, setConfidence] = useState(0.8);
  const [sourceOfTruth, setSourceOfTruth] = useState(false);
  const [linkedDocs, setLinkedDocs] = useState("");
  const [tags, setTags] = useState("decision");
  const [alternatives, setAlternatives] = useState<Alternative[]>([
    { option: "", pros: "", cons: "" },
    { option: "", pros: "", cons: "" },
  ]);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const targetPath = useMemo(() => {
    if (sourcePath) return sourcePath;
    const slug = slugify(title) || `decision-${Date.now()}`;
    return `decisions/${slug}.md`;
  }, [sourcePath, title]);

  const canSave = title.trim() !== "" && decision.trim() !== "" && owner.trim() !== "";

  async function handleSave() {
    setError(null);
    setSaving(true);
    try {
      const md = buildDecisionMarkdown({
        title: title.trim(),
        decision: decision.trim(),
        context: context.trim(),
        consequences: consequences.trim(),
        owner: owner.trim(),
        alternatives,
        impact: impact.trim(),
        reversal: reversal.trim(),
        confidence,
        sourceOfTruth,
        linkedDocs: linkedDocs.split(/\r?\n/),
        tags: tags.split(","),
      });
      await api.writeFile(targetPath, md);
      onSaved?.(targetPath);
      onOpenChange(false);
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  }

  function updateAlt(i: number, patch: Partial<Alternative>) {
    setAlternatives((prev) => {
      const next = prev.slice();
      next[i] = { ...next[i], ...patch };
      return next;
    });
  }

  function addAlt() {
    setAlternatives((prev) => [...prev, { option: "", pros: "", cons: "" }]);
  }

  function removeAlt(i: number) {
    setAlternatives((prev) => prev.filter((_, idx) => idx !== i));
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[85vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Wand2 className="h-4 w-4" />
            {isConvert ? "Convert note into a decision" : "New decision"}
          </DialogTitle>
          <DialogDescription>
            Decisions live forever. Capture the <em>why</em>, the
            alternatives you weighed, and the signal that would reverse
            the call, so future-you doesn&apos;t have to ask Slack again.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-2">
          <div className="space-y-1.5">
            <Label htmlFor="dw-title">Title</Label>
            <Input
              id="dw-title"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              placeholder="Paddle vs Stripe"
            />
            {!isConvert && title.trim() && (
              <div className="text-xs text-muted-foreground">
                Will save to <code className="font-mono">{targetPath}</code>
              </div>
            )}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="dw-decision">The decision (one sentence)</Label>
            <Textarea
              id="dw-decision"
              value={decision}
              onChange={(e) => setDecision(e.target.value)}
              placeholder="We chose Paddle over Stripe because international tax handling was easier."
              rows={2}
            />
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1.5">
              <Label htmlFor="dw-owner">Owner</Label>
              <Input
                id="dw-owner"
                value={owner}
                onChange={(e) => setOwner(e.target.value)}
                placeholder="alice@acme.com"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="dw-confidence">Confidence (0–1)</Label>
              <Input
                id="dw-confidence"
                type="number"
                step="0.1"
                min={0}
                max={1}
                value={confidence}
                onChange={(e) => setConfidence(Number(e.target.value))}
              />
            </div>
          </div>

          <div className="flex items-center gap-2 text-sm">
            <input
              id="dw-sot"
              type="checkbox"
              checked={sourceOfTruth}
              onChange={(e) => setSourceOfTruth(e.target.checked)}
            />
            <Label htmlFor="dw-sot" className="cursor-pointer">
              Mark as source-of-truth (will rank above other pages in
              search)
            </Label>
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="dw-context">Context</Label>
            <Textarea
              id="dw-context"
              value={context}
              onChange={(e) => setContext(e.target.value)}
              placeholder="Why was this decision needed?"
              rows={4}
            />
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="dw-consequences">Consequences</Label>
            <Textarea
              id="dw-consequences"
              value={consequences}
              onChange={(e) => setConsequences(e.target.value)}
              placeholder="What happens as a result? Trade-offs you accepted?"
              rows={3}
            />
          </div>

          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <Label>Alternatives considered</Label>
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={addAlt}
                className="ml-auto h-7"
              >
                <Plus className="h-3 w-3 mr-1" /> Add
              </Button>
            </div>
            {alternatives.map((a, i) => (
              <div
                key={i}
                className="space-y-1.5 border border-border rounded-md p-3 relative"
              >
                <div className="flex gap-2">
                  <Input
                    value={a.option}
                    onChange={(e) => updateAlt(i, { option: e.target.value })}
                    placeholder={`Alternative ${i + 1}`}
                  />
                  {alternatives.length > 1 && (
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      onClick={() => removeAlt(i)}
                      className="shrink-0"
                      aria-label="Remove alternative"
                      title="Remove alternative"
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  )}
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <Textarea
                    value={a.pros}
                    onChange={(e) => updateAlt(i, { pros: e.target.value })}
                    placeholder="Pros (one per line)"
                    rows={3}
                  />
                  <Textarea
                    value={a.cons}
                    onChange={(e) => updateAlt(i, { cons: e.target.value })}
                    placeholder="Cons (one per line)"
                    rows={3}
                  />
                </div>
              </div>
            ))}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="dw-impact">Impact</Label>
            <Textarea
              id="dw-impact"
              value={impact}
              onChange={(e) => setImpact(e.target.value)}
              placeholder="Expected business impact"
              rows={2}
            />
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="dw-reversal">Reversal conditions</Label>
            <Textarea
              id="dw-reversal"
              value={reversal}
              onChange={(e) => setReversal(e.target.value)}
              placeholder="What signal would make us revisit this?"
              rows={2}
            />
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1.5">
              <Label htmlFor="dw-tags">Tags (comma separated)</Label>
              <Input
                id="dw-tags"
                value={tags}
                onChange={(e) => setTags(e.target.value)}
                placeholder="billing, payments, decision"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="dw-links">Linked docs (one per line)</Label>
              <Textarea
                id="dw-links"
                value={linkedDocs}
                onChange={(e) => setLinkedDocs(e.target.value)}
                placeholder={"concepts/payments.md\nhowto/billing.md"}
                rows={2}
              />
            </div>
          </div>

          {error && (
            <div className="text-sm text-destructive">
              Save failed: {error}
            </div>
          )}
        </div>

        <DialogFooter>
          <Button variant="ghost" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={!canSave || saving}>
            {saving && <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />}
            {isConvert ? "Convert" : "Create decision"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
