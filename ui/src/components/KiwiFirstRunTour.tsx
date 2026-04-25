import { useEffect, useState } from "react";
import { BookOpen, Link2, Network, Shield, Sparkles, X } from "lucide-react";
import { Button } from "@/components/ui/button";

const TOUR_KEY = "kiwifs-tour-dismissed";
const TOUR_VERSION = "2026-04";

type Step = {
  icon: React.ReactNode;
  title: string;
  body: React.ReactNode;
};

const STEPS: Step[] = [
  {
    icon: <Sparkles className="h-5 w-5 text-primary" />,
    title: "Welcome to KiwiFS",
    body: (
      <>
        KiwiFS is a git-backed knowledge base that lives as plain markdown
        files on disk. This quick tour (five cards) points at the most
        useful spots. You can dismiss it at any time — it won't come back.
      </>
    ),
  },
  {
    icon: <BookOpen className="h-5 w-5 text-primary" />,
    title: "The sidebar is your file tree",
    body: (
      <>
        Folders and pages on the left mirror the filesystem under your
        workspace root. Right-click a page for rename / move / share /
        trust actions. New pages land next to siblings — drag-and-drop is
        coming, but you can edit on disk any time.
      </>
    ),
  },
  {
    icon: <Link2 className="h-5 w-5 text-primary" />,
    title: "Wikilinks connect your notes",
    body: (
      <>
        Type <code className="rounded bg-muted px-1">[[</code> inside the
        editor to search for a target. Link syntax is
        {" "}
        <code className="rounded bg-muted px-1">[[path/to/page|label]]</code>.
        Broken links get a dashed underline. See{" "}
        <em>concepts/wikilinks</em> in the starter workspace.
      </>
    ),
  },
  {
    icon: <Network className="h-5 w-5 text-primary" />,
    title: "Graph + Janitor",
    body: (
      <>
        Click the graph icon in the header to see your knowledge web. The
        Knowledge Janitor (broom icon) scans for stale, orphan, and
        duplicate pages on a timer; open it to review issues and accept
        fixes.
      </>
    ),
  },
  {
    icon: <Shield className="h-5 w-5 text-primary" />,
    title: "Trust + workflow",
    body: (
      <>
        Mark a page <strong>verified</strong> or{" "}
        <strong>source-of-truth</strong> and it floats to the top of
        search. Add <code className="rounded bg-muted px-1">due-date</code>,
        {" "}<code className="rounded bg-muted px-1">tasks</code>, or
        {" "}<code className="rounded bg-muted px-1">approval</code>{" "}
        frontmatter to make a page a workflow — KiwiFS will remind the
        owner when the date is close.
      </>
    ),
  },
];

export function KiwiFirstRunTour() {
  const [open, setOpen] = useState(false);
  const [step, setStep] = useState(0);

  useEffect(() => {
    try {
      const dismissed = localStorage.getItem(TOUR_KEY);
      if (dismissed !== TOUR_VERSION) setOpen(true);
    } catch {
      setOpen(true);
    }
  }, []);

  const close = (dismissForever: boolean) => {
    setOpen(false);
    if (dismissForever) {
      try {
        localStorage.setItem(TOUR_KEY, TOUR_VERSION);
      } catch {
        // localStorage unavailable — user will see the tour next load.
      }
    }
  };

  if (!open) return null;

  const s = STEPS[step];
  const isLast = step === STEPS.length - 1;

  return (
    <div
      className="fixed inset-0 z-[120] flex items-center justify-center bg-background/60 backdrop-blur-sm"
      role="dialog"
      aria-modal="true"
      aria-labelledby="kiwi-tour-title"
    >
      <div className="relative max-w-md w-[min(90vw,28rem)] rounded-xl border border-border bg-card shadow-xl">
        <Button
          variant="ghost"
          size="icon"
          className="absolute right-2 top-2 h-7 w-7"
          onClick={() => close(true)}
          aria-label="Close tour"
          title="Close tour"
        >
          <X className="h-4 w-4" />
        </Button>
        <div className="p-6 space-y-3">
          <div className="flex items-center gap-2">
            {s.icon}
            <h2 id="kiwi-tour-title" className="text-base font-semibold">
              {s.title}
            </h2>
          </div>
          <div className="text-sm text-muted-foreground leading-relaxed">
            {s.body}
          </div>
        </div>
        <div className="flex items-center justify-between border-t border-border px-4 py-3">
          <div className="flex items-center gap-1">
            {STEPS.map((_, i) => (
              <span
                key={i}
                className={
                  "h-1.5 rounded-full transition-colors " +
                  (i === step
                    ? "w-5 bg-primary"
                    : "w-1.5 bg-muted")
                }
                aria-hidden="true"
              />
            ))}
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => close(true)}
            >
              Skip tour
            </Button>
            {step > 0 && (
              <Button
                variant="outline"
                size="sm"
                onClick={() => setStep((s) => Math.max(0, s - 1))}
              >
                Back
              </Button>
            )}
            <Button
              size="sm"
              onClick={() => {
                if (isLast) close(true);
                else setStep((s) => Math.min(STEPS.length - 1, s + 1));
              }}
            >
              {isLast ? "Got it" : "Next"}
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
