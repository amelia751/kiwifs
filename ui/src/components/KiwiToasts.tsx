import { useEffect, useState } from "react";
import { AlertTriangle, Info, X, XCircle } from "lucide-react";
import { onNotify, type NotifyPayload } from "@/lib/notify";
import { cn } from "@/lib/cn";

type ActiveToast = NotifyPayload & { id: number };

const AUTO_DISMISS_MS = 6000;
const MAX_VISIBLE = 4;

export function KiwiToasts() {
  const [toasts, setToasts] = useState<ActiveToast[]>([]);

  useEffect(() => {
    return onNotify((payload) => {
      const id = Date.now() + Math.random();
      setToasts((prev) => [...prev.slice(-MAX_VISIBLE + 1), { ...payload, id }]);
      window.setTimeout(() => {
        setToasts((prev) => prev.filter((t) => t.id !== id));
      }, AUTO_DISMISS_MS);
    });
  }, []);

  if (toasts.length === 0) return null;

  return (
    <div className="fixed bottom-4 right-4 z-[100] flex flex-col gap-2 max-w-sm pointer-events-none">
      {toasts.map((t) => (
        <div
          key={t.id}
          className={cn(
            "pointer-events-auto rounded-md border px-3 py-2 text-xs shadow-md flex items-start gap-2 bg-background",
            t.kind === "error" &&
              "border-destructive/40 bg-destructive/10 text-destructive",
            t.kind === "warning" &&
              "border-amber-400/40 bg-amber-50 text-amber-900 dark:bg-amber-950/40 dark:text-amber-200",
            t.kind === "info" && "border-border text-foreground",
          )}
          role={t.kind === "error" ? "alert" : "status"}
        >
          <span className="mt-0.5 shrink-0">
            {t.kind === "error" && <XCircle className="h-4 w-4" />}
            {t.kind === "warning" && <AlertTriangle className="h-4 w-4" />}
            {t.kind === "info" && <Info className="h-4 w-4" />}
          </span>
          <div className="min-w-0 flex-1">
            <div className="font-medium break-words">{t.message}</div>
            {t.detail && (
              <div className="mt-0.5 font-mono break-words opacity-80">
                {t.detail}
              </div>
            )}
          </div>
          <button
            type="button"
            onClick={() =>
              setToasts((prev) => prev.filter((x) => x.id !== t.id))
            }
            className="shrink-0 opacity-60 hover:opacity-100"
            aria-label="Dismiss"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      ))}
    </div>
  );
}
