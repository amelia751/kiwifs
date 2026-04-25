// Minimal app-wide notification system. We don't want to pull in sonner/react-toast
// just for the handful of places where a background operation fails silently,
// so we rely on a CustomEvent on window and a tiny listener in App.tsx.

export type NotifyKind = "error" | "warning" | "info";

export type NotifyPayload = {
  kind: NotifyKind;
  message: string;
  detail?: string;
};

const EVENT_NAME = "kiwi:notify";

export function notify(payload: NotifyPayload) {
  window.dispatchEvent(new CustomEvent<NotifyPayload>(EVENT_NAME, { detail: payload }));
}

export function notifyError(message: string, err?: unknown) {
  const detail = err == null ? undefined : err instanceof Error ? err.message : String(err);
  notify({ kind: "error", message, detail });
}

export function onNotify(listener: (p: NotifyPayload) => void): () => void {
  const h = (e: Event) => {
    const ce = e as CustomEvent<NotifyPayload>;
    if (ce.detail) listener(ce.detail);
  };
  window.addEventListener(EVENT_NAME, h);
  return () => window.removeEventListener(EVENT_NAME, h);
}
