// friendlyError turns the raw error strings KiwiFS backends leak into
// UI-ready phrases. Historically we've thrown `new Error("${status}
// ${statusText}: ${body}")` which surfaces things like
// `500 Internal Server Error: sql: no rows in result set` to end users.
// That's useful for developers and terrifying for everyone else — the
// map below translates the common offenders into sentences a PM would
// write on a status page.
//
// The function is intentionally forgiving: unknown errors pass through
// unchanged (so we never hide a real unknown failure) and Error objects
// keep their original `.message` accessible via the returned
// `originalMessage` field.

export interface FriendlyError {
  title: string;
  detail: string;
  status?: number;
  originalMessage?: string;
}

type Rule = {
  test: (raw: string, status?: number) => boolean;
  build: (raw: string, status?: number) => FriendlyError;
};

const RULES: Rule[] = [
  {
    test: (_, status) => status === 401,
    build: () => ({
      title: "Sign in again",
      detail:
        "Your session expired or the API key was rejected. Refresh and log back in.",
      status: 401,
    }),
  },
  {
    test: (_, status) => status === 403,
    build: () => ({
      title: "You can't do that here",
      detail:
        "Your role doesn't allow this action on this page. Ask the page owner to share it with you.",
      status: 403,
    }),
  },
  {
    test: (raw, status) =>
      status === 404 || /\bno rows in result set\b/i.test(raw),
    build: () => ({
      title: "This page doesn't exist anymore",
      detail:
        "It may have been renamed, moved, or deleted. Check the page tree on the left.",
      status: 404,
    }),
  },
  {
    test: (_, status) => status === 409,
    build: () => ({
      title: "Someone beat you to the save",
      detail:
        "Another editor updated this page while you were working. Reload to see their version, then re-apply your changes.",
      status: 409,
    }),
  },
  {
    test: (_, status) => status === 412,
    build: () => ({
      title: "Page changed under you",
      detail:
        "The save was rejected because the page changed on the server. Reload to merge.",
      status: 412,
    }),
  },
  {
    test: (_, status) => status === 413,
    build: () => ({
      title: "That file is too large",
      detail:
        "The server limits individual uploads. Use the S3 gateway for media bigger than a few MB.",
      status: 413,
    }),
  },
  {
    test: (_, status) => status === 429,
    build: () => ({
      title: "Slow down a little",
      detail:
        "Too many requests in a short window. Wait a few seconds and try again.",
      status: 429,
    }),
  },
  {
    test: (raw, status) => status === 503 && /verified search/i.test(raw),
    build: () => ({
      title: "Verified search needs the SQLite backend",
      detail:
        "This server was started with the grep search engine. Switch to sqlite in kiwifs.toml to enable verified/trust ranking.",
      status: 503,
    }),
  },
  {
    test: (raw) => /fetch failed|NetworkError|Failed to fetch/i.test(raw),
    build: () => ({
      title: "Can't reach the server",
      detail:
        "Your network dropped or the server isn't responding. Check the connection and try again.",
    }),
  },
  {
    test: (raw) => /permission denied/i.test(raw),
    build: () => ({
      title: "Permission denied",
      detail:
        "The server refused the write. Make sure your account has editor access to this space.",
    }),
  },
  {
    test: (raw) => /invalid JSON body|invalid json/i.test(raw),
    build: () => ({
      title: "Malformed request",
      detail: "The page could not be saved because the request body was invalid.",
    }),
  },
];

export function friendlyError(e: unknown): FriendlyError {
  const rawMessage = toMessage(e);
  const status = extractStatus(rawMessage);
  const raw = rawMessage.replace(/^\d+\s+[^:]+:\s*/, "").trim();

  for (const r of RULES) {
    if (r.test(raw, status)) {
      const out = r.build(raw, status);
      out.originalMessage = rawMessage;
      return out;
    }
  }
  return {
    title: "Something went wrong",
    detail: raw || "The server returned an unexpected error.",
    status,
    originalMessage: rawMessage,
  };
}

// friendlyMessage is a one-liner escape hatch for inline banners /
// toasts — wraps friendlyError and returns "<title>: <detail>".
export function friendlyMessage(e: unknown): string {
  const f = friendlyError(e);
  return `${f.title}: ${f.detail}`;
}

function toMessage(e: unknown): string {
  if (typeof e === "string") return e;
  if (e instanceof Error) return e.message;
  if (e && typeof e === "object" && "message" in e) {
    const m = (e as { message?: unknown }).message;
    if (typeof m === "string") return m;
  }
  try {
    return JSON.stringify(e);
  } catch {
    return String(e);
  }
}

function extractStatus(raw: string): number | undefined {
  // Errors thrown by lib/api.ts are shaped as "<status> <statusText>: ..."
  const m = raw.match(/^(\d{3})\s+/);
  if (m) return Number(m[1]);
  return undefined;
}
