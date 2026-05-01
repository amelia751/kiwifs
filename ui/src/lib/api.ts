// Typed client for the KiwiFS REST API. All calls share one fetch wrapper so
// error handling and actor attribution stay consistent.

export type TreeEntry = {
  path: string;
  name: string;
  isDir: boolean;
  size?: number;
  children?: TreeEntry[];
};

export type SearchMatch = { line: number; text: string };
export type SearchResult = {
  path: string;
  score: number;
  snippet?: string;
  matches?: SearchMatch[];
};

export type SearchResponse = { query: string; results: SearchResult[] };

export type Version = {
  hash: string;
  author: string;
  date: string;
  message: string;
};

export type SemanticResult = {
  path: string;
  chunkIdx: number;
  score: number;
  snippet: string;
};

export type SemanticResponse = {
  query: string;
  topK: number;
  offset: number;
  results: SemanticResult[];
};

export type BlameLine = {
  line: number;
  hash: string;
  author: string;
  date: string;
  text: string;
};

export type BacklinkEntry = {
  path: string;
  count: number;
};

export type GraphNode = { path: string; tags?: string[] };
export type GraphEdge = { source: string; target: string };
export type GraphResponse = { nodes: GraphNode[]; edges: GraphEdge[] };

export type CommentAnchor = {
  quote: string;
  prefix?: string;
  suffix?: string;
  offset?: number;
};
export type Comment = {
  id: string;
  path: string;
  anchor: CommentAnchor;
  body: string;
  author: string;
  createdAt: string;
  resolved?: boolean;
};
export type CommentsResponse = { path: string; comments: Comment[] };

export type MetaFilter = { field: string; op: string; value: string };
export type MetaResult = {
  path: string;
  frontmatter: Record<string, unknown>;
};
export type MetaResponse = {
  count: number;
  limit: number;
  offset: number;
  results: MetaResult[];
};
export type QueryResponse = {
  columns: string[];
  rows: Record<string, unknown>[];
  total: number;
  has_more: boolean;
  groups?: { key: string; count: number }[];
};

export type SpaceMeta = {
  name: string;
  root: string;
  fileCount: number;
  lastModified?: string;
  sizeBytes: number;
};

const DEFAULT_ACTOR = "human:web-ui";

let _currentSpace: string | null = null;
const _spaceListeners = new Set<() => void>();

export function setCurrentSpace(space: string | null) {
  _currentSpace = space;
  try {
    if (space) {
      localStorage.setItem("kiwifs-space", space);
    } else {
      localStorage.removeItem("kiwifs-space");
    }
  } catch {}
  _spaceListeners.forEach((fn) => fn());
}

export function getCurrentSpace(): string | null {
  return _currentSpace;
}

export function onSpaceChange(fn: () => void): () => void {
  _spaceListeners.add(fn);
  return () => _spaceListeners.delete(fn);
}

// Restore last-used space from localStorage on load.
try {
  const saved = localStorage.getItem("kiwifs-space");
  if (saved) _currentSpace = saved;
} catch {}

function kiwiBase(): string {
  if (_currentSpace && _currentSpace !== "default") {
    return `/api/kiwi/${_currentSpace}`;
  }
  return "/api/kiwi";
}

export function sseUrl(): string {
  return `${kiwiBase()}/events`;
}

function actor(): string {
  try {
    return localStorage.getItem("kiwifs-actor") || DEFAULT_ACTOR;
  } catch {
    return DEFAULT_ACTOR;
  }
}

async function request<T>(url: string, init: RequestInit = {}): Promise<T> {
  const res = await fetch(url, {
    ...init,
    headers: {
      "X-Actor": actor(),
      ...(init.headers || {}),
    },
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}: ${text || url}`);
  }
  const ct = res.headers.get("content-type") || "";
  if (ct.includes("application/json")) {
    return (await res.json()) as T;
  }
  return (await res.text()) as unknown as T;
}

export const api = {
  // ─── Space Management ───────────────────────────────────────────────────────

  async listSpaces(): Promise<{ spaces: SpaceMeta[] }> {
    return request("/api/spaces");
  },

  async getSpace(name: string): Promise<SpaceMeta> {
    return request(`/api/spaces/${encodeURIComponent(name)}`);
  },

  async createSpace(
    name: string,
    root: string
  ): Promise<SpaceMeta> {
    return request("/api/spaces", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, root }),
    });
  },

  async deleteSpace(name: string): Promise<{ deleted: string }> {
    return request(`/api/spaces/${encodeURIComponent(name)}`, {
      method: "DELETE",
    });
  },

  // ─── Knowledge API (space-scoped) ───────────────────────────────────────────

  async health(): Promise<{ status: string }> {
    return request("/health");
  },

  async tree(path = "/"): Promise<TreeEntry> {
    const qs = new URLSearchParams({ path });
    return request(`${kiwiBase()}/tree?${qs}`);
  },

  async readFile(path: string): Promise<{ content: string; etag: string | null; lastModified: string | null }> {
    const qs = new URLSearchParams({ path });
    const res = await fetch(`${kiwiBase()}/file?${qs}`, {
      headers: { "X-Actor": actor() },
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`${res.status} ${res.statusText}: ${text}`);
    }
    const content = await res.text();
    const etag = res.headers.get("ETag");
    const lastModified = res.headers.get("Last-Modified");
    return { content, etag, lastModified };
  },

  async writeFile(
    path: string,
    content: string,
    etag?: string | null
  ): Promise<{ path: string; etag: string }> {
    const qs = new URLSearchParams({ path });
    const headers: Record<string, string> = {
      "Content-Type": "text/markdown",
      "X-Actor": actor(),
    };
    if (etag) headers["If-Match"] = etag;
    return request(`${kiwiBase()}/file?${qs}`, {
      method: "PUT",
      headers,
      body: content,
    });
  },

  async deleteFile(path: string): Promise<{ deleted: string }> {
    const qs = new URLSearchParams({ path });
    return request(`${kiwiBase()}/file?${qs}`, { method: "DELETE" });
  },

  async uploadAsset(file: File, dir: string): Promise<string> {
    const qs = new URLSearchParams();
    if (dir) qs.set("path", dir);
    const form = new FormData();
    form.append("file", file);
    const res = await fetch(`${kiwiBase()}/assets?${qs}`, {
      method: "POST",
      headers: { "X-Actor": actor() },
      body: form,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      if (res.status === 413) throw new Error("File too large (max 100 MB)");
      if (res.status === 415) throw new Error(`File type not supported: ${file.type}`);
      throw new Error(`${res.status} ${res.statusText}: ${text}`);
    }
    const body = (await res.json()) as { path: string };
    return "/raw/" + body.path;
  },

  async search(q: string, opts?: { modifiedAfter?: string }): Promise<SearchResponse> {
    const qs = new URLSearchParams({ q });
    if (opts?.modifiedAfter) qs.set("modifiedAfter", opts.modifiedAfter);
    return request(`${kiwiBase()}/search?${qs}`);
  },

  async semanticSearch(
    query: string,
    topK = 10,
    offset = 0,
    opts?: { modifiedAfter?: string }
  ): Promise<SemanticResponse> {
    return request(`${kiwiBase()}/search/semantic`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, topK, offset, ...(opts?.modifiedAfter ? { modifiedAfter: opts.modifiedAfter } : {}) }),
    });
  },

  async versions(path: string): Promise<{ path: string; versions: Version[] }> {
    const qs = new URLSearchParams({ path });
    return request(`${kiwiBase()}/versions?${qs}`);
  },

  async readVersion(path: string, version: string): Promise<string> {
    const qs = new URLSearchParams({ path, version });
    const res = await fetch(`${kiwiBase()}/version?${qs}`, {
      headers: { "X-Actor": actor() },
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`${res.status} ${res.statusText}: ${text}`);
    }
    return res.text();
  },

  async diff(path: string, from: string, to: string): Promise<string> {
    const qs = new URLSearchParams({ path, from, to });
    return request(`${kiwiBase()}/diff?${qs}`);
  },

  async blame(path: string): Promise<{ path: string; lines: BlameLine[] }> {
    const qs = new URLSearchParams({ path });
    return request(`${kiwiBase()}/blame?${qs}`);
  },

  async backlinks(path: string): Promise<{ path: string; backlinks: BacklinkEntry[] }> {
    const qs = new URLSearchParams({ path });
    return request(`${kiwiBase()}/backlinks?${qs}`);
  },

  async graph(): Promise<GraphResponse> {
    return request(`${kiwiBase()}/graph`);
  },

  async listTemplates(): Promise<{ templates: { name: string; path: string }[] }> {
    return request(`${kiwiBase()}/templates`);
  },

  async readTemplate(name: string): Promise<{ name: string; content: string }> {
    const qs = new URLSearchParams({ name });
    return request(`${kiwiBase()}/template?${qs}`);
  },

  async listComments(path: string): Promise<CommentsResponse> {
    const qs = new URLSearchParams({ path });
    return request(`${kiwiBase()}/comments?${qs}`);
  },

  async addComment(
    path: string,
    anchor: CommentAnchor,
    body: string
  ): Promise<Comment> {
    const qs = new URLSearchParams({ path });
    return request(`${kiwiBase()}/comments?${qs}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ anchor, body }),
    });
  },

  async deleteComment(
    path: string,
    id: string
  ): Promise<{ deleted: string; path: string }> {
    const qs = new URLSearchParams({ path });
    return request(`${kiwiBase()}/comments/${id}?${qs}`, { method: "DELETE" });
  },

  async resolveComment(
    path: string,
    id: string,
    resolved: boolean
  ): Promise<Comment> {
    const qs = new URLSearchParams({ path });
    return request(`${kiwiBase()}/comments/${id}?${qs}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ resolved }),
    });
  },

  async query(dql: string, opts?: {
    limit?: number;
    offset?: number;
    format?: string;
  }): Promise<QueryResponse> {
    const qs = new URLSearchParams();
    qs.set("q", dql);
    if (opts?.limit != null) qs.set("limit", String(opts.limit));
    if (opts?.offset != null) qs.set("offset", String(opts.offset));
    if (opts?.format) qs.set("format", opts.format);
    return request(`${kiwiBase()}/query?${qs}`);
  },

  async meta(opts: {
    where?: MetaFilter[];
    sort?: string;
    order?: "asc" | "desc";
    limit?: number;
    offset?: number;
  }): Promise<MetaResponse> {
    const qs = new URLSearchParams();
    for (const f of opts.where ?? []) {
      qs.append("where", `${f.field}${f.op}${f.value}`);
    }
    if (opts.sort) qs.set("sort", opts.sort);
    if (opts.order) qs.set("order", opts.order);
    if (opts.limit != null) qs.set("limit", String(opts.limit));
    if (opts.offset != null) qs.set("offset", String(opts.offset));
    return request(`${kiwiBase()}/meta?${qs}`);
  },

  async getUIConfig(): Promise<{ themeLocked: boolean }> {
    return request(`${kiwiBase()}/ui-config`);
  },

  async getTheme(): Promise<Record<string, unknown>> {
    return request(`${kiwiBase()}/theme`);
  },

  async putTheme(theme: Record<string, unknown>): Promise<Record<string, unknown>> {
    return request(`${kiwiBase()}/theme`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(theme),
    });
  },
};
