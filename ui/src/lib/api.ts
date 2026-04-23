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

export type GraphNode = { path: string };
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

const DEFAULT_ACTOR = "human:web-ui";

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
  async health(): Promise<{ status: string }> {
    return request("/health");
  },

  async tree(path = "/"): Promise<TreeEntry> {
    const qs = new URLSearchParams({ path });
    return request(`/api/kiwi/tree?${qs}`);
  },

  async readFile(path: string): Promise<{ content: string; etag: string | null }> {
    const qs = new URLSearchParams({ path });
    const res = await fetch(`/api/kiwi/file?${qs}`, {
      headers: { "X-Actor": actor() },
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`${res.status} ${res.statusText}: ${text}`);
    }
    const content = await res.text();
    const etag = res.headers.get("ETag");
    return { content, etag };
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
    return request(`/api/kiwi/file?${qs}`, {
      method: "PUT",
      headers,
      body: content,
    });
  },

  async deleteFile(path: string): Promise<{ deleted: string }> {
    const qs = new URLSearchParams({ path });
    return request(`/api/kiwi/file?${qs}`, { method: "DELETE" });
  },

  async uploadAsset(file: File, dir: string): Promise<string> {
    const qs = new URLSearchParams();
    if (dir) qs.set("path", dir);
    const form = new FormData();
    form.append("file", file);
    // Don't set Content-Type — FormData picks the multipart boundary itself,
    // and overriding it strips the boundary and kills the parse server-side.
    const res = await fetch(`/api/kiwi/assets?${qs}`, {
      method: "POST",
      headers: { "X-Actor": actor() },
      body: form,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`${res.status} ${res.statusText}: ${text}`);
    }
    const body = (await res.json()) as { path: string };
    // Return the absolute URL — BlockNote embeds it directly in the image
    // block, and /api/kiwi/file is the same endpoint that serves markdown
    // reads, so no extra routing needed.
    const p = new URLSearchParams({ path: body.path });
    return `/api/kiwi/file?${p}`;
  },

  async search(q: string): Promise<SearchResponse> {
    const qs = new URLSearchParams({ q });
    return request(`/api/kiwi/search?${qs}`);
  },

  async semanticSearch(
    query: string,
    topK = 10,
    offset = 0
  ): Promise<SemanticResponse> {
    return request(`/api/kiwi/search/semantic`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, topK, offset }),
    });
  },

  async versions(path: string): Promise<{ path: string; versions: Version[] }> {
    const qs = new URLSearchParams({ path });
    return request(`/api/kiwi/versions?${qs}`);
  },

  async readVersion(path: string, version: string): Promise<string> {
    const qs = new URLSearchParams({ path, version });
    const res = await fetch(`/api/kiwi/version?${qs}`, {
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
    return request(`/api/kiwi/diff?${qs}`);
  },

  async blame(path: string): Promise<{ path: string; lines: BlameLine[] }> {
    const qs = new URLSearchParams({ path });
    return request(`/api/kiwi/blame?${qs}`);
  },

  async backlinks(path: string): Promise<{ path: string; backlinks: BacklinkEntry[] }> {
    const qs = new URLSearchParams({ path });
    return request(`/api/kiwi/backlinks?${qs}`);
  },

  async graph(): Promise<GraphResponse> {
    return request(`/api/kiwi/graph`);
  },

  async listTemplates(): Promise<{ templates: { name: string; path: string }[] }> {
    return request(`/api/kiwi/templates`);
  },

  async readTemplate(name: string): Promise<{ name: string; content: string }> {
    const qs = new URLSearchParams({ name });
    return request(`/api/kiwi/template?${qs}`);
  },

  async listComments(path: string): Promise<CommentsResponse> {
    const qs = new URLSearchParams({ path });
    return request(`/api/kiwi/comments?${qs}`);
  },

  async addComment(
    path: string,
    anchor: CommentAnchor,
    body: string
  ): Promise<Comment> {
    const qs = new URLSearchParams({ path });
    return request(`/api/kiwi/comments?${qs}`, {
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
    return request(`/api/kiwi/comments/${id}?${qs}`, { method: "DELETE" });
  },

  async meta(opts: {
    where?: MetaFilter[];
    sort?: string;
    order?: "asc" | "desc";
    limit?: number;
    offset?: number;
  }): Promise<MetaResponse> {
    const qs = new URLSearchParams();
    // The server concatenates field+op+value into one "where" param, so do
    // the same here rather than sending JSON — keeps curl debugging viable.
    for (const f of opts.where ?? []) {
      qs.append("where", `${f.field}${f.op}${f.value}`);
    }
    if (opts.sort) qs.set("sort", opts.sort);
    if (opts.order) qs.set("order", opts.order);
    if (opts.limit != null) qs.set("limit", String(opts.limit));
    if (opts.offset != null) qs.set("offset", String(opts.offset));
    return request(`/api/kiwi/meta?${qs}`);
  },
};
