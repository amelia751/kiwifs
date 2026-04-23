// Path helpers that keep the rest of the UI free of edge cases around
// leading/trailing slashes and directory markers returned from /api/kiwi/tree.

export function stripTrailingSlash(p: string): string {
  return p.endsWith("/") && p !== "/" ? p.slice(0, -1) : p;
}

export function isMarkdown(p: string): boolean {
  return p.toLowerCase().endsWith(".md");
}

export function dirOf(p: string): string {
  const clean = stripTrailingSlash(p);
  const idx = clean.lastIndexOf("/");
  return idx < 0 ? "" : clean.slice(0, idx);
}

export function basename(p: string): string {
  const clean = stripTrailingSlash(p);
  const idx = clean.lastIndexOf("/");
  return idx < 0 ? clean : clean.slice(idx + 1);
}

export function stem(p: string): string {
  const base = basename(p);
  return base.replace(/\.md$/i, "");
}

export function titleize(p: string): string {
  const s = stem(p).replace(/[-_]+/g, " ");
  return s
    .split(/\s+/)
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : ""))
    .join(" ");
}

export function breadcrumbs(p: string): { label: string; path: string }[] {
  const clean = stripTrailingSlash(p);
  if (!clean) return [];
  const parts = clean.split("/");
  let acc = "";
  return parts.map((part, i) => {
    acc = acc ? `${acc}/${part}` : part;
    const isLast = i === parts.length - 1;
    return {
      label: isLast ? titleize(part) : part.replace(/[-_]+/g, " "),
      path: acc,
    };
  });
}
