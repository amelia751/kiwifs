// Remark plugin + resolver for [[wiki-link]] syntax.
//
// Parses `[[target]]` or `[[target|label]]` inside text nodes and replaces
// them with link nodes whose URL is a `kiwi:` pseudo-protocol. React-markdown
// then renders those as clickable spans via a custom <a> component.
//
// Resolution is fuzzy:
//   [[authentication]]         → concepts/authentication.md
//   [[concepts/auth]]          → concepts/auth.md  (exact first, then fuzzy)
//   [[Authentication]]         → case-insensitive match on the stem
//
// The resolver is built once from the file tree and re-built whenever the
// tree changes, so lookups are O(1) per link.

import { visit } from "unist-util-visit";
import type { Root } from "mdast";
import type { TreeEntry } from "@/lib/api";

export type LinkResolver = (target: string) => string | null;

function flatten(tree: TreeEntry): string[] {
  const out: string[] = [];
  const walk = (n: TreeEntry) => {
    if (!n.isDir && n.path.toLowerCase().endsWith(".md")) out.push(n.path);
    (n.children || []).forEach(walk);
  };
  walk(tree);
  return out;
}

function normalize(s: string): string {
  return s.toLowerCase().replace(/\.md$/, "").replace(/[-_\s]+/g, "-");
}

export function buildResolver(tree: TreeEntry | null): LinkResolver {
  if (!tree) return () => null;
  const paths = flatten(tree);

  // Build several lookups: exact path → path, and normalized stem → path.
  const byPath = new Map<string, string>();
  const byNormPath = new Map<string, string>();
  const byStem = new Map<string, string>();
  for (const p of paths) {
    byPath.set(p, p);
    byNormPath.set(normalize(p), p);
    const stem = p.substring(p.lastIndexOf("/") + 1).replace(/\.md$/i, "");
    byStem.set(normalize(stem), p);
  }

  return (target) => {
    if (!target) return null;
    const t = target.trim();
    if (byPath.has(t)) return byPath.get(t)!;
    if (byPath.has(t + ".md")) return byPath.get(t + ".md")!;
    const n = normalize(t);
    if (byNormPath.has(n)) return byNormPath.get(n)!;
    if (byStem.has(n)) return byStem.get(n)!;
    // Suffix match: [[auth]] resolves to concepts/authentication.md if stem
    // starts with it — useful but bounded to avoid absurd matches.
    for (const [stem, p] of byStem.entries()) {
      if (stem.startsWith(n)) return p;
    }
    return null;
  };
}

// Extract all [[wiki]] targets from a markdown string. Used by the server
// when indexing, and by the client when recomputing the link graph.
export function extractWikiTargets(md: string): string[] {
  const out: string[] = [];
  const re = /\[\[([^\]|]+)(?:\|[^\]]+)?\]\]/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(md)) !== null) out.push(m[1].trim());
  return out;
}

// Remark plugin: rewrite [[x]] occurrences in text nodes into link nodes.
// `resolver` can return null for unknown targets — those are rendered as
// "missing" wiki links (dotted underline) so authors see broken refs.
export function remarkWikiLinks(opts: { resolver: LinkResolver }) {
  const re = /\[\[([^\]|]+)(?:\|([^\]]+))?\]\]/g;

  return (tree: Root) => {
    visit(tree, "text", (node, index, parent) => {
      if (!parent || index === undefined) return;
      if (!node.value.includes("[[")) return;

      const parts: (typeof node | any)[] = [];
      let last = 0;
      let m: RegExpExecArray | null;
      re.lastIndex = 0;
      while ((m = re.exec(node.value)) !== null) {
        if (m.index > last) {
          parts.push({ type: "text", value: node.value.slice(last, m.index) });
        }
        const target = m[1].trim();
        const label = (m[2] || target).trim();
        const resolved = opts.resolver(target);
        const url = resolved
          ? `kiwi:${resolved}`
          : `kiwi-missing:${target}`;
        parts.push({
          type: "link",
          url,
          title: resolved || `Missing: ${target}`,
          children: [{ type: "text", value: label }],
          data: {
            hProperties: {
              className: resolved ? "wiki-link" : "wiki-link wiki-link-missing",
              dataKiwiTarget: resolved || target,
              dataKiwiMissing: resolved ? undefined : "true",
            },
          },
        });
        last = m.index + m[0].length;
      }
      if (last < node.value.length) {
        parts.push({ type: "text", value: node.value.slice(last) });
      }
      if (parts.length > 0) {
        (parent as any).children.splice(index, 1, ...parts);
        return index + parts.length;
      }
    });
  };
}
