// DOM utilities for locating and wrapping anchored text ranges in rendered
// markdown. Anchors are {quote, prefix, suffix} — the same TextQuote
// selector pattern used by W3C Web Annotations. The prefix/suffix
// disambiguate when the same quote appears multiple times on a page.

import type { CommentAnchor } from "./api";

export type AnchorLocation = { index: number; length: number };

// Find an anchor in a plain-text string. Tries exact context match first,
// then falls back to the raw quote. Returns null when the quote is absent.
export function locateAnchor(text: string, a: CommentAnchor): AnchorLocation | null {
  if (!a.quote) return null;
  const prefix = a.prefix || "";
  const suffix = a.suffix || "";

  if (prefix || suffix) {
    const full = prefix + a.quote + suffix;
    const hit = text.indexOf(full);
    if (hit >= 0) return { index: hit + prefix.length, length: a.quote.length };
  }
  // Fall back to plain quote match. Not perfect when the same quote recurs,
  // but better than silently dropping the comment.
  const hit = text.indexOf(a.quote);
  if (hit < 0) return null;
  return { index: hit, length: a.quote.length };
}

// Build the concatenated textContent of a container plus a parallel list of
// (node, offset) mappings so a flat-string index can be resolved back to a
// DOM Range. Skips already-wrapped highlight spans and their descendants to
// keep re-application idempotent.
function collectText(root: HTMLElement): { text: string; nodes: Text[]; offsets: number[] } {
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
    acceptNode(node) {
      let parent = node.parentElement;
      while (parent && parent !== root) {
        if (parent.dataset && parent.dataset.kiwiComment === "wrap") {
          return NodeFilter.FILTER_REJECT;
        }
        parent = parent.parentElement;
      }
      return NodeFilter.FILTER_ACCEPT;
    },
  });
  const nodes: Text[] = [];
  const offsets: number[] = [];
  let text = "";
  let n: Node | null;
  while ((n = walker.nextNode())) {
    const t = n as Text;
    nodes.push(t);
    offsets.push(text.length);
    text += t.data;
  }
  return { text, nodes, offsets };
}

// Map a flat-string index to (node, localOffset). Binary search on offsets.
function positionFor(
  index: number,
  nodes: Text[],
  offsets: number[]
): { node: Text; local: number } | null {
  if (nodes.length === 0) return null;
  let lo = 0,
    hi = nodes.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (offsets[mid] <= index) lo = mid;
    else hi = mid - 1;
  }
  const base = offsets[lo];
  const node = nodes[lo];
  const local = index - base;
  if (local < 0 || local > node.data.length) return null;
  return { node, local };
}

// A wrapped range — one hit per call. Crossing DOM boundaries produces
// multiple <span data-kiwi-comment="wrap"> wrappers for the same comment id.
export type WrapResult = { id: string; spans: HTMLSpanElement[] } | null;

export function wrapAnchor(
  root: HTMLElement,
  anchor: CommentAnchor,
  id: string,
  onClick: (id: string, rect: DOMRect) => void
): WrapResult {
  const { text, nodes, offsets } = collectText(root);
  const loc = locateAnchor(text, anchor);
  if (!loc) return null;

  const start = positionFor(loc.index, nodes, offsets);
  const end = positionFor(loc.index + loc.length, nodes, offsets);
  if (!start || !end) return null;

  const range = document.createRange();
  try {
    range.setStart(start.node, start.local);
    range.setEnd(end.node, end.local);
  } catch {
    return null;
  }

  const spans: HTMLSpanElement[] = [];

  // Split into per-text-node sub-ranges; Range.surroundContents throws when
  // the boundary straddles element edges, so we do it manually by walking
  // the involved text nodes between start and end.
  const startIdx = nodes.indexOf(start.node);
  const endIdx = nodes.indexOf(end.node);
  if (startIdx < 0 || endIdx < 0) return null;

  for (let i = startIdx; i <= endIdx; i++) {
    const node = nodes[i];
    const from = i === startIdx ? start.local : 0;
    const to = i === endIdx ? end.local : node.data.length;
    if (to <= from) continue;

    const slice = node.splitText(from);
    if (to - from < slice.data.length) {
      slice.splitText(to - from);
    }
    const span = document.createElement("span");
    span.dataset.kiwiComment = "wrap";
    span.dataset.commentId = id;
    span.className = "kiwi-comment-mark";
    slice.parentNode?.insertBefore(span, slice);
    span.appendChild(slice);
    span.addEventListener("click", (e) => {
      e.stopPropagation();
      onClick(id, span.getBoundingClientRect());
    });
    spans.push(span);
  }

  return spans.length ? { id, spans } : null;
}

// Undo all wrapping done by wrapAnchor. Merges the text back into the
// surrounding parent so React's reconciler still sees a clean subtree when
// it next renders.
export function clearWraps(root: HTMLElement) {
  const wraps = root.querySelectorAll<HTMLSpanElement>(
    'span[data-kiwi-comment="wrap"]'
  );
  wraps.forEach((span) => {
    const parent = span.parentNode;
    if (!parent) return;
    while (span.firstChild) parent.insertBefore(span.firstChild, span);
    parent.removeChild(span);
  });
  // Merge adjacent text nodes so repeated wrap/clear cycles don't fragment
  // the DOM further.
  root.normalize();
}

// Capture an anchor from the current window selection, constrained to
// `root`. Returns null if the selection is empty, collapsed, or outside.
export function anchorFromSelection(
  root: HTMLElement,
  ctxChars = 24
): CommentAnchor | null {
  const sel = window.getSelection();
  if (!sel || sel.rangeCount === 0 || sel.isCollapsed) return null;
  const range = sel.getRangeAt(0);
  if (!root.contains(range.commonAncestorContainer)) return null;

  const quote = sel.toString();
  if (!quote.trim()) return null;

  const { text, nodes, offsets } = collectText(root);

  // Walk nodes once to find the range's start/end offsets in `text`.
  let startOffset = -1;
  let endOffset = -1;
  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    if (node === range.startContainer) startOffset = offsets[i] + range.startOffset;
    if (node === range.endContainer) endOffset = offsets[i] + range.endOffset;
  }
  if (startOffset < 0 || endOffset < 0) {
    // Selection endpoints land on element nodes rather than text — fall
    // back to a simple quote lookup so the anchor is still usable.
    const hit = text.indexOf(quote);
    if (hit < 0) return { quote };
    startOffset = hit;
    endOffset = hit + quote.length;
  }

  const prefix = text.slice(Math.max(0, startOffset - ctxChars), startOffset);
  const suffix = text.slice(endOffset, Math.min(text.length, endOffset + ctxChars));

  return { quote, prefix, suffix, offset: startOffset };
}
