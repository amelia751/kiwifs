/**
 * Graph colors are defined in `src/styles/kiwi-theme.css` (--kiwi-graph-*).
 * This module reads the resolved values for WebGL (Sigma).
 */

export type KiwiGraphTheme = {
  defaultNode: string;
  edge: string;
  nodeDim: string;
  edgeGhost: string;
  edgeStrong: string;
  palette: string[];
};

const PALETTE_VAR_NAMES = Array.from(
  { length: 10 },
  (_, i) => `--kiwi-graph-palette-${i}`,
);

const DEFAULTS: KiwiGraphTheme = {
  defaultNode: "#8a8a8a",
  edge: "#d4d4d4",
  nodeDim: "#e5e5e5",
  edgeGhost: "#f0f0f0",
  edgeStrong: "#666666",
  palette: [
    "#555555", "#666666", "#777777", "#888888", "#999999",
    "#5e5e5e", "#707070", "#808080", "#909090", "#a0a0a0",
  ],
};

function pick(cs: CSSStyleDeclaration, name: string, fallback: string) {
  const v = cs.getPropertyValue(name).trim();
  return v || fallback;
}

export function readKiwiGraphTheme(
  el: Element = document.documentElement,
): KiwiGraphTheme {
  const cs = getComputedStyle(el);
  const pal = PALETTE_VAR_NAMES.map((name, i) =>
    pick(cs, name, DEFAULTS.palette[i] ?? "#808080"),
  );
  return {
    defaultNode: pick(cs, "--kiwi-graph-default-node", DEFAULTS.defaultNode),
    edge: pick(cs, "--kiwi-graph-edge", DEFAULTS.edge),
    nodeDim: pick(cs, "--kiwi-graph-node-dim", DEFAULTS.nodeDim),
    edgeGhost: pick(cs, "--kiwi-graph-edge-ghost", DEFAULTS.edgeGhost),
    edgeStrong: pick(cs, "--kiwi-graph-edge-strong", DEFAULTS.edgeStrong),
    palette: pal,
  };
}

export function colorForGraphCommunity(
  i: number,
  theme: KiwiGraphTheme,
): string {
  if (i < theme.palette.length) return theme.palette[i]!;
  const l = 42 + (i % 18) * 1.1;
  return `hsl(0 0% ${l}%)`;
}
