/**
 * External theme injection for KiwiFS.
 *
 * KiwiFS uses CSS custom properties (shadcn convention). This module lets a
 * host app override any subset of those tokens so KiwiFS matches the host's
 * design system.
 *
 * Entry points:
 *  1. applyKiwiTheme(overrides)           — JS API, works anywhere
 *  2. listenForKiwiTheme(allowedOrigins)  — postMessage listener (iframe)
 *  3. applyKiwiThemeFromUrl()             — ?theme=<base64> URL param (iframe)
 *  4. applyKiwiThemeFromThemeUrl()        — ?theme-url=<url> (fetch JSON)
 *
 * Token format:
 *  - Colour tokens use HSL triplets without the `hsl()` wrapper: "65 80% 55%"
 *  - Graph tokens use hex: "#8a8a8a"
 *  - radius uses rem: "0.625rem"
 *  - Filters use CSS filter syntax or "none"
 *
 * Cascade order (highest wins):
 *  1. postMessage overrides (host iframe → KiwiFS)
 *  2. URL param (?theme= or ?theme-url=)
 *  3. localStorage custom theme
 *  4. Server theme (GET /api/kiwi/theme)
 *  5. Built-in CSS (kiwi-theme.css)
 */

export interface KiwiTokens {
  background?: string;
  foreground?: string;
  card?: string;
  "card-foreground"?: string;
  popover?: string;
  "popover-foreground"?: string;
  primary?: string;
  "primary-foreground"?: string;
  "primary-hover"?: string;
  secondary?: string;
  "secondary-foreground"?: string;
  "secondary-hover"?: string;
  muted?: string;
  "muted-foreground"?: string;
  accent?: string;
  "accent-foreground"?: string;
  "accent-hover"?: string;
  destructive?: string;
  "destructive-foreground"?: string;
  "destructive-hover"?: string;
  border?: string;
  input?: string;
  ring?: string;
  radius?: string;
  "ghost-hover-foreground"?: string;
  "link-hover-opacity"?: string;
  "kiwi-shiki-filter"?: string;
  "kiwi-blocknote-filter"?: string;
  "kiwi-graph-default-node"?: string;
  "kiwi-graph-edge"?: string;
  "kiwi-graph-node-dim"?: string;
  "kiwi-graph-edge-ghost"?: string;
  "kiwi-graph-edge-strong"?: string;
  "kiwi-graph-palette-0"?: string;
  "kiwi-graph-palette-1"?: string;
  "kiwi-graph-palette-2"?: string;
  "kiwi-graph-palette-3"?: string;
  "kiwi-graph-palette-4"?: string;
  "kiwi-graph-palette-5"?: string;
  "kiwi-graph-palette-6"?: string;
  "kiwi-graph-palette-7"?: string;
  "kiwi-graph-palette-8"?: string;
  "kiwi-graph-palette-9"?: string;
  "font-sans"?: string;
  "font-mono"?: string;
  "font-serif"?: string;
  "font-size-base"?: string;
  "font-size-sm"?: string;
  "font-size-lg"?: string;
  "line-height-base"?: string;
  "line-height-tight"?: string;
  "spacing-unit"?: string;
  "content-max-width"?: string;
  "sidebar-width"?: string;
  "heading-1-size"?: string;
  "heading-2-size"?: string;
  "heading-3-size"?: string;
  "heading-4-size"?: string;
  "heading-scale"?: string;
  "code-font-size"?: string;
  "code-bg"?: string;
  "code-border"?: string;
  "link-decoration"?: string;
  "link-color"?: string;
  [key: string]: string | undefined;
}

export interface KiwiThemeOverrides {
  mode?: "light" | "dark" | "system";
  light?: KiwiTokens;
  dark?: KiwiTokens;
}

const STYLE_ID = "kiwi-theme-overrides";

function tokensToCss(selector: string, tokens: KiwiTokens): string {
  const entries = Object.entries(tokens).filter(
    (e): e is [string, string] => e[1] != null,
  );
  if (entries.length === 0) return "";
  const props = entries.map(([k, v]) => `  --${k}: ${v};`).join("\n");
  return `${selector} {\n${props}\n}\n`;
}

function applyMode(mode: KiwiThemeOverrides["mode"]): void {
  if (!mode) return;
  const root = document.documentElement;
  if (mode === "dark") {
    root.classList.add("dark");
  } else if (mode === "light") {
    root.classList.remove("dark");
  } else {
    const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
    if (prefersDark) root.classList.add("dark");
    else root.classList.remove("dark");
  }
}

export function applyKiwiTheme(overrides: KiwiThemeOverrides): void {
  applyMode(overrides.mode);

  let css = "";
  if (overrides.light) css += tokensToCss(":root", overrides.light);
  if (overrides.dark) css += tokensToCss(".dark", overrides.dark);
  if (!css) return;

  document.getElementById(STYLE_ID)?.remove();
  const style = document.createElement("style");
  style.id = STYLE_ID;
  style.textContent = css;
  document.head.appendChild(style);
}

export function removeKiwiTheme(): void {
  document.getElementById(STYLE_ID)?.remove();
}

export function applyKiwiThemeFromUrl(): boolean {
  const params = new URLSearchParams(window.location.search);
  const param = params.get("theme");
  if (!param) return false;
  try {
    const overrides: KiwiThemeOverrides = JSON.parse(atob(param));
    applyKiwiTheme(overrides);
    return true;
  } catch {
    return false;
  }
}

export async function applyKiwiThemeFromThemeUrl(): Promise<boolean> {
  const params = new URLSearchParams(window.location.search);
  const url = params.get("theme-url");
  if (!url) return false;
  try {
    const res = await fetch(url);
    if (!res.ok) return false;
    const overrides: KiwiThemeOverrides = await res.json();
    applyKiwiTheme(overrides);
    return true;
  } catch {
    return false;
  }
}

export function listenForKiwiTheme(
  allowedOrigins: string[] = [],
): () => void {
  const handler = (e: MessageEvent) => {
    if (allowedOrigins.length && !allowedOrigins.includes(e.origin)) return;
    const data = e.data;
    if (
      typeof data === "object" &&
      data !== null &&
      data.type === "kiwifs:theme"
    ) {
      applyKiwiTheme(data.tokens as KiwiThemeOverrides);
    }
  };
  window.addEventListener("message", handler);
  return () => window.removeEventListener("message", handler);
}
