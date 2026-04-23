// Shared Shiki highlighter, lazy-initialized once. We only load a few common
// languages by default; missing langs fall back to no-highlight plaintext.

import type { Highlighter } from "shiki";

let instance: Promise<Highlighter> | null = null;

const LANGS = [
  "typescript",
  "javascript",
  "tsx",
  "jsx",
  "json",
  "bash",
  "shell",
  "python",
  "go",
  "rust",
  "yaml",
  "toml",
  "sql",
  "markdown",
  "html",
  "css",
];

export function getHighlighter(): Promise<Highlighter> {
  if (instance) return instance;
  instance = import("shiki").then(({ createHighlighter }) =>
    createHighlighter({
      themes: ["github-light", "github-dark"],
      langs: LANGS,
    })
  );
  return instance;
}

export function hasLang(lang: string | undefined): boolean {
  if (!lang) return false;
  return LANGS.includes(lang);
}
