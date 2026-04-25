import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// Kiwifs serves the built UI from ./dist via go:embed. The Go server handles
// /api/* and /health; the dev server proxies those during `npm run dev`.
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src"),
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/api": "http://localhost:3333",
      "/health": "http://localhost:3333",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
    // Each of these is a >200 kB dependency used by exactly one UI surface.
    // Without splitting, they all land in the initial bundle and block first
    // paint on slow links. React.lazy in App.tsx + these chunks together cut
    // the critical path from ~3 MB to ~800 kB.
    rollupOptions: {
      output: {
        manualChunks: (id) => {
          if (id.includes("/node_modules/")) {
            if (id.includes("@blocknote") || id.includes("yjs") || id.includes("prosemirror") || id.includes("@tiptap"))
              return "editor";
            if (id.includes("@react-sigma") || id.includes("/sigma/") || id.includes("graphology"))
              return "graph";
            if (id.includes("react-diff-viewer"))
              return "diff";
            // Only bundle the shiki *core* into a shared chunk. The per-
            // language grammars under @shikijs/langs/* must stay in their
            // own auto-split chunks so we lazy-load one grammar per code
            // block, not the entire 9 MB grammar catalogue up front.
            if (id.includes("@shikijs/core") || id.includes("@shikijs/vscode-textmate") || id.includes("@shikijs/engine") || id.match(/shiki\/dist\/(core|wasm)/))
              return "shiki-core";
            // Keep katex together with react-markdown / rehype-katex.
            // Splitting katex into its own chunk triggers a TDZ error
            // ("Cannot access 'ae' before initialization") because
            // rehype-katex and katex form a circular reference graph
            // that Rollup can only wire safely when they share a module
            // scope.
            if (
              id.includes("katex") ||
              id.includes("react-markdown") ||
              id.includes("remark-") ||
              id.includes("rehype-") ||
              id.includes("unified") ||
              id.includes("micromark") ||
              id.includes("mdast") ||
              id.includes("hast")
            )
              return "markdown";
          }
        },
      },
    },
    chunkSizeWarningLimit: 900,
  },
});
