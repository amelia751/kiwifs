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
  },
});
