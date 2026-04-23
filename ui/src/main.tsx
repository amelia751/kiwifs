import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./index.css";
import {
  applyKiwiThemeFromUrl,
  applyKiwiThemeFromThemeUrl,
  listenForKiwiTheme,
} from "./lib/kiwiTheme";

declare global {
  interface Window {
    __KIWIFS_CONFIG__?: { allowedOrigins?: string[] };
  }
}

function getThemeOrigins(): string[] {
  const fromConfig = window.__KIWIFS_CONFIG__?.allowedOrigins;
  if (fromConfig?.length) return fromConfig;
  const param = new URLSearchParams(window.location.search).get("theme-origins");
  if (param) return param.split(",").map((o) => o.trim()).filter(Boolean);
  return [];
}

async function boot() {
  applyKiwiThemeFromUrl();
  await applyKiwiThemeFromThemeUrl();
  listenForKiwiTheme(getThemeOrigins());

  ReactDOM.createRoot(document.getElementById("root")!).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>,
  );
}

boot();
