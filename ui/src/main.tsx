import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./index.css";
import {
  applyKiwiThemeFromUrl,
  applyKiwiThemeFromThemeUrl,
  listenForKiwiTheme,
} from "./lib/kiwiTheme";

applyKiwiThemeFromUrl();
applyKiwiThemeFromThemeUrl();
listenForKiwiTheme();

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
