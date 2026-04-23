import { useCallback, useEffect, useState } from "react";
import {
  applyKiwiTheme,
  removeKiwiTheme,
  type KiwiThemeOverrides,
} from "../lib/kiwiTheme";
import { api } from "../lib/api";
import { presets, presetToOverrides, findPreset } from "../themes";

export type Theme = "light" | "dark";

const LS_THEME = "kiwifs-theme";
const LS_PRESET = "kiwifs-preset";
const LS_CUSTOM_THEME = "kiwifs-custom-theme";

function readLS(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key) || fallback;
  } catch {
    return fallback;
  }
}

function writeLS(key: string, val: string) {
  try {
    localStorage.setItem(key, val);
  } catch {
    /* ignore */
  }
}

export function getCustomTheme(): KiwiThemeOverrides | null {
  try {
    const raw = localStorage.getItem(LS_CUSTOM_THEME);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function setCustomTheme(t: KiwiThemeOverrides | null) {
  if (t) {
    writeLS(LS_CUSTOM_THEME, JSON.stringify(t));
  } else {
    try {
      localStorage.removeItem(LS_CUSTOM_THEME);
    } catch {
      /* ignore */
    }
  }
}

export function useTheme(): {
  theme: Theme;
  toggleTheme: () => void;
  preset: string;
  setPreset: (name: string) => void;
  presets: typeof presets;
} {
  const [theme, setTheme] = useState<Theme>(() => {
    if (typeof document === "undefined") return "light";
    return document.documentElement.classList.contains("dark") ? "dark" : "light";
  });

  const [preset, setPresetState] = useState(() => readLS(LS_PRESET, "Kiwi"));

  useEffect(() => {
    const root = document.documentElement;
    if (theme === "dark") root.classList.add("dark");
    else root.classList.remove("dark");
    writeLS(LS_THEME, theme);
  }, [theme]);

  // On mount, fetch the server-side team default theme. localStorage preset
  // (per-user) overrides it — the server theme only kicks in when the user
  // hasn't picked a preset yet.
  useEffect(() => {
    const custom = getCustomTheme();
    if (custom) {
      applyKiwiTheme(custom);
      return;
    }

    const saved = readLS(LS_PRESET, "");
    if (saved) return;

    api.getTheme().then((data) => {
      const name = data?.preset as string | undefined;
      if (name) {
        const found = findPreset(name);
        if (found) {
          setPresetState(name);
          applyKiwiTheme(presetToOverrides(found));
          return;
        }
      }
      if (data?.light || data?.dark) {
        applyKiwiTheme(data as KiwiThemeOverrides);
      }
    }).catch(() => {});
  }, []);

  useEffect(() => {
    const custom = getCustomTheme();
    if (custom) {
      applyKiwiTheme(custom);
      return;
    }
    const found = findPreset(preset);
    if (found) {
      applyKiwiTheme(presetToOverrides(found));
    } else {
      removeKiwiTheme();
    }
  }, [preset]);

  const toggleTheme = useCallback(
    () => setTheme((t) => (t === "dark" ? "light" : "dark")),
    [],
  );

  const setPreset = useCallback((name: string) => {
    setCustomTheme(null);
    setPresetState(name);
    writeLS(LS_PRESET, name);
  }, []);

  return { theme, toggleTheme, preset, setPreset, presets };
}
