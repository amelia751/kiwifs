import { useCallback, useEffect, useRef, useState } from "react";
import { Download, Upload, X } from "lucide-react";
import { Button } from "./ui/button";
import { Label } from "./ui/label";
import { applyKiwiTheme, type KiwiThemeOverrides, type KiwiTokens } from "../lib/kiwiTheme";
import { getCustomTheme, setCustomTheme } from "../hooks/useTheme";
import { api } from "../lib/api";

interface TokenGroup {
  label: string;
  tokens: { key: keyof KiwiTokens; label: string }[];
}

const TOKEN_GROUPS: TokenGroup[] = [
  {
    label: "Brand / Primary",
    tokens: [
      { key: "primary", label: "Primary" },
      { key: "primary-foreground", label: "Primary text" },
      { key: "primary-hover", label: "Primary hover" },
    ],
  },
  {
    label: "Backgrounds",
    tokens: [
      { key: "background", label: "Background" },
      { key: "card", label: "Card" },
      { key: "popover", label: "Popover" },
      { key: "muted", label: "Muted" },
    ],
  },
  {
    label: "Text",
    tokens: [
      { key: "foreground", label: "Foreground" },
      { key: "card-foreground", label: "Card text" },
      { key: "muted-foreground", label: "Muted text" },
    ],
  },
  {
    label: "Borders & Accents",
    tokens: [
      { key: "border", label: "Border" },
      { key: "ring", label: "Ring" },
      { key: "accent", label: "Accent" },
      { key: "accent-foreground", label: "Accent text" },
    ],
  },
  {
    label: "Destructive",
    tokens: [
      { key: "destructive", label: "Destructive" },
      { key: "destructive-foreground", label: "Destructive text" },
    ],
  },
];

function hslToHex(hsl: string): string {
  const parts = hsl.trim().split(/\s+/);
  if (parts.length < 3) return "#888888";
  const h = parseFloat(parts[0]);
  const s = parseFloat(parts[1]) / 100;
  const l = parseFloat(parts[2]) / 100;
  const a = s * Math.min(l, 1 - l);
  const f = (n: number) => {
    const k = (n + h / 30) % 12;
    const color = l - a * Math.max(Math.min(k - 3, 9 - k, 1), -1);
    return Math.round(255 * color).toString(16).padStart(2, "0");
  };
  return `#${f(0)}${f(8)}${f(4)}`;
}

function hexToHsl(hex: string): string {
  const r = parseInt(hex.slice(1, 3), 16) / 255;
  const g = parseInt(hex.slice(3, 5), 16) / 255;
  const b = parseInt(hex.slice(5, 7), 16) / 255;
  const max = Math.max(r, g, b);
  const min = Math.min(r, g, b);
  const l = (max + min) / 2;
  if (max === min) return `0 0% ${Math.round(l * 100)}%`;
  const d = max - min;
  const s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
  let h = 0;
  if (max === r) h = ((g - b) / d + (g < b ? 6 : 0)) * 60;
  else if (max === g) h = ((b - r) / d + 2) * 60;
  else h = ((r - g) / d + 4) * 60;
  return `${Math.round(h)} ${Math.round(s * 100)}% ${Math.round(l * 100)}%`;
}

function getCurrentTokens(): KiwiTokens {
  const style = getComputedStyle(document.documentElement);
  const tokens: KiwiTokens = {};
  for (const group of TOKEN_GROUPS) {
    for (const t of group.tokens) {
      const val = style.getPropertyValue(`--${t.key as string}`).trim();
      if (val) tokens[t.key as string] = val;
    }
  }
  return tokens;
}

interface Props {
  onClose: () => void;
  onPresetReset: () => void;
}

export function KiwiThemeEditor({ onClose, onPresetReset }: Props) {
  const isDark = document.documentElement.classList.contains("dark");
  const [lightTokens, setLightTokens] = useState<KiwiTokens>({});
  const [darkTokens, setDarkTokens] = useState<KiwiTokens>({});
  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const existing = getCustomTheme();
    if (existing) {
      setLightTokens(existing.light || {});
      setDarkTokens(existing.dark || {});
    } else {
      const current = getCurrentTokens();
      if (isDark) {
        setDarkTokens(current);
      } else {
        setLightTokens(current);
      }
    }
  }, [isDark]);

  const activeTokens = isDark ? darkTokens : lightTokens;
  const setActiveTokens = isDark ? setDarkTokens : setLightTokens;

  const updateToken = useCallback(
    (key: string, hex: string) => {
      const hsl = hexToHsl(hex);
      setActiveTokens((prev) => {
        const next = { ...prev, [key]: hsl };
        const overrides: KiwiThemeOverrides = isDark
          ? { light: lightTokens, dark: next }
          : { light: next, dark: darkTokens };
        applyKiwiTheme(overrides);
        return next;
      });
    },
    [isDark, lightTokens, darkTokens, setActiveTokens],
  );

  const save = useCallback(() => {
    const overrides: KiwiThemeOverrides = { light: lightTokens, dark: darkTokens };
    setCustomTheme(overrides);
    applyKiwiTheme(overrides);
    api.putTheme(overrides as unknown as Record<string, unknown>).catch(() => {});
  }, [lightTokens, darkTokens]);

  const handleExport = useCallback(() => {
    const overrides = { light: lightTokens, dark: darkTokens };
    const blob = new Blob([JSON.stringify(overrides, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "kiwifs-theme.json";
    a.click();
    URL.revokeObjectURL(url);
  }, [lightTokens, darkTokens]);

  const handleImport = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = () => {
        try {
          const data = JSON.parse(reader.result as string) as KiwiThemeOverrides;
          if (data.light) setLightTokens(data.light);
          if (data.dark) setDarkTokens(data.dark);
          applyKiwiTheme(data);
          setCustomTheme(data);
        } catch {
          /* ignore invalid */
        }
      };
      reader.readAsText(file);
      e.target.value = "";
    },
    [],
  );

  const handleReset = useCallback(() => {
    setCustomTheme(null);
    onPresetReset();
    onClose();
  }, [onPresetReset, onClose]);

  return (
    <div className="h-full flex flex-col">
      <header className="flex items-center gap-2 px-6 py-4 border-b border-border shrink-0">
        <h2 className="text-lg font-semibold flex-1">Theme Editor</h2>
        <span className="text-xs text-muted-foreground px-2 py-0.5 rounded bg-muted">
          {isDark ? "Dark" : "Light"} mode
        </span>
        <Button variant="ghost" size="icon" onClick={onClose}>
          <X className="h-4 w-4" />
        </Button>
      </header>

      <div className="flex-1 overflow-auto p-6 space-y-6 kiwi-scroll">
        {TOKEN_GROUPS.map((group) => (
          <div key={group.label}>
            <h3 className="text-sm font-medium text-muted-foreground mb-3">
              {group.label}
            </h3>
            <div className="grid grid-cols-2 gap-3">
              {group.tokens.map((t) => {
                const val = activeTokens[t.key as string] || "";
                const hex = val ? hslToHex(val) : "#888888";
                return (
                  <div key={t.key as string} className="flex items-center gap-2">
                    <input
                      type="color"
                      value={hex}
                      onChange={(e) => updateToken(t.key as string, e.target.value)}
                      className="h-8 w-8 rounded border border-border cursor-pointer shrink-0"
                    />
                    <Label className="text-xs">{t.label}</Label>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>

      <footer className="flex items-center gap-2 px-6 py-3 border-t border-border shrink-0">
        <Button variant="outline" size="sm" onClick={handleExport}>
          <Download className="h-3.5 w-3.5 mr-1.5" />
          Export
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={() => fileInputRef.current?.click()}
        >
          <Upload className="h-3.5 w-3.5 mr-1.5" />
          Import
        </Button>
        <input
          ref={fileInputRef}
          type="file"
          accept=".json"
          className="hidden"
          onChange={handleImport}
        />
        <Button variant="ghost" size="sm" onClick={handleReset}>
          Reset to preset
        </Button>
        <div className="flex-1" />
        <Button size="sm" onClick={save}>
          Save theme
        </Button>
      </footer>
    </div>
  );
}
