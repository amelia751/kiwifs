import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Download, Upload, Undo2, X } from "lucide-react";
import { HexColorPicker } from "react-colorful";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "./ui/popover";
import { applyKiwiTheme, type KiwiThemeOverrides, type KiwiTokens } from "../lib/kiwiTheme";
import { getCustomTheme, setCustomTheme } from "../hooks/useTheme";
import { api } from "../lib/api";

interface TokenGroup {
  label: string;
  tokens: { key: keyof KiwiTokens; label: string }[];
}

interface TextTokenGroup {
  label: string;
  tokens: { key: string; label: string; placeholder: string }[];
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
    label: "Secondary",
    tokens: [
      { key: "secondary", label: "Secondary" },
      { key: "secondary-foreground", label: "Secondary text" },
      { key: "secondary-hover", label: "Secondary hover" },
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
      { key: "popover-foreground", label: "Popover text" },
      { key: "muted-foreground", label: "Muted text" },
    ],
  },
  {
    label: "Borders & Accents",
    tokens: [
      { key: "border", label: "Border" },
      { key: "input", label: "Input border" },
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
  {
    label: "Code Blocks",
    tokens: [
      { key: "code-bg", label: "Code background" },
      { key: "code-border", label: "Code border" },
    ],
  },
];

const TEXT_TOKEN_GROUPS: TextTokenGroup[] = [
  {
    label: "Typography",
    tokens: [
      { key: "font-sans", label: "Sans font", placeholder: "ui-sans-serif, system-ui, sans-serif" },
      { key: "font-mono", label: "Mono font", placeholder: "ui-monospace, monospace" },
      { key: "font-serif", label: "Serif font", placeholder: "ui-serif, Georgia, serif" },
      { key: "font-size-base", label: "Base size", placeholder: "1rem" },
      { key: "font-size-sm", label: "Small size", placeholder: "0.875rem" },
      { key: "font-size-lg", label: "Large size", placeholder: "1.125rem" },
      { key: "line-height-base", label: "Line height", placeholder: "1.75" },
      { key: "line-height-tight", label: "Tight line height", placeholder: "1.3" },
    ],
  },
  {
    label: "Spacing & Layout",
    tokens: [
      { key: "spacing-unit", label: "Spacing unit", placeholder: "8px" },
      { key: "content-max-width", label: "Content width", placeholder: "48rem" },
      { key: "sidebar-width", label: "Sidebar width", placeholder: "288px" },
      { key: "radius", label: "Border radius", placeholder: "0.625rem" },
    ],
  },
  {
    label: "Headings",
    tokens: [
      { key: "heading-scale", label: "Scale multiplier", placeholder: "1" },
      { key: "heading-1-size", label: "H1 size", placeholder: "1.875rem" },
      { key: "heading-2-size", label: "H2 size", placeholder: "1.5rem" },
      { key: "heading-3-size", label: "H3 size", placeholder: "1.25rem" },
      { key: "heading-4-size", label: "H4 size", placeholder: "1.125rem" },
    ],
  },
  {
    label: "Code",
    tokens: [
      { key: "code-font-size", label: "Code font size", placeholder: "0.875em" },
    ],
  },
  {
    label: "Links",
    tokens: [
      { key: "link-color", label: "Link color", placeholder: "var(--foreground)" },
      { key: "link-decoration", label: "Decoration", placeholder: "underline" },
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
  for (const group of TEXT_TOKEN_GROUPS) {
    for (const t of group.tokens) {
      const val = style.getPropertyValue(`--${t.key}`).trim();
      if (val) tokens[t.key] = val;
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
  // baseline captures the last-saved theme at mount so we can offer a
  // working "Revert to saved" button. Without this, an editor that
  // butchers the foreground color can only recover by clearing local
  // storage manually — exactly the "sidebar disappears" scenario the
  // audit flagged.
  const [baselineLight, setBaselineLight] = useState<KiwiTokens>({});
  const [baselineDark, setBaselineDark] = useState<KiwiTokens>({});
  const [savedAt, setSavedAt] = useState<number | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const existing = getCustomTheme();
    if (existing) {
      setLightTokens(existing.light || {});
      setDarkTokens(existing.dark || {});
      setBaselineLight(existing.light || {});
      setBaselineDark(existing.dark || {});
    } else {
      const current = getCurrentTokens();
      if (isDark) {
        setDarkTokens(current);
      } else {
        setLightTokens(current);
      }
    }
  }, [isDark]);

  // Mark edits dirty so the header can show "unsaved changes" and the
  // footer can enable the revert button only when it matters.
  const isDirty = useMemo(() => {
    return (
      JSON.stringify(lightTokens) !== JSON.stringify(baselineLight) ||
      JSON.stringify(darkTokens) !== JSON.stringify(baselineDark)
    );
  }, [lightTokens, darkTokens, baselineLight, baselineDark]);

  // Warn before navigating away mid-edit — keeps a 50% typed palette
  // from vanishing on accidental Esc / Cmd+W.
  useEffect(() => {
    if (!isDirty) return undefined;
    const h = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = "";
    };
    window.addEventListener("beforeunload", h);
    return () => window.removeEventListener("beforeunload", h);
  }, [isDirty]);

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

  const updateTextToken = useCallback(
    (key: string, value: string) => {
      setActiveTokens((prev) => {
        const next = { ...prev, [key]: value || undefined };
        if (!value) delete next[key];
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
    setBaselineLight({ ...lightTokens });
    setBaselineDark({ ...darkTokens });
    setSavedAt(Date.now());
  }, [lightTokens, darkTokens]);

  const revert = useCallback(() => {
    const overrides: KiwiThemeOverrides = { light: baselineLight, dark: baselineDark };
    setLightTokens(baselineLight);
    setDarkTokens(baselineDark);
    applyKiwiTheme(overrides);
  }, [baselineLight, baselineDark]);

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
        {isDirty ? (
          <span className="text-xs text-amber-600 dark:text-amber-400 px-2 py-0.5 rounded bg-amber-500/10">
            Preview · unsaved
          </span>
        ) : savedAt ? (
          <span className="text-xs text-emerald-600 dark:text-emerald-400 px-2 py-0.5 rounded bg-emerald-500/10">
            Saved
          </span>
        ) : null}
        <span className="text-xs text-muted-foreground px-2 py-0.5 rounded bg-muted">
          {isDark ? "Dark" : "Light"} mode
        </span>
        <Button
          variant="ghost"
          size="icon"
          onClick={onClose}
          aria-label="Close theme editor"
          title="Close theme editor"
        >
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
                  <Popover key={t.key as string}>
                    <PopoverTrigger asChild>
                      <button className="flex items-center gap-2 group cursor-pointer rounded-md px-1.5 py-1 -mx-1.5 hover:bg-accent transition-colors">
                        <span
                          className="h-7 w-7 rounded-md border border-border shrink-0 shadow-sm"
                          style={{ background: hex }}
                        />
                        <div className="text-left">
                          <Label className="text-xs cursor-pointer">{t.label}</Label>
                          <div className="text-[10px] text-muted-foreground font-mono">{hex}</div>
                        </div>
                      </button>
                    </PopoverTrigger>
                    <PopoverContent side="right" align="start" className="w-auto p-3">
                      <HexColorPicker
                        color={hex}
                        onChange={(c) => updateToken(t.key as string, c)}
                      />
                      <input
                        type="text"
                        value={hex}
                        onChange={(e) => {
                          const v = e.target.value;
                          if (/^#[0-9a-fA-F]{6}$/.test(v)) updateToken(t.key as string, v);
                        }}
                        className="mt-2 w-full text-xs font-mono px-2 py-1 rounded border border-border bg-background text-foreground"
                      />
                    </PopoverContent>
                  </Popover>
                );
              })}
            </div>
          </div>
        ))}

        <div className="h-px bg-border" />

        {TEXT_TOKEN_GROUPS.map((group) => (
          <div key={group.label}>
            <h3 className="text-sm font-medium text-muted-foreground mb-3">
              {group.label}
            </h3>
            <div className="grid grid-cols-2 gap-3">
              {group.tokens.map((t) => (
                <div key={t.key}>
                  <Label className="text-xs">{t.label}</Label>
                  <Input
                    value={activeTokens[t.key] || ""}
                    onChange={(e) => updateTextToken(t.key, e.target.value)}
                    placeholder={t.placeholder}
                    className="h-8 text-xs font-mono mt-1"
                  />
                </div>
              ))}
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
        <Button
          variant="outline"
          size="sm"
          onClick={revert}
          disabled={!isDirty}
          title="Discard unsaved edits and restore the last saved theme"
        >
          <Undo2 className="h-3.5 w-3.5 mr-1.5" />
          Revert
        </Button>
        <Button
          size="sm"
          onClick={save}
          disabled={!isDirty}
          title={isDirty ? "Publish this theme to everyone" : "No unsaved changes"}
        >
          Save theme
        </Button>
      </footer>
    </div>
  );
}
