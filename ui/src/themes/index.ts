import type { KiwiThemeOverrides } from "../lib/kiwiTheme";

import kiwi from "./kiwi.json";
import neutral from "./neutral.json";
import ocean from "./ocean.json";
import sunset from "./sunset.json";
import forest from "./forest.json";

export interface ThemePreset {
  name: string;
  description: string;
  light: Record<string, string>;
  dark: Record<string, string>;
}

export const presets: ThemePreset[] = [kiwi, neutral, ocean, sunset, forest];

export function presetToOverrides(preset: ThemePreset): KiwiThemeOverrides {
  return { light: preset.light, dark: preset.dark };
}

export function findPreset(name: string): ThemePreset | undefined {
  return presets.find((p) => p.name.toLowerCase() === name.toLowerCase());
}
