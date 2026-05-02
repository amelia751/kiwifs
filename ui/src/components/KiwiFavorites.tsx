import { useEffect, useState } from "react";
import { Clock, Pin, Star } from "lucide-react";
import { titleize } from "@/lib/paths";

const RECENT_KEY = "kiwi-recent-pages";
const FAVORITES_KEY = "kiwi-favorite-pages";
const MAX_RECENT = 15;

export function trackRecent(path: string) {
  try {
    const list = JSON.parse(localStorage.getItem(RECENT_KEY) || "[]") as string[];
    const filtered = list.filter((p) => p !== path);
    filtered.unshift(path);
    localStorage.setItem(RECENT_KEY, JSON.stringify(filtered.slice(0, MAX_RECENT)));
  } catch { /* ignore */ }
}

export function getFavorites(): string[] {
  try {
    return JSON.parse(localStorage.getItem(FAVORITES_KEY) || "[]") as string[];
  } catch {
    return [];
  }
}

export function toggleFavorite(path: string): boolean {
  const favs = getFavorites();
  const idx = favs.indexOf(path);
  if (idx >= 0) {
    favs.splice(idx, 1);
    localStorage.setItem(FAVORITES_KEY, JSON.stringify(favs));
    return false;
  }
  favs.unshift(path);
  localStorage.setItem(FAVORITES_KEY, JSON.stringify(favs));
  return true;
}

type Props = {
  onSelect: (path: string) => void;
  refreshKey?: number;
};

export function KiwiFavorites({ onSelect, refreshKey }: Props) {
  const [favorites, setFavorites] = useState<string[]>([]);
  const [recents, setRecents] = useState<string[]>([]);

  useEffect(() => {
    setFavorites(getFavorites());
    try {
      setRecents(JSON.parse(localStorage.getItem(RECENT_KEY) || "[]") as string[]);
    } catch {
      setRecents([]);
    }
  }, [refreshKey]);

  if (favorites.length === 0 && recents.length === 0) return null;

  return (
    <div className="px-2 py-2 text-sm space-y-3">
      {favorites.length > 0 && (
        <div>
          <div className="flex items-center gap-1.5 px-2 py-1 text-xs font-medium text-muted-foreground uppercase tracking-wider">
            <Star className="h-3 w-3" />
            Favorites
          </div>
          {favorites.map((path) => (
            <button
              key={path}
              type="button"
              onClick={() => onSelect(path)}
              className="w-full flex items-center gap-1.5 px-2 py-1 rounded-md text-left hover:bg-accent hover:text-accent-foreground transition-colors truncate"
            >
              <Pin className="h-3 w-3 text-muted-foreground shrink-0" />
              <span className="truncate">{titleize(path)}</span>
            </button>
          ))}
        </div>
      )}
      {recents.length > 0 && (
        <div>
          <div className="flex items-center gap-1.5 px-2 py-1 text-xs font-medium text-muted-foreground uppercase tracking-wider">
            <Clock className="h-3 w-3" />
            Recent
          </div>
          {recents.slice(0, 8).map((path) => (
            <button
              key={path}
              type="button"
              onClick={() => onSelect(path)}
              className="w-full flex items-center gap-1.5 px-2 py-1 rounded-md text-left hover:bg-accent hover:text-accent-foreground transition-colors truncate"
            >
              <Clock className="h-3 w-3 text-muted-foreground shrink-0" />
              <span className="truncate">{titleize(path)}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
