import { useCallback, useEffect, useState } from "react";

const BASE_KEY = "kiwifs-recent-pages";
const MAX_RECENT = 10;

export type RecentPage = {
  path: string;
  timestamp: number;
};

function storageKey(space: string): string {
  return `${BASE_KEY}:${space}`;
}

function load(space: string): RecentPage[] {
  try {
    const raw = localStorage.getItem(storageKey(space));
    if (!raw) return [];
    return JSON.parse(raw) as RecentPage[];
  } catch {
    return [];
  }
}

function save(space: string, pages: RecentPage[]) {
  try {
    localStorage.setItem(storageKey(space), JSON.stringify(pages));
  } catch {}
}

export function useRecentPages(space: string = "default") {
  const [recent, setRecent] = useState<RecentPage[]>(() => load(space));

  useEffect(() => {
    setRecent(load(space));
  }, [space]);

  const recordVisit = useCallback(
    (path: string) => {
      setRecent((prev) => {
        const filtered = prev.filter((p) => p.path !== path);
        const next = [{ path, timestamp: Date.now() }, ...filtered].slice(
          0,
          MAX_RECENT
        );
        save(space, next);
        return next;
      });
    },
    [space]
  );

  return { recent, recordVisit };
}
