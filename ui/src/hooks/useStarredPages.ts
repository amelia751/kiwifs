import { useCallback, useEffect, useState } from "react";

const BASE_KEY = "kiwifs-starred-pages";

function storageKey(space: string): string {
  return `${BASE_KEY}:${space}`;
}

function load(space: string): string[] {
  try {
    const raw = localStorage.getItem(storageKey(space));
    if (!raw) return [];
    return JSON.parse(raw) as string[];
  } catch {
    return [];
  }
}

function save(space: string, pages: string[]) {
  try {
    localStorage.setItem(storageKey(space), JSON.stringify(pages));
  } catch {}
}

export function useStarredPages(space: string = "default") {
  const [starred, setStarred] = useState<string[]>(() => load(space));

  useEffect(() => {
    setStarred(load(space));
  }, [space]);

  const toggle = useCallback(
    (path: string) => {
      setStarred((prev) => {
        const next = prev.includes(path)
          ? prev.filter((p) => p !== path)
          : [...prev, path];
        save(space, next);
        return next;
      });
    },
    [space]
  );

  const isStarred = useCallback(
    (path: string) => starred.includes(path),
    [starred]
  );

  return { starred, toggle, isStarred };
}
