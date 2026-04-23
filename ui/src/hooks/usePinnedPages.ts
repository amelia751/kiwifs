import { useCallback, useEffect, useState } from "react";

const BASE_KEY = "kiwifs-pinned-pages";

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

function persist(space: string, pages: string[]) {
  try {
    localStorage.setItem(storageKey(space), JSON.stringify(pages));
  } catch {}
}

export function usePinnedPages(space: string = "default") {
  const [pinned, setPinned] = useState<string[]>(() => load(space));

  useEffect(() => {
    setPinned(load(space));
  }, [space]);

  const toggle = useCallback(
    (path: string) => {
      setPinned((prev) => {
        const next = prev.includes(path)
          ? prev.filter((p) => p !== path)
          : [...prev, path];
        persist(space, next);
        return next;
      });
    },
    [space]
  );

  const isPinned = useCallback(
    (path: string) => pinned.includes(path),
    [pinned]
  );

  const reorder = useCallback(
    (fromIdx: number, toIdx: number) => {
      setPinned((prev) => {
        const next = [...prev];
        const [moved] = next.splice(fromIdx, 1);
        next.splice(toIdx, 0, moved);
        persist(space, next);
        return next;
      });
    },
    [space]
  );

  return { pinned, toggle, isPinned, reorder };
}
