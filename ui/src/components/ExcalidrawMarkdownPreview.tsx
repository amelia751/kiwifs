import { Suspense, lazy, useCallback, useEffect, useMemo, useState } from "react";
import LZString from "lz-string";
import "@excalidraw/excalidraw/index.css";

type ExcalidrawScene = {
  type?: string;
  version?: number;
  source?: string;
  elements?: any[];
  appState?: Record<string, any>;
  files?: Record<string, any>;
};

type PreviewProps = {
  markdown: string;
  title?: string;
};

type EditorProps = {
  markdown: string;
  onChange: (markdown: string) => void;
};

const ExcalidrawCanvas = lazy(() =>
  import("@excalidraw/excalidraw").then((module) => ({ default: module.Excalidraw })),
);

const DRAWING_JSON_RE = /\n(##? Drawing\n[^`]*```json\n)([\s\S]*?)(```\n?)/m;
const DRAWING_COMPRESSED_RE = /\n(##? Drawing\n[^`]*```compressed-json\n)([\s\S]*?)(```\n?)/m;
const DRAWING_JSON_FALLBACK_RE = /(##? Drawing\n(?:```json\n)?)([\s\S]*?)((?:```)?(?:\n%%)?\s*)$/m;
const DRAWING_COMPRESSED_FALLBACK_RE = /(##? Drawing\n(?:```compressed-json\n)?)([\s\S]*?)((?:```)?(?:\n%%)?\s*)$/m;

export function isExcalidrawMarkdown(content: string, meta: Record<string, unknown> = {}): boolean {
  return meta["excalidraw-plugin"] === "parsed"
    || /# Excalidraw Data\s+## Text Elements/.test(content)
    || /##? Drawing\n[^`]*```(?:compressed-)?json\n/.test(content);
}

export function parseExcalidrawMarkdown(content: string): ExcalidrawScene | null {
  const compressed = matchFirst(content, DRAWING_COMPRESSED_RE, DRAWING_COMPRESSED_FALLBACK_RE);
  if (compressed) {
    const json = LZString.decompressFromBase64(compressed.replace(/[\r\n]/g, ""));
    if (!json) return null;
    return parseScene(json);
  }

  const json = matchFirst(content, DRAWING_JSON_RE, DRAWING_JSON_FALLBACK_RE);
  return json ? parseScene(json.slice(0, json.lastIndexOf("}") + 1)) : null;
}

export function serializeExcalidrawMarkdown(content: string, scene: ExcalidrawScene): string {
  const normalizedScene: ExcalidrawScene = {
    ...scene,
    type: scene.type ?? "excalidraw",
    version: scene.version ?? 2,
    source: scene.source ?? "https://github.com/zsviczian/obsidian-excalidraw-plugin",
    elements: scene.elements ?? [],
    appState: sanitizeAppStateForStorage(scene.appState),
    files: scene.files ?? {},
  };
  const json = JSON.stringify(normalizedScene);

  if (DRAWING_COMPRESSED_RE.test(content) || DRAWING_COMPRESSED_FALLBACK_RE.test(content)) {
    const compressed = LZString.compressToBase64(json);
    return replaceDrawing(content, compressed, DRAWING_COMPRESSED_RE, DRAWING_COMPRESSED_FALLBACK_RE);
  }

  return replaceDrawing(content, json, DRAWING_JSON_RE, DRAWING_JSON_FALLBACK_RE);
}

function matchFirst(content: string, primary: RegExp, fallback: RegExp): string | null {
  const primaryMatch = content.match(primary);
  if (primaryMatch?.[2]) return primaryMatch[2].trim();
  const fallbackMatch = content.match(fallback);
  if (fallbackMatch?.[2]) return fallbackMatch[2].trim();
  return null;
}

function replaceDrawing(content: string, data: string, primary: RegExp, fallback: RegExp): string {
  const replacement = (_match: string, prefix: string, _old: string, suffix: string) => `${prefix}${data}\n${suffix}`;
  if (primary.test(content)) return content.replace(primary, replacement);
  return content.replace(fallback, replacement);
}

function parseScene(raw: string): ExcalidrawScene | null {
  try {
    const parsed = JSON.parse(raw) as ExcalidrawScene;
    if (!Array.isArray(parsed.elements)) return null;
    return parsed;
  } catch {
    return null;
  }
}

function sanitizeAppStateForStorage(appState: Record<string, any> = {}): Record<string, any> {
  const { collaborators: _collaborators, ...serializableAppState } = appState;
  return serializableAppState;
}

function appStateForExcalidraw(appState: Record<string, any> = {}): Record<string, any> {
  return {
    ...sanitizeAppStateForStorage(appState),
    collaborators: new Map(),
  };
}

export function ExcalidrawMarkdownEditor({ markdown, onChange }: EditorProps) {
  const initialScene = useMemo(() => parseExcalidrawMarkdown(markdown), [markdown]);
  const [error, setError] = useState<string | null>(null);

  const handleChange = useCallback((elements: readonly any[], appState: Record<string, any>, files: Record<string, any>) => {
    if (!initialScene) return;
    const nextScene: ExcalidrawScene = {
      ...initialScene,
      elements: [...elements],
      appState: sanitizeAppStateForStorage({
        ...(initialScene.appState ?? {}),
        ...appState,
      }),
      files: files ?? {},
    };
    try {
      onChange(serializeExcalidrawMarkdown(markdown, nextScene));
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, [initialScene, markdown, onChange]);

  if (!initialScene) {
    return (
      <div className="rounded-md border border-destructive/40 bg-destructive/10 p-4 text-sm text-destructive">
        Unable to parse Excalidraw drawing data.
      </div>
    );
  }

  return (
    <div className="kiwi-excalidraw-editor h-[70vh] min-h-[520px] overflow-hidden rounded-lg border border-border bg-background">
      {error && (
        <div className="border-b border-destructive/40 bg-destructive/10 px-3 py-2 text-xs text-destructive">
          {error}
        </div>
      )}
      <Suspense fallback={<div className="p-4 text-sm text-muted-foreground">Loading Excalidraw editor…</div>}>
        <ExcalidrawCanvas
          initialData={{
            elements: initialScene.elements ?? [],
            appState: appStateForExcalidraw(initialScene.appState),
            files: initialScene.files ?? {},
          }}
          onChange={handleChange}
        />
      </Suspense>
    </div>
  );
}

export function ExcalidrawMarkdownPreview({ markdown, title }: PreviewProps) {
  const scene = useMemo(() => parseExcalidrawMarkdown(markdown), [markdown]);
  const [svg, setSvg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setSvg(null);
    setError(null);

    if (!scene) {
      setError("Unable to parse Excalidraw drawing data.");
      return () => { cancelled = true; };
    }

    import("@excalidraw/excalidraw")
      .then(({ exportToSvg }) => exportToSvg({
        elements: (scene.elements ?? []).filter((el) => el?.isDeleted !== true),
        appState: {
          ...sanitizeAppStateForStorage(scene.appState),
          exportBackground: scene.appState?.exportBackground ?? true,
        },
        files: scene.files ?? {},
        exportPadding: 16,
      }))
      .then((node: SVGSVGElement) => {
        if (cancelled) return;
        node.classList.add("kiwi-excalidraw-svg");
        node.setAttribute("role", "img");
        node.setAttribute("preserveAspectRatio", "xMidYMid meet");
        node.removeAttribute("width");
        node.removeAttribute("height");
        node.setAttribute("style", "display:block;width:100%;height:auto;max-width:100%;");
        if (title) node.setAttribute("aria-label", title);
        setSvg(node.outerHTML);
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      });

    return () => { cancelled = true; };
  }, [scene, title]);

  if (error) {
    return (
      <div className="rounded-md border border-destructive/40 bg-destructive/10 p-4 text-sm text-destructive">
        {error}
      </div>
    );
  }

  if (!svg) {
    return <div className="text-sm text-muted-foreground">Rendering Excalidraw…</div>;
  }

  return (
    <div className="kiwi-excalidraw-preview overflow-auto rounded-lg border border-border bg-white p-3">
      <div className="mx-auto w-full" dangerouslySetInnerHTML={{ __html: svg }} />
    </div>
  );
}
