import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { BlockNoteEditor, filterSuggestionItems } from "@blocknote/core";
import {
  getDefaultReactSlashMenuItems,
  SuggestionMenuController,
  useCreateBlockNote,
} from "@blocknote/react";
import { BlockNoteView } from "@blocknote/mantine";
import "@blocknote/core/fonts/inter.css";
import "@blocknote/mantine/style.css";
import { Check, Circle, Info, Link as LinkIcon, ListTree, Loader2, Save, TriangleAlert, User, X, XCircle } from "lucide-react";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { dirOf } from "@/lib/paths";
import { formatDistanceToNow } from "date-fns";

type SaveStatus = "clean" | "dirty" | "saving" | "saved" | "error";

type SaveHandle = { save: () => Promise<void> };

type Props = {
  path: string;
  onClose: () => void;
  onSaved: (path: string) => void;
  // Exposes the save action upward so a global Cmd+S can fire it without
  // requiring focus inside the editor. Cleared on unmount.
  saveRef?: React.MutableRefObject<SaveHandle | null>;
};

export function KiwiEditor({ path, onClose, onSaved, saveRef }: Props) {
  const [initialMd, setInitialMd] = useState<string | null>(null);
  const etagRef = useRef<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isDark, setIsDark] = useState<boolean>(() =>
    typeof document !== "undefined" &&
    document.documentElement.classList.contains("dark")
  );

  useEffect(() => {
    const obs = new MutationObserver(() =>
      setIsDark(document.documentElement.classList.contains("dark"))
    );
    obs.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class"],
    });
    return () => obs.disconnect();
  }, []);

  useEffect(() => {
    let cancelled = false;
    api
      .readFile(path)
      .then((r) => {
        if (cancelled) return;
        etagRef.current = r.etag;
        setInitialMd(r.content || "");
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [path]);

  if (error) {
    return (
      <div className="p-8 text-sm text-destructive font-mono">{error}</div>
    );
  }
  if (initialMd === null) {
    return (
      <div className="p-8 text-sm text-muted-foreground">Loading editor…</div>
    );
  }

  return (
    <EditorInner
      path={path}
      initialMd={initialMd}
      etagRef={etagRef}
      isDark={isDark}
      saving={saving}
      setSaving={setSaving}
      setError={setError}
      onClose={onClose}
      onSaved={onSaved}
      saveRef={saveRef}
    />
  );
}

function EditorInner({
  path,
  initialMd,
  etagRef,
  isDark,
  saving,
  setSaving,
  setError,
  onClose,
  onSaved,
  saveRef,
}: {
  path: string;
  initialMd: string;
  etagRef: React.MutableRefObject<string | null>;
  isDark: boolean;
  saving: boolean;
  setSaving: (v: boolean) => void;
  setError: (v: string | null) => void;
  onClose: () => void;
  onSaved: (p: string) => void;
  saveRef?: React.MutableRefObject<SaveHandle | null>;
}) {
  const [ready, setReady] = useState(false);
  const [saveStatus, setSaveStatus] = useState<SaveStatus>("clean");
  const autoSaveTimer = useRef<number | null>(null);
  const savedFlashTimer = useRef<number | null>(null);
  const [lastEdit, setLastEdit] = useState<{ author: string; date: string } | null>(null);

  useEffect(() => {
    let cancelled = false;
    api.versions(path).then((r) => {
      if (cancelled || !r.versions.length) return;
      const v = r.versions[0];
      setLastEdit({ author: v.author, date: v.date });
    }).catch(() => {});
    return () => { cancelled = true; };
  }, [path]);

  const uploadFile = useCallback(
    async (file: File) => {
      const targetDir = dirOf(path);
      return api.uploadAsset(file, targetDir);
    },
    [path],
  );

  const editorOptions = useMemo(() => ({ uploadFile }), [uploadFile]);
  const editor = useCreateBlockNote(editorOptions);

  useEffect(() => {
    if (!editor) return;
    let cancelled = false;
    (async () => {
      const blocks = await editor.tryParseMarkdownToBlocks(initialMd);
      if (cancelled) return;
      if (blocks && blocks.length > 0) {
        editor.replaceBlocks(editor.document, blocks);
      }
      setReady(true);
    })();
    return () => {
      cancelled = true;
    };
  }, [editor, initialMd]);

  const onSaveRef = useRef<(opts?: { close?: boolean }) => Promise<void>>(async () => {});
  onSaveRef.current = async (opts) => {
    if (!editor) return;
    setSaving(true);
    setSaveStatus("saving");
    setError(null);
    try {
      const md = await editor.blocksToMarkdownLossy(editor.document);
      const res = await api.writeFile(path, md, etagRef.current || undefined);
      etagRef.current = res.etag ? `"${res.etag}"` : null;
      setSaveStatus("saved");
      setLastEdit({ author: "you", date: new Date().toISOString() });
      if (savedFlashTimer.current) window.clearTimeout(savedFlashTimer.current);
      savedFlashTimer.current = window.setTimeout(() => setSaveStatus("clean"), 2000);
      if (opts?.close) onSaved(path);
    } catch (e) {
      setSaveStatus("error");
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  const markDirty = useCallback(() => {
    if (!ready) return;
    setSaveStatus("dirty");
    if (autoSaveTimer.current) window.clearTimeout(autoSaveTimer.current);
    autoSaveTimer.current = window.setTimeout(() => {
      onSaveRef.current();
    }, 2000);
  }, [ready]);

  useEffect(() => {
    return () => {
      if (autoSaveTimer.current) window.clearTimeout(autoSaveTimer.current);
      if (savedFlashTimer.current) window.clearTimeout(savedFlashTimer.current);
    };
  }, []);

  useEffect(() => {
    if (!saveRef) return;
    saveRef.current = { save: () => onSaveRef.current({ close: true }) };
    return () => { saveRef.current = null; };
  }, [saveRef]);

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-8 py-3 border-b border-border">
        <div className="flex items-center gap-2 min-w-0">
          <div className="text-sm text-muted-foreground font-mono truncate">
            {path}
          </div>
          <SaveIndicator status={saveStatus} />
        </div>
        <div className="flex items-center gap-2">
          <Button
            onClick={() => onSaveRef.current({ close: true })}
            disabled={saving || !ready || saveStatus === "clean"}
            size="sm"
            variant={saveStatus === "dirty" ? "default" : "outline"}
          >
            <Save className="h-3.5 w-3.5" />
            {saving ? "Saving…" : "Save"}
          </Button>
          <Button variant="outline" size="sm" onClick={onClose}>
            <X className="h-3.5 w-3.5" /> Close
          </Button>
        </div>
      </div>
      <div className="flex-1 overflow-auto kiwi-scroll py-6 px-4">
        <div className="max-w-3xl mx-auto kiwi-blocknote min-h-[50vh]">
          {editor && (
            <BlockNoteView
              editor={editor as BlockNoteEditor}
              theme={isDark ? "dark" : "light"}
              slashMenu={false}
              onChange={markDirty}
            >
              <SuggestionMenuController
                triggerCharacter="/"
                getItems={async (query) =>
                  filterSuggestionItems(
                    [
                      ...getDefaultReactSlashMenuItems(editor as BlockNoteEditor),
                      ...kiwiSlashItems(editor as BlockNoteEditor),
                    ],
                    query
                  )
                }
              />
            </BlockNoteView>
          )}
        </div>
      </div>
      {lastEdit && (
        <div className="px-8 py-2 border-t border-border text-xs text-muted-foreground flex items-center gap-2">
          <User className="h-3 w-3" />
          Last edited by {lastEdit.author} {relativeTime(lastEdit.date)}
        </div>
      )}
    </div>
  );
}

function SaveIndicator({ status }: { status: SaveStatus }) {
  switch (status) {
    case "dirty":
      return (
        <span className="flex items-center gap-1 text-xs text-amber-500">
          <Circle className="h-2.5 w-2.5 fill-current" />
          Unsaved
        </span>
      );
    case "saving":
      return (
        <span className="flex items-center gap-1 text-xs text-muted-foreground">
          <Loader2 className="h-3 w-3 animate-spin" />
          Saving…
        </span>
      );
    case "saved":
      return (
        <span className="flex items-center gap-1 text-xs text-green-500">
          <Check className="h-3 w-3" />
          Saved
        </span>
      );
    case "error":
      return (
        <span className="flex items-center gap-1 text-xs text-destructive">
          <XCircle className="h-3 w-3" />
          Error
        </span>
      );
    default:
      return null;
  }
}

function relativeTime(d: string): string {
  try {
    const parsed = new Date(d);
    if (isNaN(parsed.getTime())) return d;
    return formatDistanceToNow(parsed, { addSuffix: true });
  } catch {
    return d;
  }
}

// Kiwifs-specific slash commands. Each returns a paragraph block that renders
// as the desired output after we round-trip through markdown on save.
function kiwiSlashItems(editor: BlockNoteEditor) {
  const insertParagraph = (text: string) => {
    const cur = editor.getTextCursorPosition().block;
    editor.insertBlocks(
      [{ type: "paragraph", content: text }],
      cur,
      "after"
    );
  };

  return [
    {
      title: "Wiki link",
      subtext: "Insert a [[page-name]] link",
      aliases: ["link", "wiki", "[[", "ref"],
      group: "KiwiFS",
      icon: <LinkIcon size={18} />,
      onItemClick: () => insertParagraph("[[page-name]]"),
    },
    {
      title: "Info callout",
      subtext: "ℹ️ Highlighted info block",
      aliases: ["callout", "info", "note"],
      group: "KiwiFS",
      icon: <Info size={18} />,
      onItemClick: () => insertParagraph("ℹ️ "),
    },
    {
      title: "Warning callout",
      subtext: "⚠️ Highlighted warning block",
      aliases: ["callout", "warn", "warning"],
      group: "KiwiFS",
      icon: <TriangleAlert size={18} />,
      onItemClick: () => insertParagraph("⚠️ "),
    },
    {
      title: "Error callout",
      subtext: "🛑 Highlighted error block",
      aliases: ["callout", "error", "danger"],
      group: "KiwiFS",
      icon: <XCircle size={18} />,
      onItemClick: () => insertParagraph("🛑 "),
    },
    {
      title: "Table of contents marker",
      subtext: "Insert a <!-- toc --> marker",
      aliases: ["toc", "contents"],
      group: "KiwiFS",
      icon: <ListTree size={18} />,
      onItemClick: () => insertParagraph("<!-- toc -->"),
    },
  ];
}
