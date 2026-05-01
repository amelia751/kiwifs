import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { BlockNoteEditor, filterSuggestionItems } from "@blocknote/core";
import {
  FormattingToolbarController,
  getDefaultReactSlashMenuItems,
  SuggestionMenuController,
  useCreateBlockNote,
} from "@blocknote/react";
import { BlockNoteView } from "@blocknote/mantine";
import "@blocknote/core/fonts/inter.css";
import "@blocknote/mantine/style.css";
import { Check, ChevronDown, ChevronRight, Circle, Info, Link as LinkIcon, ListTree, Loader2, Save, TriangleAlert, User, X, XCircle } from "lucide-react";
import { Plugin, PluginKey } from "prosemirror-state";
import { Decoration, DecorationSet } from "prosemirror-view";
import matter from "gray-matter";
import { api, type TreeEntry } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { dirOf, stem, titleize } from "@/lib/paths";
import { KiwiBreadcrumb } from "./KiwiBreadcrumb";
import { ExcalidrawMarkdownEditor, isExcalidrawMarkdown } from "./ExcalidrawMarkdownPreview";
import { formatDistanceToNow } from "date-fns";

const wikiLinkPluginKey = new PluginKey("kiwi-wiki-links");

function wikiLinkDecoPlugin() {
  return new Plugin({
    key: wikiLinkPluginKey,
    state: {
      init(_, state) {
        return buildWikiDecos(state.doc);
      },
      apply(tr, old) {
        if (!tr.docChanged) return old;
        return buildWikiDecos(tr.doc);
      },
    },
    props: {
      decorations(state) {
        return wikiLinkPluginKey.getState(state);
      },
    },
  });
}

function buildWikiDecos(doc: any): DecorationSet {
  const decos: Decoration[] = [];
  const re = /\[\[([^\]]+)\]\]/g;
  doc.descendants((node: any, pos: number) => {
    if (!node.isText) return;
    const text = node.text || "";
    let m: RegExpExecArray | null;
    while ((m = re.exec(text)) !== null) {
      const from = pos + m.index;
      const to = from + m[0].length;
      decos.push(
        Decoration.inline(from, to, { class: "kiwi-editor-wikilink" })
      );
    }
  });
  return DecorationSet.create(doc, decos);
}

type SaveStatus = "clean" | "dirty" | "saving" | "saved" | "error";

type SaveHandle = { save: () => Promise<void> };

type Props = {
  path: string;
  tree?: import("@/lib/api").TreeEntry | null;
  onClose: () => void;
  onSaved: (path: string) => void;
  onNavigate?: (path: string) => void;
  saveRef?: React.MutableRefObject<SaveHandle | null>;
};

export function KiwiEditor({ path, tree, onClose, onSaved, onNavigate, saveRef }: Props) {
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

  if (isExcalidrawMarkdown(initialMd)) {
    return (
      <ExcalidrawEditorInner
        path={path}
        initialMd={initialMd}
        etagRef={etagRef}
        saving={saving}
        setSaving={setSaving}
        setError={setError}
        onClose={onClose}
        onSaved={onSaved}
        onNavigate={onNavigate}
        saveRef={saveRef}
      />
    );
  }

  return (
    <EditorInner
      path={path}
      tree={tree}
      initialMd={initialMd}
      etagRef={etagRef}
      isDark={isDark}
      saving={saving}
      setSaving={setSaving}
      setError={setError}
      onClose={onClose}
      onSaved={onSaved}
      onNavigate={onNavigate}
      saveRef={saveRef}
    />
  );
}

function ExcalidrawEditorInner({
  path,
  initialMd,
  etagRef,
  saving,
  setSaving,
  setError,
  onClose,
  onSaved,
  onNavigate,
  saveRef,
}: {
  path: string;
  initialMd: string;
  etagRef: React.MutableRefObject<string | null>;
  saving: boolean;
  setSaving: (v: boolean) => void;
  setError: (v: string | null) => void;
  onClose: () => void;
  onSaved: (p: string) => void;
  onNavigate?: (path: string) => void;
  saveRef?: React.MutableRefObject<SaveHandle | null>;
}) {
  const [currentMd, setCurrentMd] = useState(initialMd);
  const [saveStatus, setSaveStatus] = useState<SaveStatus>("clean");
  const savedFlashTimer = useRef<number | null>(null);

  const fmTitle = useMemo(() => {
    try {
      const parsed = matter(initialMd);
      if (typeof parsed.data?.title === "string") return parsed.data.title;
    } catch {}
    return null;
  }, [initialMd]);

  const onSaveRef = useRef<(opts?: { close?: boolean }) => Promise<void>>(async () => {});
  onSaveRef.current = async (opts) => {
    setSaving(true);
    setSaveStatus("saving");
    setError(null);
    try {
      const res = await api.writeFile(path, currentMd, etagRef.current || undefined);
      etagRef.current = res.etag ? `"${res.etag}"` : null;
      setSaveStatus("saved");
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

  const markChanged = useCallback((nextMd: string) => {
    setCurrentMd(nextMd);
    setSaveStatus(nextMd === initialMd ? "clean" : "dirty");
  }, [initialMd]);

  useEffect(() => {
    if (!saveRef) return;
    saveRef.current = { save: () => onSaveRef.current({ close: true }) };
    return () => { saveRef.current = null; };
  }, [saveRef]);

  useEffect(() => {
    return () => {
      if (savedFlashTimer.current) window.clearTimeout(savedFlashTimer.current);
    };
  }, []);

  return (
    <div className="flex flex-col h-full">
      <div className="sticky top-0 z-10 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80 border-b border-border shrink-0">
        <div className="px-8 py-2 max-w-6xl mx-auto">
          {onNavigate
            ? <KiwiBreadcrumb path={path} onNavigate={onNavigate} />
            : <div className="text-sm text-muted-foreground font-mono truncate">{path}</div>}
        </div>
      </div>

      <div className="flex-1 overflow-auto kiwi-scroll">
        <div className="max-w-6xl mx-auto px-8 py-6">
          <div className="mb-6">
            <div className="flex items-start justify-between gap-4">
              <div className="min-w-0">
                <h1 className="text-2xl font-bold tracking-tight text-foreground leading-tight">
                  {fmTitle || titleize(path)}
                </h1>
                <div className="flex items-center gap-2 mt-2">
                  <SaveIndicator status={saveStatus} />
                </div>
              </div>
              <div className="flex items-center gap-2 shrink-0 pt-1">
                <Button
                  onClick={() => onSaveRef.current({ close: true })}
                  disabled={saving || saveStatus === "clean"}
                  size="sm"
                  variant={saveStatus === "dirty" ? "default" : "outline"}
                >
                  <Save className="h-3.5 w-3.5" />
                  {saving ? "Saving…" : "Save & Close"}
                </Button>
                <Button variant="outline" size="sm" onClick={onClose}>
                  <X className="h-3.5 w-3.5" /> Close
                </Button>
              </div>
            </div>
          </div>

          <ExcalidrawMarkdownEditor markdown={initialMd} onChange={markChanged} />
        </div>
      </div>
    </div>
  );
}

function EditorInner({
  path,
  tree,
  initialMd,
  etagRef,
  isDark,
  saving,
  setSaving,
  setError,
  onClose,
  onSaved,
  onNavigate,
  saveRef,
}: {
  path: string;
  tree?: import("@/lib/api").TreeEntry | null;
  initialMd: string;
  etagRef: React.MutableRefObject<string | null>;
  isDark: boolean;
  saving: boolean;
  setSaving: (v: boolean) => void;
  setError: (v: string | null) => void;
  onClose: () => void;
  onSaved: (p: string) => void;
  onNavigate?: (path: string) => void;
  saveRef?: React.MutableRefObject<SaveHandle | null>;
}) {
  const [ready, setReady] = useState(false);
  const [saveStatus, setSaveStatus] = useState<SaveStatus>("clean");
  const autoSaveTimer = useRef<number | null>(null);
  const savedFlashTimer = useRef<number | null>(null);
  const [fmOpen, setFmOpen] = useState(false);
  const [fmText, setFmText] = useState<string>(() => {
    try {
      const parsed = matter(initialMd);
      const raw = parsed.matter?.trim();
      return raw || "";
    } catch { return ""; }
  });
  const bodyOnly = useMemo(() => {
    try {
      const parsed = matter(initialMd);
      let body = parsed.content;
      if (typeof parsed.data?.title === "string") {
        const h1Match = body.match(/^\s*#\s+(.+)\n?/);
        if (h1Match && h1Match[1].trim() === parsed.data.title.trim()) {
          body = body.replace(/^\s*#\s+.+\n?/, "");
        }
      }
      return body;
    } catch { return initialMd; }
  }, [initialMd]);
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

  const editorOptions = useMemo(
    () => ({
      uploadFile,
      _tiptapOptions: {
        extensions: [] as any[],
      },
    }),
    [uploadFile],
  );
  const editor = useCreateBlockNote(editorOptions);

  useEffect(() => {
    if (!editor) return;
    const pm = (editor as any)._tiptapEditor?.view;
    if (!pm) return;
    const state = pm.state;
    if (state.plugins.some((p: any) => p.key === (wikiLinkPluginKey as any).key)) return;
    const newState = state.reconfigure({
      plugins: [...state.plugins, wikiLinkDecoPlugin()],
    });
    pm.updateState(newState);
  }, [editor]);

  useEffect(() => {
    if (!editor) return;
    let cancelled = false;
    (async () => {
      const blocks = await editor.tryParseMarkdownToBlocks(bodyOnly);
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
      let md = await editor.blocksToMarkdownLossy(editor.document);
      if (fmText.trim()) {
        md = "---\n" + fmText.trim() + "\n---\n\n" + md;
      }
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

  const fmTitle = useMemo(() => {
    try {
      const parsed = matter(initialMd);
      if (typeof parsed.data?.title === "string") return parsed.data.title;
    } catch {}
    return null;
  }, [initialMd]);

  return (
    <div className="flex flex-col h-full">
      {/* ── Sticky breadcrumb — matches KiwiPage structure ── */}
      <div className="sticky top-0 z-10 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80 border-b border-border shrink-0">
        <div className="px-8 py-2 max-w-6xl mx-auto">
          {onNavigate
            ? <KiwiBreadcrumb path={path} onNavigate={onNavigate} />
            : <div className="text-sm text-muted-foreground font-mono truncate">{path}</div>}
        </div>
      </div>

      {/* ── Scrollable content ── */}
      <div className="flex-1 overflow-auto kiwi-scroll">
        <div className="max-w-6xl mx-auto px-8 py-6">
          {/* ── Page header zone — matches KiwiPage structure ── */}
          <div className="mb-6">
            <div className="flex items-start justify-between gap-4">
              <div className="min-w-0">
                <h1 className="text-2xl font-bold tracking-tight text-foreground leading-tight">
                  {fmTitle || titleize(path)}
                </h1>
                <div className="flex items-center gap-2 mt-2">
                  <SaveIndicator status={saveStatus} />
                </div>
              </div>
              <div className="flex items-center gap-2 shrink-0 pt-1">
                <Button
                  onClick={() => onSaveRef.current({ close: true })}
                  disabled={saving || !ready || saveStatus === "clean"}
                  size="sm"
                  variant={saveStatus === "dirty" ? "default" : "outline"}
                >
                  <Save className="h-3.5 w-3.5" />
                  {saving ? "Saving…" : "Save & Close"}
                </Button>
                <Button variant="outline" size="sm" onClick={onClose}>
                  <X className="h-3.5 w-3.5" /> Close
                </Button>
              </div>
            </div>

            {/* ── Metadata bar ── */}
            {lastEdit && (
              <div className="flex items-center gap-3 mt-3 text-xs text-muted-foreground">
                <span className="flex items-center gap-1">
                  <User className="h-3 w-3" />
                  Last edited by {lastEdit.author} {relativeTime(lastEdit.date)}
                </span>
              </div>
            )}
          </div>

          {/* ── Frontmatter section ── */}
          <div className="max-w-3xl mb-4">
            <button
              type="button"
              onClick={() => setFmOpen((v) => !v)}
              className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              {fmOpen ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
              Frontmatter
              {fmText.trim() && <span className="ml-1 text-[10px] opacity-60">(has data)</span>}
            </button>
            {fmOpen && (
              <Textarea
                value={fmText}
                onChange={(e) => { setFmText(e.target.value); markDirty(); }}
                placeholder={"title: My Page\ntags:\n  - draft"}
                className="mt-2 font-mono text-xs min-h-[80px] resize-y"
                rows={Math.max(3, fmText.split("\n").length)}
              />
            )}
          </div>

          {/* ── Editor content zone ── */}
          <div className="max-w-3xl kiwi-blocknote min-h-[50vh]">
            {editor && (
              <BlockNoteView
                editor={editor as BlockNoteEditor}
                theme={isDark ? "dark" : "light"}
                slashMenu={false}
                formattingToolbar={false}
                onChange={markDirty}
              >
                <FormattingToolbarController />
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
                <SuggestionMenuController
                  triggerCharacter="["
                  getItems={async (query) => {
                    const pm = (editor as any)._tiptapEditor;
                    if (pm?.view) {
                      const { state } = pm.view;
                      const pos = state.selection.from;
                      const checkPos = pos - query.length - 2;
                      if (checkPos < 0 || state.doc.textBetween(checkPos, checkPos + 1) !== "[") {
                        return [];
                      }
                    }
                    return filterSuggestionItems(
                      collectPages(tree).map((p) => {
                        const pageName = p.replace(/\.md$/i, "");
                        return {
                          title: titleize(p),
                          subtext: p,
                          aliases: [stem(p), p],
                          group: "Page link",
                          icon: <LinkIcon size={18} />,
                          onItemClick: () => {
                            queueMicrotask(() => {
                              const ttp = (editor as any)._tiptapEditor;
                              if (!ttp?.view) return;
                              const { state } = ttp.view;
                              const pos = state.selection.from;
                              if (pos > 0 && state.doc.textBetween(pos - 1, pos) === "[") {
                                ttp.view.dispatch(
                                  state.tr.delete(pos - 1, pos).insertText(`[[${pageName}]]`, pos - 1)
                                );
                              } else {
                                ttp.view.dispatch(state.tr.insertText(`[[${pageName}]]`, pos));
                              }
                            });
                          },
                        };
                      }),
                      query
                    );
                  }}
                />
              </BlockNoteView>
            )}
          </div>
        </div>
      </div>
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

function collectPages(tree: TreeEntry | null | undefined): string[] {
  if (!tree) return [];
  const pages: string[] = [];
  function walk(node: TreeEntry) {
    if (!node.isDir && node.path.toLowerCase().endsWith(".md")) {
      pages.push(node.path);
    }
    if (node.children) node.children.forEach(walk);
  }
  walk(tree);
  return pages;
}
