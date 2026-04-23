import { useEffect, useState } from "react";
import { getHighlighter, hasLang } from "@/lib/shiki";

type Props = {
  code: string;
  lang?: string;
};

// Inline code highlighter. We render the raw `<code>` first and upgrade to
// Shiki output on mount; that way copy/paste works immediately even on slow
// devices and missing languages degrade gracefully.
export function ShikiCode({ code, lang }: Props) {
  const [html, setHtml] = useState<string | null>(null);
  const isDark =
    typeof document !== "undefined" &&
    document.documentElement.classList.contains("dark");

  useEffect(() => {
    let cancelled = false;
    if (!lang || !hasLang(lang)) return;
    getHighlighter().then((hl) => {
      if (cancelled) return;
      try {
        const rendered = hl.codeToHtml(code, {
          lang,
          theme: isDark ? "github-dark" : "github-light",
        });
        setHtml(rendered);
      } catch {
        /* ignore; fall back to plaintext <pre> */
      }
    });
    return () => {
      cancelled = true;
    };
  }, [code, lang, isDark]);

  if (html) {
    return (
      <div
        className="kiwi-shiki my-4 text-sm rounded-md overflow-hidden [&>pre]:p-4 [&>pre]:overflow-x-auto"
        dangerouslySetInnerHTML={{ __html: html }}
      />
    );
  }
  return (
    <pre>
      <code>{code}</code>
    </pre>
  );
}
