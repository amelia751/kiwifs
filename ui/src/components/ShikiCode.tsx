import { useEffect, useState } from "react";
import { Check, Copy } from "lucide-react";
import { getHighlighter, hasLang } from "@/lib/shiki";

type Props = {
  code: string;
  lang?: string;
};

function CopyButton({ code }: { code: string }) {
  const [copied, setCopied] = useState(false);

  function handleCopy() {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }

  return (
    <button
      onClick={handleCopy}
      className="absolute top-2 right-2 p-1.5 rounded-md bg-background/80 border border-border text-muted-foreground hover:text-foreground transition-opacity opacity-0 group-hover:opacity-100"
      aria-label="Copy code"
    >
      {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
    </button>
  );
}

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
      <div className="relative group">
        <div
          className="kiwi-shiki my-4 text-sm rounded-md overflow-hidden [&>pre]:p-4 [&>pre]:overflow-x-auto"
          dangerouslySetInnerHTML={{ __html: html }}
        />
        <CopyButton code={code} />
      </div>
    );
  }
  return (
    <div className="relative group">
      <pre>
        <code>{code}</code>
      </pre>
      <CopyButton code={code} />
    </div>
  );
}
