import { type RefObject } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import remarkMath from "remark-math";
import rehypeSlug from "rehype-slug";
import rehypeRaw from "rehype-raw";
import rehypeKatex from "rehype-katex";
import rehypeAutolinkHeadings from "rehype-autolink-headings";
import { buildResolver, remarkWikiLinks } from "@/lib/wikiLinks";
import { mockTree } from "./data";

const resolver = buildResolver(mockTree);

/**
 * Shared markdown renderer for stories that matches the KiwiPage plugin chain.
 * Pass body-only markdown (frontmatter already stripped).
 */
export function StoryMarkdown({
  children,
  innerRef,
  className = "kiwi-prose",
}: {
  children: string;
  innerRef?: RefObject<HTMLDivElement>;
  className?: string;
}) {
  return (
    <div ref={innerRef} className={className}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm, remarkMath, [remarkWikiLinks, { resolver }]]}
        rehypePlugins={[
          rehypeRaw,
          rehypeKatex,
          rehypeSlug,
          [rehypeAutolinkHeadings, { behavior: "wrap" }],
        ]}
        components={{
          a: ({ href, children: kids, ...rest }) => {
            const h = href ?? "";
            if (h.startsWith("kiwi:")) {
              const target = h.slice("kiwi:".length);
              const exists = resolver(target) !== null;
              return (
                <a
                  href="#"
                  className={exists ? "wiki-link" : "wiki-link-missing"}
                  onClick={(e) => e.preventDefault()}
                  {...rest}
                >
                  {kids}
                </a>
              );
            }
            return <a href={h} {...rest}>{kids}</a>;
          },
          table: ({ children, node: _node, ...rest }: any) => (
            <div className="kiwi-table-wrapper">
              <table {...rest}>{children}</table>
            </div>
          ),
        }}
      >
        {children}
      </ReactMarkdown>
    </div>
  );
}
