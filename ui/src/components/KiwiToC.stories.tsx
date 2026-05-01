import type { Meta, StoryObj } from "@storybook/react";
import { useRef } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { KiwiToC } from "./KiwiToC";
import { mockMarkdownRich } from "./__mocks__/data";

const fewHeadingsMarkdown = `## Introduction

Some intro text here.

## Conclusion

Wrapping up.
`;

const noHeadingsMarkdown = `Just a paragraph of plain text with no headings at all.

Another paragraph for good measure.
`;

function ToCWrapper({ markdown }: { markdown: string }) {
  const containerRef = useRef<HTMLDivElement>(null);
  return (
    <div className="flex gap-6 max-w-6xl mx-auto p-8 bg-background text-foreground min-h-screen">
      <article className="min-w-0 flex-1">
        <div ref={containerRef} className="kiwi-prose">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>{markdown}</ReactMarkdown>
        </div>
      </article>
      {/* Force-show ToC even on smaller viewports for storybook */}
      <aside className="w-64 shrink-0">
        <KiwiToC markdown={markdown} containerRef={containerRef as React.RefObject<HTMLElement>} />
      </aside>
    </div>
  );
}

const meta: Meta<typeof KiwiToC> = {
  title: "Content/KiwiToC",
  component: KiwiToC,
  parameters: { layout: "fullscreen" },
};

export default meta;
type Story = StoryObj<typeof KiwiToC>;

export const Default: Story = {
  render: () => <ToCWrapper markdown={mockMarkdownRich} />,
};

export const FewHeadings: Story = {
  render: () => <ToCWrapper markdown={fewHeadingsMarkdown} />,
};

export const NoHeadings: Story = {
  render: () => <ToCWrapper markdown={noHeadingsMarkdown} />,
};
