import type { Meta, StoryObj } from "@storybook/react";
import { ExcalidrawMarkdownPreview } from "./ExcalidrawMarkdownPreview";
import { mockMarkdownExcalidraw } from "./__mocks__/data";

const meta: Meta<typeof ExcalidrawMarkdownPreview> = {
  title: "Content/ExcalidrawMarkdownPreview",
  component: ExcalidrawMarkdownPreview,
  parameters: { layout: "padded" },
  decorators: [
    (Story) => (
      <div className="max-w-4xl mx-auto p-8 bg-background text-foreground">
        <Story />
      </div>
    ),
  ],
};

export default meta;
type Story = StoryObj<typeof ExcalidrawMarkdownPreview>;

export const DefaultPreview: Story = {
  args: {
    markdown: mockMarkdownExcalidraw,
    title: "Architecture Diagram",
  },
};

export const ParseError: Story = {
  args: {
    markdown: `---
excalidraw-plugin: parsed
---

## Drawing

\`\`\`json
{ this is not valid json !!!
\`\`\`
`,
    title: "Broken Drawing",
  },
};
