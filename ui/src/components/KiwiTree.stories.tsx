import type { Meta, StoryObj } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import { KiwiTree } from "./KiwiTree";
import { MockApiProvider } from "./__mocks__/apiMock";
import { mockTree } from "./__mocks__/data";
import type { TreeEntry } from "@/lib/api";

const emptyTree: TreeEntry = {
  path: "",
  name: "",
  isDir: true,
  children: [],
};

const meta: Meta<typeof KiwiTree> = {
  title: "Navigation/KiwiTree",
  component: KiwiTree,
  parameters: { layout: "fullscreen" },
  args: {
    activePath: null,
    onSelect: action("select"),
    onCreateChild: action("create-child"),
    onDeleted: action("deleted"),
    onDuplicated: action("duplicated"),
    onMoved: action("moved"),
  },
};

export default meta;
type Story = StoryObj<typeof KiwiTree>;

export const Default: Story = {
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ tree: mockTree }}>
        <div className="w-64 h-screen bg-background text-foreground border-r border-border">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};

export const WithActiveFile: Story = {
  args: {
    activePath: "concepts/frontmatter.md",
  },
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ tree: mockTree }}>
        <div className="w-64 h-screen bg-background text-foreground border-r border-border">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};

export const EmptyState: Story = {
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ tree: emptyTree }}>
        <div className="w-64 h-screen bg-background text-foreground border-r border-border">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};
