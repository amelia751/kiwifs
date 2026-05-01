import type { Meta, StoryObj } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import { KiwiPage } from "./KiwiPage";
import { MockApiProvider } from "./__mocks__/apiMock";
import {
  mockTree,
  mockMarkdownSimple,
  mockMarkdownRich,
  mockMarkdownExcalidraw,
  mockMarkdownRenderingTest,
} from "./__mocks__/data";

const meta: Meta<typeof KiwiPage> = {
  title: "Pages/KiwiPage",
  component: KiwiPage,
  parameters: { layout: "fullscreen" },
  args: {
    path: "concepts/frontmatter.md",
    tree: mockTree,
    onNavigate: action("navigate"),
    onEdit: action("edit"),
    onHistory: action("history"),
    onToggleStar: action("toggle-star"),
    isStarred: false,
    onTogglePin: action("toggle-pin"),
    isPinned: false,
    onDeleted: action("deleted"),
    onDuplicated: action("duplicated"),
    onMoved: action("moved"),
    onTagClick: action("tag-click"),
  },
  argTypes: {
    isStarred: { control: "boolean" },
    isPinned: { control: "boolean" },
  },
};

export default meta;
type Story = StoryObj<typeof KiwiPage>;

export const Default: Story = {
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ fileContent: mockMarkdownRich }}>
        <div className="h-screen bg-background text-foreground">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};

export const SimplePage: Story = {
  args: {
    path: "welcome.md",
  },
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ fileContent: mockMarkdownSimple }}>
        <div className="h-screen bg-background text-foreground">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};

export const NotFound: Story = {
  args: {
    path: "missing/page.md",
  },
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ fileStatus: 404 }}>
        <div className="h-screen bg-background text-foreground">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};

export const Loading: Story = {
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ delay: 999999 }}>
        <div className="h-screen bg-background text-foreground">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};

export const WithExcalidraw: Story = {
  args: {
    path: "diagrams/architecture.excalidraw.md",
  },
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ fileContent: mockMarkdownExcalidraw }}>
        <div className="h-screen bg-background text-foreground">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};

export const Starred: Story = {
  args: {
    isStarred: true,
  },
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ fileContent: mockMarkdownRich }}>
        <div className="h-screen bg-background text-foreground">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};

export const RenderingTest: Story = {
  args: {
    path: "tests/rendering-test.md",
  },
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ fileContent: mockMarkdownRenderingTest }}>
        <div className="h-screen bg-background text-foreground">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};

export const Pinned: Story = {
  args: {
    isPinned: true,
  },
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ fileContent: mockMarkdownRich }}>
        <div className="h-screen bg-background text-foreground">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};
