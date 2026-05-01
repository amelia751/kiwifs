import type { Meta, StoryObj } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import { KiwiSearch } from "./KiwiSearch";
import { MockApiProvider } from "./__mocks__/apiMock";
import { mockTree, mockSearchResults } from "./__mocks__/data";

const meta: Meta<typeof KiwiSearch> = {
  title: "Navigation/KiwiSearch",
  component: KiwiSearch,
  parameters: { layout: "fullscreen" },
  args: {
    open: true,
    onOpenChange: action("open-change"),
    onSelect: action("select"),
    tree: mockTree,
    hideModeToggle: true,
  },
};

export default meta;
type Story = StoryObj<typeof KiwiSearch>;

export const Closed: Story = {
  args: { open: false },
  decorators: [
    (Story) => (
      <MockApiProvider>
        <div className="bg-background text-foreground min-h-screen">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};

export const OpenEmpty: Story = {
  args: { open: true },
  decorators: [
    (Story) => (
      <MockApiProvider>
        <div className="bg-background text-foreground min-h-screen">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};

export const WithResults: Story = {
  args: {
    open: true,
    initialQuery: "frontmatter",
  },
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ searchResults: mockSearchResults }}>
        <div className="bg-background text-foreground min-h-screen">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};

export const NoResults: Story = {
  args: {
    open: true,
    initialQuery: "nonexistent-query-xyz",
  },
  decorators: [
    (Story) => (
      <MockApiProvider overrides={{ searchResults: [] }}>
        <div className="bg-background text-foreground min-h-screen">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};
