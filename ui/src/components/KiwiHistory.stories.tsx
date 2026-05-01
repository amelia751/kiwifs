import type { Meta, StoryObj } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import { KiwiHistory } from "./KiwiHistory";
import { MockApiProvider } from "./__mocks__/apiMock";

const meta: Meta<typeof KiwiHistory> = {
  title: "Content/KiwiHistory",
  component: KiwiHistory,
  parameters: { layout: "fullscreen" },
  args: {
    path: "concepts/frontmatter.md",
    onClose: action("close"),
    onRestored: action("restored"),
  },
};

export default meta;
type Story = StoryObj<typeof KiwiHistory>;

export const Default: Story = {
  decorators: [
    (Story) => (
      <MockApiProvider>
        <div className="h-screen bg-background text-foreground">
          <Story />
        </div>
      </MockApiProvider>
    ),
  ],
};
