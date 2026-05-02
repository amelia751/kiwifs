import type { Meta, StoryObj } from "@storybook/react";
import { TreeSkeleton } from "./TreeSkeleton";

const meta: Meta<typeof TreeSkeleton> = {
  title: "Feedback/TreeSkeleton",
  component: TreeSkeleton,
  parameters: { layout: "padded" },
};

export default meta;
type Story = StoryObj<typeof TreeSkeleton>;

export const Default: Story = {
  decorators: [
    (Story) => (
      <div className="w-72 border border-border rounded-lg bg-background">
        <Story />
      </div>
    ),
  ],
};
