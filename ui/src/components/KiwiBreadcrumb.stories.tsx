import type { Meta, StoryObj } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import { KiwiBreadcrumb } from "./KiwiBreadcrumb";

const meta: Meta<typeof KiwiBreadcrumb> = {
  title: "Navigation/KiwiBreadcrumb",
  component: KiwiBreadcrumb,
  parameters: { layout: "padded" },
  args: {
    onNavigate: action("navigate"),
  },
  decorators: [
    (Story) => (
      <div className="bg-background text-foreground p-4">
        <Story />
      </div>
    ),
  ],
};

export default meta;
type Story = StoryObj<typeof KiwiBreadcrumb>;

export const RootLevel: Story = {
  args: { path: "index.md" },
};

export const OneLevelDeep: Story = {
  args: { path: "concepts/frontmatter.md" },
};

export const TwoLevelsDeep: Story = {
  args: { path: "projects/kiwi/architecture.md" },
};

export const DeeplyNested: Story = {
  args: { path: "team/engineering/frontend/components/button.md" },
};
