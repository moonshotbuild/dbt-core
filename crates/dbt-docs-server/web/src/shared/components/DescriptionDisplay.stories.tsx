import type { Meta, StoryObj } from '@storybook/react-vite';

import { DescriptionDisplay } from './DescriptionDisplay';

const meta: Meta<typeof DescriptionDisplay> = {
  component: DescriptionDisplay,
  args: {
    description: 'One row per customer, with their first and most recent order dates.',
  },
};

export default meta;
type Story = StoryObj<typeof DescriptionDisplay>;

export const Default: Story = {};

/** Descriptions are markdown, and dbt users write real markdown in them —
 *  `remark-gfm` is loaded specifically for the table and strikethrough syntax. */
export const Markdown: Story = {
  args: {
    description: [
      '## Customers',
      '',
      'One row per customer. Grain is **one row per `customer_id`**.',
      '',
      '| Column | Meaning |',
      '| --- | --- |',
      '| `customer_id` | Surrogate key |',
      '| `number_of_orders` | Lifetime orders |',
      '',
      '- ~~Deprecated~~ as of 2026-01',
      '- See [the runbook](https://example.com/runbook)',
      '',
      '```sql',
      'select count(*) from analytics.dbt.customers',
      '```',
    ].join('\n'),
  },
};

/** `rehype-raw` is enabled, so inline HTML in a `.yml` description renders as HTML
 *  rather than being escaped -- after `rehype-sanitize`, which keeps formatting markup
 *  like this and drops scripts, frames, event handlers and `style`. */
export const InlineHtml: Story = {
  args: {
    description:
      'Owned by <b>finance analytics</b>.<br/>Contact <i>finance@example.com</i>.',
  },
};

/** Untrusted markup in a description (a package can ship one) is neutralised: the
 *  `<script>` and the `<iframe>` below are dropped, the `onerror` handler stripped,
 *  and only the plain text survives. */
export const HostileHtml: Story = {
  args: {
    description: [
      'Looks harmless.',
      '<script>document.title = "pwned"</script>',
      '<iframe srcdoc="&lt;script&gt;alert(1)&lt;/script&gt;"></iframe>',
      '<img src="x" onerror="alert(1)" alt="an image">',
    ].join('\n'),
  },
};

/** External links are rewritten to `target="_blank"` by `rehype-external-links`. */
export const ExternalLink: Story = {
  args: { description: 'Defined upstream in [Fivetran](https://fivetran.com).' },
};

/** The undocumented case: italic placeholder copy, not an empty block. This is the
 *  most common state in a real project. */
export const NoDescription: Story = {
  args: { description: null },
};

/** Empty string takes the same branch as null — it is falsy, and an empty
 *  `description:` key in a `.yml` should read as undocumented rather than as a
 *  description that happens to be blank. */
export const EmptyString: Story = {
  args: { description: '' },
};
