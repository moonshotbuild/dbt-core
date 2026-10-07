import type { Options } from 'rehype-sanitize';
import { defaultSchema } from 'rehype-sanitize';

/**
 * The allow-list every project-authored markdown field is rendered through.
 *
 * Descriptions (`schema.yml`) and `{% docs %}` blocks come from the project and
 * from every installed package, and `rehype-raw` turns the inline HTML they
 * carry into real elements. Without a sanitiser that is stored XSS: a package's
 * model description can embed `<script>`, an `<iframe srcdoc=...>` or an event
 * handler, which runs in the browser of whoever opens the docs site.
 *
 * `defaultSchema` is GitHub's: it keeps the formatting a description needs
 * (headings, lists, tables, code, links, images, `<b>`/`<i>`/`<br>`, ...) and
 * drops scripts, frames, objects, forms, event handlers, `style` and `javascript:`
 * URLs. The one extension is `className` on `<code>`, so fenced code blocks keep
 * their `language-*` class for highlighting (the default already allows exactly
 * that prefix); it is restated here so the intent is visible at the call sites.
 */
export const markdownSanitizeSchema: Options = {
  ...defaultSchema,
  attributes: {
    ...defaultSchema.attributes,
    code: [...(defaultSchema.attributes?.code ?? []), ['className', /^language-./]],
  },
};
