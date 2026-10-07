import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { DescriptionDisplay } from './DescriptionDisplay';

/**
 * A description can come from any installed package, so the HTML it carries is
 * untrusted (advisory docs-stored-xss). Formatting markup must survive; anything
 * that executes must not.
 */
describe('DescriptionDisplay', () => {
  it('keeps formatting markup and markdown', () => {
    const { container } = render(
      <DescriptionDisplay
        description={
          'Owned by <b>finance</b>.<br/>See [docs](https://example.com).\n\n```sql\nselect 1\n```'
        }
      />,
    );
    expect(container.querySelector('b')?.textContent).toBe('finance');
    expect(container.querySelector('br')).not.toBeNull();
    expect(container.querySelector('a')?.getAttribute('href')).toBe(
      'https://example.com',
    );
    expect(container.querySelector('a')?.getAttribute('target')).toBe('_blank');
    expect(container.querySelector('code')?.className).toContain('language-sql');
  });

  it('drops scripts, frames, objects and event handlers', () => {
    const hostile = [
      'Looks harmless.',
      '<script>document.title = "pwned"</script>',
      '<iframe srcdoc="&lt;script&gt;alert(1)&lt;/script&gt;"></iframe>',
      '<object data="x"></object>',
      '<img src="x" onerror="alert(1)" alt="an image">',
      '<a href="javascript:alert(1)">click</a>',
      '<div style="position:fixed">styled</div>',
    ].join('\n');
    const { container } = render(<DescriptionDisplay description={hostile} />);

    expect(container.querySelector('script')).toBeNull();
    expect(container.querySelector('iframe')).toBeNull();
    expect(container.querySelector('object')).toBeNull();
    expect(container.querySelector('img')?.getAttribute('onerror')).toBeNull();
    expect(container.querySelector('a')?.getAttribute('href')).toBeNull();
    expect(container.querySelector('div[style]')).toBeNull();
    expect(container.innerHTML).not.toContain('pwned');
    expect(container.innerHTML).not.toContain('alert(1)');
    expect(container.textContent).toContain('Looks harmless.');
    expect(container.textContent).toContain('styled');
  });
});
