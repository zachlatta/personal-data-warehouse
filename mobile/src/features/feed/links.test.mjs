import test from 'node:test';
import assert from 'node:assert/strict';

import { cardLinks } from './links.ts';

test('the main url is link 0 and the extra links follow in order', () => {
  assert.deepEqual(
    cardLinks({
      url: 'https://www.example.com/post',
      site: 'example.com',
      links: [{ url: 'https://arxiv.org/abs/1', title: 'Paper' }, { url: 'https://news.ycombinator.com/item?id=2' }],
    }),
    [
      { url: 'https://www.example.com/post', label: 'example.com', site: 'example.com', index: 0 },
      { url: 'https://arxiv.org/abs/1', label: 'Paper', site: 'arxiv.org', index: 1 },
      { url: 'https://news.ycombinator.com/item?id=2', label: 'news.ycombinator.com', site: 'news.ycombinator.com', index: 2 },
    ],
  );
});

test('a card with no links has none', () => {
  assert.deepEqual(cardLinks({}), []);
  assert.deepEqual(cardLinks({ links: [] }), []);
});
