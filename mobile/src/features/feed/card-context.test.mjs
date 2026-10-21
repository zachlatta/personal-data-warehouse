import test from 'node:test';
import assert from 'node:assert/strict';

import { cardContext, newArrivals, profileUrl } from './card-context.ts';

const hnBody = '**80 points** · 14 comments on Hacker News · [discussion](https://news.ycombinator.com/item?id=50138490)';

test('a Hacker News card shows its points and comments as counts, not as its text', () => {
  const c = cardContext({ source: 'hn', body: hnBody, metadata: { hn_points: 80, hn_comments: 14, hn_rank: 20 } });
  assert.equal(c.preview, '');
  assert.deepEqual(c.hn, { points: 80, comments: 14, url: 'https://news.ycombinator.com/item?id=50138490' });
});

test('a Show HN body keeps its own text and loses only the counts line', () => {
  const c = cardContext({ source: 'hn', body: `Quick update here.\n\nSecond paragraph.\n\n${hnBody.replace('80', '121')}` });
  assert.equal(c.preview, 'Quick update here.\n\nSecond paragraph.');
  assert.deepEqual(c.hn, { points: 121, comments: 14, url: 'https://news.ycombinator.com/item?id=50138490' });
});

test('a counts line with no discussion link still counts', () => {
  const c = cardContext({ source: 'picks', body: 'Text.\n\n301 points · 154 comments on Hacker News · discussion' });
  assert.equal(c.preview, 'Text.');
  assert.deepEqual(c.hn, { points: 301, comments: 154, url: '' });
});

test('a Muse card names Muse and its kind above the card and keeps the reason as the text', () => {
  const c = cardContext({
    source: 'muse-wonder',
    author: 'Muse Wonder',
    body: "*Institution · Support it*\n\n**For you:** The web's memory, asking plainly for help.",
  });
  assert.equal(c.label, 'Muse Wonder · Institution · Support it');
  assert.equal(c.preview, "The web's memory, asking plainly for help.");
  assert.equal(c.author, '');
});

test('a pick names the interest it was picked for', () => {
  const c = cardContext({ source: 'picks', body: 'A historian of the Renaissance.', metadata: { taste: 'History and how places work', why: 'he read it' } });
  assert.equal(c.label, 'History and how places work');
  assert.equal(c.preview, 'A historian of the Renaissance.');
});

test('a plain card has no label and keeps its author', () => {
  const c = cardContext({ source: 'rss', body: 'Hello', author: 'Ann' });
  assert.deepEqual(c, { label: '', preview: 'Hello', author: 'Ann' });
});

test('new arrivals are cards the last list did not have, none on the first load', () => {
  const next = [{ item_id: 'c' }, { item_id: 'a' }, { item_id: 'd' }, { item_id: 'b' }];
  assert.deepEqual(newArrivals(['a', 'b'], next).map((i) => i.item_id), ['c', 'd']);
  assert.deepEqual(newArrivals([], next), []);
});

test('an X author opens on X', () => {
  assert.equal(profileUrl({ type: 'x_post', author_handle: 'WillManidis' }), 'https://x.com/WillManidis');
  assert.equal(profileUrl({ type: 'x_post' }), '');
});
