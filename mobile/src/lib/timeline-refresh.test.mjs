import test from 'node:test';
import assert from 'node:assert/strict';
import { mergeFirstPage } from './timeline-refresh.ts';

// seq defaults to a value that falls with age, as the server's does; a test
// that needs two rows at one timestamp sets it.
const row = (id, minutesAgo, extra = {}) => ({
  adapter: 'gmail',
  event_id: id,
  occurred_at: new Date(Date.UTC(2026, 10, 25, 12, 0) - minutesAgo * 60_000).toISOString(),
  seq: 1_000_000 - minutesAgo,
  ...extra,
});
const ids = (items) => items.map((item) => item.event_id);

test('new events land on top and the pages already scrolled through are kept', () => {
  const current = { items: [row('c', 10), row('d', 20), row('e', 30), row('f', 40)], cursor: 'page-3', hasMore: true };
  const page = { items: [row('a', 1), row('b', 5), row('c', 10)], cursor: 'after-c', hasMore: true };
  const merged = mergeFirstPage(current, page);
  assert.deepEqual(ids(merged.items), ['a', 'b', 'c', 'd', 'e', 'f']);
  // The older pages still continue from where the reader had scrolled to.
  assert.equal(merged.cursor, 'page-3');
  assert.equal(merged.hasMore, true);
});

test('a row the server changed is replaced by its new copy', () => {
  const current = { items: [row('a', 1, { title: 'old' }), row('b', 5)], cursor: 'x', hasMore: true };
  const page = { items: [row('a', 1, { title: 'new' }), row('b', 5)], cursor: 'y', hasMore: true };
  const merged = mergeFirstPage(current, page);
  assert.equal(merged.items[0].title, 'new');
});

test('a row the new first page dropped inside its own window is gone, older rows stay', () => {
  // b was reclassified out of these tiers; d is older than the new page reaches.
  const current = { items: [row('a', 1), row('b', 5), row('c', 10), row('d', 20)], cursor: 'x', hasMore: true };
  const page = { items: [row('a', 1), row('c', 10)], cursor: 'y', hasMore: true };
  assert.deepEqual(ids(mergeFirstPage(current, page).items), ['a', 'c', 'd']);
});

test('more new events than a page leaves a gap, so the list starts over from the page', () => {
  const current = { items: [row('x', 100), row('y', 110)], cursor: 'old', hasMore: true };
  const page = { items: [row('a', 1), row('b', 2)], cursor: 'after-b', hasMore: true };
  const merged = mergeFirstPage(current, page);
  assert.deepEqual(ids(merged.items), ['a', 'b']);
  assert.equal(merged.cursor, 'after-b');
});

test('a short first page with nothing older is the whole list', () => {
  const current = { items: [row('a', 1), row('b', 5)], cursor: undefined, hasMore: false };
  const page = { items: [row('n', 0), row('a', 1)], cursor: undefined, hasMore: false };
  const merged = mergeFirstPage(current, page);
  assert.deepEqual(ids(merged.items), ['n', 'a']);
  assert.equal(merged.hasMore, false);
});

test('an empty list takes the page as it is', () => {
  const page = { items: [row('a', 1)], cursor: 'c', hasMore: true };
  assert.deepEqual(mergeFirstPage({ items: [], cursor: undefined, hasMore: true }, page), page);
});

test('nothing new keeps the same row objects, so the list does not re-render them', () => {
  const current = { items: [row('a', 1), row('b', 5), row('c', 10)], cursor: 'x', hasMore: true };
  const page = { items: [row('a', 1), row('b', 5)], cursor: 'y', hasMore: true };
  const merged = mergeFirstPage(current, page);
  assert.equal(merged.items[0], current.items[0]);
  assert.equal(merged.items[2], current.items[2]);
});

test('a loaded row at the new page\'s last timestamp but below it in seq is kept', () => {
  // The server orders occurred_at DESC, seq DESC and cuts the page at its
  // last row; c and d share a timestamp and the cut falls between them, so
  // d is still older than the page and the old cursor continues after e.
  const current = { items: [row('a', 1), row('b', 5), row('c', 10, { seq: 20 }), row('d', 10, { seq: 19 }), row('e', 20)], cursor: 'after-e', hasMore: true };
  const page = { items: [row('n', 0), row('a', 1), row('b', 5), row('c', 10, { seq: 20 })], cursor: 'after-c', hasMore: true };
  const merged = mergeFirstPage(current, page);
  assert.deepEqual(ids(merged.items), ['n', 'a', 'b', 'c', 'd', 'e']);
  assert.equal(merged.cursor, 'after-e');
});

test('a loaded row at the last timestamp above it in seq, missing from the page, is gone', () => {
  // x sorts inside the page's window, so the page would hold it if it were
  // still in these tiers.
  const current = { items: [row('a', 1), row('x', 10, { seq: 21 }), row('c', 10, { seq: 20 }), row('e', 20)], cursor: 'after-e', hasMore: true };
  const page = { items: [row('a', 1), row('c', 10, { seq: 20 })], cursor: 'after-c', hasMore: true };
  assert.deepEqual(ids(mergeFirstPage(current, page).items), ['a', 'c', 'e']);
});
