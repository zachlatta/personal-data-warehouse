import test from 'node:test';
import assert from 'node:assert/strict';
import { refreshItems, loadWithoutWaitingForEvents } from './refresh.ts';

const cards = (...ids) => ids.map((item_id) => ({ item_id, title: item_id }));

test('background refresh does not reorder or remove the cards being read', () => {
  const current = cards('a', 'b', 'c');
  const incoming = cards('new', 'c', 'b');
  assert.deepEqual(refreshItems(current, incoming, true).map((item) => item.item_id), ['a', 'b', 'c']);
  assert.equal(refreshItems(current, incoming, true)[1], current[1]);
  assert.deepEqual(refreshItems(current, incoming, false), incoming);
});

test('a hanging analytics upload cannot block content from loading', async () => {
  let flushed = false;
  const page = await loadWithoutWaitingForEvents(() => { flushed = true; return new Promise(() => {}); }, async () => ({ items: cards('a') }));
  assert.equal(flushed, true);
  assert.equal(page.items[0].item_id, 'a');
});

test('a failed analytics upload cannot fail a successful content read', async () => {
  assert.equal(await loadWithoutWaitingForEvents(async () => { throw new Error('offline'); }, async () => 'content'), 'content');
});

test('a stale feed response cannot resurrect a dismissal still in the outbox', async () => {
  const { applyPendingDismissals } = await import('./refresh.ts');
  const acts = [{ type: 'dismiss', item_id: 'a' }];
  assert.deepEqual(applyPendingDismissals(cards('a', 'b'), acts).map((c) => c.item_id), ['b']);
  assert.deepEqual(applyPendingDismissals(cards('a', 'b'), [...acts, { type: 'undismiss', item_id: 'a' }]).map((c) => c.item_id), ['a', 'b']);
});
