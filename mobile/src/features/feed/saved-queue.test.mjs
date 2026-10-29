import test from 'node:test';
import assert from 'node:assert/strict';

import { applySave, mergeSaved } from './saved-queue.ts';

test('a save goes to the front once and an unsave takes it out', () => {
  assert.deepEqual(applySave(['a', 'b'], 'b', true), ['b', 'a']);
  assert.deepEqual(applySave(['a'], 'c', true), ['c', 'a']);
  assert.deepEqual(applySave(['a', 'b'], 'a', false), ['b']);
  assert.deepEqual(applySave([], 'a', false), []);
});

test('a reload keeps taps pending until the server reports their state, even after a long outage', () => {
  const pending = new Map([
    ['new', { save: true, at: 1000 }],
    ['gone', { save: false, at: 2000 }],
  ]);
  assert.deepEqual(mergeSaved(['gone', 'kept'], pending), ['new', 'kept']);
  assert.equal(pending.size, 2);
  // A later server read confirms both changes. Its queue ordering wins.
  assert.deepEqual(mergeSaved(['kept', 'new'], pending), ['kept', 'new']);
  assert.equal(pending.size, 0);
  // With the local tap acknowledged, a later change on another device wins.
  assert.deepEqual(mergeSaved(['kept'], pending), ['kept']);
});
