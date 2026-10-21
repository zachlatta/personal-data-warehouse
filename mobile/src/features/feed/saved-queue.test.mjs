import test from 'node:test';
import assert from 'node:assert/strict';

import { applySave, mergeSaved, PENDING_MS } from './saved-queue.ts';

test('a save goes to the front once and an unsave takes it out', () => {
  assert.deepEqual(applySave(['a', 'b'], 'b', true), ['b', 'a']);
  assert.deepEqual(applySave(['a'], 'c', true), ['c', 'a']);
  assert.deepEqual(applySave(['a', 'b'], 'a', false), ['b']);
  assert.deepEqual(applySave([], 'a', false), []);
});

test('a reload keeps a tap the server has not heard of yet', () => {
  const now = 1_000_000;
  const pending = new Map([
    ['new', { save: true, at: now - 1000 }],
    ['gone', { save: false, at: now - 500 }],
    ['old', { save: true, at: now - PENDING_MS - 1 }],
  ]);
  assert.deepEqual(mergeSaved(['gone', 'kept'], pending, now), ['new', 'kept']);
  // The server already has it: its order wins.
  assert.deepEqual(mergeSaved(['kept', 'new'], pending, now), ['kept', 'new']);
});
