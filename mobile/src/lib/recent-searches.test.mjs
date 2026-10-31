import test from 'node:test';
import assert from 'node:assert/strict';

import { addRecent, MAX_RECENT, parseRecent } from './recent-searches.ts';

test('a new search goes first and an old copy of it is dropped', () => {
  assert.deepEqual(addRecent(['b', 'a'], 'a'), ['a', 'b']);
  assert.deepEqual(addRecent(['Board Deck', 'x'], '  board   deck '), ['board deck', 'x']);
});

test('blank searches are not kept and the list is capped', () => {
  assert.deepEqual(addRecent(['a'], '   '), ['a']);
  let list = [];
  for (let i = 0; i < MAX_RECENT + 3; i++) list = addRecent(list, `q${i}`);
  assert.equal(list.length, MAX_RECENT);
  assert.equal(list[0], `q${MAX_RECENT + 2}`);
});

test('a bad file reads as no recent searches', () => {
  assert.deepEqual(parseRecent('not json'), []);
  assert.deepEqual(parseRecent('{"a":1}'), []);
  assert.deepEqual(parseRecent('["a", 3, "", "b"]'), ['a', 'b']);
});
