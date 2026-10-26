import test from 'node:test';
import assert from 'node:assert/strict';

import { createArticleReads } from './article-reads.ts';

test('a read under way is shared, and a finished one is ready at once', async () => {
  let loads = 0;
  const reads = createArticleReads(async (url) => {
    loads++;
    return { article: `body of ${url}`, stored: false };
  });
  assert.equal(reads.peek('a'), undefined);
  const [one, two] = await Promise.all([reads.read('a'), reads.read('a')]);
  assert.equal(loads, 1);
  assert.equal(one, two);
  assert.deepEqual(reads.peek('a'), { article: 'body of a', stored: false });
  await reads.read('a');
  assert.equal(loads, 1);
});

test('a failed read is forgotten, so the next open tries again', async () => {
  let fail = true;
  const reads = createArticleReads(async () => {
    if (fail) throw new Error('offline');
    return { article: 'ok', stored: true };
  });
  await assert.rejects(reads.read('a'));
  assert.equal(reads.peek('a'), undefined);
  fail = false;
  assert.deepEqual(await reads.read('a'), { article: 'ok', stored: true });
});

test('only the most recently used reads are kept', async () => {
  const reads = createArticleReads(async (url) => ({ article: url, stored: false }), 2);
  await reads.read('a');
  await reads.read('b');
  await reads.read('a');
  await reads.read('c');
  assert.equal(reads.peek('b'), undefined);
  assert.ok(reads.peek('a'));
  assert.ok(reads.peek('c'));
});
