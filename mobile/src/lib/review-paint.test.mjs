import test from 'node:test';
import assert from 'node:assert/strict';

import { markReviewOpen, takeReviewPaint } from './review-paint.ts';

test('a review reads the mark left for it once', () => {
  markReviewOpen('a', 'list', 100);
  assert.deepEqual(takeReviewPaint('a', 160), { ms: 60, via: 'list' });
  assert.equal(takeReviewPaint('a', 200), null);
});

test('a mark for another request is dropped, not kept for later', () => {
  markReviewOpen('a', 'notification', 100);
  assert.equal(takeReviewPaint('b', 150), null);
  assert.equal(takeReviewPaint('a', 160), null);
});

test('a mark without a request goes to whichever review paints next', () => {
  markReviewOpen(null, 'undo', 10);
  assert.deepEqual(takeReviewPaint('c', 45), { ms: 35, via: 'undo' });
});
