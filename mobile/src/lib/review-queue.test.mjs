import test from 'node:test';
import assert from 'node:assert/strict';

import { nextPendingRequestId, pendingReviewCount, setReviewFlash, takeReviewFlash } from './review-queue.ts';

const list = [
  { id: 'a', status: 'pending_review' },
  { id: 'b', status: 'pending_review' },
  { id: 'old', status: 'executed' },
  { id: 'c', status: 'pending_review' },
];

test('the next request is the pending one that followed this one in the list', () => {
  assert.equal(nextPendingRequestId(list, 'a'), 'b');
  assert.equal(nextPendingRequestId(list, 'b'), 'c');
});

test('after the last one the queue wraps to the first still pending, then runs out', () => {
  assert.equal(nextPendingRequestId(list, 'c'), 'a');
  assert.equal(nextPendingRequestId([{ id: 'a', status: 'approved' }, { id: 'b', status: 'pending_review' }], 'b'), null);
  assert.equal(nextPendingRequestId([], 'a'), null);
  assert.equal(nextPendingRequestId(null, 'a'), null);
});

test('the request just decided is never offered again, whatever its cached status', () => {
  // The cache flips the decided row to approved; a stale copy may still say pending.
  const stale = [{ id: 'a', status: 'pending_review' }, { id: 'b', status: 'approved' }];
  assert.equal(nextPendingRequestId(stale, 'a'), null);
});

test('a request opened from an alert that the list has not seen yet starts at the top of the queue', () => {
  assert.equal(nextPendingRequestId(list, 'fresh'), 'a');
});

test('the header counts what is left to review, this one included, and says nothing for the last one', () => {
  // A count of what is left, not "1 of 3": after a decision the queue shrinks,
  // and "1 of 3" then "1 of 2" read as going backwards.
  assert.equal(pendingReviewCount(list, 'a'), 3);
  assert.equal(pendingReviewCount(list, 'c'), 3);
  assert.equal(pendingReviewCount(list, 'old'), null);
  assert.equal(pendingReviewCount([{ id: 'a', status: 'pending_review' }], 'a'), null);
  assert.equal(pendingReviewCount(null, 'a'), null);
});

test('a flash is read once', () => {
  assert.equal(takeReviewFlash(), null);
  setReviewFlash('Sent · Reply to the vendor');
  assert.equal(takeReviewFlash(), 'Sent · Reply to the vendor');
  assert.equal(takeReviewFlash(), null);
});
