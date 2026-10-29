import test from 'node:test';
import assert from 'node:assert/strict';

import { forgetMutationRequests, peekMutationRequest, peekReviewQueue, rememberMutationRequest, rememberMutationRequests, rememberPendingMutationRequests } from './mutation-cache.ts';
import { heldInFull, prefetchMutationRequest, prefetchPendingRequests, readMutationRequest } from './review-prefetch.ts';

const full = (id, status = 'pending_review') => ({ id, status, title: `Request ${id}`, mutation_count: 1, mutations: [{ id: `m-${id}`, operation: 'slack.send_message' }] });

function counter() {
  const calls = [];
  const release = [];
  const fetchOne = (id) => {
    calls.push(id);
    return new Promise((resolve) => release.push(() => resolve(full(id))));
  };
  return { calls, fetchOne, finish: () => release.splice(0).forEach((go) => go()) };
}

test('a review screen joins the read the alert tap already started', async () => {
  forgetMutationRequests();
  const net = counter();
  prefetchMutationRequest(net.fetchOne, 'r1');
  const screen = readMutationRequest(net.fetchOne, 'r1');
  assert.deepEqual(net.calls, ['r1']);
  net.finish();
  assert.equal((await screen).id, 'r1');
  assert.equal(peekMutationRequest('r1')?.mutations?.length, 1);
});

test('a request held in full is not read ahead again; a header or alert copy is', async () => {
  forgetMutationRequests();
  rememberMutationRequest(full('r1'));
  rememberMutationRequest({ id: 'r2', status: 'pending_review', title: 'header', mutation_count: 1, partial: true });
  const net = counter();
  prefetchMutationRequest(net.fetchOne, 'r1');
  prefetchMutationRequest(net.fetchOne, 'r2');
  assert.deepEqual(net.calls, ['r2']);
  assert.equal(heldInFull('r2'), false);
  net.finish();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(heldInFull('r2'), true);
});

test('only the first few requests waiting on review are read ahead', () => {
  forgetMutationRequests();
  const net = counter();
  const rows = ['a', 'b', 'c', 'd', 'e', 'f', 'g'].map((id) => ({ id, status: 'pending_review' }));
  prefetchPendingRequests(net.fetchOne, [{ id: 'x', status: 'approved' }, ...rows], 5);
  assert.deepEqual(net.calls, ['a', 'b', 'c', 'd', 'e']);
  net.finish();
});

test('with no Mutations list, the review queue is the pending read, less requests since decided', () => {
  forgetMutationRequests();
  assert.equal(peekReviewQueue(), null);
  rememberPendingMutationRequests([full('a'), full('b'), full('c')]);
  rememberMutationRequest(full('b', 'approved'));
  assert.deepEqual(peekReviewQueue().map((row) => row.id), ['a', 'c']);
  rememberMutationRequests([full('z')]);
  assert.deepEqual(peekReviewQueue().map((row) => row.id), ['z'], 'the tab list wins once loaded');
});
