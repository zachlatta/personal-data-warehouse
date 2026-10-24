import test from 'node:test';
import assert from 'node:assert/strict';

import {
  forgetMutationRequests,
  mutationRequestFromNotificationData,
  peekMutationRequest,
  peekMutationRequests,
  peekPendingMutationRequestIds,
  rememberMutationRequest,
  rememberMutationRequests,
  rememberPendingMutationRequests,
  seedMutationRequestFromNotification,
  subscribeMutationRequests,
} from './mutation-cache.ts';

const full = { id: 'r1', status: 'pending_review', title: 'Send reply', mutation_count: 1, mutations: [{ id: 'm1', operation: 'gmail.send_email' }] };

test('a mutation alert seeds the request it carries, for the id it names only', () => {
  forgetMutationRequests();
  assert.equal(mutationRequestFromNotificationData({ kind: 'mutation_request', request_id: 'r1' }), null, 'an older server sends no request');
  assert.equal(mutationRequestFromNotificationData({ kind: 'mutation_request', request_id: 'r1', request: { ...full, id: 'r2' } }), null, 'a mismatched id is not trusted');
  assert.equal(mutationRequestFromNotificationData({ kind: 'timeline_notification', request_id: 'r1', request: full }), null);
  assert.equal(mutationRequestFromNotificationData({ kind: 'mutation_request', request_id: 'r1', request: 'r1' }), null);
  const seeded = seedMutationRequestFromNotification({ kind: 'mutation_request', request_id: 'r1', request: full });
  assert.equal(seeded?.id, 'r1');
  assert.equal(peekMutationRequest('r1')?.mutations?.length, 1);
});

test('a partial (header-only) alert never overwrites a full request, but a fresh read does', () => {
  forgetMutationRequests();
  rememberMutationRequest(full);
  seedMutationRequestFromNotification({ kind: 'mutation_request', request_id: 'r1', request: { id: 'r1', status: 'pending_review', title: 'Send reply', mutation_count: 1, partial: true } });
  assert.equal(peekMutationRequest('r1')?.mutations?.length, 1);
  rememberMutationRequest({ ...full, status: 'approved' });
  assert.equal(peekMutationRequest('r1')?.status, 'approved');
});

test('the list is remembered and a request read refreshes its row without mutations', () => {
  forgetMutationRequests();
  assert.equal(peekMutationRequests(), null);
  rememberMutationRequests([{ id: 'r1', status: 'pending_review', title: 'old', mutation_count: 1 }, { id: 'r2', status: 'rejected', title: 'x', mutation_count: 1 }]);
  rememberMutationRequest({ ...full, status: 'approved' });
  assert.deepEqual(peekMutationRequests()[0], { id: 'r1', status: 'approved', title: 'Send reply', mutation_count: 1 });
  seedMutationRequestFromNotification({ kind: 'mutation_request', request_id: 'r3', request: { ...full, id: 'r3' } });
  assert.equal(peekMutationRequests()[0].id, 'r3', 'a request the list never saw goes to the top');
  assert.equal(peekMutationRequests().length, 3);
  forgetMutationRequests();
  assert.equal(peekMutationRequests(), null);
});

test('the pending set backs the tab badge and follows every read', () => {
  forgetMutationRequests();
  const seen = [];
  const stop = subscribeMutationRequests(() => seen.push(peekPendingMutationRequestIds()?.size ?? null));
  assert.equal(peekPendingMutationRequestIds(), null, 'unknown until the pending-only read');
  rememberMutationRequest(full);
  assert.equal(peekPendingMutationRequestIds(), null, 'one request seen first is not the whole queue');
  rememberPendingMutationRequests([full, { ...full, id: 'r2' }]);
  const before = peekPendingMutationRequestIds();
  assert.deepEqual([...before].sort(), ['r1', 'r2']);
  rememberMutationRequest({ ...full, status: 'approved' });
  assert.deepEqual([...peekPendingMutationRequestIds()], ['r2'], 'an approval takes it off at once');
  assert.notEqual(peekPendingMutationRequestIds(), before, 'a change is a new set, for useSyncExternalStore');
  seedMutationRequestFromNotification({ kind: 'mutation_request', request_id: 'r3', request: { ...full, id: 'r3' } });
  assert.equal(peekPendingMutationRequestIds().size, 2, 'a new alert adds to it');
  rememberMutationRequests([{ id: 'r2', status: 'rejected', title: 'x', mutation_count: 1 }]);
  assert.deepEqual([...peekPendingMutationRequestIds()], ['r3'], 'a list read moves it too');
  const unchanged = peekPendingMutationRequestIds();
  rememberMutationRequest({ ...full, id: 'r3' });
  assert.equal(peekPendingMutationRequestIds(), unchanged, 'no change keeps the same set');
  stop();
  forgetMutationRequests();
  assert.equal(peekPendingMutationRequestIds(), null);
  assert.deepEqual(seen.slice(0, 2), [null, 2]);
});
