import test from 'node:test';
import assert from 'node:assert/strict';

import { BATCH_SIZE, EventQueue, MAX_QUEUED, newEventId } from './event-queue.ts';

function memory(initial = null) {
  const box = { value: initial };
  return { box, storage: { load: () => box.value, save: (v) => { box.value = v; } } };
}
const ev = (n, type = 'impression') => ({ event_id: `e${n}`, type, item_id: 'hn:1', session_id: 's', occurred_at: '2026-10-20T12:00:00Z' });

test('events persist the moment they are recorded and survive a restart', () => {
  const { box, storage } = memory();
  const q = new EventQueue(storage, async () => {});
  q.enqueue(ev(1));
  const again = new EventQueue({ load: () => box.value, save() {} }, async () => {});
  assert.deepEqual(again.pending().map((e) => e.event_id), ['e1']);
});

test('a flush sends in batches and clears only what was acknowledged', async () => {
  const { storage } = memory();
  const sent = [];
  const q = new EventQueue(storage, async (batch) => { sent.push(batch.length); });
  for (let i = 0; i < BATCH_SIZE + 5; i++) q.enqueue(ev(i));
  await q.flush();
  assert.deepEqual(sent, [BATCH_SIZE, 5]);
  assert.equal(q.size, 0);
});

test('a network failure keeps the events and backs off until forced', async () => {
  const { storage } = memory();
  let now = 0;
  let fail = true;
  let calls = 0;
  const q = new EventQueue(storage, async () => { calls++; if (fail) throw new Error('offline'); }, () => now);
  q.enqueue(ev(1));
  await q.flush();
  assert.equal(q.size, 1);
  await q.flush(); // inside the backoff window: not attempted
  assert.equal(calls, 1);
  fail = false;
  now = 6000;
  await q.flush();
  assert.equal(q.size, 0);
});

test('a batch the server refuses as malformed is dropped, not retried forever', async () => {
  const { storage } = memory();
  const warnings = [];
  const q = new EventQueue(storage, async () => { throw Object.assign(new Error('bad'), { status: 400 }); }, Date.now, (m) => warnings.push(m));
  q.enqueue(ev(1));
  await q.flush();
  assert.equal(q.size, 0);
  assert.equal(warnings.length, 1);
});

test('an unauthorized answer keeps the events for after re-login', async () => {
  const { storage } = memory();
  const q = new EventQueue(storage, async () => { throw Object.assign(new Error('no'), { status: 401 }); });
  q.enqueue(ev(1, 'like'));
  await q.flush();
  assert.equal(q.size, 1);
});

test('past the cap the oldest impression goes first, never a deliberate act', () => {
  const stored = JSON.stringify([ev('like', 'like'), ...Array.from({ length: MAX_QUEUED - 1 }, (_, i) => ev(i))]);
  const q = new EventQueue({ load: () => stored, save() {} }, async () => {});
  q.enqueue(ev('new', 'dismiss'));
  const ids = q.pending().map((e) => e.event_id);
  assert.equal(ids.length, MAX_QUEUED);
  assert.equal(ids[0], 'elike');
  assert.ok(!ids.includes('e0'));
});

test('event ids are unique and sortable by time', () => {
  const a = newEventId(1000);
  const b = newEventId(2000);
  assert.notEqual(newEventId(1000), newEventId(1000));
  assert.ok(a < b);
});
