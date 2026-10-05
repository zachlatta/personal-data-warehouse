import test from 'node:test';
import assert from 'node:assert/strict';

import { UNDO_WINDOW_MS, createDecisionHolder } from './undo-decision.ts';

// A hand-driven clock: `advance` fires every timer whose time has come.
function fakeClock() {
  let now = 0;
  let next = 1;
  const timers = new Map();
  return {
    setTimer(fn, ms) {
      const id = next++;
      timers.set(id, { at: now + ms, fn });
      return id;
    },
    clearTimer(id) {
      timers.delete(id);
    },
    async advance(ms) {
      now += ms;
      for (const [id, timer] of [...timers]) {
        if (timer.at <= now) {
          timers.delete(id);
          timer.fn();
        }
      }
      await new Promise((resolve) => setImmediate(resolve));
    },
    pending: () => timers.size,
  };
}

function decision(requestId, calls, { fail = false } = {}) {
  return {
    requestId,
    note: `Sent · ${requestId}`,
    commit: async () => {
      calls.push(requestId);
      if (fail) throw new Error('upstream said no');
    },
  };
}

test('the undo window is ten seconds', () => {
  assert.equal(UNDO_WINDOW_MS, 10_000);
});

test('a decision is not sent until its undo window has passed', async () => {
  const clock = fakeClock();
  const calls = [];
  const holder = createDecisionHolder(clock);
  holder.hold(decision('a', calls));
  assert.equal(holder.state()?.kind, 'held');
  assert.equal(holder.decidedRequestIds().includes('a'), true);
  await clock.advance(9_999);
  assert.deepEqual(calls, []);
  await clock.advance(1);
  assert.deepEqual(calls, ['a']);
  assert.equal(holder.state(), null);
  assert.equal(holder.decidedRequestIds().includes('a'), false);
});

test('undo inside the window cancels the decision and nothing is sent', async () => {
  const clock = fakeClock();
  const calls = [];
  const holder = createDecisionHolder(clock);
  holder.hold(decision('a', calls));
  await clock.advance(5_000);
  assert.equal(holder.undo()?.requestId, 'a');
  await clock.advance(20_000);
  assert.deepEqual(calls, []);
  assert.equal(holder.state(), null);
  assert.equal(holder.decidedRequestIds().includes('a'), false);
  assert.equal(clock.pending(), 0);
});

test('undo after the window has closed undoes nothing', async () => {
  const clock = fakeClock();
  const calls = [];
  const holder = createDecisionHolder(clock);
  holder.hold(decision('a', calls));
  await clock.advance(10_000);
  assert.equal(holder.undo(), null);
  assert.deepEqual(calls, ['a']);
});

test('only the last decision can be undone: deciding the next one sends the one before at once', async () => {
  const clock = fakeClock();
  const calls = [];
  const holder = createDecisionHolder(clock);
  holder.hold(decision('a', calls));
  await clock.advance(2_000);
  holder.hold(decision('b', calls));
  assert.deepEqual(holder.decidedRequestIds(), ['b', 'a']);
  await clock.advance(0);
  assert.deepEqual(calls, ['a']);
  assert.deepEqual(holder.decidedRequestIds(), ['b']);
  assert.equal(holder.decidedRequestIds().includes('b'), true);
  assert.equal(holder.undo()?.requestId, 'b');
  await clock.advance(20_000);
  assert.deepEqual(calls, ['a']);
});

test('the new decision gets its own full window', async () => {
  const clock = fakeClock();
  const calls = [];
  const holder = createDecisionHolder(clock);
  holder.hold(decision('a', calls));
  await clock.advance(9_000);
  holder.hold(decision('b', calls));
  await clock.advance(9_000);
  assert.deepEqual(calls, ['a']);
  await clock.advance(1_000);
  assert.deepEqual(calls, ['a', 'b']);
});

test('flush sends a held decision now (the app is leaving the foreground)', async () => {
  const clock = fakeClock();
  const calls = [];
  const holder = createDecisionHolder(clock);
  holder.hold(decision('a', calls));
  await holder.flush();
  assert.deepEqual(calls, ['a']);
  assert.equal(clock.pending(), 0);
  await clock.advance(20_000);
  assert.deepEqual(calls, ['a']);
});

test('a decision that fails upstream says so instead of vanishing', async () => {
  const clock = fakeClock();
  const calls = [];
  const holder = createDecisionHolder(clock);
  holder.hold(decision('a', calls, { fail: true }));
  await clock.advance(10_000);
  const state = holder.state();
  assert.equal(state?.kind, 'failed');
  assert.equal(state?.decision.requestId, 'a');
  assert.match(state?.error ?? '', /upstream said no/);
  holder.dismiss();
  assert.equal(holder.state(), null);
});

test('subscribers hear every change', async () => {
  const clock = fakeClock();
  const calls = [];
  const holder = createDecisionHolder(clock);
  const seen = [];
  const unsubscribe = holder.subscribe(() => seen.push(holder.state()?.kind ?? 'none'));
  holder.hold(decision('a', calls));
  holder.undo();
  holder.hold(decision('b', calls));
  await clock.advance(10_000);
  unsubscribe();
  holder.hold(decision('c', calls));
  assert.deepEqual(seen, ['held', 'none', 'held', 'sending', 'none']);
});

test('the held state says when the window closes, for the countdown', async () => {
  const clock = fakeClock();
  const holder = createDecisionHolder({ ...clock, now: () => 1_000 });
  holder.hold(decision('a', []));
  assert.equal(holder.state()?.deadline, 11_000);
});

test('while it is being sent a decision can no longer be undone but stays out of the queue', async () => {
  const clock = fakeClock();
  const holder = createDecisionHolder(clock);
  let finish;
  holder.hold({ requestId: 'a', note: 'Sent · a', commit: () => new Promise((resolve) => { finish = resolve; }) });
  await clock.advance(10_000);
  assert.equal(holder.state()?.kind, 'sending');
  assert.equal(holder.decidedRequestIds().includes('a'), true);
  assert.equal(holder.undo(), null);
  finish();
  await clock.advance(0);
  assert.equal(holder.state(), null);
  assert.equal(holder.decidedRequestIds().includes('a'), false);
});

test('a decision finishing in the background does not clear the one held after it', async () => {
  const clock = fakeClock();
  const holder = createDecisionHolder(clock);
  let finish;
  holder.hold({ requestId: 'a', note: 'a', commit: () => new Promise((resolve) => { finish = resolve; }) });
  holder.hold(decision('b', []));
  finish();
  await clock.advance(0);
  assert.equal(holder.state()?.kind, 'held');
  assert.equal(holder.decidedRequestIds().includes('b'), true);
});

test('an earlier decision failing while a later one is held is kept, and shown once nothing is undoable', async () => {
  const clock = fakeClock();
  const holder = createDecisionHolder(clock);
  holder.hold(decision('a', [], { fail: true }));
  holder.hold(decision('b', []));
  await clock.advance(0);
  assert.equal(holder.state()?.kind, 'held');
  await clock.advance(10_000);
  const state = holder.state();
  assert.equal(state?.kind, 'failed');
  assert.equal(state?.decision.requestId, 'a');
});

test('the state is the same object until something changes', () => {
  const holder = createDecisionHolder(fakeClock());
  holder.hold(decision('a', []));
  assert.equal(holder.state(), holder.state());
  assert.equal(holder.decidedRequestIds(), holder.decidedRequestIds());
});
