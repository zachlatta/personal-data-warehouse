import test from 'node:test';
import assert from 'node:assert/strict';
import { LiveRefresh } from './live-refresh.ts';

// A fake clock and interval, so the controller's decisions are checked
// without React Native or real timers.
function harness({ appState = 'active', minIntervalMs = 15_000, periodMs = 60_000 } = {}) {
  const clock = { now: 1_000_000 };
  const timers = new Map();
  let nextTimer = 1;
  const calls = [];
  const refresh = new LiveRefresh({
    refresh: (why) => calls.push(why),
    minIntervalMs,
    periodMs,
    appState,
    now: () => clock.now,
    setInterval: (fn, ms) => {
      const id = nextTimer++;
      timers.set(id, { fn, ms });
      return id;
    },
    clearInterval: (id) => timers.delete(id),
  });
  const tick = () => {
    for (const { fn } of [...timers.values()]) fn();
  };
  return { refresh, calls, clock, timers, tick };
}

test('focusing a list in the foreground reads it, and starts a periodic read', () => {
  const { refresh, calls, timers } = harness();
  refresh.focus();
  assert.deepEqual(calls, ['focus']);
  assert.equal(timers.size, 1);
  assert.equal([...timers.values()][0].ms, 60_000);
});

test('coming back to the foreground reads the focused list again: the bug was that nothing did', () => {
  const { refresh, calls, clock } = harness();
  refresh.focus();
  refresh.appState('background');
  clock.now += 5 * 60_000;
  refresh.appState('active');
  assert.deepEqual(calls, ['focus', 'foreground']);
});

test('a resume within the minimum interval of the last read does not read twice', () => {
  const { refresh, calls, clock } = harness();
  refresh.focus();
  refresh.appState('background');
  clock.now += 2_000;
  refresh.appState('active');
  assert.deepEqual(calls, ['focus']);
});

test('the read made when the list lands counts, so a cold start does not read twice', () => {
  const { refresh, calls } = harness({ appState: 'background' });
  // Cold start from a tap: the screen mounts and reads itself while the
  // AppState is still settling, and says so.
  refresh.loaded();
  refresh.focus();
  refresh.appState('active');
  assert.deepEqual(calls, []);
});

test('a blurred list is not read on resume or by the timer, and the timer stops', () => {
  const { refresh, calls, clock, timers } = harness();
  refresh.focus();
  refresh.blur();
  assert.equal(timers.size, 0);
  refresh.appState('background');
  clock.now += 5 * 60_000;
  refresh.appState('active');
  assert.deepEqual(calls, ['focus']);
  assert.equal(timers.size, 0);
});

test('returning to a tab reads it once it is older than the minimum interval', () => {
  const { refresh, calls, clock } = harness();
  refresh.focus();
  refresh.blur();
  clock.now += 1_000;
  refresh.focus();
  assert.deepEqual(calls, ['focus']);
  refresh.blur();
  clock.now += 20_000;
  refresh.focus();
  assert.deepEqual(calls, ['focus', 'focus']);
});

test('the periodic read runs only while focused and in the foreground', () => {
  const { refresh, calls, clock, timers, tick } = harness();
  refresh.focus();
  clock.now += 60_000;
  tick();
  assert.deepEqual(calls, ['focus', 'periodic']);
  refresh.appState('background');
  assert.equal(timers.size, 0);
  refresh.appState('inactive');
  assert.equal(timers.size, 0);
});

test('focusing while the app is in the background waits for the foreground', () => {
  const { refresh, calls, timers } = harness({ appState: 'background' });
  refresh.focus();
  assert.deepEqual(calls, []);
  assert.equal(timers.size, 0);
  refresh.appState('active');
  assert.deepEqual(calls, ['foreground']);
  assert.equal(timers.size, 1);
});

test('setRefresh swaps the read function the next read calls', () => {
  const { refresh, calls, clock, tick } = harness();
  refresh.focus();
  const later = [];
  refresh.setRefresh((why) => later.push(why));
  clock.now += 60_000;
  tick();
  assert.deepEqual(calls, ['focus']);
  assert.deepEqual(later, ['periodic']);
});
