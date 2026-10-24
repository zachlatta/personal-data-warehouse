import test from 'node:test';
import assert from 'node:assert/strict';

import { ActiveClock, IDLE_MS, stopwatch } from './active-time.ts';

test('time counts only up to IDLE_MS after the last touch', () => {
  const c = new ActiveClock(0);
  c.touch(10_000);
  assert.equal(c.read(10_000 + 5 * 60_000), 10_000 + IDLE_MS);
  c.touch(10_000 + 5 * 60_000);
  assert.equal(c.read(10_000 + 5 * 60_000 + 3_000), 10_000 + IDLE_MS + 3_000);
});

test('time in the background counts nothing, and coming back is a touch', () => {
  const c = new ActiveClock(0);
  c.touch(2_000);
  c.setForeground(false, 4_000);
  assert.equal(c.read(14 * 60_000), 4_000);
  c.setForeground(true, 14 * 60_000);
  assert.equal(c.read(14 * 60_000 + 10_000), 14_000);
});

test('a playing video or open browser counts without touches, but not in the background', () => {
  const c = new ActiveClock(0);
  c.setBusy(true, 0);
  assert.equal(c.read(5 * 60_000), 5 * 60_000);
  c.setForeground(false, 5 * 60_000);
  assert.equal(c.read(10 * 60_000), 5 * 60_000);
  c.setForeground(true, 10 * 60_000);
  c.setBusy(false, 11 * 60_000);
  assert.equal(c.read(11 * 60_000 + IDLE_MS + 30_000), 6 * 60_000 + IDLE_MS);
});

test('a detail view left open while he is in another app reads as the time he used it', () => {
  // The plasma-arc speaker card on 2026-10-23: opened, scrolled, a link to
  // Instagram, then back 14 minutes later to close it.
  const c = new ActiveClock(0);
  const sw = stopwatch(c, 0);
  c.touch(1_000);
  c.touch(1_800);
  c.setForeground(false, 1_830);
  c.setForeground(true, 837_000);
  const { activeMs, wallMs } = sw.stop(839_000);
  assert.equal(wallMs, 839_000);
  assert.equal(activeMs, 1_830 + 2_000);
});
