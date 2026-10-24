import test from 'node:test';
import assert from 'node:assert/strict';

import { ViewabilityTracker, layoutsFromHeights, visibleFraction } from './viewability.ts';

const screen = (top) => ({ top, height: 800 });

test('visible fraction is of the card, or of the screen for a card taller than it', () => {
  assert.equal(visibleFraction({ y: 0, height: 400 }, screen(0)), 1);
  assert.equal(visibleFraction({ y: 600, height: 400 }, screen(0)), 0.5);
  assert.equal(visibleFraction({ y: 900, height: 400 }, screen(0)), 0);
  assert.equal(visibleFraction({ y: 0, height: 2000 }, screen(0)), 1);
  assert.equal(visibleFraction({ y: 0, height: 2000 }, screen(1600)), 0.5);
});

test('cards are placed below one another and only once everything above is measured', () => {
  const layouts = layoutsFromHeights(['a', 'b', 'c'], new Map([['a', 300], ['b', 200]]), 10);
  assert.deepEqual([...layouts], [['a', { y: 10, height: 300, position: 0 }], ['b', { y: 310, height: 200, position: 1 }]]);
  assert.equal(layoutsFromHeights(['a', 'b'], new Map([['b', 200]]), 0).size, 0);
});

test('a card read and scrolled away is one impression with its dwell and peak visibility', () => {
  const t = new ViewabilityTracker();
  const layouts = layoutsFromHeights(['a', 'b'], new Map([['a', 400], ['b', 400]]), 0);
  assert.deepEqual(t.update(0, screen(0), layouts), []);
  assert.deepEqual(t.inView().sort(), ['a', 'b']);
  // 3 s fully on screen, then half off for 1 s (still in view), then gone.
  t.update(3000, screen(200), layouts);
  const ended = t.update(4000, screen(900), layouts);
  assert.equal(ended.length, 2);
  const a = ended.find((i) => i.itemId === 'a');
  assert.deepEqual(a, { itemId: 'a', position: 0, startedAt: 0, endedAt: 4000, dwellMs: 4000, visibleMs: 4000, maxFraction: 1 });
});

test('a card that only peeked never counts, and a quick fling past is a short impression', () => {
  const t = new ViewabilityTracker();
  const layouts = layoutsFromHeights(['a', 'b'], new Map([['a', 800], ['b', 400]]), 0);
  t.update(0, screen(0), layouts); // b sits below the screen
  t.update(100, screen(100), layouts); // b peeks: 100 of 400 = 0.25
  const peek = t.update(500, screen(0), layouts);
  assert.deepEqual(peek, []);
  t.update(600, screen(500), layouts); // b fully on screen
  const fling = t.update(800, screen(2000), layouts).find((i) => i.itemId === 'b');
  assert.equal(fling.dwellMs, 200);
});

test('time below the threshold counts as visible but not as dwell', () => {
  const t = new ViewabilityTracker();
  const layouts = layoutsFromHeights(['a'], new Map([['a', 400]]), 0);
  t.update(0, screen(0), layouts);
  t.update(1000, screen(300), layouts); // a is 0.25 visible from here
  const [a] = t.flush(5000);
  assert.equal(a.dwellMs, 1000);
  assert.equal(a.visibleMs, 5000);
});

test('flush ends every open stretch and starts clean', () => {
  const t = new ViewabilityTracker();
  const layouts = layoutsFromHeights(['a'], new Map([['a', 400]]), 0);
  t.update(0, screen(0), layouts);
  assert.equal(t.flush(2000)[0].dwellMs, 2000);
  assert.deepEqual(t.flush(3000), []);
  assert.deepEqual(t.inView(), []);
});

test('dwell is active time: a card left on an untouched screen stops accruing', () => {
  const t = new ViewabilityTracker();
  const layouts = layoutsFromHeights(['a'], new Map([['a', 400]]), 0);
  // Wall 0 to 10 minutes on screen, but only 70 s of it active.
  t.update(0, screen(0), layouts, 0);
  const [a] = t.flush(600_000, 70_000);
  assert.equal(a.dwellMs, 70_000);
  assert.equal(a.visibleMs, 70_000);
  assert.equal(a.endedAt - a.startedAt, 600_000);
});
