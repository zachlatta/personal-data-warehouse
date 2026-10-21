import test from 'node:test';
import assert from 'node:assert/strict';

import { ScrollGestureTracker, scrollMetadata } from './scroll-gestures.ts';

test('a fling is one gesture from finger down to the list at rest', () => {
  const t = new ScrollGestureTracker();
  assert.equal(t.beginDrag(1000, 0), null);
  t.endDrag(1200, 300, 2.5);
  t.momentumBegin();
  assert.equal(t.settle(), null, 'a coast is not over yet');
  const g = t.momentumEnd(2000, 1800);
  assert.deepEqual(g, { startedAt: 1000, fromY: 0, toY: 1800, dragMs: 200, durationMs: 1000, releaseVelocity: 2.5, momentum: true });
  assert.deepEqual(scrollMetadata(g, { view: 'all' }), {
    from_y: 0, to_y: 1800, distance: 1800, direction: 'down', drag_ms: 200,
    release_velocity: 2.5, momentum: true, view: 'all',
  });
});

test('a drag let go without a fling ends where the finger left it', () => {
  const t = new ScrollGestureTracker();
  t.beginDrag(0, 500);
  t.endDrag(400, 350, 0);
  const g = t.settle();
  assert.equal(g.toY, 350);
  assert.equal(g.durationMs, 400);
  assert.equal(g.momentum, false);
  assert.equal(scrollMetadata(g).direction, 'up');
  assert.equal(t.settle(), null, 'recorded once');
});

test('catching a coast with a new drag finishes the one before', () => {
  const t = new ScrollGestureTracker();
  t.beginDrag(0, 0);
  t.endDrag(100, 200, 3);
  t.momentumBegin();
  const caught = t.beginDrag(500, 900);
  assert.equal(caught.toY, 900);
  assert.equal(caught.durationMs, 500);
  t.endDrag(700, 1000, 0);
  assert.equal(t.settle().fromY, 900);
});
