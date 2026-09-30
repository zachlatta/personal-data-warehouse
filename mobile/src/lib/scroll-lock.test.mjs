import test from 'node:test';
import assert from 'node:assert/strict';

import { SCROLL_SETTLE_MS, nextScrollLock } from './scroll-lock.ts';

const idle = { dragging: false, momentum: false };

test('a drag locks the editor until the page has been still for a moment', () => {
  let step = nextScrollLock(idle, 'dragStart');
  assert.equal(step.locked, true);
  assert.equal(step.unlockAfterMs, null);
  step = nextScrollLock(step.state, 'dragEnd');
  assert.equal(step.locked, true);
  assert.equal(step.unlockAfterMs, SCROLL_SETTLE_MS);
});

test('momentum after a drag keeps it locked, and a touch that stops the page does not unlock it at once', () => {
  // The recording: the keyboard came up on a touch that stopped a moving page.
  let step = nextScrollLock(idle, 'dragStart');
  step = nextScrollLock(step.state, 'dragEnd');
  step = nextScrollLock(step.state, 'momentumStart');
  assert.equal(step.locked, true);
  assert.equal(step.unlockAfterMs, null);
  step = nextScrollLock(step.state, 'momentumEnd');
  assert.equal(step.locked, true);
  assert.ok(step.unlockAfterMs >= 300);
});

test('a new drag during the settle window cancels the unlock', () => {
  let step = nextScrollLock(idle, 'dragStart');
  step = nextScrollLock(step.state, 'dragEnd');
  step = nextScrollLock(step.state, 'dragStart');
  assert.equal(step.unlockAfterMs, null);
  assert.deepEqual(step.state, { dragging: true, momentum: false });
});
