import test from 'node:test';
import assert from 'node:assert/strict';
import { FeedSessionLifecycle } from './session-lifecycle.ts';

test('background ends a session even with detail visible; foreground starts another', () => {
  const lifecycle = new FeedSessionLifecycle();
  lifecycle.focus();
  lifecycle.openDetail();
  assert.equal(lifecycle.blur(), false);
  assert.equal(lifecycle.appState('background'), 'end');
  assert.equal(lifecycle.appState('active'), 'start');
  lifecycle.focus();
  lifecycle.blur();
  assert.equal(lifecycle.appState('active'), null);
});

for (const host of ['tab', 'embedded']) {
  test(`${host}: closing away from Feed releases session ownership`, () => {
    const lifecycle = new FeedSessionLifecycle();
    lifecycle.focus();
    lifecycle.openDetail();
    lifecycle.blur();
    assert.equal(lifecycle.closeDetail(false), true);
    assert.equal(lifecycle.owns, false);
    assert.equal(lifecycle.appState('active'), null);
  });
  test(`${host}: returning to Feed during close grace retains ownership`, () => {
    const lifecycle = new FeedSessionLifecycle();
    lifecycle.focus();
    lifecycle.openDetail();
    lifecycle.blur();
    lifecycle.focus();
    assert.equal(lifecycle.closeDetail(true), false);
    assert.equal(lifecycle.owns, true);
  });
}
