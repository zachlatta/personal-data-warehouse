import test from 'node:test';
import assert from 'node:assert/strict';
import { FeedSessionLifecycle, FeedSessionCoordinator } from './session-lifecycle.ts';

test('background without detail ends immediately; foreground starts another', () => {
  const lifecycle = new FeedSessionLifecycle();
  lifecycle.focus();
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

test('foreground recovery requires visibility and preserves another host ownership', () => {
  const coordinator = new FeedSessionCoordinator();
  const tab = new FeedSessionLifecycle(coordinator);
  const launcher = new FeedSessionLifecycle(coordinator);
  assert.equal(tab.appState('active', false), null);
  launcher.focus();
  assert.equal(tab.appState('active', true), null);
  assert.equal(launcher.owns, true);
});


test('embedded detail transition keeps the original session through background and active', (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const lifecycle = new FeedSessionLifecycle();
  let session = 'original';
  const events = [];
  lifecycle.focus();
  lifecycle.openDetail();
  const end = () => { session = ''; };
  if (lifecycle.appState('background', false, end) === 'end') end();
  t.mock.timers.tick(500);
  lifecycle.appState('active');
  events.push({type:'open', session});
  t.mock.timers.tick(2000);
  events.push({type:'close', session});
  assert.ok(events.every(e => e.session === 'original'));
});

test('detail background past the grace ends the session', (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const lifecycle = new FeedSessionLifecycle();
  lifecycle.focus();
  lifecycle.openDetail();
  let ended = 0;
  assert.equal(lifecycle.appState('background', false, () => ended++), null);
  t.mock.timers.tick(1499);
  assert.equal(ended, 0);
  t.mock.timers.tick(1);
  assert.equal(ended, 1);
});

test('reader entry cancels a pending detail background end', (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const lifecycle = new FeedSessionLifecycle();
  lifecycle.focus();
  lifecycle.openDetail();
  let ended = false;
  lifecycle.appState('background', false, () => { ended = true; });
  t.mock.timers.tick(500);
  lifecycle.cancelBackgroundEnd();
  t.mock.timers.tick(2000);
  assert.equal(ended, false);
});
