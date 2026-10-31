import test from 'node:test';
import assert from 'node:assert/strict';
import { FeedSessionLifecycle } from './session-lifecycle.ts';

for (const host of ['tab', 'embedded']) {
  test(`${host}: detail and browser keep impression, open and close in one session`, () => {
    const lifecycle = new FeedSessionLifecycle();
    lifecycle.focus();
    let session = 'session-1';
    const events = [{ type: 'impression', session_id: session }];
    lifecycle.openDetail();
    if (lifecycle.blur()) session = '';
    for (const type of ['open', 'web', 'close']) events.push({ type, session_id: session });
    assert.deepEqual(events.map(e => e.session_id), Array(4).fill('session-1'));
    assert.equal(lifecycle.owns, true);
    lifecycle.focus();
    assert.equal(lifecycle.blur(), true); // leaving for another tab/page
    assert.equal(lifecycle.owns, false);
  });
}

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
