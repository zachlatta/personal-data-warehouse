import assert from 'node:assert/strict';
import test from 'node:test';

import { markEventOpen, takeEventPaint } from './event-paint.ts';

test('the event the row marked reads its paint time once', () => {
  markEventOpen('gmail', 'm1', 100);
  assert.equal(takeEventPaint('gmail', 'm1', 142), 42);
  assert.equal(takeEventPaint('gmail', 'm1', 200), null);
});

test('a mark for another event gives nothing and is dropped', () => {
  markEventOpen('gmail', 'm1', 100);
  assert.equal(takeEventPaint('slack', 'm1', 150), null);
  assert.equal(takeEventPaint('gmail', 'm1', 160), null);
});
