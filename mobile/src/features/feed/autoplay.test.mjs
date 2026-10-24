import test from 'node:test';
import assert from 'node:assert/strict';

import { autoplayStep as step } from './autoplay.ts';
import { visibleFraction } from './viewability.ts';

const autoplayStep = (layouts, viewport, ...rest) =>
  step(new Map([...layouts].map(([id, l]) => [id, visibleFraction(l, viewport)])), ...rest);

const viewport = (top) => ({ top, height: 800 });
// Three 400px cards; a and c have videos, b does not.
const layouts = new Map([
  ['a', { y: 0, height: 400, position: 0 }],
  ['b', { y: 400, height: 400, position: 1 }],
  ['c', { y: 800, height: 400, position: 2 }],
]);
const keys = new Map([
  ['a', 'a'],
  ['c', 'c:quote'],
]);

test('the video card on screen starts, once', () => {
  assert.deepEqual(autoplayStep(layouts, viewport(0), keys, null, null), { focused: 'a', play: 'a' });
  assert.deepEqual(autoplayStep(layouts, viewport(0), keys, 'a', 'a'), { focused: 'a' });
});

test('a card barely on screen does not start', () => {
  // c shows 100 of 400px.
  assert.deepEqual(autoplayStep(layouts, viewport(100), new Map([['c', 'c']]), null, null), { focused: null });
});

test('scrolling on hands the focus to the next video card', () => {
  // a shows 100px, c is whole.
  assert.deepEqual(autoplayStep(layouts, viewport(300), keys, 'a', 'a'), { focused: 'c', play: 'c:quote' });
});

test('a video keeps the focus while it is mostly on screen', () => {
  // Both a (300px) and c (all) are past the threshold; a keeps playing.
  assert.deepEqual(autoplayStep(layouts, viewport(100), keys, 'a', 'a'), { focused: 'a' });
});

test('a playing video stops once its card has mostly scrolled away', () => {
  const tall = new Map([...layouts, ['c', { y: 1200, height: 400, position: 2 }]]);
  // a shows 50px of 400: under a quarter.
  assert.deepEqual(autoplayStep(tall, viewport(350), keys, 'a', 'a'), { focused: null, play: null });
  // A video playing in the detail view is not the feed's to stop.
  assert.deepEqual(autoplayStep(tall, viewport(350), keys, null, 'detail:a'), { focused: null });
});

test('a tapped video stays on while it is on screen', () => {
  // He tapped c (a quarter on screen) while a has the focus.
  assert.deepEqual(autoplayStep(layouts, viewport(100), keys, 'a', 'c:quote'), { focused: 'a' });
});
