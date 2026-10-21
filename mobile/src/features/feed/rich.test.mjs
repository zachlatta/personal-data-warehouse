import test from 'node:test';
import assert from 'node:assert/strict';

import { compactCount, mediaAspect, mediaGrid, postSegments, shortAge, sourceBubbles, videoLength } from './rich.ts';

test('counts print the way X prints them', () => {
  assert.equal(compactCount(undefined), '');
  assert.equal(compactCount(0), '');
  assert.equal(compactCount(999), '999');
  assert.equal(compactCount(1000), '1K');
  assert.equal(compactCount(1234), '1.2K');
  assert.equal(compactCount(15_600), '15K');
  assert.equal(compactCount(2_500_000), '2.5M');
});

test('ages are short: seconds, minutes, hours, days, then a date', () => {
  const now = new Date('2026-10-20T12:00:00Z');
  assert.equal(shortAge('2026-10-20T11:59:30Z', now), '30s');
  assert.equal(shortAge('2026-10-20T11:48:00Z', now), '12m');
  assert.equal(shortAge('2026-10-20T09:00:00Z', now), '3h');
  assert.equal(shortAge('2026-10-17T12:00:00Z', now), '3d');
  assert.equal(shortAge('2026-10-05T12:00:00Z', now), 'Oct 5');
  assert.equal(shortAge('2024-10-05T12:00:00Z', now), 'Oct 5, 2024');
  assert.equal(shortAge(undefined, now), '');
});

test('post text splits into tappable urls, mentions and hashtags', () => {
  assert.deepEqual(postSegments('hi @karpathy see https://example.com/a. #AI'), [
    { kind: 'text', text: 'hi ' },
    { kind: 'mention', text: '@karpathy', href: 'https://x.com/karpathy' },
    { kind: 'text', text: ' see ' },
    { kind: 'url', text: 'example.com/a', href: 'https://example.com/a' },
    { kind: 'text', text: '.' },
    { kind: 'text', text: ' ' },
    { kind: 'hashtag', text: '#AI', href: 'https://x.com/hashtag/AI' },
  ]);
  assert.deepEqual(postSegments('plain'), [{ kind: 'text', text: 'plain' }]);
});

test('media grids follow X: 1 full, 2 side by side, 3 tall plus stacked, 4 in a square', () => {
  assert.deepEqual(mediaGrid(0), []);
  assert.deepEqual(mediaGrid(1), [[[0]]]);
  assert.deepEqual(mediaGrid(2), [[[0], [1]]]);
  assert.deepEqual(mediaGrid(3), [[[0], [1, 2]]]);
  assert.deepEqual(mediaGrid(7), [[[0], [1]], [[2], [3]]]);
});

test('a photo keeps its shape between 4:5 and 1.91:1', () => {
  assert.equal(mediaAspect({ type: 'image', url: 'x', width: 1000, height: 1000 }), 1);
  assert.equal(mediaAspect({ type: 'image', url: 'x', width: 1000, height: 3000 }), 0.8);
  assert.equal(mediaAspect({ type: 'image', url: 'x', width: 4000, height: 1000 }), 1.91);
  assert.equal(mediaAspect(undefined, 1.5), 1.5);
});

test('video lengths are m:ss', () => {
  assert.equal(videoLength(9.3), '0:09');
  assert.equal(videoLength(125), '2:05');
  assert.equal(videoLength(0), '');
});

test('the source rail puts sources with unseen cards first', () => {
  const seen = { impressions: 2 };
  const unseen = { impressions: 0 };
  assert.deepEqual(
    sourceBubbles([
      { source: 'hn', engagement: seen },
      { source: 'x', engagement: unseen },
      { source: 'x', engagement: seen },
      { source: 'picks', engagement: unseen },
    ]),
    [
      { source: 'x', label: 'x', total: 2, unseen: 1 },
      { source: 'picks', label: 'picks', total: 1, unseen: 1 },
      { source: 'hn', label: 'hn', total: 1, unseen: 0 },
    ],
  );
});
