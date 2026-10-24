import test from 'node:test';
import assert from 'node:assert/strict';

import { PageVisit, parseTrackerMessage, samePage, TRACKER_JS } from './web-tracker.ts';

test('the tracker script parses as JavaScript', () => {
  assert.doesNotThrow(() => new Function(TRACKER_JS));
});

test('only the tracker messages are read', () => {
  assert.deepEqual(parseTrackerMessage('{"k":"tap","text":"Buy"}'), { k: 'tap', text: 'Buy' });
  assert.equal(parseTrackerMessage('{"k":"other"}'), null);
  assert.equal(parseTrackerMessage('hello'), null);
});

test('a #fragment is the same page', () => {
  assert.ok(samePage('https://a.test/x#one', 'https://a.test/x#two'));
  assert.ok(!samePage('https://a.test/x', 'https://a.test/y'));
});

test('a page summary keeps where he scrolled, what he stopped on and what he did', () => {
  const v = new PageVisit('https://a.test/post', 0, '');
  v.add({ k: 'page', url: 'https://a.test/post?ref=1', title: 'A post', words: 1200, text: 'Intro paragraph' }, 0);
  v.add({ k: 'scroll', y: 800, pct: 30, text: 'The middle', heading: 'Part one' }, 10_000);
  v.add({ k: 'tap', tag: 'a', text: 'a source', href: 'https://b.test/' }, 12_000);
  v.add({ k: 'select', text: 'a quote' }, 13_000);
  v.add({ k: 'scroll', y: 400, pct: 15, text: 'Skimmed', heading: 'Part one' }, 40_000);
  v.add({ k: 'scroll', y: 2400, pct: 90, text: 'The end', heading: 'Part two' }, 41_000);
  const s = v.finish(50_000);
  assert.equal(s.url, 'https://a.test/post?ref=1');
  assert.equal(s.title, 'A post');
  assert.equal(s.words, 1200);
  assert.equal(s.max_scroll_pct, 90);
  assert.equal(s.scrolls, 3);
  assert.equal(s.taps, 1);
  assert.equal(s.links_tapped, 1);
  assert.equal(s.selections, 1);
  assert.deepEqual(s.headings, ['Part one', 'Part two']);
  // Longest first; under two seconds ("Skimmed") is a pass, not a read.
  assert.deepEqual(s.read, [
    { text: 'The middle', ms: 30_000 },
    { text: 'Intro paragraph', ms: 10_000 },
    { text: 'The end', ms: 9_000 },
  ]);
});
