import assert from 'node:assert/strict';
import test from 'node:test';

import { MEDIA_LINK_FRESH_MS, mediaLinkIsStale, openMediaLabel } from './timeline-media.ts';

test('a PDF with a link gets an Open PDF button', () => {
  assert.equal(openMediaLabel({ media_url: 'https://pdw.test/objects/a?sig=1', media_kind: 'pdf', mime_type: 'application/pdf' }), 'Open PDF');
});

test('images, other files and missing links get no button', () => {
  assert.equal(openMediaLabel({ media_url: 'https://pdw.test/objects/a', media_kind: 'image' }), null);
  assert.equal(openMediaLabel({ media_url: 'https://pdw.test/objects/a', media_kind: 'file' }), null);
  assert.equal(openMediaLabel({ media_url: '', media_kind: 'pdf' }), null);
  assert.equal(openMediaLabel(null), null);
  assert.equal(openMediaLabel(undefined), null);
});

test('a link fetched within the fresh window is used as is', () => {
  assert.equal(mediaLinkIsStale(1_000, 1_000), false);
  assert.equal(mediaLinkIsStale(1_000, 1_000 + MEDIA_LINK_FRESH_MS - 1), false);
});

test('a link as old as the fresh window is fetched again', () => {
  assert.equal(mediaLinkIsStale(1_000, 1_000 + MEDIA_LINK_FRESH_MS), true);
  assert.ok(MEDIA_LINK_FRESH_MS < 60 * 60 * 1000, 'fresh window stays inside the hour-long signature');
});
