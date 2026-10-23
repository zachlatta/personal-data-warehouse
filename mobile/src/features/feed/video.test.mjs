import test from 'node:test';
import assert from 'node:assert/strict';

import { cardVideo, mediaVideo, playerHtml, videoSource, vimeoId, youTubeId } from './video.ts';

test('YouTube ids come out of every url shape and nothing else', () => {
  for (const url of [
    'https://www.youtube.com/watch?v=dQw4w9WgXcQ',
    'https://m.youtube.com/watch?v=dQw4w9WgXcQ&t=42',
    'https://youtu.be/dQw4w9WgXcQ?si=abc',
    'https://www.youtube.com/shorts/dQw4w9WgXcQ',
    'https://www.youtube.com/embed/dQw4w9WgXcQ',
    'https://www.youtube.com/live/dQw4w9WgXcQ?feature=share',
  ]) {
    assert.equal(youTubeId(url), 'dQw4w9WgXcQ', url);
  }
  assert.equal(youTubeId('https://www.youtube.com/@somechannel'), '');
  assert.equal(youTubeId('https://www.youtube.com/watch?v=short'), '');
  assert.equal(youTubeId('https://notyoutube.com/watch?v=dQw4w9WgXcQ'), '');
  assert.equal(youTubeId(undefined), '');
});

test('Vimeo ids, and a start time from t=', () => {
  assert.equal(vimeoId('https://vimeo.com/76979871'), '76979871');
  assert.equal(vimeoId('https://player.vimeo.com/video/76979871?h=1'), '76979871');
  assert.equal(vimeoId('https://vimeo.com/channels/staffpicks'), '');
  assert.deepEqual(videoSource('https://youtu.be/dQw4w9WgXcQ?t=1m30s'), {
    kind: 'youtube',
    id: 'dQw4w9WgXcQ',
    start: 90,
    poster: 'https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg',
    url: 'https://youtu.be/dQw4w9WgXcQ?t=1m30s',
  });
});

test('video files play directly; pages that are not video are not players', () => {
  assert.equal(videoSource('https://video.twimg.com/ext_tw_video/1/pu/vid/720x1280/a.mp4?tag=12')?.kind, 'file');
  assert.equal(videoSource('https://example.com/clip.webm')?.kind, 'file');
  assert.equal(videoSource('https://example.com/article'), null);
  assert.equal(mediaVideo({ type: 'image', url: 'https://example.com/a.jpg' }), null);
  assert.deepEqual(mediaVideo({ type: 'gif', url: 'https://example.com/g', thumbnail_url: 'https://example.com/t.jpg' }), {
    kind: 'file',
    url: 'https://example.com/g',
    poster: 'https://example.com/t.jpg',
  });
});

test('a card plays its YouTube page with the og:image as poster, or its one video', () => {
  const yt = cardVideo({ url: 'https://www.youtube.com/watch?v=dQw4w9WgXcQ', image_url: 'https://i.ytimg.com/og.jpg' });
  assert.equal(yt?.kind, 'youtube');
  assert.equal(yt?.poster, 'https://i.ytimg.com/og.jpg');
  const clip = cardVideo({ url: 'https://example.com/post', media: [{ type: 'video', url: 'https://example.com/a.mp4' }] });
  assert.equal(clip?.kind, 'file');
  // A carousel keeps its pages; a plain article has no player.
  assert.equal(
    cardVideo({ url: 'https://example.com/p', media: [{ type: 'video', url: 'https://e.com/a.mp4' }, { type: 'image', url: 'https://e.com/b.jpg' }] }),
    null,
  );
  assert.equal(cardVideo({ url: 'https://example.com/p', image_url: 'https://e.com/b.jpg' }), null);
});

test('the player page autoplays inline and escapes a file url', () => {
  const yt = playerHtml({ kind: 'youtube', id: 'dQw4w9WgXcQ', start: 90, poster: '', url: '' });
  assert.match(yt, /youtube\.com\/embed\/dQw4w9WgXcQ\?autoplay=1&playsinline=1/);
  assert.match(yt, /start=90/);
  assert.match(yt, /referrerpolicy="strict-origin-when-cross-origin"/);
  const file = playerHtml({ kind: 'file', url: 'https://e.com/a.mp4?x=1&y="2"' });
  assert.match(file, /<video src="https:\/\/e\.com\/a\.mp4\?x=1&amp;y=&quot;2&quot;" controls autoplay playsinline>/);
  // X's video host refuses a foreign referrer; YouTube needs one.
  assert.match(file, /<meta name="referrer" content="no-referrer">/);
  assert.doesNotMatch(yt, /no-referrer/);
});
