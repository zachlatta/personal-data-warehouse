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
  assert.match(file, /<video src="https:\/\/e\.com\/a\.mp4\?x=1&amp;y=&quot;2&quot;" controls autoplay playsinline preload="auto">/);
  // X's video host refuses a foreign referrer; YouTube needs one.
  assert.match(file, /<meta name="referrer" content="no-referrer">/);
  assert.doesNotMatch(yt, /no-referrer/);
});

test('autoplayKey matches the key the card gives its player', async () => {
  const { autoplayKey } = await import('./video.ts');
  const clip = { type: 'video', url: 'https://video.twimg.com/amplify_video/1/vid/avc1/1280x720/x.mp4?tag=29' };
  assert.equal(autoplayKey({ item_id: 'x1', embed: { type: 'x_post', media: [clip] } }), 'x1');
  assert.equal(autoplayKey({ item_id: 'x2', embed: { type: 'x_post', media: [], quote: { type: 'x_post', media: [clip] } } }), 'x2:quote');
  assert.equal(autoplayKey({ item_id: 'x3', embed: { type: 'x_post', media: [clip, clip] } }), null);
  assert.equal(autoplayKey({ item_id: 'y1', url: 'https://www.youtube.com/watch?v=dQw4w9WgXcQ' }), 'y1');
  assert.equal(autoplayKey({ item_id: 'p1', url: 'https://example.com/post', image_url: 'https://example.com/a.jpg' }), null);
});

test('an autoplayed player is muted', async () => {
  const { playerHtml } = await import('./video.ts');
  const file = { kind: 'file', url: 'https://video.twimg.com/a.mp4' };
  assert.match(playerHtml(file, { muted: true }), /<video[^>]* muted/);
  assert.doesNotMatch(playerHtml(file), /<video[^>]* muted/);
  assert.match(playerHtml({ kind: 'youtube', id: 'dQw4w9WgXcQ', poster: '', url: '' }, { muted: true }), /mute=1/);
});

test('TikTok videos and Instagram reels play in their own embeds; Instagram photo posts do not', async () => {
  const { tikTokId, instagramPost, instagramEmbedUrl, instagramPlayerScript, videoAspect } = await import('./video.ts');
  assert.equal(tikTokId('https://www.tiktok.com/@someone/video/6718335390845095173?lang=en'), '6718335390845095173');
  assert.equal(tikTokId('https://www.tiktok.com/embed/v2/6718335390845095173'), '6718335390845095173');
  assert.equal(tikTokId('https://www.tiktok.com/@someone'), '');
  assert.equal(tikTokId('https://vm.tiktok.com/ZMabc123/'), '');
  assert.deepEqual(instagramPost('https://www.instagram.com/reel/Dad3owVysaa/?igsh=x'), { code: 'Dad3owVysaa', path: 'reel' });
  assert.deepEqual(instagramPost('https://www.instagram.com/someone/reels/Dad3owVysaa/'), { code: 'Dad3owVysaa', path: 'reel' });
  assert.deepEqual(instagramPost('https://instagram.com/p/Dad3owVysaa'), { code: 'Dad3owVysaa', path: 'p' });
  assert.equal(instagramPost('https://www.instagram.com/someone/'), null);

  const tt = videoSource('https://www.tiktok.com/@someone/video/6718335390845095173', 'https://e.com/t.jpg');
  assert.equal(tt?.kind, 'tiktok');
  assert.match(playerHtml(tt, { muted: true }), /tiktok\.com\/player\/v1\/6718335390845095173\?autoplay=1&muted=1/);
  const reel = videoSource('https://www.instagram.com/reel/Dad3owVysaa/');
  assert.equal(reel?.kind, 'instagram');
  assert.equal(instagramEmbedUrl(reel), 'https://www.instagram.com/reel/Dad3owVysaa/embed/');
  assert.equal(videoSource('https://www.instagram.com/p/Dad3owVysaa/'), null);
  assert.equal(cardVideo({ url: 'https://www.instagram.com/reel/Dad3owVysaa/', image_url: 'https://e.com/r.jpg' })?.poster, 'https://e.com/r.jpg');
  assert.match(instagramPlayerScript({ muted: true }), /v\.muted=true/);
  assert.match(instagramPlayerScript(), /v\.muted=false/);
  assert.equal(videoAspect(reel), 4 / 5);
  assert.equal(videoAspect(tt), 4 / 5);
  assert.equal(videoAspect(videoSource('https://youtu.be/dQw4w9WgXcQ')), 16 / 9);
});

test('every player page takes the sound button', async () => {
  const { playerHtml, setMutedScript, instagramPlayerScript } = await import('./video.ts');
  assert.equal(setMutedScript(false), 'window.pdwSetMuted&&window.pdwSetMuted(false);true;');
  const yt = playerHtml({ kind: 'youtube', id: 'dQw4w9WgXcQ', poster: '', url: '' }, { muted: true });
  assert.match(yt, /enablejsapi=1&origin=https%3A%2F%2Fpdw\.zachlatta\.com/);
  for (const src of [
    { kind: 'youtube', id: 'dQw4w9WgXcQ', poster: '', url: '' },
    { kind: 'vimeo', id: '76979871', url: '' },
    { kind: 'tiktok', id: '6718335390845095173', url: '' },
    { kind: 'file', url: 'https://e.com/a.mp4' },
  ]) assert.match(playerHtml(src, { muted: true }), /window\.pdwSetMuted=function/);
  assert.match(instagramPlayerScript({ muted: true }), /window\.pdwSetMuted=function/);
});

test('every player page says when it goes full screen', async () => {
  const { instagramPlayerScript } = await import('./video.ts');
  for (const src of [
    { kind: 'youtube', id: 'dQw4w9WgXcQ' },
    { kind: 'vimeo', id: '76979871' },
    { kind: 'tiktok', id: '7300000000000000000' },
    { kind: 'file', url: 'https://video.example.com/a.mp4' },
  ]) {
    assert.match(playerHtml(src), /fullscreenchange/, src.kind);
  }
  assert.match(instagramPlayerScript(), /fullscreenchange/);
});
