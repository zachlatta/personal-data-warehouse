import test from 'node:test';
import assert from 'node:assert/strict';

import {
  AHEAD,
  COLD_START_MAX_AGE_MS,
  ImageBudget,
  MAX_AGE_MS,
  MAX_IMAGES,
  OFFLINE_PAGE_MAX_AGE_MS,
  RETRY_FAILED_MS,
  cardMediaUrls,
  emptyIndex,
  evict,
  feedCacheText,
  fileKey,
  imageChoice,
  isOfflineError,
  localizeArticle,
  parseFeedCache,
  parseIndex,
  pickArticleImages,
  planArticles,
  plannedCards,
} from './offline-store.ts';

const MB = 1024 * 1024;
const card = (id) => ({ item_id: id });

test('saved cards come first, then the first cards of the feed seen or not, once each', () => {
  const items = Array.from({ length: AHEAD + 5 }, (_, i) => card(`i${i}`));
  const planned = plannedCards([card('s1'), card('i3')], items);
  assert.deepEqual(planned.slice(0, 3).map((c) => c.item_id), ['s1', 'i3', 'i0']);
  assert.equal(planned.length, AHEAD + 1);
  assert.ok(!planned.some((c) => c.item_id === `i${AHEAD}`));
});

test('an X post, a photo carousel, a video poster and a card with no url still yield their pictures', () => {
  const post = cardMediaUrls({
    url: 'https://x.com/a/status/1',
    embed: {
      type: 'x_post',
      author_avatar_url: 'https://pbs.twimg.com/a.jpg',
      media: [{ type: 'video', url: 'https://video.twimg.com/v.mp4', thumbnail_url: 'https://pbs.twimg.com/v.jpg' }],
      quote: { type: 'x_post', media: [{ type: 'image', url: 'https://pbs.twimg.com/q.jpg' }] },
    },
  });
  assert.deepEqual(post, [
    'https://pbs.twimg.com/a.jpg',
    'https://pbs.twimg.com/v.jpg',
    'https://pbs.twimg.com/q.jpg',
    'https://icons.duckduckgo.com/ip3/x.com.ico',
  ]);
  assert.ok(!post.includes('https://video.twimg.com/v.mp4'), 'a video file is not downloaded');
  const carousel = cardMediaUrls({ media: [{ type: 'image', url: 'https://cdn.example/1.jpg' }, { type: 'image', url: 'https://cdn.example/2.jpg' }] });
  assert.deepEqual(carousel, ['https://cdn.example/1.jpg', 'https://cdn.example/2.jpg']);
  assert.deepEqual(cardMediaUrls({ url: 'https://www.youtube.com/watch?v=abc', image_url: 'https://i.ytimg.com/p.jpg' }), [
    'https://i.ytimg.com/p.jpg',
    'https://icons.duckduckgo.com/ip3/youtube.com.ico',
  ]);
});

test('articles already stored, failed in the last day, or missing are not downloaded again', () => {
  const now = 10 * RETRY_FAILED_MS;
  const index = emptyIndex();
  const entry = (url, file, failed_at) => ({ url, file, failed_at, images: {}, bytes: 0, saved_at: 0, used_at: 0 });
  index.entries['https://a.example/stored'] = entry('https://a.example/stored', 'a-1.json', 0);
  index.entries['https://a.example/failed'] = entry('https://a.example/failed', '', now - 1000);
  index.entries['https://a.example/old-fail'] = entry('https://a.example/old-fail', '', now - RETRY_FAILED_MS - 1);
  const plan = planArticles(['https://a.example/stored', 'https://a.example/failed', 'https://a.example/old-fail', null, 'https://a.example/new', 'https://a.example/new'], index, now);
  assert.deepEqual(plan, ['https://a.example/old-fail', 'https://a.example/new']);
});

test('a picture is downloaded as its srcset entry nearest 1080 wide, else its src', () => {
  assert.equal(imageChoice('<img src="https://a.example/s.jpg" srcset="https://a.example/640.jpg 640w, https://a.example/1200.jpg 1200w, https://a.example/2400.jpg 2400w">'), 'https://a.example/1200.jpg');
  assert.equal(imageChoice('<img src="https://a.example/s.jpg" srcset="https://a.example/300.jpg 300w, https://a.example/600.jpg 600w">'), 'https://a.example/600.jpg');
  assert.equal(imageChoice('<img src="https://a.example/s.jpg?w=1&amp;h=2">'), 'https://a.example/s.jpg?w=1&h=2');
  assert.equal(imageChoice('<img data-src="https://a.example/lazy.jpg">'), '');
});

test('at most MAX_IMAGES pictures are picked, in page order', () => {
  const html = Array.from({ length: MAX_IMAGES + 3 }, (_, i) => `<p><img src="https://a.example/${i}.jpg"></p>`).join('') + '<img src="https://a.example/0.jpg">';
  const picked = pickArticleImages(html);
  assert.equal(picked.length, MAX_IMAGES);
  assert.equal(picked[0], 'https://a.example/0.jpg');
});

test('one article keeps at most 8 MB of pictures, 3 MB each; past that they stay network images', () => {
  const budget = new ImageBudget();
  assert.equal(budget.admit(4 * MB), false, 'one picture over 3 MB');
  assert.equal(budget.admit(0), false);
  assert.equal(budget.admit(3 * MB), true);
  assert.equal(budget.admit(3 * MB), true);
  assert.equal(budget.admit(2.5 * MB), false, 'would pass 8 MB in total');
  assert.equal(budget.admit(2 * MB), true);
  assert.equal(budget.used, 8 * MB);
  assert.equal(budget.admit(1), false);
});

test('a downloaded picture reads from its data: URI; one not downloaded is untouched', () => {
  const html =
    '<figure><picture><source srcset="https://a.example/1.webp 800w" type="image/webp"><img src="https://a.example/1.jpg" alt="one"></picture></figure>' +
    '<p><img src="https://a.example/s.jpg" srcset="https://a.example/1200.jpg 1200w" sizes="100vw" alt="two"></p>' +
    '<p><img src="https://a.example/3.jpg" alt="three"></p>';
  const out = localizeArticle(html, {
    'https://a.example/1.jpg': 'data:image/jpeg;base64,AAA',
    'https://a.example/1200.jpg': 'data:image/jpeg;base64,BBB',
  });
  assert.ok(out.includes('<picture><img src="data:image/jpeg;base64,AAA" alt="one"></picture>'), out);
  assert.ok(out.includes('<img src="data:image/jpeg;base64,BBB" alt="two">'), out);
  assert.ok(!out.includes('srcset') && !out.includes('<source'), out);
  assert.ok(out.includes('<img src="https://a.example/3.jpg" alt="three">'));
});

test('only a failure to reach the server reads as offline', () => {
  assert.equal(isOfflineError(new TypeError('Network request failed')), true);
  assert.equal(isOfflineError(new Error('fetch failed: java.io.IOException: unexpected end of stream on http://localhost:8790/...')), true);
  assert.equal(isOfflineError(new Error('fetch failed: java.net.UnknownHostException: Unable to resolve host')), true);
  assert.equal(isOfflineError(new SyntaxError('Unexpected token < in JSON')), false);
  const http = Object.assign(new Error('page has no article'), { status: 422 });
  assert.equal(isOfflineError(http), false);
  assert.equal(isOfflineError(new Error('boom')), false);
});

test('old entries go unless still in the feed; over the cap the least recently used go, saved last', () => {
  const now = 100 * MAX_AGE_MS;
  const index = emptyIndex();
  const add = (url, bytes, at) => (index.entries[url] = { url, file: '', failed_at: 0, images: {}, bytes, saved_at: at, used_at: at });
  add('old', 1, now - MAX_AGE_MS - 1);
  add('old-current', 1, now - MAX_AGE_MS - 1);
  add('old-saved', 1, now - MAX_AGE_MS - 1);
  assert.deepEqual(evict(index, new Set(['old-current']), new Set(['old-saved']), now), ['old']);

  const sized = emptyIndex();
  const put = (url, bytes, at) => (sized.entries[url] = { url, file: '', failed_at: 0, images: {}, bytes, saved_at: at, used_at: at });
  put('saved-oldest', 40, now - 30);
  put('older', 40, now - 20);
  put('newer', 40, now - 10);
  assert.deepEqual(evict(sized, new Set(['older', 'newer']), new Set(['saved-oldest']), now, 80), ['older']);
  assert.deepEqual(evict(sized, new Set(), new Set(['saved-oldest']), now, 30).sort(), ['newer', 'older', 'saved-oldest']);
});

test('a bad index file reads as empty', () => {
  assert.deepEqual(parseIndex('{nope'), emptyIndex());
  assert.deepEqual(parseIndex(null), emptyIndex());
  assert.deepEqual(parseIndex('{"v":2,"entries":{}}'), emptyIndex());
});

test('the feed page file: a cold start paints it under 3 days old, offline up to 14', () => {
  const day = 24 * 60 * 60 * 1000;
  const page = { items: [{ item_id: 'a' }], saved: [], generated_at: 'x' };
  const text = feedCacheText(page, 0);
  assert.equal(parseFeedCache(text, 5 * day, COLD_START_MAX_AGE_MS), null);
  assert.deepEqual(parseFeedCache(text, 5 * day, OFFLINE_PAGE_MAX_AGE_MS), { page, at: 0 });
  assert.equal(parseFeedCache(text, 15 * day, OFFLINE_PAGE_MAX_AGE_MS), null);
  assert.equal(parseFeedCache('{"v":2,"at":0,"page":{"items":[]}}', 0, OFFLINE_PAGE_MAX_AGE_MS), null);
  assert.equal(parseFeedCache('nope', 0, OFFLINE_PAGE_MAX_AGE_MS), null);
});

test('file keys are stable, short and differ by url', () => {
  assert.equal(fileKey('https://a.example/1'), fileKey('https://a.example/1'));
  assert.notEqual(fileKey('https://a.example/1'), fileKey('https://a.example/2'));
  assert.match(fileKey('https://a.example/1'), /^[0-9a-f]{16}$/);
});
