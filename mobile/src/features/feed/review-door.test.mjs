import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';
import test from 'node:test';
import assert from 'node:assert/strict';

import { FeedJumpController, consumeFeedJump, requestFeedJump, reviewDoorPreview } from './review-door.ts';

function item(id, overrides = {}) {
  return {
    item_id: id,
    source: 'picks',
    key: id,
    title: `Title ${id}`,
    tags: [],
    score: 1,
    created_at: '2026-11-26T00:00:00Z',
    engagement: { impressions: 0, opens: 0, engaged: false, engagement_score: 0 },
    ...overrides,
  };
}

test('reviewDoorPreview picks the top unseen item and prefers server unseen_count', () => {
  const page = {
    items: [item('picks:1', { image_url: 'https://example.com/a.jpg', source: 'hn' }), item('picks:2')],
    unseen_count: 17,
    generated_at: '2026-11-26T00:00:00Z',
  };
  assert.deepEqual(reviewDoorPreview(page), {
    itemId: 'picks:1',
    title: 'Title picks:1',
    source: 'hn',
    imageUrl: 'https://example.com/a.jpg',
    unseen: 17,
  });
});

test('reviewDoorPreview falls back to counting unseen items and hides when 0 unseen', () => {
  const page = {
    items: [item('picks:1'), item('picks:2'), item('picks:3', { resurfaced: true })],
    generated_at: '2026-11-26T00:00:00Z',
  };
  assert.equal(reviewDoorPreview(page)?.unseen, 2);
  assert.equal(reviewDoorPreview({ items: [item('picks:3', { resurfaced: true })], unseen_count: 0, generated_at: '' }), null);
  assert.equal(reviewDoorPreview({ items: [], unseen_count: 0, generated_at: '' }), null);
});

test('requestFeedJump / consumeFeedJump is one-shot', () => {
  requestFeedJump('picks:42');
  assert.equal(consumeFeedJump(), 'picks:42');
  assert.equal(consumeFeedJump(), null);
});

const require = createRequire(import.meta.url);

function doorHarness({ page = null, pages = null, failure = null, feedEnabled = true } = {}) {
  const state = [];
  const refs = [];
  const effectDeps = [];
  let stateCursor = 0;
  let refCursor = 0;
  let effectCursor = 0;
  const pendingEffects = [];
  const uiEvents = [];
  const otherEvents = [];
  let openedFeed = 0;
  let fetchCount = 0;

  const react = {
    useState(initial) {
      const i = stateCursor++;
      if (!(i in state)) state[i] = typeof initial === 'function' ? initial() : initial;
      return [state[i], (value) => { state[i] = typeof value === 'function' ? value(state[i]) : value; }];
    },
    useRef(initial) {
      const i = refCursor++;
      if (!(i in refs)) refs[i] = { current: initial };
      return refs[i];
    },
    useEffect(fn, deps) {
      const i = effectCursor++;
      const prev = effectDeps[i];
      const changed = !prev || !deps || deps.length !== prev.length || deps.some((d, idx) => !Object.is(d, prev[idx]));
      if (changed) {
        effectDeps[i] = deps;
        pendingEffects.push(fn);
      }
    },
  };
  const modules = {
    react,
    'react-native': { Pressable: 'pressable', View: 'view', StyleSheet: { create: (s) => s } },
    'expo-image': { Image: 'image' },
    '@/components/themed-text': { ThemedText: 'text' },
    '@/constants/theme': { Spacing: { one: 4, two: 8, three: 12 } },
    '@/hooks/use-theme': { useTheme: () => ({ surface: '#fff', surfaceBorder: '#ddd', textSecondary: '#777', text: '#111' }) },
    './api': {
      fetchFeed: async () => {
        const idx = fetchCount++;
        if (failure) throw failure;
        if (pages) return pages[Math.min(idx, pages.length - 1)];
        return page;
      },
    },
    './engagement': {
      recordUi: (action, fields) => uiEvents.push({ action, ...fields }),
      recordFeedEvent: (type, fields) => otherEvents.push({ type, ...fields }),
    },
    './index': { FEED_ENABLED: feedEnabled },
    './review-door': { reviewDoorPreview, requestFeedJump, consumeFeedJump },
  };
  const source = ts.transpileModule(readFileSync(new URL('./review-door-card.tsx', import.meta.url), 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX },
  }).outputText;
  const exports = {};
  runInNewContext(source, { exports, require: (id) => modules[id] ?? require(id) });

  const config = { baseUrl: 'https://x.test', token: 't', clientName: 'c' };
  const onOpenFeed = () => { openedFeed++; };
  const render = (refreshKey = 0) => {
    stateCursor = 0;
    refCursor = 0;
    effectCursor = 0;
    return exports.ReviewToFeedDoor({ config, refreshKey, onOpenFeed });
  };
  const flushEffects = async () => {
    for (const fn of pendingEffects.splice(0)) fn();
    await new Promise((resolve) => setImmediate(resolve));
  };
  return { render, flushEffects, uiEvents, otherEvents, openedFeed: () => openedFeed, fetchCount: () => fetchCount };
}

function collectText(node) {
  if (node == null || typeof node === 'boolean') return [];
  if (typeof node === 'string' || typeof node === 'number') return [String(node)];
  if (Array.isArray(node)) return node.flatMap(collectText);
  return collectText(node.props?.children);
}

test('ReviewToFeedDoor shows the top unseen card, count, and one shown event for 3 unseen items; tap logs tapped and jumps', async () => {
  const page = {
    items: [
      item('picks:top', { title: 'Top unseen pick', source: 'picks', image_url: 'https://example.com/pic.jpg' }),
      item('picks:2'),
      item('picks:3'),
    ],
    unseen_count: 3,
    generated_at: '2026-11-26T00:00:00Z',
  };
  const h = doorHarness({ page });
  assert.equal(h.render(), null);
  await h.flushEffects();
  const tree = h.render();
  assert.ok(tree, 'card must render for 3 unseen items');
  await h.flushEffects();
  // Re-rendering does not duplicate the shown event.
  h.render();
  await h.flushEffects();

  const text = collectText(tree).join(' | ');
  assert.match(text, /3 new in Feed/);
  assert.match(text, /Top unseen pick/);
  assert.match(text, /picks/);

  assert.equal(h.otherEvents.length, 0, 'must not record a feed impression');
  assert.equal(h.uiEvents.length, 1);
  assert.deepEqual(JSON.parse(JSON.stringify(h.uiEvents[0])), {
    action: 'review_to_feed',
    metadata: { event: 'shown', item_id: 'picks:top', unseen: 3 },
  });

  // Tapping records tapped, queues the jump target, and navigates to Feed.
  tree.props.onPress();
  assert.equal(h.openedFeed(), 1);
  assert.equal(consumeFeedJump(), 'picks:top');
  assert.equal(h.uiEvents.length, 2);
  assert.deepEqual(JSON.parse(JSON.stringify(h.uiEvents[1])), {
    action: 'review_to_feed',
    metadata: { event: 'tapped', item_id: 'picks:top' },
  });
});

test('ReviewToFeedDoor renders nothing and logs nothing when 0 unseen or request fails', async () => {
  for (const opts of [
    { page: { items: [], unseen_count: 0, generated_at: '' } },
    { failure: new Error('offline') },
  ]) {
    const h = doorHarness(opts);
    assert.equal(h.render(), null);
    await h.flushEffects();
    assert.equal(h.render(), null);
    await h.flushEffects();
    assert.equal(h.uiEvents.length, 0);
  }
});

test('ReviewToFeedDoor refetches and logs one new shown event on a second paint without duplicating on rerender', async () => {
  const page1 = {
    items: [item('picks:first', { title: 'First pick', source: 'picks' })],
    unseen_count: 4,
    generated_at: '2026-11-26T00:00:00Z',
  };
  const page2 = {
    items: [item('picks:second', { title: 'Second pick', source: 'hn' })],
    unseen_count: 2,
    generated_at: '2026-11-26T00:05:00Z',
  };
  const h = doorHarness({ pages: [page1, page2] });
  h.render(0);
  await h.flushEffects();
  let tree = h.render(0);
  await h.flushEffects();
  assert.match(collectText(tree).join(' | '), /4 new in Feed/);
  assert.equal(h.fetchCount(), 1);
  assert.equal(h.uiEvents.length, 1);

  // Plain rerender at the same refreshKey does not refetch or log again.
  h.render(0);
  await h.flushEffects();
  assert.equal(h.fetchCount(), 1);
  assert.equal(h.uiEvents.length, 1);

  // Second screen paint (focus, resume, or live refresh) bumps refreshKey:
  // refetches and logs one new shown event.
  h.render(1);
  await h.flushEffects();
  tree = h.render(1);
  await h.flushEffects();
  assert.match(collectText(tree).join(' | '), /2 new in Feed/);
  assert.match(collectText(tree).join(' | '), /Second pick/);
  assert.equal(h.fetchCount(), 2);
  assert.equal(h.uiEvents.length, 2);
  assert.deepEqual(JSON.parse(JSON.stringify(h.uiEvents[1])), {
    action: 'review_to_feed',
    metadata: { event: 'shown', item_id: 'picks:second', unseen: 2 },
  });

  // Rerender at refreshKey=1 does not duplicate.
  h.render(1);
  await h.flushEffects();
  assert.equal(h.uiEvents.length, 2);
});

test('FeedJumpController scrolls immediately on an already-mounted list, waits for stale loads, applies held arrivals, and retries on scrollToIndexFailed', () => {
  const calls = [];
  const timers = [];
  const scroller = {
    scrollToOffset: (opts) => calls.push(['offset', opts.offset]),
    scrollToIndex: (opts) => calls.push(['index', opts.index]),
  };
  const schedule = (fn) => { timers.push(fn); return timers.length; };
  const ctrl = new FeedJumpController(schedule);

  // 1. Already-mounted list, target at index 2: jumps immediately and clears pending.
  ctrl.arm('picks:2');
  assert.equal(ctrl.hasPending(), true);
  ctrl.tryJump(['picks:0', 'picks:1', 'picks:2'], { filtered: false, loading: false }, scroller);
  assert.deepEqual(calls, [['index', 2]]);
  assert.equal(ctrl.hasPending(), false);
  // Later render does not surprise-scroll.
  ctrl.tryJump(['picks:0', 'picks:1', 'picks:2'], { filtered: false, loading: false }, scroller);
  assert.deepEqual(calls, [['index', 2]]);

  // 2. scrollToIndexFailed scrolls to estimated offset and retries scrollToIndex once.
  ctrl.onScrollToIndexFailed({ index: 2, averageItemLength: 320 }, scroller);
  assert.deepEqual(calls, [['index', 2], ['offset', 640]]);
  assert.equal(timers.length, 1);
  timers.shift()();
  assert.deepEqual(calls, [['index', 2], ['offset', 640], ['index', 2]]);
  // A second failure clears state and does not loop.
  ctrl.onScrollToIndexFailed({ index: 2, averageItemLength: 320 }, scroller);
  assert.equal(timers.length, 0);

  // 3. Stale reload in flight: waits until loading finishes, then lands on the new item.
  calls.length = 0;
  ctrl.arm('picks:new');
  ctrl.tryJump(['picks:old'], { filtered: false, loading: true }, scroller);
  assert.deepEqual(calls, []);
  assert.equal(ctrl.hasPending(), true);
  ctrl.tryJump(['picks:new', 'picks:old'], { filtered: false, loading: false }, scroller);
  assert.deepEqual(calls, [['offset', 0]]);
  assert.equal(ctrl.hasPending(), false);
});
