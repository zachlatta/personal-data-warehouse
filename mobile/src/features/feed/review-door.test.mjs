import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';
import test from 'node:test';
import assert from 'node:assert/strict';

import { consumeFeedJump, requestFeedJump, reviewDoorPreview } from './review-door.ts';

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

function doorHarness({ page = null, failure = null, feedEnabled = true } = {}) {
  const state = [];
  const refs = [];
  let stateCursor = 0;
  let refCursor = 0;
  const effects = [];
  const uiEvents = [];
  const otherEvents = [];
  let openedFeed = 0;

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
    useEffect(fn) {
      effects.push(fn);
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
        if (failure) throw failure;
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

  const render = () => {
    stateCursor = 0;
    refCursor = 0;
    effects.length = 0;
    return exports.ReviewToFeedDoor({ config: { baseUrl: 'https://x.test', token: 't', clientName: 'c' }, onOpenFeed: () => { openedFeed++; } });
  };
  const flushEffects = async () => {
    for (const fn of effects.splice(0)) fn();
    await new Promise((resolve) => setImmediate(resolve));
  };
  return { render, flushEffects, uiEvents, otherEvents, openedFeed: () => openedFeed };
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
