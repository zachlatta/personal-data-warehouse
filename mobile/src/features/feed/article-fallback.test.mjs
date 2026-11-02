import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';
import test from 'node:test';
import assert from 'node:assert/strict';
import { articleFallback, fetchReadableArticle, knownNotArticle } from './article-fallback.ts';

test('422 and other online failures offer the card page; offline keeps its message', () => {
  const url = 'https://example.com/game';
  assert.deepEqual(articleFallback(url, Object.assign(new Error('no article'), { status: 422 })), { kind: 'page', url, label: 'Open the page' });
  assert.equal(articleFallback(url, new TypeError('Network request failed')).kind, 'offline');
  assert.equal(articleFallback(url, Object.assign(new Error('server'), { status: 500 })).kind, 'page');
});

test('only a fresh explicit not-article entry skips loading, including after parsing disk JSON', () => {
  const now = 200000000;
  const entry = JSON.parse(JSON.stringify({ file: '', failed_at: now, failure_reason: 'not_article' }));
  assert.equal(knownNotArticle(entry, now), true);
  assert.equal(knownNotArticle(entry, now + 86400000), false);
  assert.equal(knownNotArticle({ ...entry, failure_reason: undefined }, now), false);
  assert.equal(knownNotArticle({ ...entry, failure_reason: 'unknown' }, now), false);
  assert.equal(knownNotArticle({ ...entry, file: 'article.json' }, now), false);
});

test('foreground 422 remembers the card url, while server and offline failures remain retryable', async () => {
  const remembered = [];
  for (const status of [422, 500, 0]) {
    const error = Object.assign(new Error('failed'), { status });
    await assert.rejects(fetchReadableArticle('https://example.com/game', async () => { throw error; },
      (url) => remembered.push(url)), (e) => e === error);
  }
  assert.deepEqual(remembered, ['https://example.com/game']);
  assert.equal(await fetchReadableArticle('https://example.com/post', async () => 'article',
    () => assert.fail('successful article was marked unavailable')), 'article');
});

// Render the real component with native primitives and I/O replaced, so the
// first frame and tap target are checked without a device or network.
const require = createRequire(import.meta.url);

function articleViewHarness({ cached = false, failure = null } = {}) {
  const state = [];
  let cursor = 0;
  const effects = [];
  const react = {
    useState(initial) {
      const i = cursor++;
      if (!(i in state)) state[i] = typeof initial === 'function' ? initial() : initial;
      return [state[i], (value) => { state[i] = value; }];
    },
    useEffect(fn) { effects.push(fn); },
    useMemo(fn) { return fn(); },
  };
  const native = { Pressable: 'button', View: 'view', ActivityIndicator: 'spinner', StyleSheet: { create: (s) => s } };
  const theme = { textSecondary: '#777' };
  const modules = {
    react,
    'react-native': native,
    'react-native-webview': { WebView: 'webview' },
    '@/components/themed-text': { ThemedText: 'text' },
    '@/constants/theme': { Spacing: { two: 8, three: 12 } },
    '@/hooks/use-theme': { useTheme: () => theme },
    '@/lib/session': { useConfig: () => ({}) },
    './api': { FeedApiError: require('../../lib/request.ts').ApiError, fetchArticle: async () => { if (failure) throw failure; return { html: '<p>Read</p>', words: 1 }; } },
    './article-fallback': { articleFallback, fetchReadableArticle },
    './article-reads': require('./article-reads.ts'),
    './article': { articleDocument: () => '<html>Read</html>', readingMinutes: () => 1 },
    './offline': { isKnownNotArticle: () => cached, readStoredArticle: async () => null, setOffline() {}, storeArticle() {}, storeFailure() {} },
    './offline-store': require('./offline-store.ts'),
    './rich-views': { ACCENT: '#f60' },
  };
  const source = ts.transpileModule(readFileSync(new URL('./article-view.tsx', import.meta.url), 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX },
  }).outputText;
  const exports = {};
  runInNewContext(source, { exports, require: (id) => modules[id] ?? require(id), setTimeout, clearTimeout });
  let opens = 0;
  const render = () => { cursor = 0; return exports.ArticleView({ url: 'https://example.com/game', shownImages: [], onLink() {}, onOutcome() {}, onOpenPage: () => { opens++; } }); };
  return { render, effects, opens: () => opens };
}

test('real view renders cached 422 button on its first frame and taps the page callback', () => {
  const h = articleViewHarness({ cached: true });
  const tree = h.render();
  assert.equal(tree.type, 'button');
  assert.equal(tree.props.children.props.children, 'Open the page');
  tree.props.onPress();
  assert.equal(h.opens(), 1);
});

test('real view changes loading into the page action for 422 and into the offline message for a network failure', async () => {
  for (const failure of [Object.assign(new Error('no article'), { status: 422 }), new TypeError('Network request failed')]) {
    const h = articleViewHarness({ failure });
    assert.equal(h.render().type, 'view');
    h.effects[0]();
    await new Promise((resolve) => setImmediate(resolve));
    const tree = h.render();
    if ('status' in failure) assert.equal(tree.type, 'button');
    else {
      assert.equal(tree.type, 'text');
      assert.match(tree.props.children, /You're offline/);
    }
  }
});

test('real view still renders a successful article instead of the fallback', async () => {
  const h = articleViewHarness();
  h.render();
  h.effects[0]();
  await new Promise((resolve) => setImmediate(resolve));
  const tree = h.render();
  assert.equal(tree.type, 'view');
  assert.equal(tree.props.children[1].type, 'webview');
  assert.equal(tree.props.children[1].props.source.html, '<html>Read</html>');
});

// Run the saved-card path with an in-memory filesystem: a network failure
// must leave the article eligible for the next online prefetch.
function savedCardHarness(error) {
  const files = new Map();
  class Directory { exists = true; }
  class File {
    constructor(_dir, name) { this.name = name; }
    get exists() { return files.has(this.name); }
    textSync() { return files.get(this.name); }
    write(text) { files.set(this.name, text); }
  }
  const modules = {
    'expo-file-system': { Directory, File, Paths: { document: '' } },
    'expo-image': { Image: {} },
    react: {},
    'react-native': { AppState: { currentState: 'active' } },
    './api': { FeedApiError: require('../../lib/request.ts').ApiError, fetchArticle: async () => { throw error; } },
    './article-fallback': { knownNotArticle },
    './article': { articleUrl: (item) => item.url },
    './engagement': { recordUi() {} },
    './offline-store': { ...require('./offline-store.ts'), cardMediaUrls: () => [] },
    './video': { cardVideo: () => null },
  };
  const source = ts.transpileModule(readFileSync(new URL('./offline.ts', import.meta.url), 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS },
  }).outputText;
  const exports = {};
  runInNewContext(source, { exports, require: (id) => modules[id] ?? require(id), setTimeout, clearTimeout });
  return { exports, files };
}

test('saving on a timeout or offline connection records no failure and permits the next prefetch', async () => {
  const { ApiError } = require('../../lib/request.ts');
  const { parseIndex, planArticles } = require('./offline-store.ts');
  const url = 'https://example.com/post';
  for (const error of [new ApiError(0, 'Connection timed out'), new TypeError('Network request failed')]) {
    const h = savedCardHarness(error);
    await h.exports.prefetchSavedCard({}, { url });
    assert.equal(h.exports.isOffline(), true);
    const index = parseIndex(h.files.get('index.json') ?? null);
    assert.equal(index.entries[url], undefined);
    assert.deepEqual(planArticles([url], index, Date.now()), [url]);
  }
});

test('saving a non-article still remembers the definitive 422 outcome', async () => {
  const { ApiError } = require('../../lib/request.ts');
  const url = 'https://example.com/game';
  const h = savedCardHarness(new ApiError(422, 'no readable article'));
  await h.exports.prefetchSavedCard({}, { url });
  assert.equal(h.exports.isKnownNotArticle(url), true);
  assert.equal(h.exports.isOffline(), false);
});
