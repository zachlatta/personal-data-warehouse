import test from 'node:test';
import assert from 'node:assert/strict';
import { registerHooks } from 'node:module';
import { FeedSessionLifecycle } from './session-lifecycle.ts';
import { ViewabilityTracker, layoutsFromHeights } from './viewability.ts';

const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (['expo-constants', 'expo-device', 'expo-updates', 'react-native', 'expo-file-system'].includes(specifier) || specifier === './api') {
      return { url: `native:${specifier}`, shortCircuit: true };
    }
    if (context.parentURL?.endsWith('/engagement.ts') && specifier.startsWith('./')) return next(specifier + '.ts', context);
    return next(specifier, context);
  },
  load(url, context, next) {
    const source = {
      'native:expo-constants': 'export default {expoConfig:{version:"test"}}',
      'native:expo-device': 'export const modelName="test", osName="test", osVersion="test";',
      'native:expo-updates': 'export const updateId="test", channel="test";',
      'native:react-native': 'export const AppState={currentState:"active",addEventListener(){}}; export const Appearance={getColorScheme(){return "light"}};',
      'native:expo-file-system': 'export const Paths={document:"test"}; export class File {exists=false; write(){} }',
      'native:./api': 'export async function postFeedEvents() {throw Error("offline fixture")}',
    }[url];
    return source ? { format: 'module', source, shortCircuit: true } : next(url, context);
  },
});
const logger = await import('./engagement.ts');
hooks.deregister();

const { ReplacedListGate, PILL_GONE_Y, REPLACED_LIST_HOLD_MS, shouldHoldReplacedList } = await import('./replaced-list.ts');

const screen = (top = 0) => ({ top, height: 800 });

function postToLogger(ended) {
  for (const imp of ended) {
    logger.recordFeedEvent('impression', {
      at: imp.startedAt,
      item_id: imp.itemId,
      position: imp.position,
      dwell_ms: imp.dwellMs,
      visible_fraction: imp.maxFraction,
      metadata: {
        visible_ms: imp.visibleMs,
        ended_at: new Date(imp.endedAt).toISOString(),
        height: imp.height,
        view: imp.view,
      },
    });
  }
}

test('shouldHoldReplacedList opens only on first or stale loads with cards near the top', () => {
  assert.equal(REPLACED_LIST_HOLD_MS, 300);
  assert.equal(PILL_GONE_Y, 120);
  assert.equal(shouldHoldReplacedList('first', true, 0), true);
  assert.equal(shouldHoldReplacedList('stale', true, PILL_GONE_Y), true);
  assert.equal(shouldHoldReplacedList('stale', true, PILL_GONE_Y + 1), false);
  assert.equal(shouldHoldReplacedList('first', false, 0), false);
  assert.equal(shouldHoldReplacedList('pull', true, 0), false);
});

test('a cached list replaced within 200 ms posts no impression', () => {
  const tracker = new ViewabilityTracker();
  const heights = new Map([['flash-top', 400]]);
  const layouts = layoutsFromHeights(['flash-top'], heights, 0);
  const gate = new ReplacedListGate(true, postToLogger);

  logger.startFeedSession(1, 'focus');
  gate.record(tracker.update(1000, screen(0), layouts, 1000), 'all', heights);
  // First load replaces the cached list at 200 ms.
  gate.record(tracker.flush(1200, 1200), 'all', heights);
  assert.deepEqual(gate.settle(), []);
  logger.endFeedSession('blur');

  const events = logger.pendingFeedEvents();
  assert.equal(events.filter((e) => e.type === 'impression' && e.item_id === 'flash-top').length, 0);
  const end = events.findLast((e) => e.type === 'session_end');
  assert.equal(end.metadata.cards_seen, 0);
});

test('a hold of 300 ms or more posts on settle and drops shorter holds', () => {
  const tracker = new ViewabilityTracker();
  const heights = new Map([['held-300', 400], ['flash-150', 400]]);
  const gate = new ReplacedListGate(true, postToLogger);

  logger.startFeedSession(2, 'focus');
  gate.record(tracker.update(2000, screen(0), layoutsFromHeights(['held-300'], heights, 0), 2000), 'all', heights);
  gate.record(tracker.flush(2300, 2300), 'all', heights);
  gate.record(tracker.update(2300, screen(0), layoutsFromHeights(['flash-150'], heights, 0), 2300), 'all', heights);
  gate.record(tracker.flush(2450, 2450), 'all', heights);

  // Nothing is posted before commit settles the gate.
  assert.equal(logger.pendingFeedEvents().filter((e) => e.item_id === 'held-300' || e.item_id === 'flash-150').length, 0);

  const settled = gate.settle();
  assert.deepEqual(settled.map((i) => i.itemId), ['held-300']);

  const posted = logger.pendingFeedEvents().filter((e) => e.item_id === 'held-300' || e.item_id === 'flash-150');
  assert.equal(posted.length, 1);
  assert.equal(posted[0].item_id, 'held-300');
  assert.equal(posted[0].dwell_ms, 300);
  assert.ok(posted[0].session_id);
  logger.endFeedSession('blur');
});

test('a failed load still releases the hold', () => {
  const tracker = new ViewabilityTracker();
  const heights = new Map([['failed-load-card', 400], ['failed-load-flash', 400]]);
  const gate = new ReplacedListGate(false, postToLogger);

  logger.startFeedSession(2, 'focus');
  assert.equal(gate.beginLoad('stale', true, 0), true);
  gate.record(tracker.update(3000, screen(0), layoutsFromHeights(['failed-load-card'], heights, 0), 3000), 'all', heights);
  gate.record(tracker.flush(3350, 3350), 'all', heights);
  gate.record(tracker.update(3350, screen(0), layoutsFromHeights(['failed-load-flash'], heights, 0), 3350), 'all', heights);
  gate.record(tracker.flush(3500, 3500), 'all', heights);

  // Even when the network request fails, commit settles the gate and releases >= 300 ms holds.
  const released = gate.settle();
  assert.equal(gate.isOpen(), false);
  assert.deepEqual(released.map((i) => i.itemId), ['failed-load-card']);

  const posted = logger.pendingFeedEvents().filter((e) => e.item_id === 'failed-load-card' || e.item_id === 'failed-load-flash');
  assert.equal(posted.length, 1);
  assert.equal(posted[0].item_id, 'failed-load-card');
  assert.equal(posted[0].dwell_ms, 350);
  logger.endFeedSession('blur');
});

test('going to the background drains before the sitting ends so released impressions keep session_id', () => {
  const tracker = new ViewabilityTracker();
  const heights = new Map([['bg-card', 400], ['bg-flash', 400]]);
  const gate = new ReplacedListGate(true, postToLogger);

  logger.startFeedSession(2, 'focus');
  gate.record(tracker.update(4000, screen(0), layoutsFromHeights(['bg-card'], heights, 0), 4000), 'all', heights);
  gate.record(tracker.flush(4100, 4100), 'all', heights); // 100 ms flash held
  // bg-card is still on screen when the app goes to background after 350 ms.
  gate.record(tracker.update(4100, screen(0), layoutsFromHeights(['bg-card'], heights, 0), 4100), 'all', heights);

  gate.leave(
    () => gate.record(tracker.flush(4450, 4450), 'all', heights),
    () => {
      logger.endFeedSession('background');
      void logger.flushFeedEvents(true);
    },
  );

  const events = logger.pendingFeedEvents();
  const impIndex = events.findIndex((e) => e.type === 'impression' && e.item_id === 'bg-card');
  const endIndex = events.findLastIndex((e) => e.type === 'session_end');
  assert.ok(impIndex >= 0 && impIndex < endIndex);
  assert.ok(events[impIndex].session_id);
  assert.equal(events[impIndex].dwell_ms, 350);
  assert.equal(events[endIndex].metadata.reason, 'background');
  assert.equal(events[endIndex].metadata.cards_seen, 1);
});

test('blur during a load posts before session_end, a later settle posts nothing twice, and refocus re-opens the gate', () => {
  const lifecycle = new FeedSessionLifecycle();
  const tracker = new ViewabilityTracker();
  const heights = new Map([['blur-card', 400]]);
  const gate = new ReplacedListGate(true, postToLogger);

  lifecycle.focus();
  logger.startFeedSession(1, 'focus');
  gate.record(tracker.update(5000, screen(0), layoutsFromHeights(['blur-card'], heights, 0), 5000), 'all', heights);

  gate.leave(
    () => gate.record(tracker.flush(5400, 5400), 'all', heights),
    () => {
      if (lifecycle.blur()) logger.endFeedSession('blur');
    },
  );

  // A settle from the load completing after blur must not post a second time.
  assert.deepEqual(gate.settle(), []);

  const events = logger.pendingFeedEvents();
  const matching = events.filter((e) => e.type === 'impression' && e.item_id === 'blur-card');
  assert.equal(matching.length, 1);
  assert.ok(matching[0].session_id);
  const impIndex = events.indexOf(matching[0]);
  const endIndex = events.findLastIndex((e) => e.type === 'session_end');
  assert.ok(impIndex < endIndex);
  assert.equal(events[endIndex].metadata.cards_seen, 1);

  // If the user refocuses while an in-flight load is still running, beginLoad before the loading guard re-opens the gate.
  assert.equal(gate.isOpen(), false);
  assert.equal(gate.beginLoad('first', true, 0), true);
  assert.equal(gate.isOpen(), true);
  gate.settle();
});

test('a 50 ms scroll after settle still posts', () => {
  const tracker = new ViewabilityTracker();
  const heights = new Map([['scroll-50', 400]]);
  const gate = new ReplacedListGate(true, postToLogger);

  logger.startFeedSession(1, 'focus');
  gate.settle();
  assert.equal(gate.isOpen(), false);

  // Pull to refresh also leaves the gate closed.
  assert.equal(gate.beginLoad('pull', true, 0), false);

  gate.record(tracker.update(6000, screen(0), layoutsFromHeights(['scroll-50'], heights, 0), 6000), 'all', heights);
  gate.record(tracker.update(6050, screen(900), layoutsFromHeights(['scroll-50'], heights, 0), 6050), 'all', heights);

  const posted = logger.pendingFeedEvents().filter((e) => e.type === 'impression' && e.item_id === 'scroll-50');
  assert.equal(posted.length, 1);
  assert.equal(posted[0].dwell_ms, 50);
  logger.endFeedSession('blur');
});

test('scrolling past PILL_GONE_Y during a slow load settles early and passes later short scrolls straight through', () => {
  const tracker = new ViewabilityTracker();
  const heights = new Map([['top-320', 400], ['top-100', 400], ['scroll-during-load', 400]]);
  const gate = new ReplacedListGate(false, postToLogger);

  logger.startFeedSession(3, 'focus');
  assert.equal(gate.beginLoad('first', true, 0), true);

  // Held while near the top before scrolling past PILL_GONE_Y.
  gate.record(tracker.update(7000, screen(0), layoutsFromHeights(['top-320'], heights, 0), 7000), 'all', heights);
  gate.record(tracker.flush(7320, 7320), 'all', heights);
  gate.record(tracker.update(7320, screen(0), layoutsFromHeights(['top-100'], heights, 0), 7320), 'all', heights);
  gate.record(tracker.flush(7420, 7420), 'all', heights);

  // Reader scrolls past PILL_GONE_Y (120 px) while the slow load is still in flight.
  const settledEarly = gate.scroll(PILL_GONE_Y + 10);
  assert.equal(gate.isOpen(), false);
  assert.deepEqual(settledEarly.map((i) => i.itemId), ['top-320']);

  // A 60 ms scroll-past while the load is still in flight now passes straight through.
  const layouts = layoutsFromHeights(['scroll-during-load'], heights, 200);
  gate.record(tracker.update(7500, screen(200), layouts, 7500), 'all', heights);
  gate.record(tracker.update(7560, screen(1100), layouts, 7560), 'all', heights);

  const postedIds = logger
    .pendingFeedEvents()
    .filter((e) => e.type === 'impression' && ['top-320', 'top-100', 'scroll-during-load'].includes(e.item_id))
    .map((e) => e.item_id);
  assert.deepEqual(postedIds, ['top-320', 'scroll-during-load']);
  logger.endFeedSession('blur');
});

test('held impressions capture view and height when the stretch ends, not when the gate settles', () => {
  const tracker = new ViewabilityTracker();
  const heights = new Map([['view-card', 420]]);
  const gate = new ReplacedListGate(true, postToLogger);

  logger.startFeedSession(1, 'focus');
  gate.record(tracker.update(8000, screen(0), layoutsFromHeights(['view-card'], heights, 0), 8000), 'all', heights);
  // Stretch ends while on the main feed ('all', height 420), e.g. when openSearch() flushes before switching view.
  gate.record(tracker.flush(8350, 8350), 'all', heights);

  // Mutate heights map before settle to prove height was captured at record time.
  heights.set('view-card', 999);
  gate.settle();

  const event = logger.pendingFeedEvents().find((e) => e.type === 'impression' && e.item_id === 'view-card');
  assert.ok(event);
  assert.equal(event.metadata.view, 'all');
  assert.equal(event.metadata.height, 420);
  logger.endFeedSession('blur');
});
