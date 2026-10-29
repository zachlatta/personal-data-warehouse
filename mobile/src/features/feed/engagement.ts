// The one engagement logger the Feed tab and its reader share: it stamps
// every event with the current feed session, queues it in the on-disk
// outbox, and sends the outbox in the background.
import Constants from 'expo-constants';
import * as Device from 'expo-device';
import * as Updates from 'expo-updates';
import { AppState, Appearance } from 'react-native';
import { File, Paths } from 'expo-file-system';

import type { AppConfig } from '@/lib/config';

import { ActiveClock, stopwatch } from './active-time';
import { postFeedEvents, type FeedItem } from './api';
import { EventQueue, newEventId, type FeedEvent, type FeedEventType } from './event-queue';

const FLUSH_EVERY_MS = 15_000;
const FLUSH_AT_SIZE = 40;

let config: AppConfig | null = null;
let queue: EventQueue | null = null;
let timer: ReturnType<typeof setInterval> | null = null;
let session: {
  id: string;
  startedAt: number;
  stop: () => { dwell_ms: number; wall_ms: number };
  seen: Set<string>;
  maxPosition: number;
  acts: Record<string, number>;
} | null = null;

// Every time the app reports (dwell, reading, watch time) is active time:
// the app in front and in use, not just open (active-time.ts).
export const activeClock = new ActiveClock(Date.now(), AppState.currentState === 'active');
AppState.addEventListener('change', (state) => activeClock.setForeground(state === 'active', Date.now()));

/** A touch anywhere on a feed screen keeps the active clock running. */
export function markActive() {
  activeClock.touch(Date.now());
}

/** The active clock's reading now, for the viewability tracker. */
export function activeNow(): number {
  return activeClock.read(Date.now());
}

/**
 * Starts timing one stretch; stop() returns dwell_ms (active time) and
 * wall_ms (for the close's metadata). busy: something he does not touch
 * (the in-app browser, a video) is on screen, so its time counts while the
 * app is in front.
 */
export function startTimer(busy = false) {
  const now = Date.now();
  if (busy) activeClock.setBusy(true, now);
  const sw = stopwatch(activeClock, now);
  let done = false;
  return () => {
    const end = Date.now();
    if (busy && !done) activeClock.setBusy(false, end);
    done = true;
    const { activeMs, wallMs } = sw.stop(end);
    return { dwell_ms: activeMs, wall_ms: wallMs };
  };
}

// Events that are not a touch: recorded after the fact or by the app itself.
const PASSIVE: ReadonlySet<FeedEventType> = new Set<FeedEventType>(['impression', 'load', 'session_start', 'session_end', 'close']);

// Cards the feed last loaded, so the reader can open one by id.
export const feedItems = new Map<string, FeedItem>();

function outbox(): EventQueue {
  if (queue) return queue;
  const file = new File(Paths.document, 'pdw-feed-outbox.json');
  queue = new EventQueue(
    {
      load: () => (file.exists ? file.textSync() : null),
      save: (value) => file.write(value),
    },
    async (events) => {
      if (!config) throw new Error('not connected');
      await postFeedEvents(config, Device.modelName ?? 'iPhone', events);
    },
    Date.now,
    (message) => console.warn(message),
  );
  return queue;
}

export function connectFeedLogger(next: AppConfig) {
  config = next;
  outbox();
  if (!timer) timer = setInterval(() => void outbox().flush(), FLUSH_EVERY_MS);
}

export function flushFeedEvents(force = false): Promise<void> {
  return outbox().flush(force);
}

export function pendingFeedEvents(): FeedEvent[] {
  return outbox().pending();
}

type Fields = Omit<FeedEvent, 'event_id' | 'type' | 'session_id' | 'occurred_at'> & { at?: number };

export function recordFeedEvent(type: FeedEventType, fields: Fields = {}) {
  const { at, ...rest } = fields;
  const when = at ?? Date.now();
  if (!PASSIVE.has(type)) markActive();
  if (session) {
    if (type === 'impression' && rest.item_id) session.seen.add(rest.item_id);
    else if (type !== 'impression' && type !== 'session_start') session.acts[type] = (session.acts[type] ?? 0) + 1;
    if (rest.item_id && typeof rest.position === 'number') session.maxPosition = Math.max(session.maxPosition, rest.position);
  }
  const q = outbox();
  q.enqueue({
    event_id: newEventId(when),
    type,
    session_id: session?.id ?? '',
    occurred_at: new Date(when).toISOString(),
    ...rest,
  });
  if (q.size >= FLUSH_AT_SIZE) void q.flush();
}

// Tap to first paint for a card's detail view (Superhuman's 100 ms rule):
// the feed notes when a card was tapped and the detail view reads it once,
// when it first lays out, and records it as a ui "open_paint" event.
let openTap: { itemId: string; at: number } | null = null;

export function markOpenTap(itemId: string) {
  openTap = { itemId, at: performance.now() };
}

export function takeOpenPaintMs(itemId: string): number | null {
  const tap = openTap;
  openTap = null;
  return tap && tap.itemId === itemId ? performance.now() - tap.at : null;
}

/**
 * Any tap or gesture that is not already its own event type: the source
 * rail, the card menu, pull to refresh, Undo, the photo viewer's zoom.
 * action names it; item_id is optional.
 */
export function recordUi(action: string, fields: Omit<Fields, 'metadata'> & { metadata?: Record<string, unknown> } = {}) {
  recordFeedEvent('ui', { ...fields, metadata: { action, ...fields.metadata } });
}

// The first sitting since the app's JavaScript started is a launch (a cold
// open); later ones come from the app returning to the front or the tab.
let launched = false;
const appStartedAt = Date.now();
let lastSessionEndedAt = 0;

// What the phone was: app version and build, the OTA update it runs, the
// OS, light or dark. Sent once per sitting, on session_start.
function deviceContext(): Record<string, unknown> {
  return {
    app_version: Constants.expoConfig?.version ?? '',
    update_id: Updates.updateId ?? 'embedded',
    channel: Updates.channel ?? '',
    os: `${Device.osName ?? ''} ${Device.osVersion ?? ''}`.trim(),
    color_scheme: Appearance.getColorScheme() ?? '',
    tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
  };
}

/**
 * The tab is on screen with the app in front: one sitting begins. trigger
 * is why: `launch` (first since the app started), `foreground` (the app
 * came back to the front) or `focus` (back to the Feed from another tab or
 * from a card's detail view).
 */
export function startFeedSession(cards: number, trigger: 'foreground' | 'focus') {
  if (session) return;
  const now = Date.now();
  const why = launched ? trigger : 'launch';
  launched = true;
  session = { id: newEventId(now), startedAt: now, stop: startTimer(), seen: new Set(), maxPosition: -1, acts: {} };
  recordFeedEvent('session_start', {
    metadata: {
      cards,
      trigger: why,
      gap_ms: lastSessionEndedAt ? now - lastSessionEndedAt : null,
      since_app_start_ms: now - appStartedAt,
      ...deviceContext(),
    },
  });
}

/** The Feed left the screen (reason `blur`: another tab or a card's detail view) or the app left the front (`background`). */
export function endFeedSession(reason: 'background' | 'blur') {
  if (!session) return;
  const ended = session;
  lastSessionEndedAt = Date.now();
  recordFeedEvent('session_end', {
    dwell_ms: ended.stop().dwell_ms,
    metadata: { wall_ms: lastSessionEndedAt - ended.startedAt, reason, cards_seen: ended.seen.size, deepest_position: ended.maxPosition, ...ended.acts },
  });
  session = null;
  void outbox().flush(true);
}
