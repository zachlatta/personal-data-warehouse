// The one engagement logger the Feed tab and its reader share: it stamps
// every event with the current feed session, queues it in the on-disk
// outbox, and sends the outbox in the background.
import Constants from 'expo-constants';
import * as Device from 'expo-device';
import * as Updates from 'expo-updates';
import { Appearance } from 'react-native';
import { File, Paths } from 'expo-file-system';

import type { AppConfig } from '@/lib/config';

import { postFeedEvents, type FeedItem } from './api';
import { EventQueue, newEventId, type FeedEvent, type FeedEventType } from './event-queue';

const FLUSH_EVERY_MS = 15_000;
const FLUSH_AT_SIZE = 40;

let config: AppConfig | null = null;
let queue: EventQueue | null = null;
let timer: ReturnType<typeof setInterval> | null = null;
let session: { id: string; startedAt: number; seen: Set<string>; maxPosition: number; acts: Record<string, number> } | null = null;

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

type Fields = Omit<FeedEvent, 'event_id' | 'type' | 'session_id' | 'occurred_at'> & { at?: number };

export function recordFeedEvent(type: FeedEventType, fields: Fields = {}) {
  const { at, ...rest } = fields;
  const when = at ?? Date.now();
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
  session = { id: newEventId(now), startedAt: now, seen: new Set(), maxPosition: -1, acts: {} };
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
    dwell_ms: lastSessionEndedAt - ended.startedAt,
    metadata: { reason, cards_seen: ended.seen.size, deepest_position: ended.maxPosition, ...ended.acts },
  });
  session = null;
  void outbox().flush(true);
}
