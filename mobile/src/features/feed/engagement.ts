// The one engagement logger the Feed tab and its reader share: it stamps
// every event with the current feed session, queues it in the on-disk
// outbox, and sends the outbox in the background.
import * as Device from 'expo-device';
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
  if (session && rest.item_id) {
    if (type === 'impression') session.seen.add(rest.item_id);
    else session.acts[type] = (session.acts[type] ?? 0) + 1;
    if (typeof rest.position === 'number') session.maxPosition = Math.max(session.maxPosition, rest.position);
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

/** The tab is on screen with the app in front: one sitting begins. */
export function startFeedSession(cards: number) {
  if (session) return;
  session = { id: newEventId(), startedAt: Date.now(), seen: new Set(), maxPosition: -1, acts: {} };
  recordFeedEvent('session_start', { metadata: { cards } });
}

/** The tab lost focus or the app went to the background. */
export function endFeedSession() {
  if (!session) return;
  const ended = session;
  recordFeedEvent('session_end', {
    dwell_ms: Date.now() - ended.startedAt,
    metadata: { cards_seen: ended.seen.size, deepest_position: ended.maxPosition, ...ended.acts },
  });
  session = null;
  void outbox().flush(true);
}
