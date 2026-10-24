// Timeline rows and event details this process has seen, in memory only
// (like mutation-cache.ts: nothing private is written beside the token).
// A list row seeds its event screen, so the subject, sender and text paint
// on the first frame; a press-in starts the detail fetch before the finger
// lifts, the way Superhuman pre-renders the thread you are about to open.
import type { AppConfig } from './config';
import { getTimelineItem, type TimelineItem, type TimelineItemDetail } from './api';

const MAX = 400;
const rows = new Map<string, TimelineItem>();
const details = new Map<string, Promise<TimelineItemDetail>>();

const key = (adapter: string, eventId: string) => `${adapter}|${eventId}`;

function trim<V>(map: Map<string, V>) {
  while (map.size > MAX) map.delete(map.keys().next().value as string);
}

export function rememberTimelineItems(items: TimelineItem[]): void {
  for (const item of items) rows.set(key(item.adapter, item.event_id), item);
  trim(rows);
}

export function peekTimelineItem(adapter: string, eventId: string): TimelineItem | null {
  return rows.get(key(adapter, eventId)) ?? null;
}

// One request per event while it is fresh; a failed one is forgotten so the
// screen's own call retries.
export function loadTimelineItem(config: AppConfig, adapter: string, eventId: string): Promise<TimelineItemDetail> {
  const k = key(adapter, eventId);
  const known = details.get(k);
  if (known) return known;
  const pending = getTimelineItem(config, adapter, eventId).catch((e) => {
    details.delete(k);
    throw e;
  });
  details.set(k, pending);
  trim(details);
  // A detail older than a few minutes is refetched on the next open.
  setTimeout(() => {
    if (details.get(k) === pending) details.delete(k);
  }, 5 * 60 * 1000);
  return pending;
}

export function prefetchTimelineItem(config: AppConfig, adapter: string, eventId: string): void {
  loadTimelineItem(config, adapter, eventId).catch(() => {});
}
