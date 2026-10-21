// His read-later queue (docs/feed.md): cards he marks to read or watch
// later, shown in the Saved bubble at the front of the source rail. The feed
// screen and the detail view share this one store; a toggle records a save
// or unsave event, which is what the server's queue is made of.
import { useSyncExternalStore } from 'react';

import type { FeedItem } from './api';
import { feedItems, recordFeedEvent } from './engagement';
import { applySave, mergeSaved, type PendingSave } from './saved-queue';

let order: string[] = [];
const pending = new Map<string, PendingSave>();
const listeners = new Set<() => void>();

function emit() {
  for (const listener of listeners) listener();
}

/** The queue as the server sent it with the feed. */
export function loadSaved(items: FeedItem[]) {
  for (const item of items) feedItems.set(item.item_id, item);
  order = mergeSaved(
    items.map((i) => i.item_id),
    pending,
    Date.now(),
  );
  emit();
}

export function isSaved(itemId: string): boolean {
  return order.includes(itemId);
}

/** Save or unsave a card; returns whether it is saved now. */
export function toggleSaved(item: FeedItem, fields: { position?: number; via: string }): boolean {
  const save = !isSaved(item.item_id);
  feedItems.set(item.item_id, item);
  order = applySave(order, item.item_id, save);
  pending.set(item.item_id, { save, at: Date.now() });
  recordFeedEvent(save ? 'save' : 'unsave', {
    item_id: item.item_id,
    position: fields.position,
    metadata: { via: fields.via, source: item.source },
  });
  emit();
  return save;
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** The saved ids, newest saved first; re-renders on every change. */
export function useSavedIds(): string[] {
  return useSyncExternalStore(subscribe, () => order);
}
