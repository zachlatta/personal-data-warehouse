// The heart (Zach, voice memo 2026-10-23: "add the like button back so I can
// mark which things I like"). The feed, its cards and the detail view share
// this one store; a toggle records a like or unlike event, the server keeps
// the latest per card (engagement.liked). A tap is kept pending for a couple
// of minutes so a reload that beats the event upload does not undo it.
import { useSyncExternalStore } from 'react';

import { celebrate, tick } from '@/lib/haptics';

import type { FeedItem } from './api';
import { recordFeedEvent } from './engagement';
import { applySave, mergeSaved, type PendingSave } from './saved-queue';

let liked: string[] = [];
const pending = new Map<string, PendingSave>();
const listeners = new Set<() => void>();
const likeListeners = new Set<(itemId: string) => void>();

function emit() {
  for (const listener of listeners) listener();
}

/** What the server says he has liked, from a feed load. */
export function loadLiked(items: FeedItem[]) {
  liked = mergeSaved(
    items.filter((i) => i.engagement.liked).map((i) => i.item_id),
    pending,
    Date.now(),
  );
  emit();
}

/** Like or unlike a card; returns whether it is liked now. */
export function toggleLiked(item: FeedItem, fields: { position?: number; via: string }): boolean {
  const like = !liked.includes(item.item_id);
  liked = applySave(liked, item.item_id, like);
  pending.set(item.item_id, { save: like, at: Date.now() });
  recordFeedEvent(like ? 'like' : 'unlike', {
    item_id: item.item_id,
    position: fields.position,
    metadata: { via: fields.via, source: item.source },
  });
  emit();
  // Every way in (the card's heart, an X post's heart, the detail view, the
  // menu) buzzes, and a like plays the hearts' burst (like-heart.tsx).
  if (like) {
    celebrate();
    for (const listener of likeListeners) listener(item.item_id);
  } else {
    tick();
  }
  return like;
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** The liked ids; re-renders on every change. */
export function useLikedIds(): string[] {
  return useSyncExternalStore(subscribe, () => liked);
}

/** Calls back with the card's id each time he likes one (not on an unlike or
 * a load), so the hearts showing it can celebrate. Returns the unsubscribe. */
export function onLiked(listener: (itemId: string) => void): () => void {
  likeListeners.add(listener);
  return () => likeListeners.delete(listener);
}
