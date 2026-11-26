// Pure logic for the Review-to-Feed door shown under the empty "Needs review"
// state: picks the Feed's top unseen card and true unseen count, and hands off
// a one-shot jump target so the Feed tab scrolls to that card on arrival.
import type { FeedItem, FeedPage } from './api';
import type { FeedMedia } from './rich';

export type ReviewDoorPreview = {
  itemId: string;
  title: string;
  source: string;
  imageUrl: string | null;
  unseen: number;
};

function isUnseen(item: FeedItem): boolean {
  return !item.resurfaced && item.engagement.impressions === 0;
}

function firstPicture(media: FeedMedia[] | undefined): string | null {
  if (!media?.length) return null;
  for (const m of media) {
    if (m.thumbnail_url) return m.thumbnail_url;
    if (m.type !== 'video' && m.url) return m.url;
  }
  return null;
}

export function reviewDoorPreview(page: FeedPage | null | undefined): ReviewDoorPreview | null {
  if (!page) return null;
  const top = page.items.find(isUnseen);
  const unseen = typeof page.unseen_count === 'number' ? page.unseen_count : page.items.filter(isUnseen).length;
  if (unseen <= 0 || !top) return null;
  const imageUrl = top.image_url || firstPicture(top.media) || firstPicture(top.embed?.media) || null;
  const title = top.title.trim() || top.embed?.text?.trim() || top.url || '';
  return { itemId: top.item_id, title, source: top.source, imageUrl, unseen };
}

let pendingJumpItemId: string | null = null;

export function requestFeedJump(itemId: string): void {
  pendingJumpItemId = itemId;
}

export function consumeFeedJump(): string | null {
  const id = pendingJumpItemId;
  pendingJumpItemId = null;
  return id;
}
