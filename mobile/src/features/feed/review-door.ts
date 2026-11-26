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

export type FeedJumpScroller = {
  scrollToOffset: (params: { offset: number; animated?: boolean }) => void;
  scrollToIndex: (params: { index: number; animated?: boolean }) => void;
};

export type ScrollToIndexFailedInfo = {
  index: number;
  averageItemLength: number;
};

// Coordinates landing on a target card when navigating from Review to Feed:
// waits for filters to clear and any stale reload to settle, scrolls to the
// card (or top if first/absent), retries once from the estimated offset if
// FlatList has not measured the target yet, and always clears state afterward.
export class FeedJumpController {
  private pending: string | null = null;
  private active: { index: number; retried: boolean } | null = null;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private schedule: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
  private cancel: (id: ReturnType<typeof setTimeout>) => void;

  constructor(
    schedule: (fn: () => void, ms: number) => ReturnType<typeof setTimeout> = setTimeout,
    cancel: (id: ReturnType<typeof setTimeout>) => void = clearTimeout,
  ) {
    this.schedule = schedule;
    this.cancel = cancel;
  }

  arm(itemId: string): void {
    this.clear();
    this.pending = itemId;
  }

  hasPending(): boolean {
    return this.pending !== null;
  }

  tryJump(ids: string[], state: { filtered: boolean; loading: boolean; settled: boolean }, scroller: FeedJumpScroller | null): boolean {
    if (!this.pending || state.filtered || state.loading || !scroller) return false;
    if (ids.length === 0) {
      if (!state.settled) return false;
      this.pending = null;
      this.active = null;
      scroller.scrollToOffset({ offset: 0, animated: false });
      return true;
    }
    const idx = ids.indexOf(this.pending);
    this.pending = null;
    if (idx <= 0) {
      this.active = null;
      scroller.scrollToOffset({ offset: 0, animated: false });
      return true;
    }
    this.active = { index: idx, retried: false };
    scroller.scrollToIndex({ index: idx, animated: false });
    return true;
  }

  onScrollToIndexFailed(info: ScrollToIndexFailedInfo, scroller: FeedJumpScroller | null): void {
    const offset = info.averageItemLength > 0 ? info.averageItemLength * info.index : 0;
    this.pending = null;
    if (!scroller) {
      this.active = null;
      return;
    }
    if (this.active && !this.active.retried && this.active.index === info.index) {
      this.active.retried = true;
      scroller.scrollToOffset({ offset, animated: false });
      const idx = info.index;
      this.timer = this.schedule(() => {
        this.timer = null;
        const cur = this.active;
        this.active = null;
        if (cur && cur.index === idx) scroller.scrollToIndex({ index: idx, animated: false });
      }, 50);
      return;
    }
    this.active = null;
    scroller.scrollToOffset({ offset, animated: false });
  }

  clear(): void {
    if (this.timer !== null) {
      this.cancel(this.timer);
      this.timer = null;
    }
    this.pending = null;
    this.active = null;
  }
}
