import type { Impression } from './viewability';

// Minimum active dwell for an impression on a list that a first or stale
// reload is about to replace; shorter flashes are dropped on settle.
export const REPLACED_LIST_HOLD_MS = 300;

// Preserve the list once the reader has left the top.
export const PILL_GONE_Y = 120;

export type LoadTrigger = 'first' | 'stale' | 'pull';

export type HeldImpression = Impression & {
  view: string;
  height: number;
};

export function shouldHoldReplacedList(trigger: LoadTrigger, hasCards: boolean, scrollTop: number): boolean {
  return (trigger === 'first' || trigger === 'stale') && hasCards && scrollTop <= PILL_GONE_Y;
}

export class ReplacedListGate {
  private open: boolean;
  private held: HeldImpression[] = [];
  private readonly post: (ended: HeldImpression[]) => void;
  private readonly minHoldMs: number;

  constructor(hasCachedCards: boolean, post: (ended: HeldImpression[]) => void, minHoldMs = REPLACED_LIST_HOLD_MS) {
    this.open = hasCachedCards;
    this.post = post;
    this.minHoldMs = minHoldMs;
  }

  isOpen(): boolean {
    return this.open;
  }

  beginLoad(trigger: LoadTrigger, hasCards: boolean, scrollTop: number): boolean {
    if (shouldHoldReplacedList(trigger, hasCards, scrollTop)) {
      this.open = true;
    }
    return this.open;
  }

  scroll(scrollTop: number): HeldImpression[] {
    if (this.open && scrollTop > PILL_GONE_Y) {
      return this.settle();
    }
    return [];
  }

  record(ended: Impression[], view = 'all', heights?: ReadonlyMap<string, number>): void {
    if (ended.length === 0) return;
    const stamped: HeldImpression[] = ended.map((imp) => ({
      ...imp,
      view,
      height: heights?.get(imp.itemId) ?? 0,
    }));
    if (this.open) {
      this.held.push(...stamped);
      return;
    }
    this.post(stamped);
  }

  settle(): HeldImpression[] {
    if (!this.open && this.held.length === 0) return [];
    this.open = false;
    if (this.held.length === 0) return [];
    const kept = this.held.filter((imp) => imp.dwellMs >= this.minHoldMs);
    this.held = [];
    if (kept.length > 0) this.post(kept);
    return kept;
  }

  leave(flushTracker: () => void, endSession: () => void): HeldImpression[] {
    flushTracker();
    const kept = this.settle();
    endSession();
    return kept;
  }
}
