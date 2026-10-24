// Per-card viewability, the way ad measurement defines it: a card is "in
// view" while at least half of it is on screen (or, for a card taller than the
// screen, while it fills at least half the screen). Each stretch in view is
// one impression carrying how long it lasted, how much of the card was ever
// visible, and how long any part of it was on screen. A card that never
// reaches the threshold was scrolled past unseen and records nothing; one
// that reaches it for under a second is a scroll-past (the marts view counts
// those as scroll_pasts).
//
// State only changes when the list scrolls, lays out, or the screen
// loses focus, so time accrues exactly between those calls: no timer. Dwell
// and visible time are active time (active-time.ts): callers pass the active
// clock's reading as activeNow, so a card left on an untouched screen stops
// accruing after a minute. startedAt and endedAt stay wall time.

export type CardLayout = { y: number; height: number; position: number };
export type Viewport = { top: number; height: number };

export type Impression = {
  itemId: string;
  position: number;
  startedAt: number;
  endedAt: number;
  // Time at or above the threshold.
  dwellMs: number;
  // Time with any part of the card on screen.
  visibleMs: number;
  maxFraction: number;
};

type Episode = {
  position: number;
  startedAt: number;
  lastActive: number;
  fraction: number;
  dwellMs: number;
  visibleMs: number;
  maxFraction: number;
};

export const VIEWABLE_THRESHOLD = 0.5;

export function visibleFraction(layout: { y: number; height: number }, viewport: Viewport): number {
  if (layout.height <= 0 || viewport.height <= 0) return 0;
  const overlap = Math.min(layout.y + layout.height, viewport.top + viewport.height) - Math.max(layout.y, viewport.top);
  if (overlap <= 0) return 0;
  return Math.min(1, overlap / Math.min(layout.height, viewport.height));
}

export class ViewabilityTracker {
  private episodes = new Map<string, Episode>();
  private readonly threshold: number;

  constructor(threshold = VIEWABLE_THRESHOLD) {
    this.threshold = threshold;
  }

  private accrue(episode: Episode, activeNow: number) {
    const elapsed = Math.max(0, activeNow - episode.lastActive);
    if (episode.fraction >= this.threshold) episode.dwellMs += elapsed;
    if (episode.fraction > 0) episode.visibleMs += elapsed;
    episode.lastActive = activeNow;
  }

  private finish(itemId: string, episode: Episode, now: number): Impression | null {
    this.episodes.delete(itemId);
    if (episode.maxFraction < this.threshold) return null;
    return {
      itemId,
      position: episode.position,
      startedAt: episode.startedAt,
      endedAt: now,
      dwellMs: Math.round(episode.dwellMs),
      visibleMs: Math.round(episode.visibleMs),
      maxFraction: Math.round(episode.maxFraction * 1000) / 1000,
    };
  }

  /** Feed the current layout; returns the impressions that just ended. */
  update(now: number, viewport: Viewport, layouts: Map<string, CardLayout>, activeNow = now): Impression[] {
    const ended: Impression[] = [];
    for (const [itemId, episode] of this.episodes) {
      this.accrue(episode, activeNow);
      const layout = layouts.get(itemId);
      const fraction = layout ? visibleFraction(layout, viewport) : 0;
      if (fraction <= 0) {
        const impression = this.finish(itemId, episode, now);
        if (impression) ended.push(impression);
        continue;
      }
      episode.fraction = fraction;
      episode.maxFraction = Math.max(episode.maxFraction, fraction);
    }
    for (const [itemId, layout] of layouts) {
      if (this.episodes.has(itemId)) continue;
      const fraction = visibleFraction(layout, viewport);
      if (fraction <= 0) continue;
      this.episodes.set(itemId, {
        position: layout.position,
        startedAt: now,
        lastActive: activeNow,
        fraction,
        dwellMs: 0,
        visibleMs: 0,
        maxFraction: fraction,
      });
    }
    return ended;
  }

  /** End every open stretch (the screen lost focus, a card was opened). */
  flush(now: number, activeNow = now): Impression[] {
    const ended: Impression[] = [];
    for (const [itemId, episode] of [...this.episodes]) {
      this.accrue(episode, activeNow);
      const impression = this.finish(itemId, episode, now);
      if (impression) ended.push(impression);
    }
    return ended;
  }

  /** Item ids currently on screen at or above the threshold. */
  inView(): string[] {
    return [...this.episodes].filter(([, e]) => e.fraction >= this.threshold).map(([id]) => id);
  }
}

/**
 * Card offsets in list content coordinates from measured heights: each card
 * sits below the ones before it. A card is placed only once every card
 * above it has been measured (the list renders top-down, so this is the
 * normal case); an unplaceable card cannot be on screen yet.
 */
export function layoutsFromHeights(ids: string[], heights: Map<string, number>, top: number): Map<string, CardLayout> {
  const layouts = new Map<string, CardLayout>();
  let y = top;
  for (let position = 0; position < ids.length; position++) {
    const height = heights.get(ids[position]);
    if (height === undefined) break;
    layouts.set(ids[position], { y, height, position });
    y += height;
  }
  return layouts;
}
