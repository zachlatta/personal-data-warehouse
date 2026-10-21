// One scroll gesture from finger down to the list coming to rest: where it
// started and stopped, how long the finger dragged, how fast it let go and
// whether the list coasted on. The feed and the detail view each record one
// `scroll` event per gesture, so the log shows how he moves through them
// (a slow read, a fast fling past a run of cards, a scroll back up).
//
// Feed it the list's onScrollBeginDrag / onScrollEndDrag /
// onMomentumScrollBegin / onMomentumScrollEnd. A drag released without a
// fling never fires a momentum event, so settle() after a short wait
// finishes it; a new drag during a coast finishes the one before.

export type ScrollGesture = {
  startedAt: number;
  fromY: number;
  toY: number;
  // Finger on the screen.
  dragMs: number;
  // Finger down to the list at rest.
  durationMs: number;
  // Points per millisecond at release, positive = down the feed.
  releaseVelocity: number;
  momentum: boolean;
};

type Open = { startedAt: number; fromY: number; releasedAt: number; releaseVelocity: number; momentum: boolean; y: number };

export class ScrollGestureTracker {
  private open: Open | null = null;

  /** Finger down. Returns the gesture a coast was still finishing, if any. */
  beginDrag(now: number, y: number): ScrollGesture | null {
    const previous = this.open ? this.finish(now, y) : null;
    this.open = { startedAt: now, fromY: y, releasedAt: 0, releaseVelocity: 0, momentum: false, y };
    return previous;
  }

  /** Finger up. velocityY is the native event's, in points per ms. */
  endDrag(now: number, y: number, velocityY: number) {
    if (!this.open) return;
    this.open.releasedAt = now;
    this.open.releaseVelocity = velocityY;
    this.open.y = y;
  }

  momentumBegin() {
    if (this.open) this.open.momentum = true;
  }

  /** The list came to rest after a coast. */
  momentumEnd(now: number, y: number): ScrollGesture | null {
    if (!this.open || !this.open.momentum) return null;
    return this.finish(now, y);
  }

  /**
   * Called a moment after the finger lifted: if no coast began, the
   * gesture is over where the finger left it.
   */
  settle(): ScrollGesture | null {
    if (!this.open || this.open.momentum || !this.open.releasedAt) return null;
    return this.finish(this.open.releasedAt, this.open.y);
  }

  private finish(endedAt: number, y: number): ScrollGesture {
    const g = this.open as Open;
    this.open = null;
    const released = g.releasedAt || endedAt;
    return {
      startedAt: g.startedAt,
      fromY: Math.round(g.fromY),
      toY: Math.round(y),
      dragMs: Math.max(0, released - g.startedAt),
      durationMs: Math.max(0, endedAt - g.startedAt),
      releaseVelocity: Math.round(g.releaseVelocity * 1000) / 1000,
      momentum: g.momentum,
    };
  }
}

/** The fields a `scroll` event carries for one gesture. */
export function scrollMetadata(g: ScrollGesture, extra: Record<string, unknown> = {}): Record<string, unknown> {
  const distance = g.toY - g.fromY;
  return {
    from_y: g.fromY,
    to_y: g.toY,
    distance,
    direction: distance > 0 ? 'down' : distance < 0 ? 'up' : 'none',
    drag_ms: g.dragMs,
    release_velocity: g.releaseVelocity,
    momentum: g.momentum,
    ...extra,
  };
}
