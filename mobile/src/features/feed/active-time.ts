// Active time: the part of wall time Zach is actually using the app (Zach,
// 2026-10-24: "Don't count inactive time as spending time on it"). Time
// counts while the app is in front and he touched it in the last IDLE_MS, or
// while something he cannot touch is on screen for him (a video playing, the
// in-app browser). Time with the app in the background (another app, the
// phone locked) or left untouched counts nothing.
//
// Pure, so it runs under node's test runner; engagement.ts wires it to
// AppState and the screens' touches.

export const IDLE_MS = 60_000;

export class ActiveClock {
  private total = 0;
  private mark: number;
  private lastTouch: number;
  private foreground: boolean;
  private busy = 0;

  constructor(now: number, foreground = true) {
    this.mark = now;
    this.lastTouch = now;
    this.foreground = foreground;
  }

  private advance(now: number) {
    if (now <= this.mark) return;
    if (this.foreground) {
      const until = this.busy > 0 ? now : Math.min(now, this.lastTouch + IDLE_MS);
      if (until > this.mark) this.total += until - this.mark;
    }
    this.mark = now;
  }

  /** Active milliseconds since the clock started; only goes up. */
  read(now: number): number {
    this.advance(now);
    return this.total;
  }

  /** He touched, scrolled or tapped something. */
  touch(now: number) {
    this.advance(now);
    this.lastTouch = Math.max(this.lastTouch, now);
  }

  /** The app came to the front (counts as a touch) or left it. */
  setForeground(foreground: boolean, now: number) {
    this.advance(now);
    this.foreground = foreground;
    if (foreground) this.lastTouch = Math.max(this.lastTouch, now);
  }

  /** Something plays or shows without touches (a video, the browser): on, then off. */
  setBusy(on: boolean, now: number) {
    this.advance(now);
    this.busy = Math.max(0, this.busy + (on ? 1 : -1));
    if (!on) this.lastTouch = Math.max(this.lastTouch, now);
  }
}

export type Stopwatch = { stop: (now: number) => { activeMs: number; wallMs: number } };

/** Times one stretch (a detail view, a video, a link) in both active and wall time. */
export function stopwatch(clock: ActiveClock, now: number): Stopwatch {
  const active = clock.read(now);
  return {
    stop: (end: number) => ({ activeMs: Math.round(clock.read(end) - active), wallMs: Math.max(0, end - now) }),
  };
}
