// When a list screen (the Timeline, Review) re-reads itself, so what opens is
// what the server has now rather than what the process last saw. Until
// 2026-11-25 the Timeline read only when it mounted and Review only on tab
// focus; an Android app resumed from the background is the same mounted,
// focused screen, so neither ran and both showed the old rows until a pull.
//
// A list is read when it gains focus, when the app returns to the foreground
// while it is focused, and every `periodMs` while both hold (a push drawn by
// the Android service never reaches JS, so this is what brings in a request
// that arrived while the app was open). Reads closer together than
// `minIntervalMs` collapse into one, which is what keeps a cold start (mount,
// focus and the first `active` all at once) to one request.
//
// Pure, with the clock and timers injected, so it is tested without React
// Native (see use-live-refresh.ts for the hook that drives it).

export type RefreshReason = 'focus' | 'foreground' | 'periodic';

type Timer = unknown;

export type LiveRefreshOptions = {
  refresh: (why: RefreshReason) => void;
  minIntervalMs: number;
  periodMs: number;
  appState: string;
  now?: () => number;
  setInterval?: (fn: () => void, ms: number) => Timer;
  clearInterval?: (timer: Timer) => void;
};

export class LiveRefresh {
  private focused = false;
  private active: boolean;
  private lastRead = Number.NEGATIVE_INFINITY;
  private timer: Timer | null = null;
  private readonly options: Required<LiveRefreshOptions>;

  constructor(options: LiveRefreshOptions) {
    this.options = {
      now: () => Date.now(),
      setInterval: (fn, ms) => setInterval(fn, ms),
      clearInterval: (timer) => clearInterval(timer as ReturnType<typeof setInterval>),
      ...options,
    };
    this.active = options.appState === 'active';
  }

  // The screen's latest read function (a hook re-renders with a new one).
  setRefresh(refresh: (why: RefreshReason) => void): void {
    this.options.refresh = refresh;
  }

  // The screen read itself (a mount, a pull, a tier change): the interval
  // counts from here, so the lifecycle does not repeat a read just made.
  loaded(): void {
    this.lastRead = this.options.now();
  }

  focus(): void {
    this.focused = true;
    this.maybeRead('focus');
    this.sync();
  }

  blur(): void {
    this.focused = false;
    this.sync();
  }

  appState(state: string): void {
    const active = state === 'active';
    const resumed = active && !this.active;
    this.active = active;
    if (resumed) this.maybeRead('foreground');
    this.sync();
  }

  private maybeRead(why: RefreshReason): void {
    if (!this.focused || !this.active) return;
    const now = this.options.now();
    if (now - this.lastRead < this.options.minIntervalMs) return;
    this.lastRead = now;
    this.options.refresh(why);
  }

  private sync(): void {
    const run = this.focused && this.active;
    if (run && this.timer === null) {
      this.timer = this.options.setInterval(() => this.maybeRead('periodic'), this.options.periodMs);
    } else if (!run && this.timer !== null) {
      this.options.clearInterval(this.timer);
      this.timer = null;
    }
  }
}
