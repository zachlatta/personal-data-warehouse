// Both list hosts write to one logger session, so only one may own it.
export class FeedSessionCoordinator {
  owner: FeedSessionLifecycle | null = null;
}
export const feedSessionCoordinator = new FeedSessionCoordinator();

export class FeedSessionLifecycle {
  detail = false;
  private backgroundTimer: ReturnType<typeof setTimeout> | undefined;
  private coordinator: FeedSessionCoordinator;
  constructor(coordinator = new FeedSessionCoordinator()) { this.coordinator = coordinator; }
  get owns() { return this.coordinator.owner === this; }
  focus() {
    this.detail = false;
    this.coordinator.owner = this;
  }
  openDetail() { this.coordinator.owner = this; this.detail = true; }
  blur() {
    if (!this.owns || this.detail) return false;
    this.coordinator.owner = null;
    return true;
  }
  closeDetail(focused: boolean) {
    if (!this.detail) return false;
    this.detail = false;
    return focused ? false : this.blur();
  }
  cancelBackgroundEnd() {
    clearTimeout(this.backgroundTimer);
    this.backgroundTimer = undefined;
  }
  appState(state: string, focusedInFront = false, endBackground: () => void = () => {}): 'start' | 'end' | null {
    if (state === 'active') this.cancelBackgroundEnd();
    if (state === 'active' && focusedInFront && !this.coordinator.owner) this.focus();
    if (!this.owns) return null;
    if (state !== 'active' && this.detail) {
      // Moving from the launcher's activity to a reader briefly backgrounds React.
      if (!this.backgroundTimer) this.backgroundTimer = setTimeout(() => {
        this.backgroundTimer = undefined;
        if (this.owns) endBackground();
      }, 1500);
      return null;
    }
    return state === 'active' ? 'start' : 'end';
  }
}

// Readers notify both hosts; only the current owner may end the session.
const closedListeners = new Set<() => void>();
export function onFeedDetailClosed(listener: () => void) {
  closedListeners.add(listener);
  return () => { closedListeners.delete(listener); };
}
export function feedDetailClosed() {
  for (const listener of closedListeners) listener();
}

export function feedDetailOpened() {
  feedSessionCoordinator.owner?.cancelBackgroundEnd();
}
