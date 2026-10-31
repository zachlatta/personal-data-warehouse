// Both list hosts write to one logger session, so only one may own it.
export class FeedSessionCoordinator {
  owner: FeedSessionLifecycle | null = null;
}
export const feedSessionCoordinator = new FeedSessionCoordinator();

export class FeedSessionLifecycle {
  detail = false;
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
  appState(state: string, focusedInFront = false): 'start' | 'end' | null {
    if (state === 'active' && focusedInFront && !this.coordinator.owner) this.focus();
    if (!this.owns) return null;
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
