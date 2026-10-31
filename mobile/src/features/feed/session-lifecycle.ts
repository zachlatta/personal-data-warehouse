// Session ownership outlives list focus while a reader is above it.
export class FeedSessionLifecycle {
  detail = false;
  owns = false;
  focus() { this.detail = false; this.owns = true; }
  openDetail() { this.detail = true; }
  blur() {
    if (this.detail) return false;
    this.owns = false;
    return true;
  }
  closeDetail(focused: boolean) {
    if (focused || !this.detail) return false;
    this.detail = false;
    return this.blur();
  }
  appState(state: string): 'start' | 'end' | null {
    if (!this.owns) return null;
    return state === 'active' ? 'start' : 'end';
  }
}

// Readers notify both hosts; only the host that opened one owns its session.
const closedListeners = new Set<() => void>();
export function onFeedDetailClosed(listener: () => void) {
  closedListeners.add(listener);
  return () => { closedListeners.delete(listener); };
}
export function feedDetailClosed() {
  for (const listener of closedListeners) listener();
}
