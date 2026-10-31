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
  appState(state: string): 'start' | 'end' | null {
    if (!this.owns) return null;
    return state === 'active' ? 'start' : 'end';
  }
}
