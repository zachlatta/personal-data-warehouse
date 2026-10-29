// Unsaved review edits survive navigation for this app session only. Never
// write message drafts beside the token. A changed server baseline wins so
// a restored edit cannot silently target a different proposal.
export class ReviewDrafts {
  private entries = new Map<string, { baseline: string; value: unknown }>();
  read<T>(key: string, baseline: string, fallback: T): T {
    const entry = this.entries.get(key);
    if (entry?.baseline === baseline) return entry.value as T;
    this.entries.delete(key);
    return fallback;
  }
  write<T>(key: string, baseline: string, value: T): void {
    this.entries.set(key, { baseline, value });
  }
  remove(key: string): void { this.entries.delete(key); }
  clear(): void { this.entries.clear(); }
}

export const reviewDrafts = new ReviewDrafts();
