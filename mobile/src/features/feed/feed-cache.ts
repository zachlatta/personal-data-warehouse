// The last feed page on disk, so a cold start paints cards at once instead
// of a spinner while the network answers (Superhuman's local-first rule:
// show what you have, then refresh). Only the cards and the saved list are
// kept; a bad or old file is ignored.
import { File, Paths } from 'expo-file-system';

import type { FeedPage } from './api';

const VERSION = 1;
// Older than this, the cards are more misleading than a short wait.
const MAX_AGE_MS = 3 * 24 * 60 * 60 * 1000;

function file(): File {
  return new File(Paths.document, 'pdw-feed-cache.json');
}

export function readFeedCache(now = Date.now()): FeedPage | null {
  try {
    const f = file();
    if (!f.exists) return null;
    const parsed = JSON.parse(f.textSync()) as { v?: number; at?: number; page?: FeedPage };
    if (parsed.v !== VERSION || !parsed.page || !Array.isArray(parsed.page.items)) return null;
    if (typeof parsed.at !== 'number' || now - parsed.at > MAX_AGE_MS) return null;
    return parsed.page;
  } catch {
    return null;
  }
}

export function writeFeedCache(page: FeedPage, now = Date.now()): void {
  try {
    file().write(JSON.stringify({ v: VERSION, at: now, page }));
  } catch {
    // A cache that cannot be written only costs the next cold start a wait.
  }
}
