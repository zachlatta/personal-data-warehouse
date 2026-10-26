// The last feed page on disk, so a cold start paints cards at once instead
// of a spinner while the network answers (Superhuman's local-first rule:
// show what you have, then refresh). Only the cards and the saved list are
// kept; a bad or old file is ignored. Offline, an older page (up to 14
// days) is shown with the offline banner (offline-store.ts).
import { File, Paths } from 'expo-file-system';

import type { FeedPage } from './api';
import { COLD_START_MAX_AGE_MS, feedCacheText, parseFeedCache, type CachedPage } from './offline-store';

function file(): File {
  return new File(Paths.document, 'pdw-feed-cache.json');
}

export function readFeedCache(now = Date.now(), maxAgeMs = COLD_START_MAX_AGE_MS): CachedPage | null {
  try {
    const f = file();
    return f.exists ? parseFeedCache(f.textSync(), now, maxAgeMs) : null;
  } catch {
    return null;
  }
}

export function writeFeedCache(page: FeedPage, now = Date.now()): void {
  try {
    file().write(feedCacheText(page, now));
  } catch {
    // A cache that cannot be written only costs the next cold start a wait.
  }
}
