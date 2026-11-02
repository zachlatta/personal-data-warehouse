import { isOfflineError, RETRY_FAILED_MS, type OfflineEntry } from './offline-store.ts';

export const OFFLINE_ARTICLE_MESSAGE = "You're offline and this article wasn't downloaded yet. It downloads the next time the feed loads online.";

export function articleFallback(url: string, error: unknown) {
  return isOfflineError(error)
    ? { kind: 'offline' as const, message: OFFLINE_ARTICLE_MESSAGE }
    : { kind: 'page' as const, url, label: 'Open the page' };
}

export function knownNotArticle(entry: Pick<OfflineEntry, 'file' | 'failed_at' | 'failure_reason'> | undefined, now: number): boolean {
  return !!entry && !entry.file && entry.failure_reason === 'not_article' && now - entry.failed_at < RETRY_FAILED_MS;
}

/** Record only the server's definitive non-article answer, including foreground reads. */
export async function fetchReadableArticle<A>(
  url: string,
  fetch: () => Promise<A>,
  remember: (url: string) => void,
): Promise<A> {
  try {
    return await fetch();
  } catch (error) {
    if (error instanceof Error && 'status' in error && error.status === 422) remember(url);
    throw error;
  }
}
