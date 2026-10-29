import { useEffect, useSyncExternalStore } from 'react';
import { AppState } from 'react-native';

import { useDecidedRequestIds } from '@/components/undo-bar';
import { getMutationRequest, listMutationRequests } from '@/lib/api';
import { peekPendingMutationRequestIds, rememberPendingMutationRequests, subscribeMutationRequests } from '@/lib/mutation-cache';
import { prefetchPendingRequests } from '@/lib/review-prefetch';
import { useConfig } from '@/lib/session';

// While the app is open, re-read the queue this often, so a request proposed
// with no alert (or an alert swiped away) still shows on the tab.
const REFRESH_MS = 60_000;

// How many mutation requests are waiting on Zach, for the Mutations tab's
// badge: the server's pending-only list, re-read on launch, on every return
// to the foreground and once a minute, and moved at once by any review, alert
// or list read in between. A decision held for undo already counts as made.
// Each read also reads the first few waiting requests in full, so a review
// opened from an alert, or the next one after a decision, paints from memory
// even if the Mutations tab was never opened.
export function usePendingMutationCount(): number {
  const config = useConfig();
  const pending = useSyncExternalStore(subscribeMutationRequests, peekPendingMutationRequestIds);
  const decided = useDecidedRequestIds();

  useEffect(() => {
    let timer: ReturnType<typeof setInterval> | null = null;
    const refresh = () => {
      listMutationRequests(config, ['pending_review'], 500)
        .then((items) => {
          rememberPendingMutationRequests(items);
          prefetchPendingRequests((id) => getMutationRequest(config, id), items);
        })
        .catch((error) => console.warn('pending mutation count will retry', error));
    };
    const start = () => {
      refresh();
      if (!timer) timer = setInterval(refresh, REFRESH_MS);
    };
    const stop = () => {
      if (timer) clearInterval(timer);
      timer = null;
    };
    if (AppState.currentState === 'active') start();
    const sub = AppState.addEventListener('change', (state) => (state === 'active' ? start() : stop()));
    return () => {
      stop();
      sub.remove();
    };
  }, [config]);

  if (!pending) return 0;
  let count = pending.size;
  for (const id of decided) if (pending.has(id)) count -= 1;
  return count;
}
