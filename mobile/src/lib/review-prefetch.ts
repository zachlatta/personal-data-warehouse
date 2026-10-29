// Superhuman preloads a thread before it is opened; this is the review's
// version. Every read of one mutation request goes through here, so a read
// already on its way (a finger on a list row, a notification tap, the queue
// warming up) is joined rather than started again, and whatever it brings is
// remembered for the next screen to paint from. Memory only, like
// mutation-cache.ts.
//
// On the Pixel (2026-10-28/29), reviews opened from an alert painted after
// 666 and 1218 ms and the next request after a decision after 801 ms, all
// from the network: the alert's request is cut to a header when its drafts
// do not fit a push, and the queue was only read in full from the Mutations
// tab.
import type { MutationRequest } from './api';
import { peekMutationRequest, rememberMutationRequest } from './mutation-cache.ts';

// How many requests waiting on review are kept read in full ahead of time.
export const PREFETCH_PENDING = 5;

type Fetch = (id: string) => Promise<MutationRequest>;

const reading = new Map<string, Promise<MutationRequest>>();

// Reads the request (or joins the read in flight) and remembers it.
export function readMutationRequest(fetchOne: Fetch, id: string): Promise<MutationRequest> {
  const inFlight = reading.get(id);
  if (inFlight) return inFlight;
  const read = fetchOne(id)
    .then(rememberMutationRequest)
    .finally(() => reading.delete(id));
  reading.set(id, read);
  return read;
}

// A request held with its messages needs no read ahead; a header (a list row,
// or an alert's partial copy) does.
export function heldInFull(id: string): boolean {
  const known = peekMutationRequest(id);
  return Boolean(known && !known.partial && known.mutations);
}

// Starts a read ahead unless the request is already held in full or on its
// way; failures are left to the screen's own read.
export function prefetchMutationRequest(fetchOne: Fetch, id: string): void {
  if (heldInFull(id) || reading.has(id)) return;
  readMutationRequest(fetchOne, id).catch(() => undefined);
}

// The first few requests still waiting on review, in queue order.
export function prefetchPendingRequests(fetchOne: Fetch, rows: readonly { id: string; status: string }[], limit: number = PREFETCH_PENDING): void {
  for (const row of rows.filter((item) => item.status === 'pending_review').slice(0, limit)) prefetchMutationRequest(fetchOne, row.id);
}
