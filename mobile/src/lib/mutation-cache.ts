// The last mutation requests this process saw, keyed by id, plus the last
// list. Two things feed it: every API read, and the push notification's own
// data.request (the Go app attaches the review JSON to the alert), so a tap
// on an alert renders the review before — or without — a network round
// trip. It is memory only: the token lives in the Keychain and a request's
// body must not be serialized beside it; a cold start seeds from the alert
// that launched the app instead.
import type { MutationRequest } from './api';

let requests = new Map<string, MutationRequest>();
let list: MutationRequest[] | null = null;
// Every request known to be waiting on review, for the tab badge. The list
// above is one page of all statuses, so it can miss older pending requests;
// this set is filled by a pending-only read and kept current by every other
// read, so an approval anywhere takes the badge down at once.
let pending: Set<string> | null = null;
// The rows of that read, in the server's order: the review queue when the
// Mutations tab has not loaded its list (a review opened from an alert).
let pendingRows: MutationRequest[] | null = null;
const listeners = new Set<() => void>();

function notify(): void {
  for (const listener of listeners) listener();
}

function track(request: Pick<MutationRequest, 'id' | 'status'>): void {
  if (!pending || (request.status === 'pending_review') === pending.has(request.id)) return;
  // A new set on every change: the badge's store compares by identity.
  pending = new Set(pending);
  if (request.status === 'pending_review') pending.add(request.id);
  else pending.delete(request.id);
}

export function subscribeMutationRequests(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

// The ids of every request waiting on review, from the server's pending-only
// list; requests seen since then move in or out of it by their status.
export function rememberPendingMutationRequests(items: MutationRequest[]): void {
  pendingRows = items.filter((item) => item.status === 'pending_review');
  pending = new Set(pendingRows.map((item) => item.id));
  notify();
}

// null until the first pending-only read lands.
export function peekPendingMutationRequestIds(): ReadonlySet<string> | null {
  return pending;
}

export function rememberMutationRequest(request: MutationRequest): MutationRequest {
  const known = requests.get(request.id);
  // A partial (header-only) copy must not overwrite a full one.
  if (request.partial && known && !known.partial) return known;
  requests.set(request.id, request);
  if (list) {
    const { mutations: _mutations, ...row } = request;
    const index = list.findIndex((item) => item.id === request.id);
    if (index >= 0) list[index] = row as MutationRequest;
    else list = [row as MutationRequest, ...list];
  }
  track(request);
  notify();
  return request;
}

export function rememberMutationRequests(items: MutationRequest[]): MutationRequest[] {
  list = items.slice();
  items.forEach(track);
  notify();
  return items;
}

export function peekMutationRequest(id: string): MutationRequest | null {
  return requests.get(id) ?? null;
}

export function peekMutationRequests(): MutationRequest[] | null {
  return list;
}

// The queue a review moves through: the Mutations tab's list, or else the
// pending-only read the tab badge keeps fresh, less any request since seen
// decided.
export function peekReviewQueue(): MutationRequest[] | null {
  if (list) return list;
  if (!pendingRows || !pending) return null;
  const still = pending;
  return pendingRows.filter((row) => still.has(row.id));
}

export function forgetMutationRequests(): void {
  requests = new Map();
  list = null;
  pending = null;
  pendingRows = null;
  notify();
}

// The request a mutation alert carries, if the alert carries one for the id
// it names; anything else (an older server, a foreign payload) is ignored
// and the screen fetches as before.
export function mutationRequestFromNotificationData(data: unknown): MutationRequest | null {
  if (!data || typeof data !== 'object') return null;
  const d = data as Record<string, unknown>;
  if (d.kind !== 'mutation_request' || typeof d.request_id !== 'string') return null;
  const request = d.request;
  if (!request || typeof request !== 'object' || Array.isArray(request)) return null;
  const r = request as Record<string, unknown>;
  if (r.id !== d.request_id || typeof r.status !== 'string' || typeof r.title !== 'string') return null;
  return r as unknown as MutationRequest;
}

export function seedMutationRequestFromNotification(data: unknown): MutationRequest | null {
  const request = mutationRequestFromNotificationData(data);
  return request ? rememberMutationRequest(request) : null;
}
