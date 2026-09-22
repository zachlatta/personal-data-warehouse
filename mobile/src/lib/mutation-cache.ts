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
  return request;
}

export function rememberMutationRequests(items: MutationRequest[]): MutationRequest[] {
  list = items.slice();
  return items;
}

export function peekMutationRequest(id: string): MutationRequest | null {
  return requests.get(id) ?? null;
}

export function peekMutationRequests(): MutationRequest[] | null {
  return list;
}

export function forgetMutationRequests(): void {
  requests = new Map();
  list = null;
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
