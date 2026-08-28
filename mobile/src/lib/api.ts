import type { AppConfig } from './config';

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function request<T>(config: AppConfig, path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set('Authorization', `Bearer ${config.clientName}:${config.token}`);
  headers.set('Accept', 'application/json');
  headers.set('X-PDW-Client', config.clientName);
  if (init.body && !headers.has('Content-Type')) headers.set('Content-Type', 'application/json');
  const response = await fetch(`${config.baseUrl}${path}`, { ...init, headers });
  const text = await response.text();
  let parsed: unknown = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = null;
  }
  if (!response.ok) {
    const message =
      (parsed && typeof parsed === 'object' && 'error' in parsed && typeof (parsed as { error: unknown }).error === 'string'
        ? (parsed as { error: string }).error
        : text.trim()) || `HTTP ${response.status}`;
    throw new ApiError(response.status, message);
  }
  return parsed as T;
}

// --- timeline ---------------------------------------------------------------

export type Priority = 'self' | 'direct' | 'cc' | 'noise' | 'background' | 'unclassified';
export const PRIORITIES: Priority[] = ['self', 'direct', 'cc', 'noise', 'background'];

// "Open in its source": url works anywhere a browser does; app_url is a
// native scheme a phone should try first (Slack, Messages, Notes, WhatsApp).
export type TimelineDeepLink = { url: string; label: string; app_url?: string };

export type TimelineItem = {
  adapter: string;
  event_id: string;
  source: string;
  kind: string;
  priority: Priority;
  event_ts: string;
  end_ts: string | null;
  actor: string;
  title: string;
  snippet: string;
  context: string;
  source_table: string;
  source_pk: Record<string, unknown>;
  metadata: Record<string, unknown>;
  seq: number;
  open?: TimelineDeepLink;
  // Set on rows returned by the context endpoint for the event asked about.
  is_anchor?: boolean;
};

export type TimelinePage = { items: TimelineItem[]; has_more: boolean; next_cursor?: string };

export type TimelineListParams = {
  priorities?: Priority[];
  sources?: string[];
  before?: string;
  // RFC3339 instant to start just below (the API's jump); used for "from now".
  jump?: string;
  limit?: number;
};

export function listTimeline(config: AppConfig, params: TimelineListParams): Promise<TimelinePage> {
  const query = new URLSearchParams();
  if (params.priorities?.length) query.set('priorities', params.priorities.join(','));
  if (params.sources?.length) query.set('sources', params.sources.join(','));
  if (params.before) query.set('before', params.before);
  else if (params.jump) query.set('jump', params.jump);
  if (params.limit) query.set('limit', String(params.limit));
  const suffix = query.toString();
  return request<TimelinePage>(config, `/api/timeline${suffix ? `?${suffix}` : ''}`);
}

export type TimelineMedia = { media_url: string; media_kind: string; mime_type?: string; filename?: string };
export type TimelineChildRows = Record<string, unknown>[] | { error: string };

// The conversation around one event, from timeline.context(): an email's
// thread, a Slack message's thread or its channel, the surrounding chat
// messages, the neighboring turns of a session.
export type TimelineContextPage = { items: TimelineItem[]; before: number; after: number };
export const TIMELINE_CONTEXT_MAX_WINDOW = 50;

export type TimelineItemDetail = {
  item: TimelineItem;
  context?: TimelineContextPage;
  context_error?: string;
  source_row?: Record<string, unknown> | null;
  source_row_error?: string;
  item_media?: TimelineMedia | null;
  children?: Record<string, TimelineChildRows>;
  children_meta?: Record<string, { has_more: boolean; next_offset: number }>;
};

export function getTimelineItem(config: AppConfig, adapter: string, eventId: string): Promise<TimelineItemDetail> {
  const query = new URLSearchParams({ adapter, event_id: eventId });
  return request<TimelineItemDetail>(config, `/api/timeline/item?${query.toString()}`);
}

export function getTimelineContext(
  config: AppConfig,
  adapter: string,
  eventId: string,
  window: { before: number; after: number },
): Promise<TimelineContextPage> {
  const query = new URLSearchParams({ adapter, event_id: eventId, before: String(window.before), after: String(window.after) });
  return request<TimelineContextPage>(config, `/api/timeline/item/context?${query.toString()}`);
}

// --- mutations --------------------------------------------------------------

export type MutationRequestStatus =
  | 'pending_review'
  | 'approved'
  | 'rejected'
  | 'executing'
  | 'executed'
  | 'failed'
  | 'superseded';

export type Mutation = {
  id: string;
  request_id: string;
  request_index: number;
  provider: string;
  operation: string;
  account: string;
  status: string;
  title: string;
  reason: string;
  payload: Record<string, unknown>;
  preview: Record<string, unknown>;
  result: Record<string, unknown>;
  error: string;
  created_at: string | null;
  approved_at: string | null;
  executed_at: string | null;
};

export type MutationRequest = {
  id: string;
  status: MutationRequestStatus;
  title: string;
  reason: string;
  context: Record<string, unknown>;
  result: Record<string, unknown>;
  error: string;
  superseded_by: string;
  requested_by: string;
  approved_by: string;
  created_at: string | null;
  updated_at: string | null;
  approved_at: string | null;
  executed_at: string | null;
  observed_at: string | null;
  mutation_count: number;
  review_url: string;
  mutations?: Mutation[];
};

export async function listMutationRequests(config: AppConfig, statuses?: MutationRequestStatus[]): Promise<MutationRequest[]> {
  const query = statuses?.length ? `?status=${statuses.join(',')}` : '';
  const body = await request<{ requests: MutationRequest[] }>(config, `/api/mutations/requests${query}`);
  return body.requests;
}

export async function getMutationRequest(config: AppConfig, id: string): Promise<MutationRequest> {
  const body = await request<{ request: MutationRequest }>(config, `/api/mutations/requests/${encodeURIComponent(id)}`);
  return body.request;
}

export async function approveMutationRequest(config: AppConfig, id: string): Promise<MutationRequest> {
  const body = await request<{ request: MutationRequest }>(config, `/api/mutations/requests/${encodeURIComponent(id)}/approve`, {
    method: 'POST',
  });
  return body.request;
}

export async function rejectMutationRequest(config: AppConfig, id: string, reason: string): Promise<MutationRequest> {
  const body = await request<{ request: MutationRequest }>(config, `/api/mutations/requests/${encodeURIComponent(id)}/reject`, {
    method: 'POST',
    body: JSON.stringify({ reason }),
  });
  return body.request;
}

export async function removeMutation(config: AppConfig, requestId: string, mutationId: string): Promise<Mutation> {
  const body = await request<{ mutation: Mutation }>(
    config,
    `/api/mutations/requests/${encodeURIComponent(requestId)}/mutations/${encodeURIComponent(mutationId)}/remove`,
    { method: 'POST' },
  );
  return body.mutation;
}

// --- push -------------------------------------------------------------------

export type PushDevice = {
  expo_push_token: string;
  client_name: string;
  device_name: string;
  platform: string;
  app_version: string;
  status: string;
  registered_at: string;
};

export async function registerPushDevice(
  config: AppConfig,
  input: { expo_push_token: string; device_name: string; platform: string; app_version: string },
): Promise<PushDevice> {
  const body = await request<{ device: PushDevice }>(config, '/api/push/register', {
    method: 'POST',
    body: JSON.stringify(input),
  });
  return body.device;
}

export type PushReport = { devices: number; sent: number; failed: number; disabled: number; errors?: string[] };

export type PushAction = {
  id: string;
  title: string;
  destructive: boolean;
  opens_app: boolean;
  text_input?: { placeholder: string; submit_title: string };
};
export type PushCategory = { id: string; actions: PushAction[] };

// The server owns the category list (app/internal/push/categories.go); the
// app registers whatever it publishes so a new button needs no app release.
export async function fetchPushCategories(config: AppConfig): Promise<PushCategory[]> {
  const body = await request<{ categories: PushCategory[] }>(config, '/api/push/categories');
  return body.categories;
}

export async function sendTestPush(config: AppConfig): Promise<PushReport> {
  const body = await request<{ report: PushReport }>(config, '/api/push/test', { method: 'POST' });
  return body.report;
}

// A cheap authenticated probe used by the login screen.
export async function probe(config: AppConfig): Promise<void> {
  await request<unknown>(config, '/api/timeline?limit=1');
}

// --- search (the app's hybrid search tool, POST /api/tools/search) --------

export type SearchMode = 'hybrid' | 'keyword' | 'exact';

export type SearchHit = {
  source: string;
  subsource?: string;
  context?: string;
  who?: string;
  occurred_at: string;
  account?: string;
  ref: string;
  text: string;
  score?: number;
  event_ts?: string;
  title?: string;
  source_table?: string;
  source_pk?: Record<string, unknown> | string;
  priority?: Priority;
};

export type SearchResult = {
  query: string;
  mode: string;
  total_rows: number;
  rows: SearchHit[];
  hint?: string;
  fallback_reason?: string;
  error?: string;
};

export async function search(
  config: AppConfig,
  input: { query: string; mode?: SearchMode; max_results?: number; priorities?: Priority[]; sources?: string[]; since?: string },
): Promise<SearchResult> {
  const body = await request<{ data: SearchResult }>(config, '/api/tools/search', {
    method: 'POST',
    body: JSON.stringify(input),
  });
  return { ...body.data, rows: body.data.rows ?? [] };
}

// A search hit's ref is "<adapter>:<event_id>"; the timeline item endpoint
// wants the two halves.
export function splitRef(ref: string): { adapter: string; eventId: string } | null {
  const idx = ref.indexOf(':');
  if (idx <= 0) return null;
  return { adapter: ref.slice(0, idx), eventId: ref.slice(idx + 1) };
}
