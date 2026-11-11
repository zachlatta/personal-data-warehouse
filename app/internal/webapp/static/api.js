// The browser's client for the app's JSON API. It is the same API the iOS app
// calls (mobile/src/lib/api.ts): the bearer is "<client_name>:<PDW_SECRET_TOKEN>",
// every request names the client, and the paths below are the phone's paths.

const TOKEN_KEY = "pdw_timeline_token";
export const CLIENT_NAME = "web";

// Accept a one-time token handoff via the URL fragment (#token=...) or, as a
// fallback for terminals that cut links at the "#", a ?token= query param.
// Either way it is stored and immediately stripped from the address bar.
export function adoptTokenFromLocation() {
  if (location.hash.indexOf("#token=") === 0) {
    setToken(decodeURIComponent(location.hash.slice(7)));
    history.replaceState(null, "", location.pathname);
  } else if (location.search.indexOf("token=") !== -1) {
    const qtoken = new URLSearchParams(location.search).get("token");
    if (qtoken) {
      setToken(qtoken);
      history.replaceState(null, "", location.pathname);
    }
  }
}

export function getToken() { return localStorage.getItem(TOKEN_KEY) || ""; }
export function setToken(value) { localStorage.setItem(TOKEN_KEY, (value || "").trim()); }
export function clearToken() { localStorage.removeItem(TOKEN_KEY); }

export class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

function queryString(params) {
  const qs = new URLSearchParams();
  for (const key of Object.keys(params || {})) {
    const value = params[key];
    if (value === "" || value === null || value === undefined) continue;
    qs.set(key, Array.isArray(value) ? value.join(",") : String(value));
  }
  const text = qs.toString();
  return text ? "?" + text : "";
}

export async function request(path, options = {}) {
  const client = options.client || CLIENT_NAME;
  const headers = new Headers(options.headers || {});
  headers.set("Authorization", "Bearer " + client + ":" + getToken());
  headers.set("X-PDW-Client", client);
  headers.set("Accept", "application/json");
  const init = { method: options.method || "GET", headers };
  if (options.keepalive) init.keepalive = true;
  if (options.body !== undefined) {
    headers.set("Content-Type", "application/json");
    init.body = JSON.stringify(options.body);
  }
  const response = await fetch(path + queryString(options.params), init);
  const text = await response.text();
  let parsed = null;
  try { parsed = text ? JSON.parse(text) : null; } catch (e) { parsed = null; }
  if (response.status === 401 || response.status === 403) {
    document.dispatchEvent(new CustomEvent("pdw:unauthorized"));
    throw new ApiError(response.status, "unauthorized");
  }
  if (!response.ok) {
    const message = (parsed && typeof parsed.error === "string" && parsed.error) || text.trim() || ("HTTP " + response.status);
    throw new ApiError(response.status, message);
  }
  return parsed;
}

// --- timeline ---------------------------------------------------------------

export const PRIORITIES = ["self", "direct", "cc", "noise", "background"];
export const TIMELINE_CONTEXT_MAX_WINDOW = 50;

export const timeline = {
  list(params) { return request("/api/timeline", { params }); },
  sources() { return request("/api/timeline/sources"); },
  item(adapter, eventId) { return request("/api/timeline/item", { params: { adapter, event_id: eventId } }); },
  context(adapter, eventId, before, after) {
    return request("/api/timeline/item/context", { params: { adapter, event_id: eventId, before, after } });
  },
  children(adapter, eventId, child, offset) {
    return request("/api/timeline/item/children", { params: { adapter, event_id: eventId, child, offset } });
  },
};

// --- mutations --------------------------------------------------------------

const REQUESTS = "/api/mutations/requests";
function requestPath(id, ...rest) {
  return [REQUESTS, encodeURIComponent(id), ...rest.map(encodeURIComponent)].join("/");
}

export const mutations = {
  async list(options = {}) {
    const params = {};
    if (options.statuses && options.statuses.length) params.status = options.statuses.join(",");
    if (options.limit) params.limit = options.limit;
    const body = await request(REQUESTS, { params });
    return body.requests || [];
  },
  // page: { offset, limit } — the window of the request's mutations to read.
  async get(id, page) { return (await request(requestPath(id), { params: page || {} })).request; },
  async approve(id) { return (await request(requestPath(id, "approve"), { method: "POST" })).request; },
  // Mutation policies: GET lists them with their grants and the kill switch;
  // the two writes are reviewer-only switches.
  async policies() { return request("/api/mutations/policies"); },
  async setPolicyMode(id, mode) { return request("/api/mutations/policies/" + encodeURIComponent(id) + "/mode", { method: "POST", body: { mode } }); },
  async setAutoApprove(enabled) { return request("/api/mutations/policies/settings", { method: "POST", body: { auto_approve: enabled } }); },
  // Undo a thermostat change that ran: the answer is the new request.
  async undo(id) { return (await request(requestPath(id, "undo"), { method: "POST" })).request; },
  async reject(id, reason) { return (await request(requestPath(id, "reject"), { method: "POST", body: { reason: reason || "" } })).request; },
  async supersede(id, supersededBy) {
    return (await request(requestPath(id, "supersede"), { method: "POST", body: { superseded_by: supersededBy } })).request;
  },
  async remove(requestId, mutationId) {
    return (await request(requestPath(requestId, "mutations", mutationId, "remove"), { method: "POST" })).mutation;
  },
  // input: { delivery_mode, selected_variant_id, message: { to, cc, bcc, subject, body_text, body_html, reply_to_thread_id, in_reply_to, references } }
  async updateEmail(requestId, mutationId, input) {
    return (await request(requestPath(requestId, "mutations", mutationId, "update-email"), { method: "POST", body: input })).mutation;
  },
  async updateSlackMessage(requestId, mutationId, input) {
    return (await request(requestPath(requestId, "mutations", mutationId, "update-slack-message"), { method: "POST", body: input })).mutation;
  },
  // input: { text } -- the words of a pending android_messages.send_message
  async updateTextMessage(requestId, mutationId, input) {
    return (await request(requestPath(requestId, "mutations", mutationId, "update-text-message"), { method: "POST", body: input })).mutation;
  },
};

// --- search (the app's hybrid search tool, POST /api/tools/search) ----------

export const search = {
  async run(input) {
    const body = await request("/api/tools/search", { method: "POST", body: input });
    const data = (body && body.data) || {};
    return { ...data, rows: data.rows || [] };
  },
};

// --- feed (the same routes as the phone's Feed tab) -------------------------

// The marts count these as Zach, so a card seen here leaves the fresh part on
// his phone. A browser cannot read its hostname, so the names are fixed: any
// browser on the live page counts as this laptop (docs/feed.md).
export const FEED_CLIENT = "crobat-web";
export const FEED_DEVICE = "crobat";

export const feed = {
  page(limit = 200) { return request("/api/feed", { client: FEED_CLIENT, params: { limit } }); },
  article(url) { return request("/api/feed/article", { client: FEED_CLIENT, params: { url } }); },
  postEvents(events, options = {}) {
    return request("/api/feed/events", { client: FEED_CLIENT, method: "POST", body: { device: FEED_DEVICE, events }, keepalive: options.keepalive });
  },
};

// A search hit's ref is "<adapter>:<event_id>"; the item endpoint wants the halves.
export function splitRef(ref) {
  const idx = (ref || "").indexOf(":");
  if (idx <= 0) return null;
  return { adapter: ref.slice(0, idx), eventId: ref.slice(idx + 1) };
}

// A cheap authenticated probe used by the token gate.
export function probe() { return request("/api/timeline", { params: { limit: 1 } }); }
