// A per-page memory of what the review API last returned, so navigating to
// the list or back into a request paints the last known answer immediately
// and the network only has to confirm it. Nothing here outlives the tab: a
// decision (approve, deny, drop) clears it, and every render still refetches.
export class RequestCache {
  constructor() { this.list = null; this.requests = new Map(); }
  rememberList(requests) {
    this.list = Array.isArray(requests) ? requests.slice() : null;
    return this.list;
  }
  remember(request) {
    if (!request || typeof request !== "object" || !request.id) return request;
    this.requests.set(request.id, request);
    // A detail read is also the freshest list row for that request.
    if (this.list) {
      const index = this.list.findIndex((item) => item && item.id === request.id);
      const row = { ...request }; delete row.mutations;
      if (index >= 0) this.list[index] = row; else this.list.unshift(row);
    }
    return request;
  }
  peekList() { return this.list; }
  peek(id) { return this.requests.get(id) || null; }
  invalidate() { this.list = null; this.requests.clear(); }
}
