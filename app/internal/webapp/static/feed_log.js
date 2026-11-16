// What the Feed page measures, the way the phone measures it. There is no build
// step here, so this file ports the phone's pure logic instead of importing it:
// ActiveClock from mobile/src/features/feed/active-time.ts, ViewabilityTracker
// from viewability.ts, EventQueue from event-queue.ts, FeedLog from engagement.ts,
// the taps' rules from refresh.ts and saved-queue.ts.
// Change the rule in both places; the tests reuse the phone's numbers.
// currentIndex alone is the page's own (the phone has no outlined card or keys);
// it is here to judge "on screen" with the same visibleFraction and threshold.

// Active time: the part of wall time Zach is actually using the page. Time
// counts while the tab is visible and he touched it in the last IDLE_MS, or
// while a video plays.
export const IDLE_MS = 60_000;

export class ActiveClock {
  constructor(now, foreground = true) {
    this.total = 0;
    this.mark = now;
    this.lastTouch = now;
    this.foreground = foreground;
    this.busy = 0;
  }

  advance(now) {
    if (now <= this.mark) return;
    if (this.foreground) {
      const until = this.busy > 0 ? now : Math.min(now, this.lastTouch + IDLE_MS);
      if (until > this.mark) this.total += until - this.mark;
    }
    this.mark = now;
  }

  /** Active milliseconds since the clock started; only goes up. */
  read(now) {
    this.advance(now);
    return this.total;
  }

  touch(now) {
    this.advance(now);
    this.lastTouch = Math.max(this.lastTouch, now);
  }

  /** The tab became visible (counts as a touch) or hidden. */
  setForeground(foreground, now) {
    this.advance(now);
    this.foreground = foreground;
    if (foreground) this.lastTouch = Math.max(this.lastTouch, now);
  }

  /** Something plays without touches: on, then off. */
  setBusy(on, now) {
    this.advance(now);
    this.busy = Math.max(0, this.busy + (on ? 1 : -1));
    if (!on) this.lastTouch = Math.max(this.lastTouch, now);
  }
}

/** Times one stretch (the reader, a video) in both active and wall time. */
export function stopwatch(clock, now) {
  const active = clock.read(now);
  return { stop: (end) => ({ activeMs: Math.round(clock.read(end) - active), wallMs: Math.max(0, end - now) }) };
}

// A hide still in the outbox beats whatever the server just answered.
export function applyPendingDismissals(items, events) {
  const hidden = new Set();
  for (const event of events) {
    if (!event.item_id) continue;
    if (event.type === "dismiss") hidden.add(event.item_id);
    if (event.type === "undismiss") hidden.delete(event.item_id);
  }
  return items.filter((item) => !hidden.has(item.item_id));
}

export function applySave(order, id, save) {
  const rest = order.filter((x) => x !== id);
  return save ? [id, ...rest] : rest;
}

// A like or save stays pending until a server read agrees, so a reload cannot undo it.
export function mergeSaved(server, pending) {
  let order = [...server];
  for (const [id, p] of [...pending.entries()].sort((a, b) => a[1].at - b[1].at)) {
    if (server.includes(id) === p.save) { pending.delete(id); continue; }
    order = applySave(order, id, p.save);
  }
  return order;
}

// A card is in view while at least half of it is on screen (or, for a card
// taller than the screen, while it fills at least half the screen). Each stretch
// in view is one impression; one that never reaches the threshold records nothing.
export const VIEWABLE_THRESHOLD = 0.5;

export function visibleFraction(layout, viewport) {
  if (layout.height <= 0 || viewport.height <= 0) return 0;
  const overlap = Math.min(layout.y + layout.height, viewport.top + viewport.height) - Math.max(layout.y, viewport.top);
  if (overlap <= 0) return 0;
  return Math.min(1, overlap / Math.min(layout.height, viewport.height));
}

// The web page's current card (its outline, and the card its keys act on): the
// topmost card an impression would count as in view, so what is outlined is
// always on screen. When none is, as a gap between two tall cards can leave
// it, the most visible, the upper on a tie; with none on screen (scrolled
// past the end), the last. -1 for no cards.
export function currentIndex(layouts, viewport, threshold = VIEWABLE_THRESHOLD) {
  let best = -1;
  let bestFraction = 0;
  for (let i = 0; i < layouts.length; i++) {
    const fraction = visibleFraction(layouts[i], viewport);
    if (fraction >= threshold) return i;
    if (fraction > bestFraction) { best = i; bestFraction = fraction; }
  }
  return best === -1 && layouts.length ? layouts.length - 1 : best;
}

export class ViewabilityTracker {
  constructor(threshold = VIEWABLE_THRESHOLD) {
    this.episodes = new Map();
    this.threshold = threshold;
  }

  accrue(episode, activeNow) {
    const elapsed = Math.max(0, activeNow - episode.lastActive);
    if (episode.fraction >= this.threshold) episode.dwellMs += elapsed;
    if (episode.fraction > 0) episode.visibleMs += elapsed;
    episode.lastActive = activeNow;
  }

  finish(itemId, episode, now) {
    this.episodes.delete(itemId);
    if (episode.maxFraction < this.threshold) return null;
    return {
      itemId,
      position: episode.position,
      startedAt: episode.startedAt,
      endedAt: now,
      dwellMs: Math.round(episode.dwellMs),
      visibleMs: Math.round(episode.visibleMs),
      maxFraction: Math.round(episode.maxFraction * 1000) / 1000,
    };
  }

  /** Feed the current layout; returns the impressions that just ended. */
  update(now, viewport, layouts, activeNow = now) {
    const ended = [];
    for (const [itemId, episode] of this.episodes) {
      this.accrue(episode, activeNow);
      const layout = layouts.get(itemId);
      const fraction = layout ? visibleFraction(layout, viewport) : 0;
      if (fraction <= 0) {
        const impression = this.finish(itemId, episode, now);
        if (impression) ended.push(impression);
        continue;
      }
      episode.fraction = fraction;
      episode.maxFraction = Math.max(episode.maxFraction, fraction);
    }
    for (const [itemId, layout] of layouts) {
      if (this.episodes.has(itemId)) continue;
      const fraction = visibleFraction(layout, viewport);
      if (fraction <= 0) continue;
      this.episodes.set(itemId, {
        position: layout.position, startedAt: now, lastActive: activeNow, fraction,
        dwellMs: 0, visibleMs: 0, maxFraction: fraction,
      });
    }
    return ended;
  }

  /** End every open stretch (the tab hid, the page was left). */
  flush(now, activeNow = now) {
    const ended = [];
    for (const [itemId, episode] of [...this.episodes]) {
      this.accrue(episode, activeNow);
      const impression = this.finish(itemId, episode, now);
      if (impression) ended.push(impression);
    }
    return ended;
  }
}

export function newEventId(now = Date.now()) {
  const hex = (n) => Array.from({ length: n }, () => Math.floor(Math.random() * 16).toString(16)).join("");
  return `${now.toString(36).padStart(10, "0")}-${hex(8)}-${hex(8)}-${hex(8)}`;
}

export const BATCH_SIZE = 200;
// localStorage holds about 5 MB, far less than the phone's disk.
export const MAX_QUEUED = 5000;
// A browser refuses a keepalive request past 64 KB in flight.
const KEEPALIVE_BYTES = 60_000;
const MIN_BACKOFF_MS = 5_000;
const MAX_BACKOFF_MS = 5 * 60_000;

// The outbox. Storage is the truth, read on every use, because two tabs share
// it. Ids are made here, so a batch sent twice is counted once by the server.
export class EventQueue {
  constructor(storage, send, now = Date.now, warn = () => {}) {
    this.storage = storage;
    this.send = send;
    this.now = now;
    this.warn = warn;
    this.sending = false;
    this.backoffMs = 0;
    this.retryAt = 0;
  }

  pending() {
    try {
      const parsed = JSON.parse(this.storage.load() || "[]");
      return Array.isArray(parsed) ? parsed : [];
    } catch (e) {
      return [];
    }
  }

  get size() { return this.pending().length; }

  save(events) {
    try {
      this.storage.save(JSON.stringify(events));
    } catch (e) {
      this.warn("feed outbox could not be saved: " + e.message);
    }
  }

  enqueue(event) {
    const events = this.pending();
    events.push(event);
    if (events.length > MAX_QUEUED) {
      const drop = events.findIndex((e) => e.type === "impression");
      events.splice(drop >= 0 ? drop : 0, 1);
    }
    this.save(events);
  }

  remove(batch) {
    const sent = new Set(batch.map((e) => e.event_id));
    this.save(this.pending().filter((e) => !sent.has(e.event_id)));
  }

  /** Send everything queued, batch by batch; stops at the first failure. */
  async flush(force = false, keepalive = false) {
    if (keepalive) return this.flushKeepalive();
    if (this.sending || this.size === 0) return;
    if (!force && this.now() < this.retryAt) return;
    this.sending = true;
    try {
      for (let batch = this.pending().slice(0, BATCH_SIZE); batch.length; batch = this.pending().slice(0, BATCH_SIZE)) {
        try {
          await this.send(batch);
        } catch (e) {
          if (e && (e.status === 400 || e.status === 413 || e.status === 422)) {
            // The server refused the content; resending would wedge everything behind it.
            this.warn(`feed events refused (${e.status}): ${e.message}; dropped ${batch.length}`);
          } else {
            this.backoffMs = Math.min(MAX_BACKOFF_MS, this.backoffMs ? this.backoffMs * 2 : MIN_BACKOFF_MS);
            this.retryAt = this.now() + this.backoffMs;
            return;
          }
        }
        this.remove(batch);
        this.backoffMs = 0;
        this.retryAt = 0;
      }
    } finally {
      this.sending = false;
    }
  }

  // The page may be going away: one request that outlives it, even with a send
  // already in flight (which the browser may cancel). What does not fit waits
  // for the next visit.
  async flushKeepalive() {
    const batch = [];
    let bytes = 0;
    for (const event of this.pending().slice(0, BATCH_SIZE)) {
      bytes += new TextEncoder().encode(JSON.stringify(event)).length + 1;
      if (bytes > KEEPALIVE_BYTES) break;
      batch.push(event);
    }
    if (!batch.length) return;
    try {
      await this.send(batch, { keepalive: true });
      this.remove(batch);
    } catch (e) {
      /* it stays queued */
    }
  }
}

// Not touches: recorded after the fact or by the page itself.
const PASSIVE = new Set(["impression", "load", "session_start", "session_end", "close"]);

// Stamps every event with the current sitting and queues it.
export class FeedLog {
  // lifetime outlives a mount: { launched, startedAt, lastEndedAt }.
  constructor({ queue, clock, context, lifetime, flushAt = 40 }) {
    Object.assign(this, { queue, clock, context, lifetime, flushAt });
    this.session = null;
  }

  sessionId() { return this.session ? this.session.id : ""; }
  inSession() { return !!this.session; }
  flush(force = false, keepalive = false) { return this.queue.flush(force, keepalive); }
  pending() { return this.queue.pending(); }
  ui(action, fields = {}) { this.record("ui", { ...fields, metadata: { action, ...fields.metadata } }); }

  // sessionId: an answer that arrives after the sitting ended belongs to the
  // sitting that asked.
  record(type, { at, sessionId, ...fields } = {}) {
    const when = at === undefined ? Date.now() : at;
    if (!PASSIVE.has(type)) this.clock.touch(Date.now());
    const session = this.session;
    if (session) {
      if (type === "impression" && fields.item_id) session.seen.add(fields.item_id);
      else if (type !== "impression" && type !== "session_start") session.acts[type] = (session.acts[type] || 0) + 1;
      if (fields.item_id && typeof fields.position === "number") session.maxPosition = Math.max(session.maxPosition, fields.position);
    }
    this.queue.enqueue({
      event_id: newEventId(when),
      type,
      session_id: sessionId === undefined ? this.sessionId() : sessionId,
      occurred_at: new Date(when).toISOString(),
      ...fields,
    });
    if (this.queue.size >= this.flushAt) void this.queue.flush();
  }

  /** One sitting begins: launch (first since the page started), foreground, or focus (back from another view). */
  startSession(cards, trigger) {
    if (this.session) return;
    const now = Date.now();
    const { lifetime } = this;
    const why = lifetime.launched ? trigger : "launch";
    lifetime.launched = true;
    this.session = { id: newEventId(now), startedAt: now, active: this.clock.read(now), seen: new Set(), maxPosition: -1, acts: {} };
    this.record("session_start", {
      metadata: {
        cards,
        trigger: why,
        gap_ms: lifetime.lastEndedAt ? now - lifetime.lastEndedAt : null,
        since_app_start_ms: now - lifetime.startedAt,
        ...this.context(),
      },
    });
  }

  /** The tab hid or closed (background), or the page was left inside the app (blur). */
  endSession(reason) {
    if (!this.session) return;
    const ended = this.session;
    const now = Date.now();
    this.lifetime.lastEndedAt = now;
    this.record("session_end", {
      dwell_ms: Math.round(this.clock.read(now) - ended.active),
      metadata: { wall_ms: now - ended.startedAt, reason, cards_seen: ended.seen.size, deepest_position: ended.maxPosition, ...ended.acts },
    });
    this.session = null;
    void this.queue.flush(true, true);
  }
}
