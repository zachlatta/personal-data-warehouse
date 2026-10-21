// The offline-safe outbox for engagement events. Every event is written to
// storage the moment it is recorded, sent in batches, and removed only after
// the server acknowledged it. Event ids are made on the device, so a batch
// that was delivered but whose answer was lost is sent again and the server
// counts it as duplicates rather than double-counting.

export type FeedEventType =
  | 'session_start'
  | 'session_end'
  | 'impression'
  | 'open'
  | 'close'
  | 'expand'
  | 'dismiss'
  | 'undismiss'
  | 'share'
  | 'media'
  | 'save'
  | 'unsave'
  | 'ui'
  | 'scroll'
  | 'load';

export type FeedEvent = {
  event_id: string;
  type: FeedEventType;
  item_id?: string;
  session_id: string;
  occurred_at: string;
  dwell_ms?: number;
  visible_fraction?: number;
  position?: number;
  metadata?: Record<string, unknown>;
};

export type QueueStorage = { load(): string | null; save(value: string): void };
// Resolves when the server accepted the batch. Rejects with { status } on
// an HTTP error, or anything else on a network failure.
export type BatchSender = (events: FeedEvent[]) => Promise<void>;

export const BATCH_SIZE = 200;
// Past this many unsent events the oldest impressions go first: a phone
// offline for weeks keeps every deliberate act.
export const MAX_QUEUED = 20000;
const MIN_BACKOFF_MS = 5_000;
const MAX_BACKOFF_MS = 5 * 60_000;

export function newEventId(now: number = Date.now(), random: () => number = Math.random): string {
  const hex = (n: number) =>
    Array.from({ length: n }, () => Math.floor(random() * 16).toString(16)).join('');
  return `${now.toString(36).padStart(10, "0")}-${hex(8)}-${hex(8)}-${hex(8)}`;
}

export class EventQueue {
  private events: FeedEvent[] = [];
  private sending = false;
  private backoffMs = 0;
  private retryAt = 0;

  private readonly storage: QueueStorage;
  private readonly send: BatchSender;
  private readonly now: () => number;
  private readonly warn: (message: string) => void;

  constructor(storage: QueueStorage, send: BatchSender, now: () => number = Date.now, warn: (message: string) => void = () => {}) {
    this.storage = storage;
    this.send = send;
    this.now = now;
    this.warn = warn;
    try {
      const raw = storage.load();
      const parsed = raw ? JSON.parse(raw) : [];
      if (Array.isArray(parsed)) this.events = parsed;
    } catch {
      this.events = [];
    }
  }

  get size(): number {
    return this.events.length;
  }

  pending(): FeedEvent[] {
    return [...this.events];
  }

  private persist() {
    try {
      this.storage.save(JSON.stringify(this.events));
    } catch (e) {
      this.warn(`feed outbox could not be saved: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  enqueue(event: FeedEvent) {
    this.events.push(event);
    if (this.events.length > MAX_QUEUED) {
      const drop = this.events.findIndex((e) => e.type === 'impression');
      this.events.splice(drop >= 0 ? drop : 0, 1);
    }
    this.persist();
  }

  /** Send everything queued, batch by batch; stops at the first failure. */
  async flush(force = false): Promise<void> {
    if (this.sending || this.events.length === 0) return;
    if (!force && this.now() < this.retryAt) return;
    this.sending = true;
    try {
      while (this.events.length > 0) {
        const batch = this.events.slice(0, BATCH_SIZE);
        try {
          await this.send(batch);
        } catch (e) {
          const status = typeof e === 'object' && e !== null && 'status' in e ? Number((e as { status: unknown }).status) : 0;
          if (status === 400 || status === 413 || status === 422) {
            // The server refused the batch's content; resending it forever
            // would wedge every event behind it.
            this.warn(`feed events refused (${status}): ${e instanceof Error ? e.message : String(e)}; dropped ${batch.length}`);
          } else {
            this.backoffMs = Math.min(MAX_BACKOFF_MS, this.backoffMs ? this.backoffMs * 2 : MIN_BACKOFF_MS);
            this.retryAt = this.now() + this.backoffMs;
            return;
          }
        }
        const sent = new Set(batch.map((e) => e.event_id));
        this.events = this.events.filter((e) => !sent.has(e.event_id));
        this.persist();
        this.backoffMs = 0;
        this.retryAt = 0;
      }
    } finally {
      this.sending = false;
    }
  }
}
