// Small independent records keep each Keychain value below its size limit.
// Only scoped open capabilities are queued: no message text or warehouse token.
type KV = { get(key: string): Promise<string | null>; set(key: string, value: string): Promise<void>; remove(key: string): Promise<void> };
export type QueuedOpen = { delivery_id: string; open_proof: string; baseUrl: string };
const JOURNAL = 'pdw.notification-opens.enqueue';
const HEAD = 'pdw.notification-opens.head';
const itemKey = (id: string) => `pdw.notification-opens.${id.replace(/[^a-zA-Z0-9.-]/g, '_')}`;
export class OpenQueue {
  private serial: Promise<unknown> = Promise.resolve();
  private kv: KV;
  constructor(kv: KV) { this.kv = kv; }
  private run<T>(work: () => Promise<T>): Promise<T> {
    const task = this.serial.then(work, work); this.serial = task.catch(() => {}); return task;
  }
  private async recover(): Promise<void> {
    const pending = await this.kv.get(JOURNAL);
    if (!pending) return;
    const node = JSON.parse(pending) as { record: QueuedOpen; next: string | null };
    await this.kv.set(itemKey(node.record.delivery_id), pending);
    await this.kv.set(HEAD, node.record.delivery_id);
    await this.kv.remove(JOURNAL);
  }
  add(record: QueuedOpen): Promise<void> {
    return this.run(async () => {
      await this.recover();
      const key = itemKey(record.delivery_id);
      if (await this.kv.get(key)) return;
      const next = await this.kv.get(HEAD);
      // Replay this bounded journal after a crash between linked-list writes.
      await this.kv.set(JOURNAL, JSON.stringify({ record, next }));
      await this.recover();
    });
  }
  flush(send: (record: QueuedOpen) => Promise<boolean>): Promise<void> {
    return this.run(async () => {
      await this.recover();
      // Bound each foreground flush; later launches/foregrounds keep draining.
      for (let i = 0; i < 100; i++) {
        const id = await this.kv.get(HEAD); if (!id) return;
        const raw = await this.kv.get(itemKey(id)); if (!raw) return;
        const { record, next } = JSON.parse(raw) as { record: QueuedOpen; next: string | null };
        try { if (!await send(record)) return; } catch { return; }
        if (next) await this.kv.set(HEAD, next); else await this.kv.remove(HEAD);
        await this.kv.remove(itemKey(id));
      }
    });
  }
}

export function notificationOpenLink(value: unknown): { url: string; label: string; app_url?: string } | null {
  if (!value || typeof value !== 'object') return null;
  const v = value as Record<string, unknown>;
  const allowed = (u: unknown): u is string => typeof u === 'string' && /^(https:\/\/|slack:|sms:|imessage:|whatsapp:|applenotes:|mobilenotes:|addressbook:)/i.test(u);
  if (!allowed(v.url) || typeof v.label !== 'string') return null;
  return { url: v.url, label: v.label, ...(allowed(v.app_url) ? { app_url: v.app_url } : {}) };
}

// Navigation and persistence have separate completion signals. In particular,
// the PDW fallback must not wait behind an older offline upload holding the queue.
export async function prepareNotificationOpen(
  openSource: () => Promise<boolean>,
  persist: () => Promise<void>,
): Promise<{ sourceOpened: boolean; recorded: Promise<boolean> }> {
  const opening = openSource().catch(() => false);
  const recorded = persist().then(() => true, () => false);
  return { sourceOpened: await opening, recorded };
}
