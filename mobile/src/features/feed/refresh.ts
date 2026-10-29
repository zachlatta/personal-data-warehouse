// Analytics is durable in its own outbox. Slow uploads must never stand
// between the reader and a page; local save/like overlays bridge the race.
export async function loadWithoutWaitingForEvents<T>(flush: () => Promise<void>, read: () => Promise<T>): Promise<T> {
  void flush().catch(() => {});
  return read();
}

// Keep the actual objects as well as their order while reading. An edited
// card can change height just as surely as prepending a new one can move it.
export function refreshItems<T>(current: T[], incoming: T[], preserve: boolean): T[] {
  return preserve && current.length > 0 ? current : incoming;
}

export function applyPendingDismissals<T extends { item_id: string }>(items: T[], events: { type: string; item_id?: string }[]): T[] {
  const hidden = new Set<string>();
  for (const event of events) {
    if (!event.item_id) continue;
    if (event.type === 'dismiss') hidden.add(event.item_id);
    if (event.type === 'undismiss') hidden.delete(event.item_id);
  }
  return items.filter((item) => !hidden.has(item.item_id));
}
