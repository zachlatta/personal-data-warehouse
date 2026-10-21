// The read-later queue's order, newest saved first, kept on the phone as
// ids. A save or unsave made here is applied at once and kept as pending for
// a couple of minutes, so a feed reload that beats the event to the server
// does not undo it on screen.

export const PENDING_MS = 2 * 60_000;

export type PendingSave = { save: boolean; at: number };

export function applySave(order: string[], id: string, save: boolean): string[] {
  const rest = order.filter((x) => x !== id);
  return save ? [id, ...rest] : rest;
}

export function mergeSaved(server: string[], pending: Map<string, PendingSave>, now: number): string[] {
  let order = [...server];
  const recent = [...pending.entries()].filter(([, p]) => now - p.at < PENDING_MS).sort((a, b) => a[1].at - b[1].at);
  for (const [id, p] of recent) {
    if (p.save && order.includes(id)) continue;
    order = applySave(order, id, p.save);
  }
  return order;
}
