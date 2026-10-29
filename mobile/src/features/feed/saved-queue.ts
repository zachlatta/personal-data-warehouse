// The read-later queue's order, newest saved first, kept on the phone as
// ids. A save or unsave made here is applied at once and kept pending until
// a server read agrees, so a reload after a long outage cannot undo it.

export type PendingSave = { save: boolean; at: number };

export function applySave(order: string[], id: string, save: boolean): string[] {
  const rest = order.filter((x) => x !== id);
  return save ? [id, ...rest] : rest;
}

export function mergeSaved(server: string[], pending: Map<string, PendingSave>): string[] {
  let order = [...server];
  const recent = [...pending.entries()].sort((a, b) => a[1].at - b[1].at);
  for (const [id, p] of recent) {
    if (server.includes(id) === p.save) {
      pending.delete(id);
      continue;
    }
    order = applySave(order, id, p.save);
  }
  return order;
}
