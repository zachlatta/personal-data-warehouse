// Reviewing is a queue, not a list of detail pages: after a decision the
// phone opens the next request still waiting, in the order the Mutations tab
// shows them. Measured on a real session (2026-09-29), each request cost ~3s
// of back-tap, list reload and re-open on top of the review itself; these
// helpers are what let the screen skip that.

type QueueRow = { id: string; status: string };

function pendingIds(list: QueueRow[] | null | undefined): string[] {
  return (list ?? []).filter((row) => row.status === 'pending_review').map((row) => row.id);
}

// The pending request after `currentId` in list order, wrapping to the top;
// never `currentId` itself, whatever status the cached row still carries.
// A request the list has not seen (opened from an alert) starts the queue at
// its head.
export function nextPendingRequestId(list: QueueRow[] | null | undefined, currentId: string): string | null {
  const rows = list ?? [];
  const at = rows.findIndex((row) => row.id === currentId);
  const ordered = at < 0 ? rows : [...rows.slice(at + 1), ...rows.slice(0, at)];
  return pendingIds(ordered).find((id) => id !== currentId) ?? null;
}

// How many requests are left to review, this one included, for the header
// ("3 to review"); nothing for the last one or a request the list does not
// hold as pending. A count rather than "1 of 3", which after a decision
// shrinks to "1 of 2" and reads as going backwards.
export function pendingReviewCount(list: QueueRow[] | null | undefined, currentId: string): number | null {
  const ids = pendingIds(list);
  if (!ids.includes(currentId) || ids.length < 2) return null;
  return ids.length;
}

// One line saying what the last decision did ("Sent · Reply to …"), shown by
// whichever screen the reviewer lands on next and then gone.
let flash: string | null = null;

export function setReviewFlash(message: string): void {
  flash = message;
}

export function takeReviewFlash(): string | null {
  const message = flash;
  flash = null;
  return message;
}
