// Tap to first paint for a timeline event (Superhuman's 100 ms rule), the
// event screen's counterpart of the feed's open_paint and the review's
// review_paint: the row notes when it was tapped, and the event screen reads
// it once, the first frame its subject is on screen.

type Mark = { key: string; at: number };

let mark: Mark | null = null;

const keyOf = (adapter: string, eventId: string) => `${adapter}|${eventId}`;

export function markEventOpen(adapter: string, eventId: string, at: number = performance.now()) {
  mark = { key: keyOf(adapter, eventId), at };
}

// Read once; a mark for another event is dropped (an event opened from its
// conversation, a deep link) and gives nothing.
export function takeEventPaint(adapter: string, eventId: string, now: number = performance.now()): number | null {
  const taken = mark;
  mark = null;
  if (!taken || taken.key !== keyOf(adapter, eventId)) return null;
  return Math.max(0, now - taken.at);
}
