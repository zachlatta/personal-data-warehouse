// Tap to first paint for a review (Superhuman's 100 ms rule), the review's
// counterpart of the feed's open_paint: whatever opens a review notes when
// and how, and the review screen reads it once, the first frame the request
// is on screen with its messages. A notification that cold-starts the app is
// timed from when JavaScript hears the tap, not from the tap itself.

export type ReviewOpenVia = 'list' | 'notification' | 'next' | 'skip' | 'undo';

type Mark = { requestId: string | null; via: ReviewOpenVia; at: number };

let mark: Mark | null = null;

// `requestId` null: the opener does not know which request will show (the
// list's Undo reopens whichever was undone); the next review takes it.
export function markReviewOpen(requestId: string | null, via: ReviewOpenVia, at: number = performance.now()) {
  mark = { requestId, via, at };
}

// Read once: a later repaint of the same review, or a review opened some
// other way, gets nothing. A mark for another request is dropped too.
export function takeReviewPaint(requestId: string, now: number = performance.now()): { ms: number; via: ReviewOpenVia } | null {
  const taken = mark;
  mark = null;
  if (!taken || (taken.requestId !== null && taken.requestId !== requestId)) return null;
  return { ms: Math.max(0, now - taken.at), via: taken.via };
}
