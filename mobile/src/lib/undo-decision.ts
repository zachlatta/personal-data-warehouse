// A decision on a mutation request is not sent the moment it is tapped: it is
// held for UNDO_WINDOW_MS while the phone moves straight on to the next
// request, and an Undo in that window cancels it before anything reaches the
// server. This replaced a confirm dialog on every Approve/Deny, which cost a
// second tap per request and was tapped through unread. The hold is in this
// process only, so the failure mode is the safe one: if the app dies inside
// the window, the request is simply still waiting for review.
//
// Only the last decision can be undone: deciding the next request sends the
// one before at once, so a held decision never outlives the next tap.

export const UNDO_WINDOW_MS = 10_000;

export type HeldDecision = {
  requestId: string;
  // One line saying what the decision does ("Sent to vendor@… · Reply to …").
  note: string;
  commit: () => Promise<void>;
};

// held: inside the undo window. sending: the window closed and the server has
// not answered yet — no longer undoable. failed: the server refused it, shown
// until dismissed. The screen shows one of them, in that order of urgency
// for the reviewer: the one they can still undo first.
export type DecisionState =
  | { kind: 'held'; decision: HeldDecision; deadline: number }
  | { kind: 'sending'; decision: HeldDecision }
  | { kind: 'failed'; decision: HeldDecision; error: string };

type Clock = {
  setTimer: (fn: () => void, ms: number) => unknown;
  clearTimer: (handle: unknown) => void;
  now?: () => number;
};

export type DecisionHolder = ReturnType<typeof createDecisionHolder>;

export function createDecisionHolder(clock: Clock, windowMs: number = UNDO_WINDOW_MS) {
  const now = clock.now ?? (() => Date.now());
  let held: { decision: HeldDecision; deadline: number } | null = null;
  let timer: unknown = null;
  const inFlight: HeldDecision[] = [];
  let failure: { decision: HeldDecision; error: string } | null = null;
  // Snapshots are rebuilt only on a change, so React's external-store read
  // sees the same object until something actually moved.
  let snapshot: DecisionState | null = null;
  let decided: readonly string[] = [];
  const listeners = new Set<() => void>();
  const emit = () => {
    if (held) snapshot = { kind: 'held', ...held };
    else if (inFlight.length) snapshot = { kind: 'sending', decision: inFlight[inFlight.length - 1] };
    else snapshot = failure ? { kind: 'failed', ...failure } : null;
    decided = [...(held ? [held.decision.requestId] : []), ...inFlight.map((decision) => decision.requestId)];
    listeners.forEach((listener) => listener());
  };

  const take = (): HeldDecision | null => {
    if (!held) return null;
    const { decision } = held;
    if (timer !== null) clock.clearTimer(timer);
    timer = null;
    held = null;
    return decision;
  };

  const send = async (decision: HeldDecision) => {
    inFlight.push(decision);
    emit();
    try {
      await decision.commit();
    } catch (e) {
      failure = { decision, error: e instanceof Error ? e.message : String(e) };
    }
    inFlight.splice(inFlight.indexOf(decision), 1);
    emit();
  };

  return {
    hold(decision: HeldDecision): void {
      const previous = take();
      held = { decision, deadline: now() + windowMs };
      timer = clock.setTimer(() => {
        timer = null;
        const due = take();
        if (due) void send(due);
      }, windowMs);
      if (previous) void send(previous);
      else emit();
    },
    undo(): HeldDecision | null {
      const decision = take();
      if (decision) emit();
      return decision;
    },
    async flush(): Promise<void> {
      const decision = take();
      if (decision) await send(decision);
    },
    dismiss(): void {
      if (!failure) return;
      failure = null;
      emit();
    },
    // Requests decided here that the server may not have heard yet — held or
    // in flight. It still lists them as pending; the queue must not.
    decidedRequestIds(): readonly string[] {
      return decided;
    },
    state(): DecisionState | null {
      return snapshot;
    },
    subscribe(listener: () => void): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
}

// The one holder the app uses.
export const decisions = createDecisionHolder({
  setTimer: (fn, ms) => setTimeout(fn, ms),
  clearTimer: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
});
