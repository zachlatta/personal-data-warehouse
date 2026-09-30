// The email body is an open editor, and on 2026-09-29 that meant the keyboard
// came up in five of nine reviews without being asked for: a touch that stops
// a moving page lands natively on the text view and focuses it. So the editor
// is locked (not focusable) while the page moves and for a moment after, and a
// tap on still text still puts the cursor exactly where it lands.

export const SCROLL_SETTLE_MS = 350;

export type ScrollLockState = { dragging: boolean; momentum: boolean };
export type ScrollLockEvent = 'dragStart' | 'dragEnd' | 'momentumStart' | 'momentumEnd';

// The next state, whether the editor is locked now, and — when the page has
// just come to rest — how long until it may unlock. A start event cancels any
// pending unlock (unlockAfterMs null).
export function nextScrollLock(state: ScrollLockState, event: ScrollLockEvent): { state: ScrollLockState; locked: boolean; unlockAfterMs: number | null } {
  const next: ScrollLockState = {
    dragging: event === 'dragStart' ? true : event === 'dragEnd' ? false : state.dragging,
    momentum: event === 'momentumStart' ? true : event === 'momentumEnd' || event === 'dragStart' ? false : state.momentum,
  };
  const moving = next.dragging || next.momentum;
  return { state: next, locked: true, unlockAfterMs: moving ? null : SCROLL_SETTLE_MS };
}
