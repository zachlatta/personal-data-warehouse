import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { nextScrollLock, type ScrollLockEvent, type ScrollLockState } from '@/lib/scroll-lock';

// The scroll handlers for a review list and whether its editors are locked
// right now (see lib/scroll-lock.ts for why).
export function useScrollLock() {
  const [locked, setLocked] = useState(false);
  const state = useRef<ScrollLockState>({ dragging: false, momentum: false });
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const handle = useCallback((event: ScrollLockEvent) => {
    const step = nextScrollLock(state.current, event);
    state.current = step.state;
    if (timer.current) clearTimeout(timer.current);
    timer.current = null;
    setLocked(step.locked);
    if (step.unlockAfterMs !== null) {
      timer.current = setTimeout(() => {
        timer.current = null;
        setLocked(false);
      }, step.unlockAfterMs);
    }
  }, []);
  useEffect(() => () => {
    if (timer.current) clearTimeout(timer.current);
  }, []);
  const handlers = useMemo(() => ({
    onScrollBeginDrag: () => handle('dragStart'),
    onScrollEndDrag: () => handle('dragEnd'),
    onMomentumScrollBegin: () => handle('momentumStart'),
    onMomentumScrollEnd: () => handle('momentumEnd'),
  }), [handle]);
  return { locked, handlers };
}
