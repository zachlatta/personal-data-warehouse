import { useFocusEffect } from 'expo-router';
import { useCallback, useEffect, useState } from 'react';
import { AppState } from 'react-native';

import { LiveRefresh, type RefreshReason } from '@/lib/live-refresh';

// Reads closer together than this are one read: a cold start's mount, focus
// and first `active` arrive together, and so do a tab switch and a resume.
const MIN_INTERVAL_MS = 10_000;
// While a list is on screen with the app open, how often it re-reads: the
// pending-count badge's cadence, so the list and its badge move together.
const PERIOD_MS = 60_000;

// Keeps a list screen current: it re-reads on focus, on every return to the
// foreground while focused, and once a minute while both hold (see
// src/lib/live-refresh.ts). The returned controller's `loaded()` tells it a
// read the screen made itself (mount, pull, filter change) just happened.
export function useLiveRefresh(refresh: (why: RefreshReason) => void): LiveRefresh {
  const [live] = useState(
    () =>
      new LiveRefresh({
        refresh,
        minIntervalMs: MIN_INTERVAL_MS,
        periodMs: PERIOD_MS,
        appState: AppState.currentState,
      }),
  );

  useEffect(() => {
    live.setRefresh(refresh);
  });

  useEffect(() => {
    const sub = AppState.addEventListener('change', (state) => live.appState(state));
    // Unmounting also runs the focus cleanup below, which stops the timer.
    return () => sub.remove();
  }, [live]);

  useFocusEffect(
    useCallback(() => {
      live.focus();
      return () => live.blur();
    }, [live]),
  );

  return live;
}
