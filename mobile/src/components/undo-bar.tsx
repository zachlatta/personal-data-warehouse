import { useEffect, useState, useSyncExternalStore } from 'react';
import { Pressable, StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { UNDO_WINDOW_MS, decisions, type DecisionState, type HeldDecision } from '@/lib/undo-decision';

export function useHeldDecision(): DecisionState | null {
  return useSyncExternalStore(decisions.subscribe, decisions.state);
}

// Requests decided on this phone that the server may not have heard yet: it
// still lists them as pending, so screens leave them out of the queue.
export function useDecidedRequestIds(): readonly string[] {
  return useSyncExternalStore(decisions.subscribe, decisions.decidedRequestIds);
}

function useSecondsLeft(deadline: number | null): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (deadline === null) return;
    const timer = setInterval(() => setNow(Date.now()), 250);
    return () => clearInterval(timer);
  }, [deadline]);
  // `now` may trail a deadline set since the last tick; never show more than the window.
  return deadline === null ? 0 : Math.min(UNDO_WINDOW_MS / 1000, Math.max(0, Math.ceil((deadline - now) / 1000)));
}

// What the last decision did ("Sent to vendor@… · Reply to …") and an Undo
// with the seconds left, on whichever screen the reviewer is on: the next
// request in the queue, or the list once it is empty. It sits above the
// action bar, not over the header, where the old note covered the title of
// the request being read. If the decision fails when it is finally sent, the
// bar turns red and says so; a tap opens the request again.
export function UndoBar({ bottom = Spacing.three, onOpen }: { bottom?: number; onOpen: (decision: HeldDecision) => void }) {
  const state = useHeldDecision();
  const seconds = useSecondsLeft(state?.kind === 'held' ? state.deadline : null);
  if (!state) return null;
  if (state.kind === 'sending') {
    return (
      <View pointerEvents="none" style={[styles.wrap, { bottom }]}>
        <View style={styles.pill}>
          <ThemedText type="smallBold" style={[styles.text, styles.note]} numberOfLines={1}>{state.decision.note}</ThemedText>
        </View>
      </View>
    );
  }
  if (state.kind === 'failed') {
    return (
      <View style={[styles.wrap, { bottom }]} accessibilityLiveRegion="assertive">
        <Pressable
          accessibilityRole="button"
          accessibilityHint="Opens the request again"
          onPress={() => {
            decisions.dismiss();
            onOpen(state.decision);
          }}
          style={[styles.pill, styles.failed]}>
          <ThemedText type="smallBold" style={styles.text} numberOfLines={2}>
            Didn’t go through · {state.decision.note}: {state.error}
          </ThemedText>
        </Pressable>
      </View>
    );
  }
  return (
    <View style={[styles.wrap, { bottom }]} accessibilityLiveRegion="polite">
      <View style={styles.pill}>
        <ThemedText type="smallBold" style={[styles.text, styles.note]} numberOfLines={1}>{state.decision.note}</ThemedText>
        <Pressable
          accessibilityRole="button"
          accessibilityLabel={`Undo, ${seconds} seconds left`}
          hitSlop={10}
          onPress={() => {
            const undone = decisions.undo();
            if (undone) onOpen(undone);
          }}>
          <ThemedText type="smallBold" style={styles.undo}>Undo {seconds}s</ThemedText>
        </Pressable>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: { position: 'absolute', left: Spacing.three, right: Spacing.three, alignItems: 'center', zIndex: 10 },
  pill: { flexDirection: 'row', alignItems: 'center', gap: 14, backgroundColor: '#1F2937', borderRadius: 18, paddingHorizontal: 14, paddingVertical: 9, maxWidth: '100%', shadowColor: '#000', shadowOpacity: 0.18, shadowRadius: 8, shadowOffset: { width: 0, height: 2 } },
  failed: { backgroundColor: '#B91C1C' },
  text: { color: '#FFFFFF' },
  note: { flexShrink: 1 },
  undo: { color: '#FDE68A', fontVariant: ['tabular-nums'] },
});
