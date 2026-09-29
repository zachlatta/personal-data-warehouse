import { useFocusEffect } from 'expo-router';
import { useCallback, useEffect, useState } from 'react';
import { StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { takeReviewFlash } from '@/lib/review-queue';

// The note a decision leaves behind ("Sent · Reply to …") on whichever screen
// the reviewer lands on next — the next request in the queue, or the list
// once the queue is empty — so moving on never hides what just happened. It
// sits above the action bar, not over the header, where it covered the title
// of the request being read.
export function ReviewFlash({ bottom = Spacing.three }: { bottom?: number }) {
  const [message, setMessage] = useState<string | null>(null);
  useFocusEffect(
    useCallback(() => {
      const next = takeReviewFlash();
      if (next) setMessage(next);
    }, []),
  );
  useEffect(() => {
    if (!message) return;
    const timer = setTimeout(() => setMessage(null), 3500);
    return () => clearTimeout(timer);
  }, [message]);
  if (!message) return null;
  return (
    <View pointerEvents="none" style={[styles.wrap, { bottom }]} accessibilityLiveRegion="polite">
      <View style={styles.pill}>
        <ThemedText type="smallBold" style={styles.text} numberOfLines={2}>✓ {message}</ThemedText>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: { position: 'absolute', left: Spacing.three, right: Spacing.three, alignItems: 'center', zIndex: 10 },
  pill: { backgroundColor: '#15803D', borderRadius: 18, paddingHorizontal: 14, paddingVertical: 8, maxWidth: '100%', shadowColor: '#000', shadowOpacity: 0.18, shadowRadius: 8, shadowOffset: { width: 0, height: 2 } },
  text: { color: '#FFFFFF' },
});
