import { useState } from 'react';
import { Alert, Pressable, StyleSheet } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import type { TimelineDeepLink } from '@/lib/api';
import { openDeepLink } from '@/lib/deep-link';

// The "open in Slack / Gmail / Messages…" affordance. `compact` is the
// list-row form: an arrow only, sized for a thumb.
export function OpenInSourceButton({ link, compact = false }: { link?: TimelineDeepLink; compact?: boolean }) {
  const [busy, setBusy] = useState(false);
  if (!link?.url) return null;
  const onPress = async () => {
    if (busy) return;
    setBusy(true);
    try {
      await openDeepLink(link);
    } catch (e) {
      Alert.alert(`Could not open ${link.label}`, e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };
  if (compact) {
    return (
      <Pressable
        onPress={onPress}
        hitSlop={10}
        accessibilityLabel={`Open in ${link.label}`}
        style={({ pressed }) => [styles.compact, pressed && styles.pressed]}>
        <ThemedText style={styles.compactText}>↗</ThemedText>
      </Pressable>
    );
  }
  return (
    <Pressable onPress={onPress} accessibilityLabel={`Open in ${link.label}`} style={({ pressed }) => [styles.button, pressed && styles.pressed]}>
      <ThemedText style={styles.buttonText}>↗  Open in {link.label}</ThemedText>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  button: { alignSelf: 'flex-start', backgroundColor: '#208AEF', borderRadius: 10, paddingHorizontal: 14, paddingVertical: 9 },
  buttonText: { color: '#fff', fontWeight: '700', fontSize: 15 },
  compact: { borderWidth: 1, borderColor: '#208AEF', borderRadius: 999, width: 28, height: 28, alignItems: 'center', justifyContent: 'center' },
  compactText: { color: '#208AEF', fontWeight: '700', fontSize: 14, lineHeight: 18 },
  pressed: { opacity: 0.6 },
});
