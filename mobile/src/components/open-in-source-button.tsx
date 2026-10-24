import { useState } from 'react';
import { Alert, Pressable, StyleSheet } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import type { TimelineDeepLink } from '@/lib/api';
import { useTheme } from '@/hooks/use-theme';
import { openDeepLink } from '@/lib/deep-link';

// The "open in Slack / Gmail / Messages…" affordance. `compact` is the
// list-row form: an arrow only, sized for a thumb.
export function OpenInSourceButton({ link, compact = false }: { link?: TimelineDeepLink; compact?: boolean }) {
  const [busy, setBusy] = useState(false);
  const theme = useTheme();
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
        style={({ pressed }) => [styles.compact, { backgroundColor: theme.backgroundElement }, pressed && styles.pressed]}>
        <ThemedText style={[styles.compactText, { color: theme.textSecondary }]}>↗</ThemedText>
      </Pressable>
    );
  }
  return (
    <Pressable onPress={onPress} accessibilityLabel={`Open in ${link.label}`} style={({ pressed }) => [styles.button, { backgroundColor: theme.surface, borderColor: theme.surfaceBorder }, pressed && styles.pressed]}>
      <ThemedText style={[styles.buttonText, { color: theme.text }]}>↗  Open in {link.label}</ThemedText>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  button: { alignSelf: 'flex-start', borderRadius: 999, borderWidth: StyleSheet.hairlineWidth, paddingHorizontal: 16, paddingVertical: 9 },
  buttonText: { fontWeight: '600', fontSize: 15 },
  compact: { borderRadius: 999, width: 24, height: 24, alignItems: 'center', justifyContent: 'center' },
  compactText: { fontWeight: '600', fontSize: 13, lineHeight: 16 },
  pressed: { opacity: 0.6 },
});
