import { Pressable, StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { Palette, Spacing } from '@/constants/theme';

// The count only matters near the limit; under it, "793 / 4000" was noise
// (and pushed off the right edge by the hint on a Pixel, 2026-10-27).
const COUNT_FROM = 0.9;

// The line under an editable message: one hint, the character count when it
// is close to the limit, and "Save without sending" once the words change.
// The action bar's button already reads "Save & send", so saving is a quiet
// link here, not a second filled button.
export function ComposeMeta({
  hint,
  length,
  max,
  dirty,
  busy,
  onSave,
}: {
  hint: string;
  length: number;
  max: number;
  dirty: boolean;
  busy: boolean;
  onSave: () => void;
}) {
  const showCount = length >= max * COUNT_FROM;
  return (
    <View style={styles.row}>
      <ThemedText type="small" themeColor="textSecondary" style={styles.hint}>{hint}</ThemedText>
      {showCount ? (
        <ThemedText type="small" style={{ color: length >= max ? Palette.danger : Palette.warning }}>{length} / {max}</ThemedText>
      ) : null}
      {dirty ? (
        <Pressable
          accessibilityRole="button"
          accessibilityState={{ disabled: busy }}
          disabled={busy}
          hitSlop={10}
          onPress={onSave}
          style={({ pressed }) => [pressed && styles.pressed, busy && styles.disabled]}>
          <ThemedText type="smallBold" style={styles.save}>{busy ? 'Saving…' : 'Save without sending'}</ThemedText>
        </Pressable>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  row: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, minHeight: 22 },
  hint: { flex: 1, minWidth: 0 },
  save: { color: Palette.warning },
  pressed: { opacity: 0.6 },
  disabled: { opacity: 0.45 },
});
