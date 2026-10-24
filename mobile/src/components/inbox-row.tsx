import type { ReactNode } from 'react';
import { Pressable, ScrollView, StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { useTheme } from '@/hooks/use-theme';

// One list row laid out like a Superhuman thread cell: a coloured dot in the
// left gutter, the sender in semibold with the time at the right, a one-line
// subject, two lines of preview in the secondary colour, then small tinted
// label tags. Timeline, Search and Mutations all draw their rows with it.
export function InboxRow({
  dot,
  sender,
  time,
  subject,
  preview,
  previewLines = 2,
  labels,
  accessory,
  onPress,
}: {
  dot?: string;
  sender: string;
  time?: string;
  subject?: string;
  preview?: string;
  previewLines?: number;
  labels?: { text: string; color?: string }[];
  accessory?: ReactNode;
  onPress: () => void;
}) {
  const theme = useTheme();
  return (
    <Pressable
      onPress={onPress}
      android_ripple={{ color: theme.separator }}
      style={({ pressed }) => [styles.row, { borderBottomColor: theme.separator }, pressed && { backgroundColor: theme.backgroundElement }]}>
      <View style={styles.gutter}>{dot ? <View style={[styles.dot, { backgroundColor: dot }]} /> : null}</View>
      <View style={styles.body}>
        <View style={styles.line}>
          <ThemedText style={styles.sender} numberOfLines={1}>
            {sender}
          </ThemedText>
          {time ? (
            <ThemedText style={styles.time} themeColor="textSecondary">
              {time}
            </ThemedText>
          ) : null}
          {accessory}
        </View>
        {subject ? (
          <ThemedText style={styles.subject} numberOfLines={1}>
            {subject}
          </ThemedText>
        ) : null}
        {preview ? (
          <ThemedText style={styles.preview} themeColor="textSecondary" numberOfLines={previewLines}>
            {preview}
          </ThemedText>
        ) : null}
        {labels && labels.length > 0 ? (
          <View style={styles.labels}>
            {labels.map((label) => (
              <Label key={label.text} text={label.text} color={label.color} />
            ))}
          </View>
        ) : null}
      </View>
    </Pressable>
  );
}

// Superhuman's label tag: small, square-ish, tinted with its own colour.
export function Label({ text, color }: { text: string; color?: string }) {
  const theme = useTheme();
  return (
    <View style={[styles.label, { backgroundColor: color ? `${color}2E` : theme.backgroundSelected }]}>
      <ThemedText style={[styles.labelText, { color: color ?? theme.textSecondary }]} numberOfLines={1}>
        {text}
      </ThemedText>
    </View>
  );
}

// Superhuman's split row ("Inbox 245  Calendar 2  Notifications 188"): text
// tabs, the selected ones in a soft pill and full-strength text.
export function SplitChips<T extends string>({
  items,
  selected,
  onToggle,
  dotColor,
}: {
  items: readonly T[];
  selected: readonly T[];
  onToggle: (item: T) => void;
  dotColor?: (item: T) => string;
}) {
  const theme = useTheme();
  return (
    <ScrollView horizontal showsHorizontalScrollIndicator={false} style={styles.chipsBar} contentContainerStyle={styles.chips}>
      {items.map((item) => {
        const on = selected.includes(item);
        return (
          <Pressable
            key={item}
            accessibilityRole="button"
            accessibilityState={{ selected: on }}
            onPress={() => onToggle(item)}
            style={[styles.chip, { backgroundColor: on ? theme.backgroundSelected : 'transparent' }]}>
            {dotColor ? <View style={[styles.chipDot, { backgroundColor: dotColor(item), opacity: on ? 1 : 0.5 }]} /> : null}
            <ThemedText style={[styles.chipText, { color: on ? theme.text : theme.textSecondary }]}>{item}</ThemedText>
          </Pressable>
        );
      })}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  row: { flexDirection: 'row', paddingRight: 16, paddingVertical: 12, borderBottomWidth: StyleSheet.hairlineWidth },
  gutter: { width: 26, alignItems: 'center', paddingTop: 7 },
  dot: { width: 7, height: 7, borderRadius: 4 },
  body: { flex: 1, minWidth: 0, gap: 2 },
  line: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  sender: { flex: 1, fontSize: 15, lineHeight: 20, fontWeight: '600' },
  time: { fontSize: 12, lineHeight: 16, fontVariant: ['tabular-nums'] },
  subject: { fontSize: 15, lineHeight: 20, fontWeight: '500' },
  preview: { fontSize: 14, lineHeight: 19 },
  labels: { flexDirection: 'row', flexWrap: 'wrap', gap: 6, marginTop: 5 },
  label: { borderRadius: 4, paddingHorizontal: 5, paddingVertical: 1, maxWidth: 220 },
  labelText: { fontSize: 11, lineHeight: 15, fontWeight: '500' },
  // flexGrow: 0 keeps the chip bar at its content height: a pull-to-refresh
  // empties the list, and without it the ScrollView fills the freed space and
  // stretches every pill to the full screen height.
  chipsBar: { flexGrow: 0, flexShrink: 0 },
  chips: { paddingHorizontal: 12, paddingVertical: 6, gap: 4, alignItems: 'center' },
  chip: { flexDirection: 'row', alignItems: 'center', gap: 6, borderRadius: 999, paddingHorizontal: 12, paddingVertical: 6 },
  chipDot: { width: 6, height: 6, borderRadius: 3 },
  chipText: { fontSize: 14, fontWeight: '600' },
});
