import { StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import type { Priority } from '@/lib/api';

// From Superhuman's swipe and label palette: self in its lavender, direct in
// the unread blue (it is what needs Zach), cc in star amber, the rest grey.
export const PRIORITY_COLORS: Record<Priority, string> = {
  self: '#9B83D9',
  direct: '#4FA6DB',
  cc: '#D3913F',
  noise: '#8A8784',
  background: '#A19D99',
  unclassified: '#8A8784',
};

export function PriorityBadge({ priority }: { priority: Priority }) {
  const color = PRIORITY_COLORS[priority] ?? '#8A8784';
  return (
    <View style={[styles.badge, { backgroundColor: `${color}2E` }]}>
      <ThemedText style={[styles.text, { color }]}>{priority}</ThemedText>
    </View>
  );
}

const styles = StyleSheet.create({
  badge: { borderRadius: 4, paddingHorizontal: 5, paddingVertical: 1, alignSelf: 'flex-start' },
  text: { fontSize: 11, fontWeight: '500', lineHeight: 15 },
});
