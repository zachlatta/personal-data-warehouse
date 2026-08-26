import { StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import type { Priority } from '@/lib/api';

export const PRIORITY_COLORS: Record<Priority, string> = {
  self: '#7C3AED',
  direct: '#DC2626',
  cc: '#D97706',
  noise: '#6B7280',
  background: '#9CA3AF',
  unclassified: '#000000',
};

export function PriorityBadge({ priority }: { priority: Priority }) {
  const color = PRIORITY_COLORS[priority] ?? '#6B7280';
  return (
    <View style={[styles.badge, { backgroundColor: `${color}22`, borderColor: color }]}>
      <ThemedText style={[styles.text, { color }]}>{priority}</ThemedText>
    </View>
  );
}

const styles = StyleSheet.create({
  badge: { borderRadius: 6, borderWidth: 1, paddingHorizontal: 6, paddingVertical: 1 },
  text: { fontSize: 11, fontWeight: '600', lineHeight: 14 },
});
