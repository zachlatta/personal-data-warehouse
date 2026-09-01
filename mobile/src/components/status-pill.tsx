import { StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';

export const STATUS_COLORS: Record<string, string> = {
  pending_review: '#D97706',
  approved: '#2563EB',
  executing: '#2563EB',
  executed: '#16A34A',
  succeeded: '#16A34A',
  observed: '#16A34A',
  rejected: '#6B7280',
  failed: '#DC2626',
  superseded: '#6B7280',
};

export function StatusPill({ status }: { status: string }) {
  const color = STATUS_COLORS[status] ?? '#6B7280';
  return (
    <View style={[styles.pill, { borderColor: color, backgroundColor: `${color}22` }]}>
      <ThemedText style={[styles.pillText, { color }]}>{status.replace(/_/g, ' ')}</ThemedText>
    </View>
  );
}

const styles = StyleSheet.create({
  pill: { borderRadius: 6, borderWidth: 1, paddingHorizontal: 6, paddingVertical: 1, alignSelf: 'flex-start' },
  pillText: { fontSize: 11, fontWeight: '600', lineHeight: 14 },
});
