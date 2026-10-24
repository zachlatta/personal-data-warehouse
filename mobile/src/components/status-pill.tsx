import { StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';

export const STATUS_COLORS: Record<string, string> = {
  pending_review: '#E0A05C',
  approved: '#4FA6DB',
  executing: '#4FA6DB',
  executed: '#4FAFA3',
  succeeded: '#4FAFA3',
  observed: '#4FAFA3',
  rejected: '#8A8784',
  failed: '#F2838D',
  superseded: '#8A8784',
  withdrawn: '#8A8784',
};

export function StatusPill({ status }: { status: string }) {
  const color = STATUS_COLORS[status] ?? '#6B7280';
  return (
    <View style={[styles.pill, { backgroundColor: `${color}2E` }]}>
      <ThemedText style={[styles.pillText, { color }]}>{status.replace(/_/g, ' ')}</ThemedText>
    </View>
  );
}

const styles = StyleSheet.create({
  pill: { borderRadius: 4, paddingHorizontal: 5, paddingVertical: 1, alignSelf: 'flex-start' },
  pillText: { fontSize: 11, fontWeight: '500', lineHeight: 15 },
});
