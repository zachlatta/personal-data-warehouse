import { useFocusEffect, useRouter } from 'expo-router';
import { useCallback, useEffect, useState } from 'react';
import { Pressable, RefreshControl, SectionList, StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { StatusPill } from '@/components/status-pill';
import { UndoBar, useDecidedRequestIds } from '@/components/undo-bar';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { listMutationRequests, type MutationRequest } from '@/lib/api';
import { peekMutationRequests, rememberMutationRequests } from '@/lib/mutation-cache';
import { formatWhen } from '@/lib/format';
import { decisions } from '@/lib/undo-decision';
import { useConfig } from '@/lib/session';

export default function MutationsScreen() {
  const config = useConfig();
  const theme = useTheme();
  const router = useRouter();
  // The last list paints at once; the focus effect below refreshes it.
  const [requests, setRequests] = useState<MutationRequest[]>(() => peekMutationRequests() ?? []);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setRefreshing(true);
    setError(null);
    try {
      setRequests(rememberMutationRequests(await listMutationRequests(config)));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRefreshing(false);
    }
  }, [config]);

  // Coming back from a review, the cache already knows what was decided:
  // paint that first, so a request just approved is not still listed under
  // "Needs review" while the refresh is in flight.
  useFocusEffect(
    useCallback(() => {
      const cached = peekMutationRequests();
      if (cached) setRequests(cached.slice());
      void load();
    }, [load]),
  );

  // A decision being held for undo is not "needs review" any more, though the
  // server has not heard it yet; when it is sent (or undone) the cache moves
  // and the list repaints from it.
  const decided = useDecidedRequestIds();
  useEffect(
    () =>
      decisions.subscribe(() => {
        const cached = peekMutationRequests();
        if (cached) setRequests(cached.slice());
      }),
    [],
  );
  const pending = requests.filter((r) => r.status === 'pending_review' && !decided.includes(r.id));
  const past = requests.filter((r) => r.status !== 'pending_review');
  const sections = [
    { title: `Needs review (${pending.length})`, data: pending, empty: 'Nothing waiting on you.' },
    { title: 'Past requests', data: past, empty: 'No approved or denied requests yet.' },
  ];

  return (
    <ThemedView style={styles.container}>
      <UndoBar onOpen={(undone) => router.push({ pathname: '/mutations/[id]', params: { id: undone.requestId } })} />
      {error ? (
        <Pressable onPress={load} style={styles.errorBox}>
          <ThemedText style={styles.errorText}>{error} — tap to retry</ThemedText>
        </Pressable>
      ) : null}
      <SectionList
        sections={sections}
        keyExtractor={(item) => item.id}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={load} />}
        renderSectionHeader={({ section }) => (
          <ThemedView style={styles.sectionHeader}>
            <ThemedText type="smallBold" themeColor="textSecondary">
              {section.title}
            </ThemedText>
          </ThemedView>
        )}
        renderSectionFooter={({ section }) =>
          section.data.length === 0 && !refreshing ? (
            <ThemedText type="small" themeColor="textSecondary" style={styles.empty}>
              {section.empty}
            </ThemedText>
          ) : null
        }
        renderItem={({ item }) => (
          <Pressable
            onPress={() => router.push({ pathname: '/mutations/[id]', params: { id: item.id } })}
            style={({ pressed }) => [styles.row, { borderBottomColor: theme.backgroundElement }, pressed && { backgroundColor: theme.backgroundElement }]}>
            <View style={styles.rowHeader}>
              <StatusPill status={item.status} />
              <ThemedText type="small" themeColor="textSecondary">
                {formatWhen(item.created_at)}
              </ThemedText>
            </View>
            <ThemedText type="smallBold">{item.title}</ThemedText>
            <ThemedText type="small" themeColor="textSecondary" numberOfLines={2}>
              {item.reason}
            </ThemedText>
            <ThemedText type="small" themeColor="textSecondary">
              {item.mutation_count} mutation{item.mutation_count === 1 ? '' : 's'} · by {item.requested_by || 'unknown'}
            </ThemedText>
          </Pressable>
        )}
      />
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  sectionHeader: { paddingHorizontal: Spacing.three, paddingTop: Spacing.three, paddingBottom: Spacing.two },
  row: { paddingHorizontal: Spacing.three, paddingVertical: 10, gap: 4, borderBottomWidth: StyleSheet.hairlineWidth },
  rowHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  empty: { paddingHorizontal: Spacing.three, paddingBottom: Spacing.three },
  errorBox: { margin: Spacing.three, padding: Spacing.three, borderRadius: 10, backgroundColor: '#D0342C22' },
  errorText: { color: '#D0342C' },
});
