import { useFocusEffect, useRouter } from 'expo-router';
import { useCallback, useEffect, useState } from 'react';
import { Pressable, RefreshControl, SectionList, StyleSheet } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { InboxRow } from '@/components/inbox-row';
import { STATUS_COLORS } from '@/components/status-pill';
import { UndoBar, useDecidedRequestIds } from '@/components/undo-bar';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { getMutationRequest, listMutationRequests, type MutationRequest } from '@/lib/api';
import type { AppConfig } from '@/lib/config';
import { peekMutationRequests, rememberMutationRequests } from '@/lib/mutation-cache';
import { prefetchMutationRequest, prefetchPendingRequests } from '@/lib/review-prefetch';
import { formatWhen } from '@/lib/format';
import { markReviewOpen } from '@/lib/review-paint';
import { decisions } from '@/lib/undo-decision';
import { useConfig } from '@/lib/session';
import { useTabBarSpace } from '@/hooks/use-tab-bar-space';

// The list's rows carry no messages, so a review opened from it used to wait
// on a spinner for its own read. Superhuman preloads threads before you open
// them: the first few requests waiting on review are read in full as the list
// lands, and any other row as a finger touches it.
function prefetchRequest(config: AppConfig, id: string) {
  prefetchMutationRequest((one) => getMutationRequest(config, one), id);
}

export default function MutationsScreen() {
  const tabSpace = useTabBarSpace();
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
      const list = rememberMutationRequests(await listMutationRequests(config));
      setRequests(list);
      prefetchPendingRequests((one) => getMutationRequest(config, one), list);
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
    { title: pending.length ? `Needs review (${pending.length})` : 'Needs review', data: pending, empty: 'All caught up. Nothing is waiting on you.' },
    { title: 'Past requests', data: past, empty: 'No approved or denied requests yet.' },
  ];

  return (
    <ThemedView style={styles.container}>
      {error ? (
        <Pressable onPress={load} style={styles.errorBox}>
          <ThemedText style={styles.errorText}>{error} — tap to retry</ThemedText>
        </Pressable>
      ) : null}
      <SectionList
        contentContainerStyle={{ paddingBottom: tabSpace }}
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
          section.data.length === 0 && !refreshing && !error ? (
            <ThemedText type="small" themeColor="textSecondary" style={styles.empty}>
              {section.empty}
            </ThemedText>
          ) : null
        }
        renderItem={({ item }) => (
          <InboxRow
            dot={item.status === 'pending_review' ? theme.unread : undefined}
            sender={item.requested_by || 'unknown'}
            time={formatWhen(item.created_at)}
            subject={item.title}
            preview={item.reason}
            labels={[
              { text: item.status.replace(/_/g, ' '), color: STATUS_COLORS[item.status] },
              { text: `${item.mutation_count} mutation${item.mutation_count === 1 ? '' : 's'}` },
            ]}
            onPressIn={() => prefetchRequest(config, item.id)}
            onPress={() => {
              markReviewOpen(item.id, 'list');
              router.push({ pathname: '/mutations/[id]', params: { id: item.id } });
            }}
          />
        )}
      />
      {/* After the list, so the toast draws above the rows it floats over. */}
      <UndoBar bottom={tabSpace + 8} onOpen={(undone) => {
        markReviewOpen(undone.requestId, 'undo');
        router.push({ pathname: '/mutations/[id]', params: { id: undone.requestId } });
      }} />
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  sectionHeader: { paddingLeft: 26, paddingRight: Spacing.three, paddingTop: Spacing.three, paddingBottom: 6 },
  empty: { paddingLeft: 26, paddingRight: Spacing.three, paddingVertical: Spacing.three },
  errorBox: { margin: Spacing.three, padding: Spacing.three, borderRadius: 10, backgroundColor: '#E5636D22' },
  errorText: { color: '#E5636D' },
});
