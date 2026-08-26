import { useRouter } from 'expo-router';
import { useEffect, useMemo, useReducer } from 'react';
import { ActivityIndicator, FlatList, Pressable, RefreshControl, ScrollView, StyleSheet, View } from 'react-native';

import { PRIORITY_COLORS, PriorityBadge } from '@/components/priority-badge';
import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { listTimeline, PRIORITIES, type Priority, type TimelineItem } from '@/lib/api';
import { formatWhen, humanSource, truncate } from '@/lib/format';
import { useConfig } from '@/lib/session';

// Attention tiers by default: noise is 82% of the corpus and is exactly what a
// glance at the timeline should not open with.
const DEFAULT_TIERS: Priority[] = ['self', 'direct', 'cc'];
const PAGE = 50;

type State = {
  tiers: Priority[];
  items: TimelineItem[];
  cursor?: string;
  hasMore: boolean;
  // A fetch is in flight for this generation; bumping it discards stale pages.
  generation: number;
  fetching: 'none' | 'reset' | 'more';
  error: string | null;
};

type Action =
  | { type: 'reset'; tiers?: Priority[] }
  | { type: 'more' }
  | { type: 'page'; generation: number; reset: boolean; items: TimelineItem[]; cursor?: string; hasMore: boolean }
  | { type: 'error'; generation: number; message: string };

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case 'reset':
      return { ...state, tiers: action.tiers ?? state.tiers, items: [], cursor: undefined, hasMore: true, generation: state.generation + 1, fetching: 'reset', error: null };
    case 'more':
      if (state.fetching !== 'none' || !state.hasMore || !state.cursor) return state;
      return { ...state, generation: state.generation + 1, fetching: 'more', error: null };
    case 'page':
      if (action.generation !== state.generation) return state;
      return { ...state, items: action.reset ? action.items : [...state.items, ...action.items], cursor: action.cursor, hasMore: action.hasMore, fetching: 'none' };
    case 'error':
      if (action.generation !== state.generation) return state;
      return { ...state, fetching: 'none', error: action.message };
  }
}

export default function TimelineScreen() {
  const config = useConfig();
  const theme = useTheme();
  const router = useRouter();
  const [state, dispatch] = useReducer(reducer, { tiers: DEFAULT_TIERS, items: [], hasMore: true, generation: 1, fetching: 'reset', error: null });
  const { tiers, items, cursor, generation, fetching, error } = state;

  // One fetch per generation. The reducer decides what a generation means
  // (reset vs. next page); this effect only carries it out.
  useEffect(() => {
    if (fetching === 'none') return;
    const reset = fetching === 'reset';
    let cancelled = false;
    listTimeline(config, { priorities: tiers, before: reset ? undefined : cursor, limit: PAGE })
      .then((page) => {
        if (!cancelled) dispatch({ type: 'page', generation, reset, items: page.items, cursor: page.next_cursor, hasMore: page.has_more });
      })
      .catch((e) => {
        if (!cancelled) dispatch({ type: 'error', generation, message: e instanceof Error ? e.message : String(e) });
      });
    return () => {
      cancelled = true;
    };
    // cursor/tiers are captured at the moment the generation was created.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [config, generation]);

  const toggleTier = (tier: Priority) => {
    const next = tiers.includes(tier) ? tiers.filter((t) => t !== tier) : [...tiers, tier];
    dispatch({ type: 'reset', tiers: next.length === 0 ? DEFAULT_TIERS : PRIORITIES.filter((p) => next.includes(p)) });
  };

  const chips = useMemo(
    () => (
      <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.chips}>
        {PRIORITIES.map((tier) => {
          const on = tiers.includes(tier);
          const color = PRIORITY_COLORS[tier];
          return (
            <Pressable
              key={tier}
              onPress={() => toggleTier(tier)}
              style={[styles.chip, { borderColor: color, backgroundColor: on ? color : 'transparent' }]}>
              <ThemedText style={[styles.chipText, { color: on ? '#fff' : color }]}>{tier}</ThemedText>
            </Pressable>
          );
        })}
      </ScrollView>
    ),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [tiers],
  );

  return (
    <ThemedView style={styles.container}>
      {chips}
      {error ? (
        <Pressable onPress={() => dispatch({ type: 'reset' })} style={styles.errorBox}>
          <ThemedText style={styles.errorText}>{error} — tap to retry</ThemedText>
        </Pressable>
      ) : null}
      <FlatList
        data={items}
        keyExtractor={(item) => `${item.adapter}|${item.event_id}`}
        renderItem={({ item }) => (
          <Pressable
            onPress={() =>
              router.push({
                pathname: '/timeline/[adapter]/[eventId]',
                params: { adapter: item.adapter, eventId: item.event_id },
              })
            }
            style={({ pressed }) => [styles.row, { borderBottomColor: theme.backgroundElement }, pressed && { backgroundColor: theme.backgroundElement }]}>
            <View style={styles.rowHeader}>
              <ThemedText type="small" themeColor="textSecondary" style={styles.source}>
                {humanSource(item.source)}
                {item.actor ? ` · ${truncate(item.actor, 40)}` : ''}
              </ThemedText>
              <ThemedText type="small" themeColor="textSecondary">
                {formatWhen(item.event_ts)}
              </ThemedText>
            </View>
            <View style={styles.titleRow}>
              <PriorityBadge priority={item.priority} />
              <ThemedText type="smallBold" style={styles.title} numberOfLines={2}>
                {item.title || '(untitled)'}
              </ThemedText>
            </View>
            {item.snippet ? (
              <ThemedText type="small" themeColor="textSecondary" numberOfLines={2}>
                {truncate(item.snippet, 200)}
              </ThemedText>
            ) : null}
          </Pressable>
        )}
        onEndReachedThreshold={0.6}
        onEndReached={() => dispatch({ type: 'more' })}
        refreshControl={<RefreshControl refreshing={fetching === 'reset' && items.length > 0} onRefresh={() => dispatch({ type: 'reset' })} />}
        ListFooterComponent={fetching !== 'none' ? <ActivityIndicator style={styles.footer} /> : null}
        ListEmptyComponent={
          fetching === 'none' ? (
            <ThemedText themeColor="textSecondary" style={styles.empty}>
              Nothing here for these tiers.
            </ThemedText>
          ) : null
        }
      />
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  chips: { paddingHorizontal: Spacing.three, paddingVertical: Spacing.two, gap: Spacing.two },
  chip: { borderWidth: 1, borderRadius: 999, paddingHorizontal: 12, paddingVertical: 5 },
  chipText: { fontSize: 13, fontWeight: '600' },
  row: { paddingHorizontal: Spacing.three, paddingVertical: 10, gap: 4, borderBottomWidth: StyleSheet.hairlineWidth },
  rowHeader: { flexDirection: 'row', justifyContent: 'space-between', gap: Spacing.two },
  source: { flexShrink: 1, textTransform: 'capitalize' },
  titleRow: { flexDirection: 'row', alignItems: 'flex-start', gap: Spacing.two },
  title: { flex: 1 },
  footer: { paddingVertical: Spacing.three },
  empty: { textAlign: 'center', padding: Spacing.five },
  errorBox: { margin: Spacing.three, padding: Spacing.three, borderRadius: 10, backgroundColor: '#D0342C22' },
  errorText: { color: '#D0342C' },
});
