import { useRouter } from 'expo-router';
import { useEffect, useReducer } from 'react';
import { ActivityIndicator, FlatList, Pressable, RefreshControl, StyleSheet } from 'react-native';

import { OpenInSourceButton } from '@/components/open-in-source-button';
import { InboxRow, SplitChips } from '@/components/inbox-row';
import { PRIORITY_COLORS } from '@/components/priority-badge';
import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { listTimeline, PRIORITIES, type Priority, type TimelineItem } from '@/lib/api';
import { formatWhen, humanSource, truncate } from '@/lib/format';
import { useConfig } from '@/lib/session';
import { prefetchTimelineItem, rememberTimelineItems } from '@/lib/timeline-cache';
import { useTabBarSpace } from '@/hooks/use-tab-bar-space';

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

// The last first page per tier set, for this process: switching tiers or
// coming back paints rows at once and refreshes underneath, instead of an
// empty list and a spinner.
const firstPages = new Map<string, TimelineItem[]>();
const tierKey = (tiers: Priority[]) => tiers.join(',');

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case 'reset': {
      const tiers = action.tiers ?? state.tiers;
      const items = firstPages.get(tierKey(tiers)) ?? (tierKey(tiers) === tierKey(state.tiers) ? state.items : []);
      return { ...state, tiers, items, cursor: undefined, hasMore: true, generation: state.generation + 1, fetching: 'reset', error: null };
    }
    case 'more':
      if (state.fetching !== 'none' || !state.hasMore || !state.cursor) return state;
      return { ...state, generation: state.generation + 1, fetching: 'more', error: null };
    case 'page':
      if (action.generation !== state.generation) return state;
      if (action.reset) firstPages.set(tierKey(state.tiers), action.items);
      return { ...state, items: action.reset ? action.items : [...state.items, ...action.items], cursor: action.cursor, hasMore: action.hasMore, fetching: 'none' };
    case 'error':
      if (action.generation !== state.generation) return state;
      return { ...state, fetching: 'none', error: action.message };
  }
}

export default function TimelineScreen() {
  const tabSpace = useTabBarSpace();
  const config = useConfig();
  const router = useRouter();
  const [state, dispatch] = useReducer(reducer, { tiers: DEFAULT_TIERS, items: firstPages.get(tierKey(DEFAULT_TIERS)) ?? [], hasMore: true, generation: 1, fetching: 'reset', error: null });
  const { tiers, items, cursor, generation, fetching, error } = state;

  // One fetch per generation. The reducer decides what a generation means
  // (reset vs. next page); this effect only carries it out.
  useEffect(() => {
    if (fetching === 'none') return;
    const reset = fetching === 'reset';
    let cancelled = false;
    // A reset starts at now: the timeline is "what happened", and calendar
    // holds events years out that would otherwise open the list.
    listTimeline(config, { priorities: tiers, before: reset ? undefined : cursor, jump: reset ? new Date().toISOString() : undefined, limit: PAGE })
      .then((page) => {
        rememberTimelineItems(page.items);
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

  return (
    <ThemedView style={styles.container}>
      <SplitChips items={PRIORITIES} selected={tiers} onToggle={toggleTier} dotColor={(tier) => PRIORITY_COLORS[tier]} />
      {error ? (
        <Pressable onPress={() => dispatch({ type: 'reset' })} style={styles.errorBox}>
          <ThemedText style={styles.errorText}>{error} — tap to retry</ThemedText>
        </Pressable>
      ) : null}
      <FlatList
        contentContainerStyle={{ paddingBottom: tabSpace }}
        data={items}
        keyExtractor={(item) => `${item.adapter}|${item.event_id}`}
        renderItem={({ item }) => {
          const headline = item.title || item.context || truncate(item.snippet, 80) || '(untitled)';
          return (
            <InboxRow
              dot={PRIORITY_COLORS[item.priority]}
              sender={item.actor ? truncate(item.actor, 60) : humanSource(item.source)}
              time={formatWhen(item.event_ts)}
              subject={headline}
              preview={item.snippet && (item.title || item.context) ? truncate(item.snippet, 200) : undefined}
              labels={[{ text: humanSource(item.source) }]}
              accessory={<OpenInSourceButton link={item.open} compact />}
              onPressIn={() => prefetchTimelineItem(config, item.adapter, item.event_id)}
              onPress={() =>
                router.push({
                  pathname: '/timeline/[adapter]/[eventId]',
                  params: { adapter: item.adapter, eventId: item.event_id },
                })
              }
            />
          );
        }}
        onEndReachedThreshold={0.6}
        onEndReached={() => dispatch({ type: 'more' })}
        refreshControl={<RefreshControl refreshing={fetching === 'reset' && items.length > 0} onRefresh={() => dispatch({ type: 'reset' })} />}
        ListFooterComponent={fetching === 'more' ? <ActivityIndicator style={styles.footer} /> : null}
        ListEmptyComponent={
          fetching === 'none' ? (
            <ThemedText themeColor="textSecondary" style={styles.empty}>
              Nothing here for these tiers.
            </ThemedText>
          ) : (
            <ActivityIndicator style={styles.footer} />
          )
        }
      />
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  footer: { paddingVertical: Spacing.three },
  empty: { textAlign: 'center', padding: Spacing.five },
  errorBox: { margin: Spacing.three, padding: Spacing.three, borderRadius: 10, backgroundColor: '#E5636D22' },
  errorText: { color: '#E5636D' },
});
