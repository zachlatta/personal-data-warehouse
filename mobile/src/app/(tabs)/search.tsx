import { useFocusEffect, useRouter } from 'expo-router';
import { useCallback, useEffect, useRef, useState } from 'react';
import { ActivityIndicator, FlatList, Keyboard, Pressable, StyleSheet, TextInput, View } from 'react-native';

import { Icon } from '@/components/icon';
import { InboxRow, SplitChips } from '@/components/inbox-row';
import { PRIORITY_COLORS } from '@/components/priority-badge';
import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { connectFeedLogger, recordUi } from '@/features/feed/engagement';
import { effectiveMobileSearchPriorities, MOBILE_DEFAULT_SEARCH_PRIORITIES, PRIORITIES, search, splitRef, type Priority, type SearchHit, type SearchMode, type SearchResult } from '@/lib/api';
import { markEventOpen } from '@/lib/event-paint';
import { formatWhen, humanSource, truncate } from '@/lib/format';
import { addRecent } from '@/lib/recent-searches';
import { readRecentSearches, writeRecentSearches } from '@/lib/recent-searches-store';
import { prefetchTimelineItem } from '@/lib/timeline-cache';
import { useConfig } from '@/lib/session';
import { useTabBarSpace } from '@/hooks/use-tab-bar-space';

const MODES: SearchMode[] = ['hybrid', 'keyword', 'exact'];

const sameTiers = (a: Priority[], b: Priority[]) => a.length === b.length && a.every((t, i) => t === b[i]);

// Search opens with the keyboard up and the last searches one tap away; the
// mode and tier chips sit behind one quiet Filters line (Superhuman keeps
// its search box bare). search_paint times submit to results on screen.
export default function SearchScreen() {
  const tabSpace = useTabBarSpace();
  const config = useConfig();
  const theme = useTheme();
  const router = useRouter();
  const [query, setQuery] = useState('');
  const [mode, setMode] = useState<SearchMode>('hybrid');
  const [tiers, setTiers] = useState<Priority[]>(MOBILE_DEFAULT_SEARCH_PRIORITIES);
  const [result, setResult] = useState<SearchResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [recent, setRecent] = useState<string[]>(() => readRecentSearches());
  const [filtersOpen, setFiltersOpen] = useState(false);
  const generation = useRef(0);
  const input = useRef<TextInput>(null);
  // Set at submit, read once on the first frame with the answer.
  const paint = useRef<{ gen: number; at: number; via: string } | null>(null);

  useEffect(() => {
    connectFeedLogger(config);
  }, [config]);

  // The screen stays mounted between visits: coming back to an empty box
  // brings the keyboard up again, as the first visit's autoFocus did.
  const empty = useRef(true);
  useEffect(() => {
    empty.current = !query.trim() && !result;
  }, [query, result]);
  useFocusEffect(
    useCallback(() => {
      if (!empty.current) return;
      const t = setTimeout(() => input.current?.focus(), 50);
      return () => clearTimeout(t);
    }, []),
  );

  useEffect(() => {
    const p = paint.current;
    if (!p || busy || p.gen !== generation.current || (!result && !error)) return;
    paint.current = null;
    const rows = result?.rows.length ?? 0;
    requestAnimationFrame(() => recordUi('search_paint', { dwell_ms: Math.round(performance.now() - p.at), metadata: { via: p.via, mode: result?.mode ?? mode, rows, error: Boolean(error) } }));
  }, [busy, result, error, mode]);

  const run = useCallback(
    async (override?: { mode?: SearchMode; tiers?: Priority[]; query?: string; via?: string }) => {
      const q = (override?.query ?? query).trim();
      if (!q) return;
      Keyboard.dismiss();
      const gen = ++generation.current;
      paint.current = { gen, at: performance.now(), via: override?.via ?? 'typed' };
      setBusy(true);
      setError(null);
      setRecent((list) => {
        const next = addRecent(list, q);
        writeRecentSearches(next);
        return next;
      });
      try {
        const useTiers = override?.tiers ?? tiers;
        const res = await search(config, {
          query: q,
          mode: override?.mode ?? mode,
          max_results: 30,
          // Every tier selected means no filter: unclassified rows are then reachable too.
          priorities: effectiveMobileSearchPriorities(useTiers),
        });
        if (gen !== generation.current) return;
        setResult(res);
        if (res.error) setError(res.error);
      } catch (e) {
        if (gen === generation.current) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (gen === generation.current) setBusy(false);
      }
    },
    [config, query, mode, tiers],
  );

  const toggleTier = (tier: Priority) => {
    const next = tiers.includes(tier) ? tiers.filter((t) => t !== tier) : [...tiers, tier];
    const ordered = next.length === 0 ? MOBILE_DEFAULT_SEARCH_PRIORITIES : PRIORITIES.filter((p) => next.includes(p));
    setTiers(ordered);
    if (result) void run({ tiers: ordered, via: 'filter' });
  };
  const pickMode = (m: SearchMode) => {
    setMode(m);
    if (result) void run({ mode: m, via: 'filter' });
  };
  const pickRecent = (q: string) => {
    setQuery(q);
    void run({ query: q, via: 'recent' });
  };
  const clear = () => {
    generation.current++;
    setQuery('');
    setResult(null);
    setError(null);
    setBusy(false);
    input.current?.focus();
  };
  const forgetRecent = () => {
    setRecent([]);
    writeRecentSearches([]);
  };

  const filtered = mode !== 'hybrid' || !sameTiers(tiers, MOBILE_DEFAULT_SEARCH_PRIORITIES);
  const showRecent = !result && !busy && !error && query.trim() === '' && recent.length > 0;

  const prefetch = (hit: SearchHit) => {
    const parts = splitRef(hit.ref);
    if (parts) prefetchTimelineItem(config, parts.adapter, parts.eventId);
  };
  const open = (hit: SearchHit) => {
    const parts = splitRef(hit.ref);
    if (!parts) return;
    markEventOpen(parts.adapter, parts.eventId);
    router.push({ pathname: '/timeline/[adapter]/[eventId]', params: { adapter: parts.adapter, eventId: parts.eventId } });
  };

  return (
    <ThemedView style={styles.container}>
      <View style={styles.searchRow}>
        <View style={[styles.inputBox, { backgroundColor: theme.surface, borderColor: theme.surfaceBorder }]}>
          <Icon name="magnifyingglass" tintColor={theme.textSecondary} size={18} />
          <TextInput
            ref={input}
            autoFocus
            style={[styles.input, { color: theme.text }]}
            placeholder="Words the answer would contain"
            placeholderTextColor={theme.textSecondary}
            value={query}
            onChangeText={setQuery}
            onSubmitEditing={() => run()}
            returnKeyType="search"
            autoCapitalize="none"
            autoCorrect={false}
          />
          {query ? (
            <Pressable accessibilityRole="button" accessibilityLabel="Clear search" hitSlop={10} onPress={clear}>
              <Icon name="xmark" tintColor={theme.textSecondary} size={18} />
            </Pressable>
          ) : null}
        </View>
      </View>
      <Pressable accessibilityRole="button" onPress={() => setFiltersOpen((open) => !open)} hitSlop={4} style={styles.filterLine}>
        <ThemedText type="small" themeColor={filtered ? 'text' : 'textSecondary'} numberOfLines={1}>
          {mode} · {tiers.length === PRIORITIES.length ? 'all tiers' : tiers.join(', ')}
        </ThemedText>
        <ThemedText type="small" themeColor="textSecondary">
          {filtersOpen ? 'Hide filters' : 'Filters'}
        </ThemedText>
      </Pressable>
      {filtersOpen ? (
        <>
          <SplitChips items={MODES} selected={[mode]} onToggle={pickMode} />
          <SplitChips items={PRIORITIES} selected={tiers} onToggle={toggleTier} dotColor={(tier) => PRIORITY_COLORS[tier]} />
        </>
      ) : null}
      {error ? <ThemedText style={styles.error}>{error}</ThemedText> : null}
      {result?.hint ? (
        <ThemedText type="small" themeColor="textSecondary" style={styles.hint}>
          {result.hint}
        </ThemedText>
      ) : null}
      {result?.fallback_reason ? (
        <ThemedText type="small" themeColor="textSecondary" style={styles.hint}>
          keyword-only: {result.fallback_reason}
        </ThemedText>
      ) : null}
      {busy ? <ActivityIndicator style={styles.spinner} /> : null}
      <FlatList
        contentContainerStyle={{ paddingBottom: tabSpace }}
        data={result?.rows ?? []}
        keyExtractor={(hit, index) => `${hit.ref}|${index}`}
        keyboardShouldPersistTaps="handled"
        renderItem={({ item }) => (
          <InboxRow
            dot={item.priority ? PRIORITY_COLORS[item.priority] : undefined}
            sender={item.who ? truncate(item.who, 60) : humanSource(item.source)}
            time={formatWhen(item.occurred_at)}
            subject={item.title || item.context || truncate(item.text, 80)}
            preview={truncate(item.text, 260)}
            previewLines={3}
            labels={[{ text: humanSource(item.source) }]}
            onPressIn={() => prefetch(item)}
            onPress={() => open(item)}
          />
        )}
        ListEmptyComponent={
          showRecent ? (
            <View>
              <View style={styles.recentHead}>
                <ThemedText type="smallBold" themeColor="textSecondary">
                  Recent
                </ThemedText>
                <Pressable accessibilityRole="button" hitSlop={8} onPress={forgetRecent}>
                  <ThemedText type="small" themeColor="textSecondary">
                    Clear
                  </ThemedText>
                </Pressable>
              </View>
              {recent.map((q) => (
                <Pressable key={q} onPress={() => pickRecent(q)} android_ripple={{ color: theme.separator }} style={({ pressed }) => [styles.recentRow, { borderBottomColor: theme.separator }, pressed && { backgroundColor: theme.backgroundElement }]}>
                  <Icon name="clock" tintColor={theme.textSecondary} size={18} />
                  <ThemedText numberOfLines={1} style={styles.recentText}>
                    {q}
                  </ThemedText>
                </Pressable>
              ))}
            </View>
          ) : result && !busy && !error ? (
            <ThemedText themeColor="textSecondary" style={styles.empty}>
              No hits. Try other words, or widen the tiers.
            </ThemedText>
          ) : null
        }
        ListFooterComponent={result ? <ThemedText type="small" themeColor="textSecondary" style={styles.footer}>{result.total_rows} hits · {result.mode} · scope {result.priority_scope === 'all' ? 'all tiers' : result.selected_priorities.join(', ')}</ThemedText> : null}
      />
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  searchRow: { flexDirection: 'row', gap: Spacing.two, paddingHorizontal: Spacing.three, paddingTop: Spacing.two },
  inputBox: { flex: 1, flexDirection: 'row', alignItems: 'center', gap: Spacing.two, borderRadius: 22, borderWidth: StyleSheet.hairlineWidth, paddingHorizontal: 14 },
  input: { flex: 1, paddingVertical: 10, fontSize: 16 },
  filterLine: { flexDirection: 'row', justifyContent: 'space-between', gap: Spacing.two, paddingHorizontal: Spacing.three + 4, paddingVertical: Spacing.two },
  recentHead: { flexDirection: 'row', justifyContent: 'space-between', paddingHorizontal: Spacing.three, paddingTop: Spacing.two, paddingBottom: Spacing.one },
  recentRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.three, paddingHorizontal: Spacing.three, paddingVertical: 12, borderBottomWidth: StyleSheet.hairlineWidth },
  recentText: { flex: 1 },
  hint: { paddingHorizontal: Spacing.three, paddingBottom: Spacing.one },
  spinner: { paddingVertical: Spacing.two },
  empty: { textAlign: 'center', padding: Spacing.five },
  footer: { textAlign: 'center', padding: Spacing.three },
  error: { color: '#E5636D', paddingHorizontal: Spacing.three },
});
