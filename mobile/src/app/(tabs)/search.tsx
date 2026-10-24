import { useRouter } from 'expo-router';
import { useCallback, useRef, useState } from 'react';
import { ActivityIndicator, FlatList, Keyboard, Pressable, StyleSheet, TextInput, View } from 'react-native';

import { InboxRow, SplitChips } from '@/components/inbox-row';
import { PRIORITY_COLORS } from '@/components/priority-badge';
import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { effectiveMobileSearchPriorities, MOBILE_DEFAULT_SEARCH_PRIORITIES, PRIORITIES, search, splitRef, type Priority, type SearchHit, type SearchMode, type SearchResult } from '@/lib/api';
import { formatWhen, humanSource, truncate } from '@/lib/format';
import { useConfig } from '@/lib/session';

const MODES: SearchMode[] = ['hybrid', 'keyword', 'exact'];
export default function SearchScreen() {
  const config = useConfig();
  const theme = useTheme();
  const router = useRouter();
  const [query, setQuery] = useState('');
  const [mode, setMode] = useState<SearchMode>('hybrid');
  const [tiers, setTiers] = useState<Priority[]>(MOBILE_DEFAULT_SEARCH_PRIORITIES);
  const [result, setResult] = useState<SearchResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const generation = useRef(0);

  const run = useCallback(
    async (override?: { mode?: SearchMode; tiers?: Priority[] }) => {
      const q = query.trim();
      if (!q) return;
      Keyboard.dismiss();
      const gen = ++generation.current;
      setBusy(true);
      setError(null);
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
    if (result) void run({ tiers: ordered });
  };
  const pickMode = (m: SearchMode) => {
    setMode(m);
    if (result) void run({ mode: m });
  };

  const open = (hit: SearchHit) => {
    const parts = splitRef(hit.ref);
    if (!parts) return;
    router.push({ pathname: '/timeline/[adapter]/[eventId]', params: { adapter: parts.adapter, eventId: parts.eventId } });
  };

  return (
    <ThemedView style={styles.container}>
      <View style={styles.searchRow}>
        <TextInput
          style={[styles.input, { backgroundColor: theme.surface, borderColor: theme.surfaceBorder, color: theme.text }]}
          placeholder="Words the answer would contain…"
          placeholderTextColor={theme.textSecondary}
          value={query}
          onChangeText={setQuery}
          onSubmitEditing={() => run()}
          returnKeyType="search"
          autoCapitalize="none"
          autoCorrect={false}
          clearButtonMode="while-editing"
        />
        <Pressable onPress={() => run()} disabled={busy || !query.trim()} style={[styles.go, { backgroundColor: theme.text }, (busy || !query.trim()) && styles.disabled]}>
          <ThemedText style={[styles.goText, { color: theme.background }]}>Search</ThemedText>
        </Pressable>
      </View>
      <SplitChips items={MODES} selected={[mode]} onToggle={pickMode} />
      <SplitChips items={PRIORITIES} selected={tiers} onToggle={toggleTier} dotColor={(tier) => PRIORITY_COLORS[tier]} />
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
            onPress={() => open(item)}
          />
        )}
        ListEmptyComponent={
          result && !busy && !error ? (
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
  input: { flex: 1, borderRadius: 22, borderWidth: StyleSheet.hairlineWidth, paddingHorizontal: 18, paddingVertical: 10, fontSize: 16 },
  go: { borderRadius: 22, paddingHorizontal: 16, justifyContent: 'center' },
  goText: { fontWeight: '600' },
  disabled: { opacity: 0.5 },
  hint: { paddingHorizontal: Spacing.three, paddingBottom: Spacing.one },
  spinner: { paddingVertical: Spacing.two },
  empty: { textAlign: 'center', padding: Spacing.five },
  footer: { textAlign: 'center', padding: Spacing.three },
  error: { color: '#D0342C', paddingHorizontal: Spacing.three },
});
