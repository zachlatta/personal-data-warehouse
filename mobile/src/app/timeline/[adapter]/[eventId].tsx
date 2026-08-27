import { Image } from 'expo-image';
import { Stack, useLocalSearchParams, useRouter } from 'expo-router';
import { useCallback, useEffect, useState } from 'react';
import { ActivityIndicator, Pressable, ScrollView, StyleSheet, View } from 'react-native';

import { OpenInSourceButton } from '@/components/open-in-source-button';
import { PriorityBadge } from '@/components/priority-badge';
import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import {
  getTimelineContext,
  getTimelineItem,
  TIMELINE_CONTEXT_MAX_WINDOW,
  type TimelineContextPage,
  type TimelineItem,
  type TimelineItemDetail,
} from '@/lib/api';
import { cleanSnippet, dayLabel, formatWhen, humanSource, pretty } from '@/lib/format';
import { useConfig } from '@/lib/session';

const HIDDEN_SOURCE_KEYS = new Set(['raw_json', 'raw', 'body_html', 'search_text']);
const CONTEXT_STEP = 15;

// Raw provider blobs (*_json) belong to SQL, not a phone screen.
function isHiddenKey(key: string): boolean {
  return HIDDEN_SOURCE_KEYS.has(key) || key.endsWith('_json');
}

// One line per child row: the values that read as text, in column order.
function compactRow(row: Record<string, unknown>): string {
  return Object.entries(row)
    .filter(([key, value]) => !isHiddenKey(key) && value !== null && value !== '' && value !== undefined)
    .map(([, value]) => (typeof value === 'string' ? value : pretty(value)))
    .join(' · ')
    .slice(0, 600);
}

function dayOf(iso: string): string {
  const date = new Date(iso);
  return Number.isNaN(date.getTime()) ? iso : date.toDateString();
}

// The conversation around the event: the surrounding channel/DM messages,
// the neighboring turns of a session, the same calendar's adjacent events.
// The anchor row is highlighted; every other row opens that event.
function ConversationCard({
  page,
  loading,
  onWiden,
  onOpen,
}: {
  page: TimelineContextPage;
  loading: boolean;
  onWiden: (side: 'before' | 'after') => void;
  onOpen: (item: TimelineItem) => void;
}) {
  const theme = useTheme();
  return (
    <View style={[styles.card, { backgroundColor: theme.backgroundElement }]}>
      <View style={styles.convoHeader}>
        <ThemedText type="smallBold" themeColor="textSecondary">
          conversation
        </ThemedText>
        <ThemedText type="small" themeColor="textSecondary">
          {page.items.length} events
        </ThemedText>
      </View>
      <Pressable
        onPress={() => onWiden('before')}
        disabled={loading || page.before >= TIMELINE_CONTEXT_MAX_WINDOW}
        style={({ pressed }) => [styles.widen, (pressed || loading) && styles.pressed]}>
        <ThemedText type="small" style={styles.widenText}>
          {page.before >= TIMELINE_CONTEXT_MAX_WINDOW ? 'earliest loaded' : '↑ earlier'}
        </ThemedText>
      </Pressable>
      {page.items.map((row, index) => {
        const showDay = index === 0 || dayOf(page.items[index - 1].event_ts) !== dayOf(row.event_ts);
        const text = cleanSnippet(row.snippet || (row.title ? '' : `(${row.kind.replace(/_/g, ' ')})`));
        return (
          <View key={`${row.adapter}|${row.event_id}`}>
            {showDay ? (
              <ThemedText type="small" themeColor="textSecondary" style={styles.convoDay}>
                {dayLabel(row.event_ts)}
              </ThemedText>
            ) : null}
            <Pressable
              onPress={() => (row.is_anchor ? undefined : onOpen(row))}
              disabled={!!row.is_anchor}
              style={({ pressed }) => [
                styles.convoRow,
                row.is_anchor && { backgroundColor: theme.backgroundSelected, borderLeftColor: '#208AEF' },
                pressed && !row.is_anchor && { backgroundColor: theme.backgroundSelected },
              ]}>
              <View style={styles.convoRowHeader}>
                <ThemedText type="smallBold" style={row.is_anchor ? { color: '#208AEF' } : undefined} numberOfLines={1}>
                  {row.actor || '—'}
                  {row.title && row.title !== row.snippet ? `  ·  ${row.title}` : ''}
                </ThemedText>
                <ThemedText type="small" themeColor="textSecondary">
                  {formatWhen(row.event_ts)}
                </ThemedText>
              </View>
              {text ? <ThemedText selectable>{text}</ThemedText> : null}
            </Pressable>
          </View>
        );
      })}
      <Pressable
        onPress={() => onWiden('after')}
        disabled={loading || page.after >= TIMELINE_CONTEXT_MAX_WINDOW}
        style={({ pressed }) => [styles.widen, (pressed || loading) && styles.pressed]}>
        <ThemedText type="small" style={styles.widenText}>
          {page.after >= TIMELINE_CONTEXT_MAX_WINDOW ? 'latest loaded' : '↓ later'}
        </ThemedText>
      </Pressable>
      {loading ? <ActivityIndicator /> : null}
    </View>
  );
}

export default function TimelineItemScreen() {
  const { adapter, eventId } = useLocalSearchParams<{ adapter: string; eventId: string }>();
  const config = useConfig();
  const theme = useTheme();
  const router = useRouter();
  const [detail, setDetail] = useState<TimelineItemDetail | null>(null);
  const [context, setContext] = useState<TimelineContextPage | null>(null);
  const [contextLoading, setContextLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!adapter || !eventId) return;
    // Each push mounts a fresh screen, so there is no stale detail to clear.
    getTimelineItem(config, adapter, eventId)
      .then((d) => {
        setDetail(d);
        setContext(d.context ?? null);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, [config, adapter, eventId]);

  const widen = useCallback(
    (side: 'before' | 'after') => {
      if (!adapter || !eventId || !context) return;
      const next = {
        before: side === 'before' ? Math.min(TIMELINE_CONTEXT_MAX_WINDOW, context.before + CONTEXT_STEP) : context.before,
        after: side === 'after' ? Math.min(TIMELINE_CONTEXT_MAX_WINDOW, context.after + CONTEXT_STEP) : context.after,
      };
      setContextLoading(true);
      getTimelineContext(config, adapter, eventId, next)
        .then(setContext)
        .catch((e) => setError(e instanceof Error ? e.message : String(e)))
        .finally(() => setContextLoading(false));
    },
    [config, adapter, eventId, context],
  );

  const openItem = useCallback(
    (item: TimelineItem) => router.push({ pathname: '/timeline/[adapter]/[eventId]', params: { adapter: item.adapter, eventId: item.event_id } }),
    [router],
  );

  if (error && !detail) {
    return (
      <ThemedView style={styles.center}>
        <ThemedText style={{ color: '#D0342C' }}>{error}</ThemedText>
      </ThemedView>
    );
  }
  if (!detail) {
    return (
      <ThemedView style={styles.center}>
        <ActivityIndicator />
      </ThemedView>
    );
  }
  const item = detail.item;
  const sourceRow = (detail.source_row ?? null) as Record<string, unknown> | null;
  const media = detail.item_media ?? null;
  const children = Object.entries(detail.children ?? {}).filter(([, rows]) => Array.isArray(rows) ? rows.length > 0 : true);
  const rows = sourceRow
    ? Object.entries(sourceRow).filter(([key, value]) => !isHiddenKey(key) && value !== null && value !== '' && value !== undefined)
    : [];

  return (
    <ThemedView style={styles.container}>
      <Stack.Screen options={{ title: humanSource(item.source) }} />
      <ScrollView contentContainerStyle={styles.content}>
        <View style={styles.headerRow}>
          <PriorityBadge priority={item.priority} />
          <ThemedText type="small" themeColor="textSecondary">
            {item.kind.replace(/_/g, ' ')} · {formatWhen(item.event_ts)}
          </ThemedText>
        </View>
        <ThemedText type="subtitle">{item.title || item.context || '(untitled)'}</ThemedText>
        {item.actor ? <ThemedText themeColor="textSecondary">{item.actor}</ThemedText> : null}
        {item.context && item.title ? (
          <ThemedText type="small" themeColor="textSecondary">
            {item.context}
          </ThemedText>
        ) : null}
        <OpenInSourceButton link={item.open} />
        {media?.media_kind === 'image' && media.media_url ? <Image source={{ uri: media.media_url }} style={styles.image} contentFit="contain" /> : null}
        {item.snippet ? (
          <View style={[styles.card, { backgroundColor: theme.backgroundElement }]}>
            <ThemedText selectable>{cleanSnippet(item.snippet)}</ThemedText>
          </View>
        ) : null}
        {error ? (
          <ThemedText type="small" style={{ color: '#D0342C' }}>
            {error}
          </ThemedText>
        ) : null}
        {context && context.items.length > 1 ? <ConversationCard page={context} loading={contextLoading} onWiden={widen} onOpen={openItem} /> : null}
        {detail.context_error ? (
          <ThemedText type="small" style={{ color: '#D0342C' }}>
            {detail.context_error}
          </ThemedText>
        ) : null}
        {detail.source_row_error ? (
          <ThemedText type="small" style={{ color: '#D0342C' }}>
            {detail.source_row_error}
          </ThemedText>
        ) : null}
        {children.map(([name, childRows]) => (
          <View key={name} style={[styles.card, { backgroundColor: theme.backgroundElement }]}>
            <ThemedText type="smallBold" themeColor="textSecondary" style={styles.cardTitle}>
              {name.replace(/_/g, ' ')}
              {Array.isArray(childRows) ? ` (${childRows.length}${detail.children_meta?.[name]?.has_more ? '+' : ''})` : ''}
            </ThemedText>
            {Array.isArray(childRows) ? (
              childRows.slice(0, 50).map((row, index) => (
                <ThemedText key={index} type="small" selectable>
                  {compactRow(row)}
                </ThemedText>
              ))
            ) : (
              <ThemedText type="small" style={{ color: '#D0342C' }}>
                {childRows.error}
              </ThemedText>
            )}
          </View>
        ))}
        {rows.length > 0 ? (
          <View style={[styles.card, { backgroundColor: theme.backgroundElement }]}>
            <ThemedText type="smallBold" themeColor="textSecondary" style={styles.cardTitle}>
              {item.source_table}
            </ThemedText>
            {rows.map(([key, value]) => (
              <View key={key} style={styles.field}>
                <ThemedText type="small" themeColor="textSecondary">
                  {key}
                </ThemedText>
                <ThemedText type="small" selectable>
                  {pretty(value)}
                </ThemedText>
              </View>
            ))}
          </View>
        ) : null}
      </ScrollView>
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center', padding: Spacing.four },
  content: { padding: Spacing.three, gap: Spacing.three },
  headerRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two },
  image: { width: '100%', height: 280, borderRadius: 12 },
  card: { borderRadius: 12, padding: Spacing.three, gap: Spacing.two },
  cardTitle: { marginBottom: Spacing.one },
  field: { gap: 2 },
  convoHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'baseline' },
  convoDay: { marginTop: Spacing.two, textTransform: 'uppercase', letterSpacing: 1, fontSize: 12 },
  convoRow: { borderRadius: 8, paddingVertical: 6, paddingHorizontal: 8, gap: 2, borderLeftWidth: 3, borderLeftColor: 'transparent' },
  convoRowHeader: { flexDirection: 'row', justifyContent: 'space-between', gap: Spacing.two },
  widen: { alignSelf: 'center', paddingVertical: 6, paddingHorizontal: 12 },
  widenText: { color: '#208AEF', fontWeight: '600' },
  pressed: { opacity: 0.5 },
});
