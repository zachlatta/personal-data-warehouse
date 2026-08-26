import { Image } from 'expo-image';
import { Stack, useLocalSearchParams } from 'expo-router';
import { useEffect, useState } from 'react';
import { ActivityIndicator, ScrollView, StyleSheet, View } from 'react-native';

import { PriorityBadge } from '@/components/priority-badge';
import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { getTimelineItem, type TimelineItemDetail } from '@/lib/api';
import { cleanSnippet, formatWhen, humanSource, pretty } from '@/lib/format';
import { useConfig } from '@/lib/session';

const HIDDEN_SOURCE_KEYS = new Set(['raw_json', 'raw', 'body_html', 'search_text']);

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

export default function TimelineItemScreen() {
  const { adapter, eventId } = useLocalSearchParams<{ adapter: string; eventId: string }>();
  const config = useConfig();
  const theme = useTheme();
  const [detail, setDetail] = useState<TimelineItemDetail | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!adapter || !eventId) return;
    getTimelineItem(config, adapter, eventId)
      .then(setDetail)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, [config, adapter, eventId]);

  if (error) {
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
        {media?.media_kind === 'image' && media.media_url ? <Image source={{ uri: media.media_url }} style={styles.image} contentFit="contain" /> : null}
        {item.snippet ? (
          <View style={[styles.card, { backgroundColor: theme.backgroundElement }]}>
            <ThemedText>{cleanSnippet(item.snippet)}</ThemedText>
          </View>
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
});
