import { useFocusEffect } from 'expo-router';
import { useCallback, useState } from 'react';
import { FlatList, Image, Pressable, RefreshControl, StyleSheet, Switch, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { fetchNotificationLedger, setNotificationsEnabled, type NotificationEvent, type NotificationLedger } from '@/lib/api';
import { formatWhen, humanSource } from '@/lib/format';
import { notificationOutcome, summarizeNotifications, type NotificationOutcome } from '@/lib/notifications';
import { useConfig } from '@/lib/session';

const PAGE = 50;

const TONE_COLORS: Record<NotificationOutcome['tone'], string> = {
  good: '#16A34A',
  neutral: '#2563EB',
  muted: '#6B7280',
  bad: '#DC2626',
};

function OutcomePill({ outcome }: { outcome: NotificationOutcome }) {
  const color = TONE_COLORS[outcome.tone];
  return (
    <View style={[styles.pill, { borderColor: color, backgroundColor: `${color}22` }]}>
      <ThemedText style={[styles.pillText, { color }]}>{outcome.label}</ThemedText>
    </View>
  );
}

function deliveryLine(event: NotificationEvent): string {
  const parts = [`${event.accepted}/${event.devices} delivered`, `${event.opened} opened`];
  if (event.suppressed_read) parts.push(`${event.suppressed_read} skipped: read`);
  if (event.suppressed_replied) parts.push(`${event.suppressed_replied} skipped: replied`);
  if (event.failed) parts.push(`${event.failed} failed`);
  return parts.join(' · ');
}

export default function NotificationsScreen() {
  const config = useConfig();
  const theme = useTheme();
  const [ledger, setLedger] = useState<NotificationLedger | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setRefreshing(true);
    try {
      setLedger(await fetchNotificationLedger(config, { limit: PAGE }));
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRefreshing(false);
    }
  }, [config]);

  useFocusEffect(
    useCallback(() => {
      void load();
      const timer = setInterval(() => void load(), 15000);
      return () => clearInterval(timer);
    }, [load]),
  );

  const loadMore = async () => {
    if (!ledger?.has_more || !ledger.next_cursor || loadingMore) return;
    setLoadingMore(true);
    try {
      const next = await fetchNotificationLedger(config, { limit: PAGE, before: ledger.next_cursor });
      setLedger({ ...next, events: [...ledger.events, ...next.events] });
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoadingMore(false);
    }
  };

  // The switch is global: it pauses delivery to every device, not only this
  // one, and cancels anything still queued. It is optimistic so the toggle
  // moves under the thumb; the ledger refetch is what makes it true.
  const toggle = async (enabled: boolean) => {
    if (!ledger) return;
    setSaving(true);
    setLedger({ ...ledger, enabled });
    try {
      await setNotificationsEnabled(config, enabled);
      await load();
    } catch (e) {
      setLedger({ ...ledger, enabled: !enabled });
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const summary = summarizeNotifications(ledger?.events ?? []);
  const card = [styles.card, { backgroundColor: theme.backgroundElement }];

  const header = (
    <View style={styles.header}>
      {error ? (
        <Pressable onPress={load} style={styles.errorBox}>
          <ThemedText style={styles.errorText}>{error} — tap to retry</ThemedText>
        </Pressable>
      ) : null}
      <View style={card}>
        <View style={styles.switchRow}>
          <View style={styles.switchWords}>
            <ThemedText type="smallBold">Timeline notifications</ThemedText>
            <ThemedText type="small" themeColor="textSecondary">
              {ledger ? (ledger.enabled ? 'On for every device' : 'Off — nothing is sent to any device') : 'Loading…'}
            </ThemedText>
          </View>
          <Switch value={ledger?.enabled ?? false} disabled={!ledger || saving} onValueChange={(value) => void toggle(value)} />
        </View>
        <ThemedText type="small" themeColor="textSecondary">
          Every new direct or CC event, with its source link. Turning this off pauses delivery everywhere and cancels anything still queued.
          {ledger ? ` Worker: ${ledger.status}${ledger.error ? ` · ${ledger.error}` : ''}.` : ''}
        </ThemedText>
      </View>
      <View style={card}>
        <ThemedText type="smallBold" themeColor="textSecondary">
          Last {summary.total} notification{summary.total === 1 ? '' : 's'}
        </ThemedText>
        <View style={styles.stats}>
          <Stat label="delivered" value={summary.delivered} />
          <Stat label="opened" value={summary.opened} />
          <Stat label="skipped" value={summary.skipped} />
          <Stat label="failed" value={summary.failed} />
          <Stat label="open rate" value={summary.openRate === null ? '—' : `${Math.round(summary.openRate * 100)}%`} />
        </View>
        <ThemedText type="small" themeColor="textSecondary">
          Delivered means the push service accepted it, not that it appeared on screen. No observed open does not mean ignored.
        </ThemedText>
      </View>
    </View>
  );

  return (
    <ThemedView style={styles.container}>
      <FlatList
        data={ledger?.events ?? []}
        keyExtractor={(item) => item.id}
        refreshControl={<RefreshControl refreshing={refreshing && !loadingMore} onRefresh={load} />}
        ListHeaderComponent={header}
        ListEmptyComponent={
          ledger && !refreshing ? (
            <ThemedText type="small" themeColor="textSecondary" style={styles.empty}>
              {ledger.enabled ? 'Nothing sent yet.' : 'Nothing sent yet. Turn notifications on to start.'}
            </ThemedText>
          ) : null
        }
        ListFooterComponent={
          ledger?.has_more ? (
            <Pressable onPress={() => void loadMore()} disabled={loadingMore} style={[styles.more, loadingMore && styles.disabled]}>
              <ThemedText type="smallBold">{loadingMore ? 'Loading…' : 'Load older'}</ThemedText>
            </Pressable>
          ) : null
        }
        onEndReached={() => void loadMore()}
        onEndReachedThreshold={0.4}
        renderItem={({ item }) => {
          const preview = item.preview ?? {};
          const headline = preview.title || item.actor || item.title || 'New item';
          const detail = preview.subtitle || (preview.title ? item.title : '');
          return (
            <View style={[styles.row, { borderBottomColor: theme.backgroundElement }]}>
              <View style={styles.rowHeader}>
                <OutcomePill outcome={notificationOutcome(item)} />
                <ThemedText type="small" themeColor="textSecondary">
                  {formatWhen(item.created_at)}
                </ThemedText>
              </View>
              <View style={styles.rowBody}>
                {preview.icon ? <Image source={{ uri: preview.icon }} style={styles.icon} /> : null}
                <View style={styles.rowWords}>
                  <ThemedText type="smallBold" numberOfLines={1}>
                    {headline}
                  </ThemedText>
                  {detail ? (
                    <ThemedText type="small" numberOfLines={1}>
                      {detail}
                    </ThemedText>
                  ) : null}
                  <ThemedText type="small" themeColor="textSecondary" numberOfLines={2}>
                    {preview.body ?? item.body}
                  </ThemedText>
                </View>
              </View>
              <ThemedText type="small" themeColor="textSecondary">
                {humanSource(item.source)} · {item.priority} · {deliveryLine(item)}
              </ThemedText>
            </View>
          );
        }}
      />
    </ThemedView>
  );
}

function Stat({ label, value }: { label: string; value: number | string }) {
  return (
    <View style={styles.stat}>
      <ThemedText type="smallBold">{value}</ThemedText>
      <ThemedText type="small" themeColor="textSecondary">
        {label}
      </ThemedText>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  header: { padding: Spacing.three, gap: Spacing.three },
  card: { borderRadius: 12, padding: Spacing.three, gap: Spacing.two },
  switchRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two },
  switchWords: { flex: 1, gap: 2 },
  stats: { flexDirection: 'row', flexWrap: 'wrap', gap: Spacing.three },
  stat: { alignItems: 'flex-start' },
  row: { paddingHorizontal: Spacing.three, paddingVertical: 10, gap: 4, borderBottomWidth: StyleSheet.hairlineWidth },
  rowHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  rowBody: { flexDirection: 'row', gap: Spacing.two, alignItems: 'flex-start' },
  rowWords: { flex: 1, gap: 2 },
  icon: { width: 36, height: 36, borderRadius: 8 },
  pill: { borderRadius: 6, borderWidth: 1, paddingHorizontal: 6, paddingVertical: 1, alignSelf: 'flex-start' },
  pillText: { fontSize: 11, fontWeight: '600', lineHeight: 14 },
  empty: { paddingHorizontal: Spacing.three, paddingBottom: Spacing.three },
  more: { margin: Spacing.three, borderRadius: 10, paddingVertical: 10, alignItems: 'center', borderWidth: 1, borderColor: '#208AEF' },
  disabled: { opacity: 0.6 },
  errorBox: { padding: Spacing.three, borderRadius: 10, backgroundColor: '#D0342C22' },
  errorText: { color: '#D0342C' },
});
