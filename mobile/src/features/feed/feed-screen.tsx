import { useFocusEffect, useRouter } from 'expo-router';
import * as WebBrowser from 'expo-web-browser';
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import {
  ActionSheetIOS,
  ActivityIndicator,
  Alert,
  AppState,
  FlatList,
  LayoutAnimation,
  Linking,
  Platform,
  Pressable,
  RefreshControl,
  StyleSheet,
  View,
  type NativeScrollEvent,
  type NativeSyntheticEvent,
} from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { useConfig } from '@/lib/session';

import { fetchFeed, FeedApiError, type FeedItem } from './api';
import {
  connectFeedLogger,
  endFeedSession,
  feedItems,
  flushFeedEvents,
  recordFeedEvent,
  startFeedSession,
} from './engagement';
import { FeedCard } from './feed-card';
import type { CardLink } from './links';
import { shareDraft, shareUrls } from './share';
import { layoutsFromHeights, ViewabilityTracker, type Impression } from './viewability';

// Reload on return to the tab after this long away.
const STALE_MS = 10 * 60_000;
const UNDO_MS = 5_000;

type Undo = { item: FeedItem; index: number; timer: ReturnType<typeof setTimeout> };

export default function FeedScreen() {
  const config = useConfig();
  const theme = useTheme();
  const router = useRouter();
  const [items, setItems] = useState<FeedItem[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [disabled, setDisabled] = useState(false);
  const [undo, setUndo] = useState<Undo | null>(null);

  const tracker = useRef(new ViewabilityTracker());
  const heights = useRef(new Map<string, number>());
  const viewport = useRef({ top: 0, height: 0 });
  const ids = useRef<string[]>([]);
  const focused = useRef(false);
  const loadedAt = useRef(0);
  // The error banner sits above the first card when it is showing.
  const headerHeight = useRef(0);

  useLayoutEffect(() => {
    ids.current = items.map((i) => i.item_id);
  }, [items]);

  const recordImpressions = useCallback((ended: Impression[]) => {
    for (const imp of ended) {
      recordFeedEvent('impression', {
        at: imp.startedAt,
        item_id: imp.itemId,
        position: imp.position,
        dwell_ms: imp.dwellMs,
        visible_fraction: imp.maxFraction,
        metadata: { visible_ms: imp.visibleMs },
      });
    }
  }, []);

  const measure = useCallback(() => {
    if (!focused.current || AppState.currentState !== 'active' || viewport.current.height === 0) return;
    const layouts = layoutsFromHeights(ids.current, heights.current, headerHeight.current);
    recordImpressions(tracker.current.update(Date.now(), viewport.current, layouts));
  }, [recordImpressions]);

  const pause = useCallback(() => {
    recordImpressions(tracker.current.flush(Date.now()));
  }, [recordImpressions]);

  const load = useCallback(async () => {
    setRefreshing(true);
    try {
      const page = await fetchFeed(config);
      pause();
      for (const item of page.items) feedItems.set(item.item_id, item);
      setItems(page.items);
      setError(null);
      headerHeight.current = 0;
      setDisabled(false);
      loadedAt.current = Date.now();
    } catch (e) {
      if (e instanceof FeedApiError && e.status === 404) setDisabled(true);
      else setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRefreshing(false);
      setLoaded(true);
    }
  }, [config, pause]);

  useEffect(() => {
    connectFeedLogger(config);
  }, [config]);

  // Re-measure once new cards have laid out.
  useEffect(() => {
    const t = setTimeout(measure, 50);
    return () => clearTimeout(t);
  }, [items, measure]);

  useFocusEffect(
    useCallback(() => {
      focused.current = true;
      if (AppState.currentState === 'active') startFeedSession(ids.current.length);
      if (Date.now() - loadedAt.current > STALE_MS) void load();
      else measure();
      return () => {
        focused.current = false;
        pause();
        endFeedSession();
      };
    }, [load, measure, pause]),
  );

  useEffect(() => {
    const sub = AppState.addEventListener('change', (state) => {
      if (!focused.current) return;
      if (state === 'active') {
        startFeedSession(ids.current.length);
        if (Date.now() - loadedAt.current > STALE_MS) void load();
        else measure();
      } else {
        pause();
        endFeedSession();
        void flushFeedEvents(true);
      }
    });
    return () => sub.remove();
  }, [load, measure, pause]);

  const onScroll = useCallback(
    (e: NativeSyntheticEvent<NativeScrollEvent>) => {
      viewport.current = { top: e.nativeEvent.contentOffset.y, height: e.nativeEvent.layoutMeasurement.height };
      measure();
    },
    [measure],
  );

  const onCardLayout = useCallback(
    (itemId: string, height: number) => {
      if (heights.current.get(itemId) === height) return;
      heights.current.set(itemId, height);
      measure();
    },
    [measure],
  );

  const position = (item: FeedItem) => ids.current.indexOf(item.item_id);

  // Opens the card's main url, or one of its extra links: the open and close
  // record which (url, link_index) so engagement is per link.
  const openInBrowser = useCallback(
    async (item: FeedItem, via: string, link?: CardLink) => {
      const url = link?.url ?? item.url;
      if (!url) return;
      const which = { url, link_index: link?.index ?? 0 };
      pause();
      const startedAt = Date.now();
      recordFeedEvent('open', { item_id: item.item_id, position: position(item), metadata: { mode: 'browser', via, ...which } });
      try {
        await WebBrowser.openBrowserAsync(url, { dismissButtonStyle: 'close', controlsColor: '#208AEF' });
      } finally {
        recordFeedEvent('close', {
          item_id: item.item_id,
          dwell_ms: Date.now() - startedAt,
          metadata: { mode: 'browser', via, ...which },
        });
        measure();
      }
    },
    [measure, pause],
  );

  const onOpen = useCallback(
    (item: FeedItem) => {
      if (item.url) void openInBrowser(item, 'card');
      else router.push({ pathname: '/feed/[id]', params: { id: item.item_id, position: String(position(item)) } });
    },
    [openInBrowser, router],
  );

  const onRead = useCallback(
    (item: FeedItem) => router.push({ pathname: '/feed/[id]', params: { id: item.item_id, position: String(position(item)) } }),
    [router],
  );

  const onOpenLink = useCallback(
    (item: FeedItem, link: CardLink) => void openInBrowser(item, 'link', link),
    [openInBrowser],
  );

  // A swipe opens a Superhuman draft of the card for him to address. An
  // unhandled scheme rejects, so each url falls through to the next.
  const onShare = useCallback(async (item: FeedItem, preview: string) => {
    const urls = shareUrls(shareDraft(item, preview));
    for (const url of urls) {
      try {
        await Linking.openURL(url);
        const via = url.startsWith('mailto:') ? 'mailto' : 'superhuman';
        recordFeedEvent('share', { item_id: item.item_id, position: position(item), metadata: { via, url: item.url ?? '' } });
        return;
      } catch {}
    }
    Alert.alert('Could not open a mail app', 'Superhuman and Mail are not available.');
  }, []);

  const onExpand = useCallback((item: FeedItem) => {
    recordFeedEvent('expand', { item_id: item.item_id, position: position(item) });
  }, []);

  const dismiss = useCallback(
    (item: FeedItem, reason: string) => {
      const index = position(item);
      recordFeedEvent('dismiss', { item_id: item.item_id, position: index, metadata: { reason, source: item.source, site: item.site ?? '' } });
      LayoutAnimation.configureNext(LayoutAnimation.Presets.easeInEaseOut);
      setItems((prev) => prev.filter((i) => i.item_id !== item.item_id));
      setUndo((prev) => {
        if (prev) clearTimeout(prev.timer);
        return { item, index, timer: setTimeout(() => setUndo(null), UNDO_MS) };
      });
    },
    [],
  );

  const undoDismiss = useCallback(() => {
    if (!undo) return;
    clearTimeout(undo.timer);
    recordFeedEvent('undismiss', { item_id: undo.item.item_id, position: undo.index });
    LayoutAnimation.configureNext(LayoutAnimation.Presets.easeInEaseOut);
    setItems((prev) => {
      const next = [...prev];
      next.splice(Math.min(undo.index, next.length), 0, undo.item);
      return next;
    });
    setUndo(null);
  }, [undo]);

  const onMenu = useCallback(
    (item: FeedItem) => {
      const from = item.site || item.source;
      const options = ['Not interested', `Less from ${from}`, 'Cancel'];
      const pick = (i: number) => {
        if (i === 0) dismiss(item, 'not_interested');
        if (i === 1) dismiss(item, 'less_like_this');
      };
      if (Platform.OS === 'ios') {
        ActionSheetIOS.showActionSheetWithOptions({ options, cancelButtonIndex: 2, destructiveButtonIndex: 0, title: item.title || undefined }, pick);
      } else {
        Alert.alert('Hide this card?', undefined, [
          { text: options[0], onPress: () => pick(0) },
          { text: options[1], onPress: () => pick(1) },
          { text: 'Cancel', style: 'cancel' },
        ]);
      }
    },
    [dismiss],
  );

  const renderItem = useCallback(
    ({ item }: { item: FeedItem }) => (
      <FeedCard
        item={item}
        onLayout={onCardLayout}
        onOpen={onOpen}
        onRead={onRead}
        onOpenLink={onOpenLink}
        onExpand={onExpand}
        onMenu={onMenu}
        onShare={onShare}
      />
    ),
    [onCardLayout, onOpen, onRead, onOpenLink, onExpand, onMenu, onShare],
  );

  if (!loaded) {
    return (
      <ThemedView style={styles.center}>
        <ActivityIndicator />
      </ThemedView>
    );
  }

  return (
    <ThemedView style={styles.fill}>
      <FlatList
        data={items}
        keyExtractor={(item) => item.item_id}
        renderItem={renderItem}
        onScroll={onScroll}
        scrollEventThrottle={16}
        onLayout={(e) => {
          viewport.current = { ...viewport.current, height: e.nativeEvent.layout.height };
          measure();
        }}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={() => void load()} />}
        windowSize={11}
        initialNumToRender={6}
        ListHeaderComponent={
          error ? (
            <View
              style={[styles.banner, { backgroundColor: theme.backgroundElement }]}
              onLayout={(e) => {
                const { height, y } = e.nativeEvent.layout;
                headerHeight.current = height + y * 2;
                measure();
              }}>
              <ThemedText style={styles.bannerText}>Could not refresh: {error}</ThemedText>
            </View>
          ) : null
        }
        ListEmptyComponent={
          <View style={styles.empty}>
            <ThemedText style={styles.emptyTitle}>{disabled ? 'The feed is turned off' : 'Nothing here yet'}</ThemedText>
            <ThemedText style={[styles.emptyBody, { color: theme.textSecondary }]}>
              {disabled
                ? 'This PDW server does not have the Feed module on (PDW_FEED_ENABLED).'
                : 'Agents add cards with `pdw feed push`. Pull down to refresh.'}
            </ThemedText>
          </View>
        }
        ListFooterComponent={
          items.length ? (
            <ThemedText style={[styles.footer, { color: theme.textSecondary }]}>That is everything for now.</ThemedText>
          ) : null
        }
      />
      {undo ? (
        <View style={[styles.undo, { backgroundColor: theme.text }]}>
          <ThemedText style={[styles.undoText, { color: theme.background }]} numberOfLines={1}>
            Hidden
          </ThemedText>
          <Pressable hitSlop={10} onPress={undoDismiss}>
            <ThemedText style={styles.undoAction}>Undo</ThemedText>
          </Pressable>
        </View>
      ) : null}
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  fill: { flex: 1 },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center' },
  banner: { margin: Spacing.three, padding: Spacing.three, borderRadius: 10 },
  bannerText: { fontSize: 14 },
  empty: { padding: Spacing.five, alignItems: 'center', gap: Spacing.two },
  emptyTitle: { fontSize: 18, fontWeight: '600' },
  emptyBody: { fontSize: 15, textAlign: 'center' },
  footer: { textAlign: 'center', padding: Spacing.five, fontSize: 14 },
  undo: {
    position: 'absolute',
    left: Spacing.three,
    right: Spacing.three,
    bottom: Spacing.three,
    borderRadius: 12,
    paddingHorizontal: Spacing.three,
    paddingVertical: Spacing.three,
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
  },
  undoText: { fontSize: 15, flex: 1 },
  undoAction: { fontSize: 15, fontWeight: '700', color: '#208AEF' },
});
