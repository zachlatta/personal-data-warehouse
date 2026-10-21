import { useFocusEffect, useRouter } from 'expo-router';
import { SymbolView } from 'expo-symbols';
import * as WebBrowser from 'expo-web-browser';
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
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
  ScrollView,
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
import { sourceBubbles } from './rich';
import { ACCENT, SourceAvatar, type MediaAction } from './rich-views';
import { loadSaved, toggleSaved, useSavedIds } from './saved';
import { mailtoUrl, shareDraft } from './share';
import { layoutsFromHeights, ViewabilityTracker, type Impression } from './viewability';

// Reload on return to the tab after this long away.
const STALE_MS = 10 * 60_000;
const UNDO_MS = 5_000;
// The rail's Saved bubble: his read-later queue instead of one source.
const SAVED = '\u0000saved';

type Undo = { text: string; undo: () => void; timer: ReturnType<typeof setTimeout> };

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
  // The rail across the top narrows the feed to one source.
  const [only, setOnly] = useState<string | null>(null);
  const bubbles = useMemo(() => sourceBubbles(items), [items]);
  const savedIds = useSavedIds();
  const shown = useMemo(() => {
    if (only === SAVED) return savedIds.map((id) => feedItems.get(id)).filter((i): i is FeedItem => !!i);
    return only ? items.filter((i) => i.source === only) : items;
  }, [items, only, savedIds]);

  const tracker = useRef(new ViewabilityTracker());
  const heights = useRef(new Map<string, number>());
  const viewport = useRef({ top: 0, height: 0 });
  const ids = useRef<string[]>([]);
  const focused = useRef(false);
  const loadedAt = useRef(0);
  // The error banner sits above the first card when it is showing.
  const headerHeight = useRef(0);

  useLayoutEffect(() => {
    ids.current = shown.map((i) => i.item_id);
  }, [shown]);

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
      // Send taps first, so the queue the server answers with includes them.
      await flushFeedEvents(true).catch(() => {});
      const page = await fetchFeed(config);
      pause();
      for (const item of page.items) feedItems.set(item.item_id, item);
      loadSaved(page.saved ?? []);
      setItems(page.items);
      setError(null);
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
  }, [shown, measure]);

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

  // A tap opens the card's detail view (the whole post, every photo, the
  // summary and all its links); the compass goes straight to the page.
  const onOpen = useCallback(
    (item: FeedItem) => router.push({ pathname: '/feed/[id]', params: { id: item.item_id, position: String(position(item)) } }),
    [router],
  );

  // A link inside a post (index -1) or one of the card's links.
  const onBrowse = useCallback(
    (item: FeedItem, link?: CardLink) => void openInBrowser(item, link ? (link.index < 0 ? 'post_link' : 'link') : 'card', link),
    [openInBrowser],
  );

  // A carousel swipe or a video play. A video plays in the system player;
  // its watch time is recorded like reading time.
  const onMedia = useCallback(
    async (item: FeedItem, a: MediaAction) => {
      recordFeedEvent('media', { item_id: item.item_id, position: position(item), metadata: { action: a.action, index: a.index } });
      if (a.action !== 'play' || !a.url) return;
      pause();
      const startedAt = Date.now();
      try {
        await WebBrowser.openBrowserAsync(a.url, { dismissButtonStyle: 'close' });
      } finally {
        recordFeedEvent('close', { item_id: item.item_id, dwell_ms: Date.now() - startedAt, metadata: { mode: 'video', index: a.index } });
        measure();
      }
    },
    [measure, pause],
  );

  // A swipe opens a draft of the card in the default mail app for him to
  // address.
  const onShare = useCallback(async (item: FeedItem, preview: string) => {
    try {
      await Linking.openURL(mailtoUrl(shareDraft(item, preview)));
    } catch {
      Alert.alert('Could not open a mail app', 'No mail app is set up on this phone.');
      return;
    }
    recordFeedEvent('share', { item_id: item.item_id, position: position(item), metadata: { via: 'mailto', url: item.url ?? '' } });
  }, []);

  const onExpand = useCallback((item: FeedItem) => {
    recordFeedEvent('expand', { item_id: item.item_id, position: position(item) });
  }, []);

  const showUndo = useCallback((text: string, undoIt: () => void) => {
    setUndo((prev) => {
      if (prev) clearTimeout(prev.timer);
      return { text, undo: undoIt, timer: setTimeout(() => setUndo(null), UNDO_MS) };
    });
  }, []);

  const dismiss = useCallback(
    (item: FeedItem, reason: string) => {
      const index = position(item);
      recordFeedEvent('dismiss', { item_id: item.item_id, position: index, metadata: { reason, source: item.source, site: item.site ?? '' } });
      LayoutAnimation.configureNext(LayoutAnimation.Presets.easeInEaseOut);
      setItems((prev) => prev.filter((i) => i.item_id !== item.item_id));
      showUndo('Hidden', () => {
        recordFeedEvent('undismiss', { item_id: item.item_id, position: index });
        LayoutAnimation.configureNext(LayoutAnimation.Presets.easeInEaseOut);
        setItems((prev) => {
          const next = [...prev];
          next.splice(Math.min(index, next.length), 0, item);
          return next;
        });
      });
    },
    [showUndo],
  );

  // The bookmark, a swipe right or the menu: in or out of the read-later
  // queue (the Saved bubble).
  const onSave = useCallback(
    (item: FeedItem, via: string) => {
      if (only === SAVED) LayoutAnimation.configureNext(LayoutAnimation.Presets.easeInEaseOut);
      const saved = toggleSaved(item, { position: position(item), via });
      showUndo(saved ? 'Saved for later' : 'Removed from Saved', () => {
        LayoutAnimation.configureNext(LayoutAnimation.Presets.easeInEaseOut);
        toggleSaved(item, { position: position(item), via: 'undo' });
      });
    },
    [only, showUndo],
  );

  const runUndo = useCallback(() => {
    if (!undo) return;
    clearTimeout(undo.timer);
    undo.undo();
    setUndo(null);
  }, [undo]);

  const onMenu = useCallback(
    (item: FeedItem) => {
      const from = item.site || item.source;
      const saveLabel = savedIds.includes(item.item_id) ? 'Remove from Saved' : 'Save for later';
      const options = [saveLabel, 'Not interested', `Less from ${from}`, 'Cancel'];
      const pick = (i: number) => {
        if (i === 0) onSave(item, 'menu');
        if (i === 1) dismiss(item, 'not_interested');
        if (i === 2) dismiss(item, 'less_like_this');
      };
      if (Platform.OS === 'ios') {
        ActionSheetIOS.showActionSheetWithOptions({ options, cancelButtonIndex: 3, destructiveButtonIndex: 1, title: item.title || undefined }, pick);
      } else {
        Alert.alert(item.title || 'This card', undefined, [
          { text: options[0], onPress: () => pick(0) },
          { text: options[1], onPress: () => pick(1) },
          { text: options[2], onPress: () => pick(2) },
          { text: 'Cancel', style: 'cancel' },
        ]);
      }
    },
    [dismiss, onSave, savedIds],
  );

  const renderItem = useCallback(
    ({ item }: { item: FeedItem }) => (
      <FeedCard
        item={item}
        saved={savedIds.includes(item.item_id)}
        onSave={onSave}
        onLayout={onCardLayout}
        onOpen={onOpen}
        onBrowse={onBrowse}
        onMedia={onMedia}
        onExpand={onExpand}
        onMenu={onMenu}
        onShare={onShare}
      />
    ),
    [savedIds, onSave, onCardLayout, onOpen, onBrowse, onMedia, onExpand, onMenu, onShare],
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
        data={shown}
        extraData={savedIds}
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
          <View
            onLayout={(e) => {
              headerHeight.current = e.nativeEvent.layout.height;
              measure();
            }}>
            {bubbles.length > 1 || savedIds.length > 0 || only === SAVED ? (
              <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.rail}>
                {savedIds.length > 0 || only === SAVED ? (
                  <Pressable
                    style={[styles.bubble, only !== null && only !== SAVED && styles.bubbleDim]}
                    onPress={() => {
                      pause();
                      setOnly(only === SAVED ? null : SAVED);
                    }}
                    accessibilityLabel={`Saved for later, ${savedIds.length}`}>
                    <View style={[styles.savedBubble, only === SAVED && styles.savedBubbleOn]}>
                      <SymbolView name="bookmark.fill" tintColor="#fff" size={24} />
                      {savedIds.length ? (
                        <View style={styles.savedCount}>
                          <ThemedText style={styles.savedCountText}>{savedIds.length}</ThemedText>
                        </View>
                      ) : null}
                    </View>
                    <ThemedText style={[styles.bubbleLabel, only === SAVED && styles.bubbleLabelOn]} numberOfLines={1}>
                      Saved
                    </ThemedText>
                  </Pressable>
                ) : null}
                {[{ source: '', label: 'All', unseen: 0, total: items.length }, ...bubbles].map((b) => {
                  const active = (only ?? '') === b.source;
                  return (
                    <Pressable
                      key={b.source || '*'}
                      style={[styles.bubble, !active && only !== null && styles.bubbleDim]}
                      onPress={() => {
                        pause();
                        setOnly(b.source && !active ? b.source : null);
                      }}
                      accessibilityLabel={b.source ? `Only ${b.label}` : 'Everything'}>
                      <SourceAvatar name={b.source ? b.label : '*'} size={58} ring={b.unseen > 0 || active} />
                      <ThemedText style={[styles.bubbleLabel, active && styles.bubbleLabelOn]} numberOfLines={1}>
                        {b.label}
                      </ThemedText>
                    </Pressable>
                  );
                })}
              </ScrollView>
            ) : null}
            {error ? (
              <View style={[styles.banner, { backgroundColor: theme.backgroundElement }]}>
                <ThemedText style={styles.bannerText}>Could not refresh: {error}</ThemedText>
              </View>
            ) : null}
          </View>
        }
        ListEmptyComponent={
          only === SAVED ? (
            <View style={styles.empty}>
              <ThemedText style={styles.emptyTitle}>Nothing saved</ThemedText>
              <ThemedText style={[styles.emptyBody, { color: theme.textSecondary }]}>
                Tap the bookmark on a card, or swipe it right, to keep it here until you get to it.
              </ThemedText>
            </View>
          ) : (
          <View style={styles.empty}>
            <ThemedText style={styles.emptyTitle}>{disabled ? 'The feed is turned off' : 'You are all caught up'}</ThemedText>
            <ThemedText style={[styles.emptyBody, { color: theme.textSecondary }]}>
              {disabled
                ? 'This PDW server does not have the Feed module on (PDW_FEED_ENABLED).'
                : 'Cards you have seen do not come back. New ones arrive every hour; pull down to refresh.'}
            </ThemedText>
          </View>
          )
        }
        ListFooterComponent={
          shown.length ? (
            <ThemedText style={[styles.footer, { color: theme.textSecondary }]}>That is everything for now.</ThemedText>
          ) : null
        }
      />
      {undo ? (
        <View style={[styles.undo, { backgroundColor: theme.text }]}>
          <ThemedText style={[styles.undoText, { color: theme.background }]} numberOfLines={1}>
            {undo.text}
          </ThemedText>
          <Pressable hitSlop={10} onPress={runUndo}>
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
  rail: { paddingHorizontal: Spacing.two, paddingTop: Spacing.two, paddingBottom: Spacing.two, gap: Spacing.two },
  bubble: { width: 70, alignItems: 'center', gap: 4 },
  bubbleDim: { opacity: 0.45 },
  bubbleLabel: { fontSize: 12, lineHeight: 15, maxWidth: 70 },
  bubbleLabelOn: { fontWeight: '700' },
  savedBubble: { width: 58, height: 58, borderRadius: 29, backgroundColor: '#FF9500', alignItems: 'center', justifyContent: 'center' },
  savedBubbleOn: { borderWidth: 3, borderColor: ACCENT },
  savedCount: {
    position: 'absolute',
    top: -2,
    right: -2,
    minWidth: 20,
    height: 20,
    borderRadius: 10,
    paddingHorizontal: 5,
    backgroundColor: ACCENT,
    alignItems: 'center',
    justifyContent: 'center',
  },
  savedCountText: { color: '#fff', fontSize: 12, fontWeight: '700', lineHeight: 15 },
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
