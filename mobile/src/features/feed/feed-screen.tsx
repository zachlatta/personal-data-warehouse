import { useFocusEffect, useNavigation, useRouter } from 'expo-router';
import * as WebBrowser from 'expo-web-browser';
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import {
  ActivityIndicator,
  Alert,
  AppState,
  FlatList,
  LayoutAnimation,
  Keyboard,
  Linking,
  Pressable,
  RefreshControl,
  ScrollView,
  StyleSheet,
  TextInput,
  View,
  type NativeScrollEvent,
  type NativeSyntheticEvent,
} from 'react-native';

import { ActionSheet, showActionSheet, type ActionSheetRequest } from '@/components/action-sheet';
import { Icon } from '@/components/icon';
import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { useConfig } from '@/lib/session';

import { fetchFeed, FeedApiError, searchFeed, type FeedItem } from './api';
import { newArrivals } from './card-context';
import {
  activeNow,
  connectFeedLogger,
  endFeedSession,
  feedItems,
  flushFeedEvents,
  markActive,
  recordFeedEvent,
  recordUi,
  startFeedSession,
  startTimer,
} from './engagement';
import { FeedCard } from './feed-card';
import type { CardLink } from './links';
import { sourceBubbles } from './rich';
import { ACCENT, SourceAvatar, type MediaAction } from './rich-views';
import { loadLiked, toggleLiked, useLikedIds } from './liked';
import { loadSaved, toggleSaved, useSavedIds } from './saved';
import { ScrollGestureTracker, scrollMetadata } from './scroll-gestures';
import { mailtoUrl, shareDraft } from './share';
import { autoplayStep } from './autoplay';
import { openTrackedBrowser } from './tracked-browser';
import { autoplayKey } from './video';
import { autoplayVideo, playingVideo, VideoWarmup } from './video-player';
import { layoutsFromHeights, ViewabilityTracker, visibleFraction, type Impression } from './viewability';

// Reload on return to the tab after this long away.
const STALE_MS = 10 * 60_000;
const UNDO_MS = 5_000;
// The rail's Saved bubble: his read-later queue instead of one source.
const SAVED = '\u0000saved';
// A drag let go with no fling has ended if no coast began by then.
const SETTLE_MS = 80;
// The new-cards pill shows only when he is this far down, and goes at the top.
const PILL_FROM_Y = 400;
const PILL_GONE_Y = 120;

// The served order, kept under the server's 16 KB metadata cap (one
// oversized event would get its whole batch refused).
const SERVED_BYTES = 12_000;
function servedIds(items: FeedItem[]): { item_ids: string[]; item_ids_truncated: boolean } {
  const ids: string[] = [];
  let bytes = 0;
  for (const item of items) {
    bytes += item.item_id.length + 3;
    if (bytes > SERVED_BYTES) break;
    ids.push(item.item_id);
  }
  return { item_ids: ids, item_ids_truncated: ids.length < items.length };
}

// Wait this long after the last keystroke before searching.
const SEARCH_DEBOUNCE_MS = 250;

// What the list is showing, as the log names it.
function viewName(only: string | null, searching = false): string {
  if (searching) return 'search';
  return only === SAVED ? 'saved' : only ? `source:${only}` : 'all';
}

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
  const [sheet, setSheet] = useState<ActionSheetRequest | null>(null);
  // Cards a reload brought that were not in the list, for X's "new posts" pill.
  const [arrivals, setArrivals] = useState<FeedItem[]>([]);
  const list = useRef<FlatList<FeedItem>>(null);
  const loadedIds = useRef<string[]>([]);
  // The rail across the top narrows the feed to one source.
  const [only, setOnly] = useState<string | null>(null);
  const bubbles = useMemo(() => sourceBubbles(items), [items]);
  const savedIds = useSavedIds();
  const likedIds = useLikedIds();
  // The search box (Zach, voice memo 2026-10-24: "let me interactively
  // search through all my past entries"): while it is open the list is the
  // cards from his whole feed history that match what he has typed.
  const [searching, setSearching] = useState(false);
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<FeedItem[] | null>(null);
  const [searchBusy, setSearchBusy] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);
  const searchGeneration = useRef(0);
  // The last page the server sent, so search results can merge their likes in.
  const pageItems = useRef<FeedItem[]>([]);
  const shown = useMemo(() => {
    if (searching) return results ?? [];
    if (only === SAVED) return savedIds.map((id) => feedItems.get(id)).filter((i): i is FeedItem => !!i);
    return only ? items.filter((i) => i.source === only) : items;
  }, [items, only, savedIds, searching, results]);

  const tracker = useRef(new ViewabilityTracker());
  const gestures = useRef(new ScrollGestureTracker());
  const settleTimer = useRef<ReturnType<typeof setTimeout>>(undefined);
  const view = useRef('all');
  const endLogged = useRef(false);
  const heights = useRef(new Map<string, number>());
  const viewport = useRef({ top: 0, height: 0 });
  const ids = useRef<string[]>([]);
  // Cards with a video to autoplay (item id -> player key), and the one
  // that has the focus.
  const videoKeys = useRef(new Map<string, string>());
  const videoFocus = useRef<string | null>(null);
  const focused = useRef(false);
  const loadedAt = useRef(0);
  // The error banner sits above the first card when it is showing.
  const headerHeight = useRef(0);

  useLayoutEffect(() => {
    ids.current = shown.map((i) => i.item_id);
    videoKeys.current = new Map(
      shown.flatMap((i) => {
        const key = autoplayKey(i);
        return key ? [[i.item_id, key] as const] : [];
      }),
    );
    view.current = viewName(only, searching);
    endLogged.current = false;
  }, [shown, only, searching]);

  const recordImpressions = useCallback((ended: Impression[]) => {
    for (const imp of ended) {
      recordFeedEvent('impression', {
        at: imp.startedAt,
        item_id: imp.itemId,
        position: imp.position,
        dwell_ms: imp.dwellMs,
        visible_fraction: imp.maxFraction,
        metadata: { visible_ms: imp.visibleMs, ended_at: new Date(imp.endedAt).toISOString(), height: heights.current.get(imp.itemId) ?? 0, view: view.current },
      });
    }
  }, []);

  const measure = useCallback(() => {
    if (!focused.current || AppState.currentState !== 'active' || viewport.current.height === 0) return;
    const layouts = layoutsFromHeights(ids.current, heights.current, headerHeight.current);
    recordImpressions(tracker.current.update(Date.now(), viewport.current, layouts, activeNow()));
    const fractions = new Map([...layouts].map(([id, l]) => [id, visibleFraction(l, viewport.current)]));
    const step = autoplayStep(fractions, videoKeys.current, videoFocus.current, playingVideo());
    videoFocus.current = step.focused;
    if (step.play !== undefined) autoplayVideo(step.play);
  }, [recordImpressions]);

  // Leaving the screen stops the video (InlineVideo does); coming back
  // starts the one in view again.
  const pause = useCallback(() => {
    recordImpressions(tracker.current.flush(Date.now(), activeNow()));
    videoFocus.current = null;
  }, [recordImpressions]);

  // trigger: first (the tab's first load), stale (back after STALE_MS) or
  // pull (pull to refresh). The load event logs exactly what was served, in
  // order, so a later query can tell what he was shown but never reached.
  const load = useCallback(async (trigger: 'first' | 'stale' | 'pull') => {
    setRefreshing(true);
    const startedAt = Date.now();
    try {
      // Send taps first, so the queue the server answers with includes them.
      await flushFeedEvents(true).catch(() => {});
      const page = await fetchFeed(config);
      pause();
      recordFeedEvent('load', {
        at: startedAt,
        dwell_ms: Date.now() - startedAt,
        metadata: {
          trigger,
          ok: true,
          cards: page.items.length,
          saved: (page.saved ?? []).length,
          ...servedIds(page.items),
        },
      });
      for (const item of page.items) feedItems.set(item.item_id, item);
      loadSaved(page.saved ?? []);
      pageItems.current = [...page.items, ...(page.saved ?? [])];
      loadLiked(pageItems.current);
      const fresh = trigger === 'stale' && viewport.current.top > PILL_FROM_Y ? newArrivals(loadedIds.current, page.items) : [];
      setArrivals(fresh);
      if (fresh.length) recordUi('new_pill_shown', { metadata: { count: fresh.length, view: view.current } });
      loadedIds.current = page.items.map((i) => i.item_id);
      setItems(page.items);
      setError(null);
      setDisabled(false);
      loadedAt.current = Date.now();
    } catch (e) {
      recordFeedEvent('load', {
        at: startedAt,
        dwell_ms: Date.now() - startedAt,
        metadata: { trigger, ok: false, status: e instanceof FeedApiError ? e.status : 0, error: e instanceof Error ? e.message.slice(0, 300) : String(e) },
      });
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
      if (AppState.currentState === 'active') startFeedSession(ids.current.length, 'focus');
      if (Date.now() - loadedAt.current > STALE_MS) void load(loadedAt.current ? 'stale' : 'first');
      else measure();
      return () => {
        focused.current = false;
        pause();
        endFeedSession('blur');
      };
    }, [load, measure, pause]),
  );

  useEffect(() => {
    const sub = AppState.addEventListener('change', (state) => {
      if (!focused.current) return;
      if (state === 'active') {
        startFeedSession(ids.current.length, 'foreground');
        if (Date.now() - loadedAt.current > STALE_MS) void load(loadedAt.current ? 'stale' : 'first');
        else measure();
      } else {
        pause();
        endFeedSession('background');
        void flushFeedEvents(true);
      }
    });
    return () => sub.remove();
  }, [load, measure, pause]);

  // Search as he types, a beat after the last keystroke; an empty box lists
  // his whole history, newest seen first.
  useEffect(() => {
    if (!searching) return;
    const gen = ++searchGeneration.current;
    const q = query.trim();
    const t = setTimeout(async () => {
      const startedAt = Date.now();
      try {
        const page = await searchFeed(config, q);
        if (gen !== searchGeneration.current) return;
        pause();
        for (const item of page.items) feedItems.set(item.item_id, item);
        loadLiked([...pageItems.current, ...page.items]);
        setResults(page.items);
        setSearchError(null);
        recordUi('search', { dwell_ms: Date.now() - startedAt, metadata: { query: q.slice(0, 200), results: page.items.length } });
      } catch (e) {
        if (gen === searchGeneration.current) setSearchError(e instanceof Error ? e.message : String(e));
      } finally {
        if (gen === searchGeneration.current) setSearchBusy(false);
      }
    }, q ? SEARCH_DEBOUNCE_MS : 0);
    return () => clearTimeout(t);
  }, [config, pause, query, searching]);

  const openSearch = useCallback(() => {
    pause();
    recordUi('search_open', { metadata: { from: viewName(only) } });
    setSearching(true);
    setSearchBusy(true);
    list.current?.scrollToOffset({ offset: 0, animated: false });
  }, [only, pause]);

  const closeSearch = useCallback(() => {
    pause();
    Keyboard.dismiss();
    recordUi('search_close', { metadata: { query: query.trim().slice(0, 200), results: results?.length ?? 0 } });
    searchGeneration.current++;
    setSearching(false);
    setQuery('');
    setResults(null);
    setSearchBusy(false);
    setSearchError(null);
  }, [pause, query, results]);

  const navigation = useNavigation<{
    addListener(type: 'tabPress', cb: () => void): () => void;
    isFocused(): boolean;
    setOptions(options: { headerRight?: () => ReactNode }): void;
  }>();

  // The magnifying glass in the header, like the Timeline's.
  useLayoutEffect(() => {
    navigation.setOptions({
      headerRight: () => (
        <Pressable
          accessibilityRole="button"
          accessibilityLabel={searching ? 'Close search' : 'Search past cards'}
          hitSlop={6}
          onPress={searching ? closeSearch : openSearch}
          style={[styles.headerButton, { backgroundColor: theme.surface, borderColor: theme.surfaceBorder }]}>
          <Icon name={searching ? 'xmark' : 'magnifyingglass'} tintColor={theme.text} size={20} />
        </Pressable>
      ),
    });
  }, [navigation, searching, openSearch, closeSearch, theme]);

  const onScroll = useCallback(
    (e: NativeSyntheticEvent<NativeScrollEvent>) => {
      markActive();
      viewport.current = { top: e.nativeEvent.contentOffset.y, height: e.nativeEvent.layoutMeasurement.height };
      if (viewport.current.top < PILL_GONE_Y) setArrivals((a) => (a.length ? [] : a));
      measure();
    },
    [measure],
  );

  // Tapping Feed in the tab bar while already on it, so a double tap from
  // anywhere, scrolls back to the top, as X and Instagram do.
  useEffect(
    () =>
      navigation.addListener('tabPress', () => {
        if (!navigation.isFocused() || viewport.current.top <= 0) return;
        recordUi('tab_to_top', { metadata: { from_y: Math.round(viewport.current.top), view: view.current } });
        setArrivals([]);
        list.current?.scrollToOffset({ offset: 0, animated: true });
      }),
    [navigation],
  );

  // The card at the top of the screen at a scroll offset, so a scroll event
  // says which cards it moved between.
  const cardAt = useCallback((y: number) => {
    const layouts = layoutsFromHeights(ids.current, heights.current, headerHeight.current);
    for (const l of layouts.values()) if (l.y + l.height > y) return l.position;
    return -1;
  }, []);

  const recordScroll = useCallback(
    (g: ReturnType<ScrollGestureTracker['settle']>) => {
      if (!g) return;
      recordFeedEvent('scroll', {
        at: g.startedAt,
        dwell_ms: g.durationMs,
        metadata: scrollMetadata(g, {
          from_card: cardAt(g.fromY),
          to_card: cardAt(g.toY),
          viewport: Math.round(viewport.current.height),
          view: view.current,
        }),
      });
    },
    [cardAt],
  );

  const scrollHandlers = useMemo(
    () => ({
      onScrollBeginDrag: (e: NativeSyntheticEvent<NativeScrollEvent>) => {
        clearTimeout(settleTimer.current);
        recordScroll(gestures.current.beginDrag(Date.now(), e.nativeEvent.contentOffset.y));
      },
      onScrollEndDrag: (e: NativeSyntheticEvent<NativeScrollEvent>) => {
        gestures.current.endDrag(Date.now(), e.nativeEvent.contentOffset.y, e.nativeEvent.velocity?.y ?? 0);
        settleTimer.current = setTimeout(() => recordScroll(gestures.current.settle()), SETTLE_MS);
      },
      onMomentumScrollBegin: () => {
        clearTimeout(settleTimer.current);
        gestures.current.momentumBegin();
      },
      onMomentumScrollEnd: (e: NativeSyntheticEvent<NativeScrollEvent>) => {
        recordScroll(gestures.current.momentumEnd(Date.now(), e.nativeEvent.contentOffset.y));
      },
    }),
    [recordScroll],
  );

  useEffect(() => () => clearTimeout(settleTimer.current), []);

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
      recordFeedEvent('open', { item_id: item.item_id, position: position(item), metadata: { mode: 'browser', via, ...which } });
      // The feed's own browser: what he does on the page is recorded
      // (tracked-browser.tsx), and its time is active time.
      const stop = startTimer();
      let seen = { pages: 0, final_url: url, external: false };
      try {
        seen = await openTrackedBrowser(url, { item_id: item.item_id, position: position(item), via, link_index: which.link_index });
      } finally {
        const { dwell_ms, wall_ms } = stop();
        recordFeedEvent('close', {
          item_id: item.item_id,
          dwell_ms,
          metadata: { mode: 'browser', via, ...which, wall_ms, ...seen },
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

  // A link inside a post or the card's text (index -1, via says which) or
  // one of the card's links.
  const onBrowse = useCallback(
    (item: FeedItem, link?: CardLink, via?: string) =>
      void openInBrowser(item, via ?? (link ? (link.index < 0 ? 'post_link' : 'link') : 'card'), link),
    [openInBrowser],
  );

  // A carousel swipe or a video play. A video plays in the card (inline);
  // one among photos in an X post still plays in the system player. Watch
  // time is recorded like reading time: a close with mode video.
  const onMedia = useCallback(
    async (item: FeedItem, a: MediaAction) => {
      if (a.action === 'stop') {
        recordFeedEvent('close', { item_id: item.item_id, dwell_ms: a.dwellMs, metadata: { mode: 'video', index: a.index, inline: true } });
        return;
      }
      recordFeedEvent('media', {
        item_id: item.item_id,
        position: position(item),
        dwell_ms: a.dwellMs,
        metadata: { action: a.action, index: a.index, ...(a.inline ? { inline: true } : {}), ...(a.auto ? { autoplay: true } : {}) },
      });
      if (a.action !== 'play' || !a.url || a.inline) return;
      pause();
      const stop = startTimer(true);
      try {
        await WebBrowser.openBrowserAsync(a.url, { dismissButtonStyle: 'close' });
      } finally {
        const { dwell_ms, wall_ms } = stop();
        recordFeedEvent('close', { item_id: item.item_id, dwell_ms, metadata: { mode: 'video', index: a.index, wall_ms } });
        measure();
      }
    },
    [measure, pause],
  );

  // The paper plane opens a draft of the card in the default mail app for
  // him to address.
  const onShare = useCallback(async (item: FeedItem, preview: string) => {
    try {
      await Linking.openURL(mailtoUrl(shareDraft(item, preview)));
    } catch {
      Alert.alert('Could not open a mail app', 'No mail app is set up on this phone.');
      recordUi('share_failed', { item_id: item.item_id, position: position(item) });
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
      const inFeed = items.findIndex((i) => i.item_id === item.item_id);
      const inResults = results?.findIndex((i) => i.item_id === item.item_id) ?? -1;
      const putBack = (list: FeedItem[], at: number) => {
        const next = [...list];
        next.splice(Math.min(at, next.length), 0, item);
        return next;
      };
      setItems((prev) => prev.filter((i) => i.item_id !== item.item_id));
      setResults((prev) => prev && prev.filter((i) => i.item_id !== item.item_id));
      showUndo('Hidden', () => {
        recordFeedEvent('undismiss', { item_id: item.item_id, position: index });
        LayoutAnimation.configureNext(LayoutAnimation.Presets.easeInEaseOut);
        if (inFeed >= 0) setItems((prev) => putBack(prev, inFeed));
        if (inResults >= 0) setResults((prev) => prev && putBack(prev, inResults));
      });
    },
    [items, results, showUndo],
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
    recordUi('undo', { metadata: { what: undo.text } });
    clearTimeout(undo.timer);
    undo.undo();
    setUndo(null);
  }, [undo]);

  // The heart on a card, an X post's heart, the detail view or the menu.
  const onLike = useCallback((item: FeedItem, via: string) => {
    toggleLiked(item, { position: position(item), via });
  }, []);

  const onMenu = useCallback(
    (item: FeedItem) => {
      const from = item.site || item.source;
      const saveLabel = savedIds.includes(item.item_id) ? 'Remove from Saved' : 'Save for later';
      const likeLabel = likedIds.includes(item.item_id) ? 'Unlike' : 'Like';
      const options = [saveLabel, 'Not interested', `Less from ${from}`, likeLabel, 'Cancel'];
      const openedAt = Date.now();
      recordUi('menu_open', { item_id: item.item_id, position: position(item) });
      const pick = (i: number) => {
        recordUi('menu_pick', {
          item_id: item.item_id,
          position: position(item),
          dwell_ms: Date.now() - openedAt,
          metadata: { choice: ['save', 'not_interested', 'less_like_this', 'like', 'cancel'][i] ?? 'cancel' },
        });
        if (i === 0) onSave(item, 'menu');
        if (i === 1) dismiss(item, 'not_interested');
        if (i === 2) dismiss(item, 'less_like_this');
        if (i === 3) onLike(item, 'menu');
      };
      showActionSheet({ options, destructiveIndex: 1, title: item.title || undefined, pick }, setSheet);
    },
    [dismiss, onSave, onLike, savedIds, likedIds],
  );

  // A swipe begun, whether or not it went far enough to save or hide.
  const onSwipeStart = useCallback((item: FeedItem, toward: 'save' | 'dismiss') => {
    recordUi('swipe_start', { item_id: item.item_id, position: position(item), metadata: { toward } });
  }, []);

  const renderItem = useCallback(
    ({ item }: { item: FeedItem }) => (
      <FeedCard
        item={item}
        saved={savedIds.includes(item.item_id)}
        liked={likedIds.includes(item.item_id)}
        onSave={onSave}
        onLike={onLike}
        onDismiss={dismiss}
        onLayout={onCardLayout}
        onOpen={onOpen}
        onBrowse={onBrowse}
        onMedia={onMedia}
        onExpand={onExpand}
        onMenu={onMenu}
        onShare={onShare}
        onSwipeStart={onSwipeStart}
      />
    ),
    [savedIds, likedIds, onSave, onLike, dismiss, onCardLayout, onOpen, onBrowse, onMedia, onExpand, onMenu, onShare, onSwipeStart],
  );

  if (!loaded) {
    return (
      <ThemedView style={styles.center}>
        <ActivityIndicator />
      </ThemedView>
    );
  }

  return (
    <ThemedView style={styles.fill} onTouchStart={markActive}>
      <VideoWarmup />
      {searching ? (
        <View style={styles.searchRow}>
          <TextInput
            style={[styles.searchInput, { backgroundColor: theme.surface, borderColor: theme.surfaceBorder, color: theme.text }]}
            placeholder="Search everything you've seen"
            placeholderTextColor={theme.textSecondary}
            value={query}
            onChangeText={(text) => {
              setQuery(text);
              setSearchBusy(true);
            }}
            onSubmitEditing={() => Keyboard.dismiss()}
            autoFocus
            returnKeyType="search"
            autoCapitalize="none"
            autoCorrect={false}
            clearButtonMode="while-editing"
          />
          {searchBusy ? <ActivityIndicator style={styles.searchSpinner} /> : null}
        </View>
      ) : null}
      <FlatList
        ref={list}
        data={shown}
        extraData={[savedIds, likedIds]}
        keyExtractor={(item) => item.item_id}
        renderItem={renderItem}
        onScroll={onScroll}
        {...scrollHandlers}
        keyboardShouldPersistTaps="handled"
        keyboardDismissMode="on-drag"
        onScrollToTop={() => recordUi('scroll_to_top', { metadata: { view: view.current } })}
        onEndReachedThreshold={0.2}
        onEndReached={() => {
          if (endLogged.current || !shown.length) return;
          endLogged.current = true;
          recordUi('end_reached', { metadata: { cards: shown.length, view: view.current } });
        }}
        scrollEventThrottle={16}
        onLayout={(e) => {
          viewport.current = { ...viewport.current, height: e.nativeEvent.layout.height };
          measure();
        }}
        refreshControl={
          searching ? undefined : <RefreshControl
            refreshing={refreshing}
            onRefresh={() => {
              recordUi('refresh', { metadata: { view: view.current } });
              void load('pull');
            }}
          />
        }
        windowSize={11}
        initialNumToRender={6}
        ListHeaderComponent={
          <View
            onLayout={(e) => {
              headerHeight.current = e.nativeEvent.layout.height;
              measure();
            }}>
            {searching ? null : bubbles.length > 1 || savedIds.length > 0 || only === SAVED ? (
              <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.rail}>
                {savedIds.length > 0 || only === SAVED ? (
                  <Pressable
                    style={[styles.bubble, only !== null && only !== SAVED && styles.bubbleDim]}
                    onPress={() => {
                      pause();
                      const next = only === SAVED ? null : SAVED;
                      recordUi('filter', { metadata: { from: viewName(only), to: viewName(next), saved: savedIds.length } });
                      setOnly(next);
                    }}
                    accessibilityLabel={`Saved for later, ${savedIds.length}`}>
                    <View style={[styles.savedBubble, only === SAVED && styles.savedBubbleOn]}>
                      <Icon name="bookmark.fill" tintColor="#fff" size={24} />
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
                        const next = b.source && !active ? b.source : null;
                        recordUi('filter', { metadata: { from: viewName(only), to: viewName(next), unseen: b.unseen, total: b.total } });
                        setOnly(next);
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
            {searching && searchError ? (
              <View style={[styles.banner, { backgroundColor: theme.backgroundElement }]}>
                <ThemedText style={styles.bannerText}>Could not search: {searchError}</ThemedText>
              </View>
            ) : null}
            {error && !searching ? (
              <View style={[styles.banner, { backgroundColor: theme.backgroundElement }]}>
                <ThemedText style={styles.bannerText}>Could not refresh: {error}</ThemedText>
              </View>
            ) : null}
          </View>
        }
        ListEmptyComponent={
          searching ? (
            <View style={styles.empty}>
              {results === null ? (
                <ActivityIndicator />
              ) : searchError ? null : (
                <>
                  <ThemedText style={styles.emptyTitle}>{query.trim() ? 'No matches' : 'Nothing seen yet'}</ThemedText>
                  <ThemedText style={[styles.emptyBody, { color: theme.textSecondary }]}>
                    {query.trim() ? 'No card you have seen has all of those words.' : 'Cards show up here once you have seen them in the feed.'}
                  </ThemedText>
                </>
              )}
            </View>
          ) : only === SAVED ? (
            <View style={styles.empty}>
              <ThemedText style={styles.emptyTitle}>Nothing saved</ThemedText>
              <ThemedText style={[styles.emptyBody, { color: theme.textSecondary }]}>
                Tap the bookmark on a card, or swipe it right, to keep it here until you get to it.
              </ThemedText>
            </View>
          ) : (
          <View style={styles.empty}>
            {disabled ? (
              <>
                <ThemedText style={styles.emptyTitle}>The feed is turned off</ThemedText>
                <ThemedText style={[styles.emptyBody, { color: theme.textSecondary }]}>
                  This PDW server does not have the Feed module on (PDW_FEED_ENABLED).
                </ThemedText>
              </>
            ) : (
              // Only before the first card is ever pushed: once there are cards
              // the server resurfaces seen ones instead of an empty feed.
              <ActivityIndicator />
            )}
          </View>
          )
        }
      />
      {arrivals.length && only === null && !searching ? (
        <Pressable
          style={({ pressed }) => [styles.pill, pressed && styles.pillPressed]}
          onPress={() => {
            recordUi('new_pill', { metadata: { count: arrivals.length, from_y: Math.round(viewport.current.top) } });
            setArrivals([]);
            list.current?.scrollToOffset({ offset: 0, animated: true });
          }}
          accessibilityLabel={`${arrivals.length} new cards, scroll to top`}>
          <Icon name="arrow.up" tintColor="#fff" size={14} />
          {[...new Set(arrivals.map((i) => i.site || i.source))].slice(0, 3).map((name, n) => (
            <View key={name} style={[styles.pillFace, n > 0 && styles.pillFaceOverlap]}>
              <SourceAvatar name={name} url={arrivals.find((i) => (i.site || i.source) === name)?.url} size={22} />
            </View>
          ))}
          <ThemedText style={styles.pillText}>{arrivals.length} new</ThemedText>
        </Pressable>
      ) : null}
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
      <ActionSheet request={sheet} onClose={() => setSheet(null)} />
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
  pill: {
    position: 'absolute',
    top: Spacing.two,
    alignSelf: 'center',
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    paddingLeft: 12,
    paddingRight: 14,
    paddingVertical: 6,
    borderRadius: 20,
    backgroundColor: ACCENT,
    shadowColor: '#000',
    shadowOpacity: 0.3,
    shadowRadius: 6,
    shadowOffset: { width: 0, height: 2 },
  },
  pillPressed: { opacity: 0.85 },
  pillFace: { borderRadius: 13, borderWidth: 2, borderColor: ACCENT },
  pillFaceOverlap: { marginLeft: -12 },
  pillText: { color: '#fff', fontSize: 14, fontWeight: '700', lineHeight: 18 },
  headerButton: { width: 40, height: 40, borderRadius: 20, borderWidth: StyleSheet.hairlineWidth, alignItems: 'center', justifyContent: 'center', marginRight: 14 },
  searchRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, paddingHorizontal: Spacing.three, paddingVertical: Spacing.two },
  searchInput: { flex: 1, borderRadius: 22, borderWidth: StyleSheet.hairlineWidth, paddingHorizontal: 18, paddingVertical: 10, fontSize: 16 },
  searchSpinner: { position: 'absolute', right: Spacing.three + 40 },
  undoAction: { fontSize: 15, fontWeight: '700', color: '#36A0DA' },
});
