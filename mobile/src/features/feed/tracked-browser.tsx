// The feed's own browser. Links from feed cards open here, in a WebView,
// rather than in the system browser (Safari View Controller, Chrome Custom
// Tabs), which shows the app nothing of what happens inside it. Here
// web-tracker.ts reports what he does on each page, recorded as `web`
// events on the card: one `page` summary per page he visited, and one event
// per tap, text selection, copy and media play.
//
// Time inside counts only while he is using the page (a touch, scroll or key
// in the last minute, or a video or audio playing), like the rest of the
// feed's active time.
import * as WebBrowser from 'expo-web-browser';
import { useEffect, useRef, useState, useSyncExternalStore } from 'react';
import { BackHandler, Linking, Modal, Platform, Pressable, StyleSheet, View } from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';
import { WebView, type WebViewMessageEvent, type WebViewNavigation } from 'react-native-webview';

import { Icon } from '@/components/icon';
import { ThemedText } from '@/components/themed-text';
import { useTheme } from '@/hooks/use-theme';

import { activeClock, activeNow, markActive, recordFeedEvent } from './engagement';
import { PageVisit, parseTrackerMessage, samePage, TRACKER_JS, type TrackerMessage } from './web-tracker';

export type BrowserContext = { item_id: string; position?: number; via: string; link_index?: number };
export type BrowserResult = { pages: number; final_url: string; external: boolean };

type Request = { url: string; ctx: BrowserContext; done: (r: BrowserResult) => void };

let current: Request | null = null;
let mounted = 0;
const listeners = new Set<() => void>();
function setCurrent(next: Request | null) {
  current = next;
  for (const l of listeners) l();
}
function subscribe(l: () => void) {
  listeners.add(l);
  return () => listeners.delete(l);
}

const WEB_SCHEMES = /^(https?:|about:|data:|blob:)/i;
// Sites that are really apps: the system browser hands them to the app he
// is signed in to (a reel opens in Instagram), where a WebView would show a
// login wall.
const APP_HOSTS = /^(?:[a-z0-9-]+\.)*(instagram\.com|x\.com|twitter\.com|tiktok\.com|threads\.net|threads\.com|facebook\.com)$/i;
// iOS-only WebView props: Android must not get them (an unknown native prop
// can abort the app there; mobile/README.md).
const IOS_PROPS = Platform.OS === 'ios' ? { allowsBackForwardNavigationGestures: true, pullToRefreshEnabled: true } : {};
const MAX_TAPS_PER_PAGE = 200;

function hostOf(url: string): string {
  const m = /^[a-z]+:\/\/([^/?#]+)/i.exec(url);
  return m ? m[1].replace(/^www\./, '') : url;
}

/**
 * Opens url in the feed's browser and resolves when he closes it. Without
 * the browser on screen (it lives in the root layout) it falls back to the
 * system browser.
 */
export async function openTrackedBrowser(url: string, ctx: BrowserContext): Promise<BrowserResult> {
  if (!mounted || !WEB_SCHEMES.test(url) || APP_HOSTS.test(hostOf(url))) {
    // The system browser shows nothing inside: its time counts as in use.
    activeClock.setBusy(true, Date.now());
    try {
      await WebBrowser.openBrowserAsync(url, { dismissButtonStyle: 'close' });
    } finally {
      activeClock.setBusy(false, Date.now());
    }
    return { pages: 0, final_url: url, external: true };
  }
  current?.done({ pages: 0, final_url: current.url, external: false });
  return new Promise((resolve) => setCurrent({ url, ctx, done: resolve }));
}

/** Mounted once, in the root layout. */
export function TrackedBrowserHost() {
  const request = useSyncExternalStore(subscribe, () => current);
  useEffect(() => {
    mounted += 1;
    return () => {
      mounted -= 1;
    };
  }, []);
  if (!request) return null;
  return <TrackedBrowser key={`${request.url}:${request.ctx.item_id}`} request={request} />;
}

function TrackedBrowser({ request }: { request: Request }) {
  const theme = useTheme();
  const insets = useSafeAreaInsets();
  const web = useRef<WebView>(null);
  const { ctx } = request;
  const [nav, setNav] = useState({ url: request.url, title: '', canGoBack: false, loading: true });
  const visit = useRef<PageVisit | null>(null);
  const pages = useRef(0);
  const started = useRef(activeNow());
  const playing = useRef(0);
  const taps = useRef(0);
  const finished = useRef(false);

  const base = { item_id: ctx.item_id, ...(typeof ctx.position === 'number' ? { position: ctx.position } : {}) };
  const where = { via: ctx.via, link_index: ctx.link_index ?? 0 };

  const record = (action: string, metadata: Record<string, unknown>) =>
    recordFeedEvent('web', { ...base, metadata: { action, ...where, ...metadata } });

  // He left a page: one summary, with its active time.
  const endPage = () => {
    const v = visit.current;
    if (!v) return;
    visit.current = null;
    const now = activeNow();
    const summary = v.finish(now);
    recordFeedEvent('web', { ...base, dwell_ms: Math.round(now - started.current), metadata: { action: 'page', ...where, ...summary } });
  };
  const startPage = (url: string) => {
    const from = visit.current?.url ?? '';
    endPage();
    started.current = activeNow();
    taps.current = 0;
    visit.current = new PageVisit(url, pages.current, from);
    pages.current += 1;
  };

  const close = (external = false) => {
    if (finished.current) return;
    finished.current = true;
    const finalUrl = visit.current?.url ?? nav.url;
    endPage();
    while (playing.current > 0) {
      playing.current -= 1;
      activeClock.setBusy(false, Date.now());
    }
    if (current === request) setCurrent(null);
    request.done({ pages: pages.current, final_url: finalUrl, external });
  };
  const closeRef = useRef(close);
  useEffect(() => {
    closeRef.current = close;
  });
  useEffect(() => () => closeRef.current(), []);

  // Android's back button goes back a page, then closes.
  useEffect(() => {
    const sub = BackHandler.addEventListener('hardwareBackPress', () => {
      if (nav.canGoBack) web.current?.goBack();
      else closeRef.current();
      return true;
    });
    return () => sub.remove();
  }, [nav.canGoBack]);

  const onMessage = (e: WebViewMessageEvent) => {
    const m = parseTrackerMessage(e.nativeEvent.data);
    if (!m) return;
    if (m.k !== 'page' && m.k !== 'media') markActive();
    if (m.k === 'page') {
      // A single-page app moved to a new address: a new page.
      const v = visit.current;
      if (!v || (v.loaded && !samePage(v.url, m.url))) startPage(m.url);
    }
    visit.current?.add(m, activeNow());
    onAct(m);
  };

  // The acts worth their own event, so the order of what he did is kept.
  const onAct = (m: TrackerMessage) => {
    const url = visit.current?.url ?? nav.url;
    switch (m.k) {
      case 'tap':
        if (taps.current++ < MAX_TAPS_PER_PAGE) record('tap', { url, tag: m.tag ?? '', text: m.text ?? '', href: m.href ?? '' });
        break;
      case 'select':
        record('select', { url, text: m.text });
        break;
      case 'copy':
        record('copy', { url, text: m.text ?? '' });
        break;
      case 'field':
        record('field', { url, field_type: m.type ?? '' });
        break;
      case 'media':
        // A playing video or song is in use with no touches.
        if (m.action === 'play') {
          playing.current += 1;
          activeClock.setBusy(true, Date.now());
        } else if (playing.current > 0) {
          playing.current -= 1;
          activeClock.setBusy(false, Date.now());
        }
        record('media', { url, media_action: m.action, kind: m.kind ?? '', src: m.src ?? '', t: m.t ?? 0, duration: m.duration ?? null });
        break;
      default:
        break;
    }
  };

  const onLoadStart = (e: { nativeEvent: WebViewNavigation }) => {
    const { url } = e.nativeEvent;
    const v = visit.current;
    if (!v || !samePage(v.url, url)) startPage(url);
  };

  const openOutside = async () => {
    const url = visit.current?.url ?? nav.url;
    record('open_external', { url });
    close(true);
    await Linking.openURL(url).catch(() => {});
  };

  return (
    <Modal visible animationType="slide" presentationStyle="fullScreen" onRequestClose={() => close()} statusBarTranslucent>
      <View style={[styles.root, { backgroundColor: theme.background, paddingTop: insets.top, paddingBottom: insets.bottom }]}>
        <View style={[styles.bar, { borderBottomColor: theme.backgroundSelected }]}>
          <Pressable onPress={() => close()} hitSlop={10} style={styles.button} accessibilityRole="button" accessibilityLabel="Close">
            <Icon name="xmark" tintColor={theme.text} size={20} />
          </Pressable>
          <Pressable
            onPress={() => web.current?.goBack()}
            disabled={!nav.canGoBack}
            hitSlop={10}
            style={[styles.button, !nav.canGoBack && styles.disabled]}
            accessibilityRole="button"
            accessibilityLabel="Back">
            <Icon name="chevron.left" tintColor={theme.text} size={20} />
          </Pressable>
          <View style={styles.titleBox}>
            <ThemedText style={styles.title} numberOfLines={1}>
              {nav.title || hostOf(nav.url)}
            </ThemedText>
            <ThemedText style={[styles.host, { color: theme.textSecondary }]} numberOfLines={1}>
              {nav.loading ? 'Loading…' : hostOf(nav.url)}
            </ThemedText>
          </View>
          <Pressable onPress={() => void openOutside()} hitSlop={10} style={styles.button} accessibilityRole="button" accessibilityLabel="Open in browser">
            <Icon name="safari" tintColor={theme.text} size={20} />
          </Pressable>
        </View>
        <WebView
          ref={web}
          source={{ uri: request.url }}
          injectedJavaScript={TRACKER_JS}
          onMessage={onMessage}
          onLoadStart={onLoadStart}
          onNavigationStateChange={(n) => setNav({ url: n.url, title: n.title ?? '', canGoBack: n.canGoBack, loading: n.loading ?? false })}
          onShouldStartLoadWithRequest={(req) => {
            if (WEB_SCHEMES.test(req.url) && !(req.isTopFrame !== false && APP_HOSTS.test(hostOf(req.url)))) return true;
            // mailto:, tel:, an app's own link, Instagram or X: hand it to the phone.
            record('open_external', { url: req.url });
            void Linking.openURL(req.url).catch(() => {});
            return false;
          }}
          setSupportMultipleWindows={false}
          allowsInlineMediaPlayback
          allowsFullscreenVideo
          {...IOS_PROPS}
          style={{ flex: 1, backgroundColor: theme.background }}
        />
      </View>
    </Modal>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1 },
  bar: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 8, paddingVertical: 6, borderBottomWidth: StyleSheet.hairlineWidth, gap: 4 },
  button: { width: 40, height: 40, alignItems: 'center', justifyContent: 'center' },
  disabled: { opacity: 0.3 },
  titleBox: { flex: 1, alignItems: 'center', paddingHorizontal: 4 },
  title: { fontSize: 15, fontWeight: '600', lineHeight: 20 },
  host: { fontSize: 12, lineHeight: 16 },
});
