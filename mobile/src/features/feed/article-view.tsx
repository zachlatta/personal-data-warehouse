import { useEffect, useMemo, useState } from 'react';
import { ActivityIndicator, Pressable, StyleSheet, View } from 'react-native';
import { WebView } from 'react-native-webview';

import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { AppConfig } from '@/lib/config';
import { useConfig } from '@/lib/session';

import { fetchArticle, FeedApiError } from './api';
import { articleFallback, fetchReadableArticle } from './article-fallback';
import { createArticleReads } from './article-reads';
import { articleDocument, parseArticleMessage, readingMinutes, type FeedArticle } from './article';
import { isKnownNotArticle, readStoredArticle, setOffline, storeArticle, storeFailure } from './offline';
import { isOfflineError } from './offline-store';
import { ACCENT } from './rich-views';

// The copy downloaded for offline (offline.ts) is read first; one read
// online is kept.
const reads = createArticleReads<FeedArticle, AppConfig>(async (url, config) => {
  const local = await readStoredArticle(url);
  if (local) return { article: local, stored: true };
  if (isKnownNotArticle(url)) throw new FeedApiError(422, 'no readable article on this page');
  const a = await fetchReadableArticle(url, () => fetchArticle(config, url),
    (failedUrl) => storeFailure(failedUrl, new FeedApiError(422, 'no readable article on this page')));
  setOffline(false);
  void storeArticle(url, a);
  return { article: a, stored: false };
}, 6);

/** Starts reading the article at url (see article-reads.ts); errors wait for the view. */
export function readArticle(config: AppConfig, url: string) {
  return reads.read(url, config);
}

// The one script the article page may run carries this.
const NONCE = Math.random().toString(36).slice(2);

// offline: read from the copy on the phone (or, failed, because there is no connection).
export type ArticleOutcome = { ok: true; words: number; ms: number; offline: boolean } | { ok: false; error: string; ms: number; offline: boolean };

// The article at url, read by the server and shown in a WebView as tall as
// its content (the detail view's ScrollView does the scrolling). A tapped
// link goes to onLink; a failed online read puts the card's page first.
export function ArticleView({
  url,
  shownImages,
  onLink,
  onOpenPage,
  onOutcome,
}: {
  url: string;
  shownImages: string[];
  onLink: (url: string) => void;
  onOpenPage: () => void;
  onOutcome: (o: ArticleOutcome) => void;
}) {
  const config = useConfig();
  const theme = useTheme();
  // A read that finished before this view mounted (the card started it
  // when touched) shows on the first frame.
  const [ready] = useState(() => reads.peek(url));
  const [article, setArticle] = useState<FeedArticle | null>(ready?.article ?? null);
  const [stored, setStored] = useState(ready?.stored ?? false);
  const [error, setError] = useState<unknown>(() => isKnownNotArticle(url) ? new FeedApiError(422, 'no readable article on this page') : null);
  const [height, setHeight] = useState(0);

  useEffect(() => {
    let live = true;
    const startedAt = Date.now();
    readArticle(config, url).then(
      (r) => {
        onOutcome({ ok: true, words: r.article.words, ms: Date.now() - startedAt, offline: r.stored });
        if (live) {
          setStored(r.stored);
          setArticle(r.article);
        }
      },
      (e: unknown) => {
        const message = e instanceof Error ? e.message : String(e);
        const off = isOfflineError(e);
        if (off) setOffline(true);
        onOutcome({ ok: false, error: message, ms: Date.now() - startedAt, offline: off });
        if (live) setError(e);
      },
    );
    return () => {
      live = false;
    };
    // onOutcome is a fresh closure each render; one read per url.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [config, url]);

  const shownKey = shownImages.join('\n');
  const html = useMemo(
    () =>
      article
        ? articleDocument(
            article,
            {
              text: theme.text,
              secondary: theme.textSecondary,
              background: theme.background,
              element: theme.backgroundElement,
              rule: theme.backgroundSelected,
              link: ACCENT,
            },
            shownKey ? shownKey.split('\n') : [],
            NONCE,
          )
        : null,
    [article, theme, shownKey],
  );

  if (error) {
    const fallback = articleFallback(url, error);
    if (fallback.kind === 'offline') {
      return <ThemedText style={[styles.note, { color: theme.textSecondary }]}>{fallback.message}</ThemedText>;
    }
    return (
      <Pressable accessibilityRole="button" onPress={onOpenPage}
        style={({ pressed }) => [styles.openPage, { backgroundColor: ACCENT }, pressed && { opacity: 0.8 }]}>
        <ThemedText style={styles.openLabel}>{fallback.label}</ThemedText>
      </Pressable>
    );
  }

  if (!article || !html) {
    return (
      <View style={styles.loading}>
        <ActivityIndicator />
        <ThemedText style={[styles.note, { color: theme.textSecondary }]}>Loading the full article…</ThemedText>
      </View>
    );
  }
  return (
    <View>
      <View style={[styles.divider, { borderTopColor: theme.backgroundSelected }]}>
        <ThemedText style={[styles.meta, { color: theme.textSecondary }]}>
          {[article.byline, `${readingMinutes(article.words)} min read`, stored ? 'Saved offline' : ''].filter(Boolean).join(' · ')}
        </ThemedText>
      </View>
      <WebView
        source={{ html, baseUrl: article.url }}
        originWhitelist={['*']}
        scrollEnabled={false}
        bounces={false}
        overScrollMode="never"
        showsVerticalScrollIndicator={false}
        javaScriptEnabled
        allowsInlineMediaPlayback
        setSupportMultipleWindows={false}
        style={[styles.web, { height: Math.max(height, 200), backgroundColor: theme.background }]}
        onMessage={(e) => {
          const m = parseArticleMessage(e.nativeEvent.data);
          if (m?.type === 'height') setHeight(m.height);
          else if (m?.type === 'link') onLink(m.url);
        }}
        onShouldStartLoadWithRequest={(req) => {
          // The article itself and its in-page anchors load here; any
          // other navigation the click handler missed goes to the app.
          if (!req.isTopFrame || req.url === 'about:blank' || req.url === article.url || req.url.startsWith(`${article.url}#`)) return true;
          if (/^(https?|mailto):/i.test(req.url)) onLink(req.url);
          return false;
        }}
      />
    </View>
  );
}

// The feed's video warmup starts Android's web engine, but the first
// article of each run still drew about half a second after its page (later
// ones drew with it; emulator, 2026-10-26). The feed keeps this hidden
// article page, in the same template, made a moment after it appears, so
// the first article opens like the rest.
export function ArticleWarmup() {
  const theme = useTheme();
  const [on, setOn] = useState(false);
  useEffect(() => {
    const t = setTimeout(() => setOn(true), 1500);
    return () => clearTimeout(t);
  }, []);
  const html = useMemo(
    () =>
      articleDocument(
        { url: 'about:blank', html: '<p>.</p>', words: 1 },
        { text: theme.text, secondary: theme.textSecondary, background: theme.background, element: theme.backgroundElement, rule: theme.backgroundSelected, link: ACCENT },
        [],
        NONCE,
      ),
    [theme],
  );
  return on ? (
    <View style={styles.warm} pointerEvents="none" importantForAccessibility="no-hide-descendants">
      <WebView source={{ html }} originWhitelist={['*']} javaScriptEnabled scrollEnabled={false} style={styles.warm} />
    </View>
  ) : null;
}

const styles = StyleSheet.create({
  openPage: { marginHorizontal: Spacing.three, padding: Spacing.three, alignItems: 'center', borderRadius: 12 },
  openLabel: { color: '#fff', fontSize: 18, fontWeight: '600' },
  warm: { position: 'absolute', width: 1, height: 1, opacity: 0 },
  loading: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, paddingHorizontal: Spacing.three },
  note: { fontSize: 14, paddingHorizontal: Spacing.three },
  divider: { borderTopWidth: StyleSheet.hairlineWidth, marginHorizontal: Spacing.three, paddingTop: Spacing.three, marginBottom: Spacing.two },
  meta: { fontSize: 13 },
  web: { width: '100%' },
});
