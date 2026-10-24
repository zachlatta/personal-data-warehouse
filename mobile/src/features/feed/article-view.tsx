import { useEffect, useMemo, useState } from 'react';
import { ActivityIndicator, StyleSheet, View } from 'react-native';
import { WebView } from 'react-native-webview';

import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { useConfig } from '@/lib/session';

import { fetchArticle } from './api';
import { articleDocument, parseArticleMessage, readingMinutes, type FeedArticle } from './article';
import { ACCENT } from './rich-views';

// Read articles stay for the app's life, so reopening a card is instant.
const articles = new Map<string, FeedArticle>();
// The one script the article page may run carries this.
const NONCE = Math.random().toString(36).slice(2);

export type ArticleOutcome = { ok: true; words: number; ms: number } | { ok: false; error: string; ms: number };

// The article at url, read by the server and shown in a WebView as tall as
// its content (the detail view's ScrollView does the scrolling). A tapped
// link goes to onLink; a page with no article shows nothing but a note,
// and the detail view's open button is still there.
export function ArticleView({
  url,
  shownImages,
  onLink,
  onOutcome,
}: {
  url: string;
  shownImages: string[];
  onLink: (url: string) => void;
  onOutcome: (o: ArticleOutcome) => void;
}) {
  const config = useConfig();
  const theme = useTheme();
  const [article, setArticle] = useState<FeedArticle | null>(articles.get(url) ?? null);
  const [error, setError] = useState<string | null>(null);
  const [height, setHeight] = useState(0);

  useEffect(() => {
    if (articles.has(url)) return;
    let live = true;
    const startedAt = Date.now();
    fetchArticle(config, url)
      .then((a) => {
        articles.set(url, a);
        onOutcome({ ok: true, words: a.words, ms: Date.now() - startedAt });
        if (live) setArticle(a);
      })
      .catch((e: unknown) => {
        const message = e instanceof Error ? e.message : String(e);
        onOutcome({ ok: false, error: message, ms: Date.now() - startedAt });
        if (live) setError(message);
      });
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
    return (
      <ThemedText style={[styles.note, { color: theme.textSecondary }]}>
        The full article could not be loaded here. Open the page below to read it.
      </ThemedText>
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
          {[article.byline, `${readingMinutes(article.words)} min read`].filter(Boolean).join(' · ')}
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

const styles = StyleSheet.create({
  loading: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, paddingHorizontal: Spacing.three },
  note: { fontSize: 14, paddingHorizontal: Spacing.three },
  divider: { borderTopWidth: StyleSheet.hairlineWidth, marginHorizontal: Spacing.three, paddingTop: Spacing.three, marginBottom: Spacing.two },
  meta: { fontSize: 13 },
  web: { width: '100%' },
});
