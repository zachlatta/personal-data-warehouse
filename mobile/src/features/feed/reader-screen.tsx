import { Image } from 'expo-image';
import { Stack, useFocusEffect, useLocalSearchParams } from 'expo-router';
import * as WebBrowser from 'expo-web-browser';
import { Fragment, useCallback, useRef } from 'react';
import { Linking, Pressable, ScrollView, StyleSheet, View, type NativeScrollEvent, type NativeSyntheticEvent } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { formatWhen } from '@/lib/format';

import { feedItems, recordFeedEvent } from './engagement';
import { cardLinks } from './links';
import { parseMarkdown, type Span } from './markdown';

function Spans({ spans }: { spans: Span[] }) {
  return (
    <>
      {spans.map((span, i) => (
        <ThemedText
          key={i}
          onPress={span.url ? () => void WebBrowser.openBrowserAsync(span.url as string).catch(() => Linking.openURL(span.url as string)) : undefined}
          style={[
            span.bold && styles.bold,
            span.italic && styles.italic,
            span.code && styles.code,
            span.url ? styles.link : null,
          ]}>
          {span.text}
        </ThemedText>
      ))}
    </>
  );
}

// The in-app reader for a card's body. Time here is the card's reading time
// (an open, then a close carrying dwell_ms and how far down it was read).
export default function FeedReaderScreen() {
  const params = useLocalSearchParams<{ id: string; position?: string }>();
  const theme = useTheme();
  const item = feedItems.get(params.id);
  const depth = useRef(0);
  const position = params.position ? Number(params.position) : undefined;

  useFocusEffect(
    useCallback(() => {
      if (!item) return undefined;
      const startedAt = Date.now();
      depth.current = 0;
      recordFeedEvent('open', { item_id: item.item_id, position, metadata: { mode: 'reader' } });
      return () => {
        recordFeedEvent('close', {
          item_id: item.item_id,
          dwell_ms: Date.now() - startedAt,
          metadata: { mode: 'reader', scroll_depth: Math.round(depth.current * 100) / 100 },
        });
      };
    }, [item, position]),
  );

  const onScroll = (e: NativeSyntheticEvent<NativeScrollEvent>) => {
    const { contentOffset, contentSize, layoutMeasurement } = e.nativeEvent;
    const seen = contentSize.height <= 0 ? 1 : (contentOffset.y + layoutMeasurement.height) / contentSize.height;
    depth.current = Math.max(depth.current, Math.min(1, seen));
  };

  if (!item) {
    return (
      <ThemedView style={styles.center}>
        <ThemedText>This card is no longer loaded. Go back and refresh the feed.</ThemedText>
      </ThemedView>
    );
  }
  const byline = [item.site || item.source, item.author, formatWhen(item.published_at ?? item.created_at)].filter(Boolean).join(' · ');

  return (
    <ThemedView style={styles.fill}>
      <Stack.Screen options={{ title: item.site || item.source }} />
      <ScrollView
        contentContainerStyle={styles.content}
        onScroll={onScroll}
        scrollEventThrottle={100}
        onContentSizeChange={(_, h) => {
          if (h > 0) depth.current = Math.max(depth.current, Math.min(1, 700 / h));
        }}>
        <ThemedText style={[styles.byline, { color: theme.textSecondary }]}>{byline}</ThemedText>
        {item.title ? <ThemedText style={styles.title}>{item.title}</ThemedText> : null}
        {item.image_url ? (
          <Image source={{ uri: item.image_url }} style={[styles.image, { backgroundColor: theme.backgroundElement }]} contentFit="cover" transition={200} />
        ) : null}
        {parseMarkdown(item.body ?? '').map((block, i) => (
          <Fragment key={i}>
            {block.kind === 'heading' ? (
              <ThemedText style={[styles.heading, block.level <= 2 && styles.headingBig]}>
                <Spans spans={block.spans} />
              </ThemedText>
            ) : block.kind === 'bullet' ? (
              <View style={styles.bulletRow}>
                <ThemedText style={styles.paragraph}>{block.ordered ? `${block.ordered}.` : '•'}</ThemedText>
                <ThemedText style={[styles.paragraph, styles.flex]}>
                  <Spans spans={block.spans} />
                </ThemedText>
              </View>
            ) : block.kind === 'quote' ? (
              <View style={[styles.quote, { borderLeftColor: theme.backgroundSelected }]}>
                <ThemedText style={[styles.paragraph, { color: theme.textSecondary }]}>
                  <Spans spans={block.spans} />
                </ThemedText>
              </View>
            ) : block.kind === 'code' ? (
              <ThemedText style={[styles.codeBlock, { backgroundColor: theme.backgroundElement }]}>{block.text}</ThemedText>
            ) : (
              <ThemedText style={styles.paragraph}>
                <Spans spans={block.spans} />
              </ThemedText>
            )}
          </Fragment>
        ))}
        {cardLinks(item).map((link) => (
          <Pressable
            key={link.index}
            style={[styles.button, link.index > 0 && styles.buttonNext, { backgroundColor: theme.backgroundElement }]}
            onPress={async () => {
              const which = { mode: 'browser', via: 'reader', url: link.url, link_index: link.index };
              const startedAt = Date.now();
              recordFeedEvent('open', { item_id: item.item_id, position, metadata: which });
              await WebBrowser.openBrowserAsync(link.url, { dismissButtonStyle: 'close' });
              recordFeedEvent('close', { item_id: item.item_id, dwell_ms: Date.now() - startedAt, metadata: which });
            }}>
            <ThemedText style={styles.buttonText} numberOfLines={1}>
              {link.index === 0 ? `Open ${link.label || 'link'}` : link.label}
            </ThemedText>
          </Pressable>
        ))}
      </ScrollView>
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  fill: { flex: 1 },
  flex: { flex: 1 },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center', padding: Spacing.four },
  content: { padding: Spacing.three, paddingBottom: Spacing.six, gap: Spacing.three },
  byline: { fontSize: 13 },
  title: { fontSize: 26, lineHeight: 32, fontWeight: '700' },
  image: { width: '100%', aspectRatio: 1.91, borderRadius: 10 },
  heading: { fontSize: 18, lineHeight: 24, fontWeight: '700', marginTop: Spacing.two },
  headingBig: { fontSize: 21, lineHeight: 27 },
  paragraph: { fontSize: 17, lineHeight: 26 },
  bulletRow: { flexDirection: 'row', gap: Spacing.two },
  quote: { borderLeftWidth: 3, paddingLeft: Spacing.three },
  codeBlock: { fontFamily: 'Menlo', fontSize: 14, padding: Spacing.three, borderRadius: 8 },
  bold: { fontWeight: '700' },
  italic: { fontStyle: 'italic' },
  code: { fontFamily: 'Menlo', fontSize: 15 },
  link: { color: '#208AEF' },
  button: { marginTop: Spacing.three, padding: Spacing.three, borderRadius: 12, alignItems: 'center' },
  buttonNext: { marginTop: 0 },
  buttonText: { fontSize: 16, fontWeight: '600', color: '#208AEF' },
});
