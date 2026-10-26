import { Stack, useFocusEffect, useLocalSearchParams } from 'expo-router';
import * as WebBrowser from 'expo-web-browser';
import { Fragment, useCallback, useRef } from 'react';
import { Linking, Pressable, ScrollView, StyleSheet, View, type NativeScrollEvent, type NativeSyntheticEvent } from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

import { Icon } from '@/components/icon';
import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { formatWhen } from '@/lib/format';

import type { FeedItem } from './api';
import { articleUrl } from './article';
import { ArticleView, type ArticleOutcome } from './article-view';
import { cardContext } from './card-context';
import { cardMedia } from './feed-card';
import { feedItems, markActive, recordFeedEvent, recordUi, startTimer, takeOpenPaintMs } from './engagement';
import { ScrollGestureTracker, scrollMetadata, type ScrollGesture } from './scroll-gestures';
import { cardLinks } from './links';
import { parseMarkdown, previewSpans, type Span } from './markdown';
import type { FeedEmbed } from './rich';
import {
  ACCENT,
  LIKE_RED,
  MediaCarousel,
  MediaGrid,
  PostAvatar,
  PostByline,
  PostMetrics,
  PostText,
  QuotePost,
  SourceAvatar,
  type MediaAction,
} from './rich-views';
import { LikeHeart } from './like-heart';
import { toggleLiked, useLikedIds } from './liked';
import { toggleSaved, useSavedIds } from './saved';
import { openTrackedBrowser } from './tracked-browser';
import { shareCard } from './share-sheet';
import { cardVideo, videoAspect } from './video';
import { InlineVideo } from './video-player';

function fullDate(iso: string | undefined): string {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  return `${d.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' })} · ${d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })}`;
}

// The card's summary as plain text, as the feed card shares it.
function sharePreview(item: FeedItem): string {
  const { preview } = cardContext(item);
  return preview ? previewSpans(preview).map((s) => s.text).join('') : '';
}

// One post of the detail view; thread posts hang off a line from the avatar.
function DetailPost({
  embed,
  first,
  more,
  playerKey,
  onLink,
  onMedia,
}: {
  embed: FeedEmbed;
  first: boolean;
  more: boolean;
  playerKey: string;
  onLink: (url: string) => void;
  onMedia: (a: MediaAction) => void;
}) {
  const theme = useTheme();
  return (
    <View style={styles.postRow}>
      <View style={styles.postGutter}>
        <PostAvatar embed={embed} size={first ? 44 : 40} />
        {more ? <View style={[styles.threadLine, { backgroundColor: theme.backgroundSelected }]} /> : null}
      </View>
      <View style={styles.postMain}>
        <PostByline embed={embed} big={first} />
        {embed.text ? <PostText text={embed.text} style={first ? styles.postTextBig : styles.postText} onLink={onLink} /> : null}
        {embed.media?.length ? (
          <View style={styles.postMedia}>
            <MediaGrid media={embed.media} onMedia={onMedia} playerKey={playerKey} />
          </View>
        ) : null}
        {embed.quote ? <QuotePost embed={embed.quote} onLink={onLink} onMedia={onMedia} playerKey={`${playerKey}:quote`} /> : null}
        {first ? (
          <ThemedText style={[styles.postDate, { color: theme.textSecondary }]}>{fullDate(embed.created_at)}</ThemedText>
        ) : null}
        {first ? <PostMetrics embed={embed} /> : null}
      </View>
    </View>
  );
}

function Spans({ spans, onLink }: { spans: Span[]; onLink: (url: string) => void }) {
  return (
    <>
      {spans.map((span, i) => (
        <ThemedText
          key={i}
          onPress={span.url ? () => onLink(span.url as string) : undefined}
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

// The detail view a tap on a card opens: the whole X post and its thread,
// every photo, the full summary, the whole article inline (article-view)
// and all of the card's links. Time here is
// the card's reading time (an open, then a close carrying dwell_ms and how
// far down it was read). dwell_ms is active time: time with the app in the
// background, or the screen untouched past a minute, does not count.
export default function FeedReaderScreen() {
  const insets = useSafeAreaInsets();
  const params = useLocalSearchParams<{ id: string; position?: string }>();
  const theme = useTheme();
  const item = feedItems.get(params.id);
  const depth = useRef(0);
  // Watch and link time from this view, recorded as closes of their own and
  // taken out of the view's reading time so it is not counted twice.
  const nested = useRef(0);
  const gestures = useRef(new ScrollGestureTracker());
  const settleTimer = useRef<ReturnType<typeof setTimeout>>(undefined);
  const contentHeight = useRef(0);
  const position = params.position ? Number(params.position) : undefined;
  const saved = useSavedIds().includes(params.id);
  const liked = useLikedIds().includes(params.id);

  useFocusEffect(
    useCallback(() => {
      if (!item) return undefined;
      depth.current = 0;
      nested.current = 0;
      recordFeedEvent('open', { item_id: item.item_id, position, metadata: { mode: 'detail' } });
      const stop = startTimer();
      return () => {
        const { dwell_ms, wall_ms } = stop();
        recordFeedEvent('close', {
          item_id: item.item_id,
          dwell_ms: Math.max(0, dwell_ms - nested.current),
          metadata: { mode: 'detail', scroll_depth: Math.round(depth.current * 100) / 100, wall_ms, nested_ms: nested.current },
        });
      };
    }, [item, position]),
  );

  // The frame after the first layout is the first paint of this view.
  const onFirstLayout = () => {
    if (!item) return;
    requestAnimationFrame(() => {
      const ms = takeOpenPaintMs(item.item_id);
      if (ms !== null) recordUi('open_paint', { item_id: item.item_id, position, dwell_ms: Math.round(ms) });
    });
  };

  const onScroll = (e: NativeSyntheticEvent<NativeScrollEvent>) => {
    markActive();
    const { contentOffset, contentSize, layoutMeasurement } = e.nativeEvent;
    const seen = contentSize.height <= 0 ? 1 : (contentOffset.y + layoutMeasurement.height) / contentSize.height;
    depth.current = Math.max(depth.current, Math.min(1, seen));
  };

  const itemId = item?.item_id;
  // Each scroll gesture in the detail view, like the feed's, with how far
  // down the page it ended.
  const recordScroll = (g: ScrollGesture | null) => {
    if (!g || !itemId) return;
    recordFeedEvent('scroll', {
      item_id: itemId,
      position,
      at: g.startedAt,
      dwell_ms: g.durationMs,
      metadata: scrollMetadata(g, { in: 'detail', content_height: Math.round(contentHeight.current) }),
    });
  };
  const scrollHandlers = {
    onScrollBeginDrag: (e: NativeSyntheticEvent<NativeScrollEvent>) => {
      clearTimeout(settleTimer.current);
      recordScroll(gestures.current.beginDrag(Date.now(), e.nativeEvent.contentOffset.y));
    },
    onScrollEndDrag: (e: NativeSyntheticEvent<NativeScrollEvent>) => {
      gestures.current.endDrag(Date.now(), e.nativeEvent.contentOffset.y, e.nativeEvent.velocity?.y ?? 0);
      settleTimer.current = setTimeout(() => recordScroll(gestures.current.settle()), 80);
    },
    onMomentumScrollBegin: () => {
      clearTimeout(settleTimer.current);
      gestures.current.momentumBegin();
    },
    onMomentumScrollEnd: (e: NativeSyntheticEvent<NativeScrollEvent>) =>
      recordScroll(gestures.current.momentumEnd(Date.now(), e.nativeEvent.contentOffset.y)),
  };
  const browse = useCallback(
    async (url: string, linkIndex: number, via = 'detail') => {
      if (!itemId) return;
      const which = { mode: 'browser', via, url, link_index: linkIndex };
      recordFeedEvent('open', { item_id: itemId, position, metadata: which });
      const stop = startTimer();
      let seen = { pages: 0, final_url: url, external: false };
      try {
        seen = await openTrackedBrowser(url, { item_id: itemId, position, via, link_index: linkIndex });
      } catch {
        await Linking.openURL(url);
      }
      const { dwell_ms, wall_ms } = stop();
      nested.current += dwell_ms;
      recordFeedEvent('close', { item_id: itemId, dwell_ms, metadata: { ...which, wall_ms, ...seen } });
    },
    [itemId, position],
  );
  const onMedia = useCallback(
    async (a: MediaAction) => {
      if (!itemId) return;
      if (a.action === 'stop') {
        nested.current += a.dwellMs ?? 0;
        recordFeedEvent('close', { item_id: itemId, dwell_ms: a.dwellMs, metadata: { mode: 'video', index: a.index, inline: true, in: 'detail' } });
        return;
      }
      recordFeedEvent('media', {
        item_id: itemId,
        position,
        dwell_ms: a.dwellMs,
        metadata: { action: a.action, index: a.index, in: 'detail', ...(a.inline ? { inline: true } : {}) },
      });
      if (a.action !== 'play' || !a.url || a.inline) return;
      const stop = startTimer(true);
      await WebBrowser.openBrowserAsync(a.url, { dismissButtonStyle: 'close' });
      const { dwell_ms, wall_ms } = stop();
      nested.current += dwell_ms;
      recordFeedEvent('close', { item_id: itemId, dwell_ms, metadata: { mode: 'video', index: a.index, wall_ms } });
    },
    [itemId, position],
  );

  if (!item) {
    return (
      <ThemedView style={styles.center}>
        <ThemedText>This card is no longer loaded. Go back and refresh the feed.</ThemedText>
      </ThemedView>
    );
  }

  const onPostLink = (url: string) => void browse(url, -1, 'post_link');
  const onBodyLink = (url: string) => void browse(url, -1, 'body_link');
  const onArticleLink = (url: string) => void browse(url, -1, 'article_link');
  // Whether the inline article loaded, its length and how long it took.
  const onArticle = (o: ArticleOutcome) =>
    recordUi('article', {
      item_id: item.item_id,
      position,
      dwell_ms: o.ms,
      metadata: o.ok ? { ok: true, words: o.words, offline: o.offline } : { ok: false, error: o.error.slice(0, 300), offline: o.offline },
    });

  const embed = item.embed?.type === 'x_post' ? item.embed : undefined;
  const media = cardMedia(item);
  const video = embed ? null : cardVideo(item);
  const links = cardLinks(item);
  const primary = links.find((link) => link.index === 0);
  const name = item.site || item.source;
  const posts = embed ? [embed, ...(embed.thread ?? [])] : [];
  const article = articleUrl(item, !!video);

  return (
    <ThemedView style={styles.fill} onTouchStart={markActive}>
      <Stack.Screen
        options={{
          title: embed ? 'Post' : name,
          headerRight: () => (
            <View style={styles.headerButtons}>
              <Pressable
                hitSlop={12}
                onPress={() => toggleLiked(item, { position, via: 'detail' })}
                accessibilityLabel={liked ? 'Unlike' : 'Like'}>
                <LikeHeart itemId={item.item_id} liked={liked} size={22} tint={theme.text} likedTint={LIKE_RED} />
              </Pressable>
              <Pressable
                hitSlop={12}
                onPress={() => toggleSaved(item, { position, via: 'detail' })}
                accessibilityLabel={saved ? 'Remove from Saved' : 'Save for later'}>
                <Icon name={saved ? 'bookmark.fill' : 'bookmark'} tintColor={saved ? ACCENT : theme.text} size={22} />
              </Pressable>
              <Pressable
                hitSlop={12}
                onPress={() => void shareCard(item, embed?.text || sharePreview(item), { position, via: 'detail' })}
                accessibilityLabel="Share">
                <Icon name="square.and.arrow.up" tintColor={theme.text} size={22} />
              </Pressable>
            </View>
          ),
        }}
      />
      <ScrollView
        contentContainerStyle={styles.content}
        onLayout={onFirstLayout}
        onScroll={onScroll}
        {...scrollHandlers}
        scrollEventThrottle={100}
        onContentSizeChange={(_, h) => {
          contentHeight.current = h;
          if (h > 0) depth.current = Math.max(depth.current, Math.min(1, 700 / h));
        }}>
        {embed ? (
          <View style={styles.pad}>
            {item.title ? (
              <ThemedText style={[styles.context, { color: theme.textSecondary }]}>{item.title}</ThemedText>
            ) : null}
            {posts.map((post, i) => (
              <DetailPost
                key={post.id || i}
                embed={post}
                first={i === 0}
                more={i < posts.length - 1}
                playerKey={`detail:${item.item_id}:${i}`}
                onLink={onPostLink}
                onMedia={onMedia}
              />
            ))}
          </View>
        ) : (
          <>
            <View style={[styles.pad, styles.sourceRow]}>
              <SourceAvatar name={name} url={item.url} size={40} />
              <View style={styles.flex}>
                <ThemedText style={styles.sourceName}>{name}</ThemedText>
                <ThemedText style={[styles.byline, { color: theme.textSecondary }]}>
                  {[item.author, formatWhen(item.published_at ?? item.created_at)].filter(Boolean).join(' · ')}
                </ThemedText>
              </View>
            </View>
            {video ? (
              <InlineVideo
                source={video}
                playerKey={`detail:${item.item_id}`}
                aspect={videoAspect(video)}
                radius={0}
                durationSec={media.find((m) => m.type !== 'image')?.duration_sec}
                onPlay={() => void onMedia({ action: 'play', index: 0, inline: true })}
                onStop={(ms) => void onMedia({ action: 'stop', index: 0, inline: true, dwellMs: ms })}
              />
            ) : media.length ? (
              <MediaCarousel media={media} onMedia={onMedia} recyclingKey={`detail:${item.item_id}`} />
            ) : null}
            <View style={[styles.pad, styles.gap]}>
              {item.title ? <ThemedText style={styles.title}>{item.title}</ThemedText> : null}
              {item.tags.length ? (
                <ThemedText style={[styles.byline, { color: ACCENT }]}>{item.tags.map((t) => `#${t}`).join('  ')}</ThemedText>
              ) : null}
              {parseMarkdown(item.body ?? '').map((block, i) => (
                <Fragment key={i}>
                  {block.kind === 'heading' ? (
                    <ThemedText style={[styles.heading, block.level <= 2 && styles.headingBig]}>
                      <Spans spans={block.spans} onLink={onBodyLink} />
                    </ThemedText>
                  ) : block.kind === 'bullet' ? (
                    <View style={styles.bulletRow}>
                      <ThemedText style={styles.paragraph}>{block.ordered ? `${block.ordered}.` : '•'}</ThemedText>
                      <ThemedText style={[styles.paragraph, styles.flex]}>
                        <Spans spans={block.spans} onLink={onBodyLink} />
                      </ThemedText>
                    </View>
                  ) : block.kind === 'quote' ? (
                    <View style={[styles.quote, { borderLeftColor: theme.backgroundSelected }]}>
                      <ThemedText style={[styles.paragraph, { color: theme.textSecondary }]}>
                        <Spans spans={block.spans} onLink={onBodyLink} />
                      </ThemedText>
                    </View>
                  ) : block.kind === 'code' ? (
                    <ThemedText style={[styles.codeBlock, { backgroundColor: theme.backgroundElement }]}>{block.text}</ThemedText>
                  ) : (
                    <ThemedText style={styles.paragraph}>
                      <Spans spans={block.spans} onLink={onBodyLink} />
                    </ThemedText>
                  )}
                </Fragment>
              ))}
            </View>
            {article ? (
              <ArticleView
                url={article}
                shownImages={media.flatMap((m) => [m.url, m.thumbnail_url ?? ''].filter(Boolean))}
                onLink={onArticleLink}
                onOutcome={onArticle}
              />
            ) : null}
          </>
        )}
        {links.length > 1 ? (
          <View style={[styles.pad, styles.links]}>
            {links.filter((link) => link.index > 0).map((link) => (
              <Pressable
                key={link.index}
                style={({ pressed }) => [
                  styles.linkRow,
                  link.index === 0 ? { backgroundColor: ACCENT } : { backgroundColor: theme.backgroundElement },
                  pressed && { opacity: 0.7 },
                ]}
                onPress={() => void browse(link.url, link.index)}>
                {link.index === 0 ? (
                  <Icon name={embed ? 'arrow.up.right.square' : 'safari'} tintColor="#fff" size={20} />
                ) : (
                  <SourceAvatar name={link.site || link.label} url={link.url} size={26} />
                )}
                <View style={styles.flex}>
                  <ThemedText style={[styles.linkLabel, link.index === 0 && { color: '#fff' }]} numberOfLines={1}>
                    {link.index === 0 ? (embed ? 'Open on X' : `Open ${link.label || 'link'}`) : link.label}
                  </ThemedText>
                  {link.index > 0 && link.site !== link.label ? (
                    <ThemedText style={[styles.linkSite, { color: theme.textSecondary }]} numberOfLines={1}>
                      {link.site}
                    </ThemedText>
                  ) : null}
                </View>
                <Icon name="chevron.right" tintColor={link.index === 0 ? '#fff' : theme.textSecondary} size={14} />
              </Pressable>
            ))}
          </View>
        ) : null}
      </ScrollView>
      {primary ? (
        // Superhuman keeps the main action in a pill floating at the thumb,
        // not at the end of the page: one tap from anywhere in the post.
        <View pointerEvents="box-none" style={[styles.floatWrap, { bottom: Math.max(insets.bottom, 10) + 6 }]}>
          <Pressable
            accessibilityRole="link"
            style={({ pressed }) => [styles.floatPill, { backgroundColor: theme.surface, borderColor: theme.surfaceBorder }, pressed && { opacity: 0.8 }]}
            onPress={() => void browse(primary.url, primary.index)}>
            <Icon name={embed ? 'arrow.up.right.square' : 'safari'} tintColor={ACCENT} size={19} />
            <ThemedText style={styles.floatLabel} numberOfLines={1}>
              {embed ? 'Open on X' : `Open ${primary.label || 'link'}`}
            </ThemedText>
          </Pressable>
        </View>
      ) : null}
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  fill: { flex: 1 },
  flex: { flex: 1 },
  headerButtons: { flexDirection: 'row', alignItems: 'center', gap: 18 },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center', padding: Spacing.four },
  content: { paddingTop: Spacing.three, paddingBottom: 120, gap: Spacing.three },
  floatWrap: { position: 'absolute', left: 0, right: 0, alignItems: 'center' },
  floatPill: { flexDirection: 'row', alignItems: 'center', gap: 8, height: 50, maxWidth: '86%', paddingHorizontal: 22, borderRadius: 25, borderWidth: StyleSheet.hairlineWidth, elevation: 8, shadowColor: '#000', shadowOpacity: 0.25, shadowRadius: 12, shadowOffset: { width: 0, height: 4 } },
  floatLabel: { fontSize: 16, fontWeight: '600', flexShrink: 1 },
  pad: { paddingHorizontal: Spacing.three },
  gap: { gap: Spacing.three },
  sourceRow: { flexDirection: 'row', alignItems: 'center', gap: 10 },
  sourceName: { fontSize: 15, fontWeight: '700' },
  context: { fontSize: 14, fontWeight: '600', marginBottom: Spacing.two },
  postRow: { flexDirection: 'row', gap: 10 },
  postGutter: { alignItems: 'center' },
  threadLine: { width: 2, flex: 1, marginTop: 4, marginBottom: -Spacing.two, borderRadius: 1 },
  postMain: { flex: 1, paddingBottom: Spacing.three },
  postText: { fontSize: 16, lineHeight: 22, marginTop: 2 },
  postTextBig: { fontSize: 19, lineHeight: 26, marginTop: 4 },
  postMedia: { marginTop: Spacing.two },
  postDate: { fontSize: 14, marginTop: Spacing.three },
  links: { gap: Spacing.two },
  linkRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, paddingHorizontal: Spacing.three, paddingVertical: 12, borderRadius: 12 },
  linkLabel: { fontSize: 15, fontWeight: '600', color: ACCENT },
  linkSite: { fontSize: 12 },
  byline: { fontSize: 13 },
  title: { fontSize: 26, lineHeight: 32, fontWeight: '700' },
  heading: { fontSize: 18, lineHeight: 24, fontWeight: '700', marginTop: Spacing.two },
  headingBig: { fontSize: 21, lineHeight: 27 },
  paragraph: { fontSize: 17, lineHeight: 26 },
  bulletRow: { flexDirection: 'row', gap: Spacing.two },
  quote: { borderLeftWidth: 3, paddingLeft: Spacing.three },
  codeBlock: { fontFamily: 'Menlo', fontSize: 14, padding: Spacing.three, borderRadius: 8 },
  bold: { fontWeight: '700' },
  italic: { fontStyle: 'italic' },
  code: { fontFamily: 'Menlo', fontSize: 15 },
  link: { color: '#36A0DA' },
});
