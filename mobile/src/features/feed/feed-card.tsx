import { SymbolView } from 'expo-symbols';
import { memo, useMemo, useRef, useState } from 'react';
import { Pressable, StyleSheet, View, useWindowDimensions, type LayoutChangeEvent } from 'react-native';
import ReanimatedSwipeable, { SwipeDirection, type SwipeableMethods } from 'react-native-gesture-handler/ReanimatedSwipeable';

import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';

import type { FeedItem } from './api';
import { cardContext, profileUrl, type CardContext } from './card-context';
import { cardLinks, type CardLink } from './links';
import { previewSpans } from './markdown';
import { compactCount, shortAge, type FeedMedia } from './rich';
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
import { cardVideo } from './video';
import { InlineVideo } from './video-player';

const PREVIEW_LINES = 4;
const POST_LINES = 8;
const AVATAR = 36;
const GAP = 12;

// Laid out like Muse's feed (Zach, voice memo 2026-10-23: "I like how the
// Muse feed is displayed"): a row per card, the source's icon in a column on
// the left and everything else beside it: the headline with its age and a
// "..." menu on one line, a few lines of text with its links in blue, the
// picture or video inset with rounded corners, and a row of actions led by
// the heart. An X post keeps X's own shape, which is the same row.
// A tap opens the detail view; a video plays in place (video.ts).
// Swiping left hides the card (Undo brings it back); swiping right saves it
// for later (not on a carousel, whose swipe pages the photos).
type Props = {
  item: FeedItem;
  saved: boolean;
  liked: boolean;
  onSave: (item: FeedItem, via: string) => void;
  onLike: (item: FeedItem, via: string) => void;
  onDismiss: (item: FeedItem, reason: string) => void;
  onLayout: (itemId: string, height: number) => void;
  onOpen: (item: FeedItem) => void;
  onBrowse: (item: FeedItem, link?: CardLink, via?: string) => void;
  onMedia: (item: FeedItem, action: MediaAction) => void;
  onExpand: (item: FeedItem) => void;
  onMenu: (item: FeedItem) => void;
  onShare: (item: FeedItem, preview: string) => void;
  onSwipeStart?: (item: FeedItem, toward: 'save' | 'dismiss') => void;
};

function HideAction() {
  return (
    <View style={[styles.swipe, styles.hide]}>
      <SymbolView name="eye.slash.fill" tintColor="#fff" size={22} />
      <ThemedText style={styles.swipeLabel}>Hide</ThemedText>
    </View>
  );
}

function SaveAction({ saved }: { saved: boolean }) {
  return (
    <View style={[styles.swipe, styles.save]}>
      <SymbolView name={saved ? 'bookmark.slash' : 'bookmark.fill'} tintColor="#fff" size={22} />
      <ThemedText style={styles.swipeLabel}>{saved ? 'Unsave' : 'Save'}</ThemedText>
    </View>
  );
}

export function cardMedia(item: FeedItem): FeedMedia[] {
  if (item.media?.length) return item.media;
  return item.image_url ? [{ type: 'image', url: item.image_url }] : [];
}

// The heart: his own "I like this", the one signal he gives on purpose.
export function LikeButton({ liked, onPress, size = 20 }: { liked: boolean; onPress: () => void; size?: number }) {
  const theme = useTheme();
  return (
    <Pressable hitSlop={10} onPress={onPress} accessibilityLabel={liked ? 'Unlike' : 'Like'} accessibilityState={{ selected: liked }} style={styles.action}>
      <SymbolView name={liked ? 'heart.fill' : 'heart'} tintColor={liked ? LIKE_RED : theme.textSecondary} size={size} />
    </Pressable>
  );
}

// The line above a card saying why it is here, like X's "reposted" line.
function ContextLine({ label, inset }: { label: string; inset: number }) {
  const theme = useTheme();
  return (
    <View style={[styles.context, { paddingLeft: inset }]}>
      <SymbolView name="sparkles" tintColor={theme.textSecondary} size={13} />
      <ThemedText style={[styles.contextText, { color: theme.textSecondary }]} numberOfLines={2}>
        {label}
      </ThemedText>
    </View>
  );
}

// Points and comments as counts, like X's row; a tap opens the discussion.
function HnCounts({ hn, onPress }: { hn: NonNullable<CardContext['hn']>; onPress?: () => void }) {
  const theme = useTheme();
  return (
    <Pressable
      hitSlop={8}
      disabled={!onPress}
      onPress={onPress}
      accessibilityLabel={`${hn.points} points, ${hn.comments} comments on Hacker News`}
      style={({ pressed }) => [styles.hn, pressed && styles.pressed]}>
      <SymbolView name="arrowtriangle.up.fill" tintColor="#FF6600" size={13} />
      <ThemedText style={[styles.hnText, { color: theme.textSecondary }]}>{compactCount(hn.points) || '0'}</ThemedText>
      <SymbolView name="bubble.right" tintColor={theme.textSecondary} size={14} />
      <ThemedText style={[styles.hnText, { color: theme.textSecondary }]}>{compactCount(hn.comments) || '0'}</ThemedText>
    </Pressable>
  );
}

function FeedCardImpl({
  item,
  saved,
  liked,
  onSave,
  onLike,
  onDismiss,
  onLayout,
  onOpen,
  onBrowse,
  onMedia,
  onExpand,
  onMenu,
  onShare,
  onSwipeStart,
}: Props) {
  const theme = useTheme();
  const { width: screen } = useWindowDimensions();
  const swipeable = useRef<SwipeableMethods>(null);
  const [expanded, setExpanded] = useState(false);
  const [truncated, setTruncated] = useState(false);
  const ctx = useMemo(() => cardContext(item), [item]);
  const spans = useMemo(() => (ctx.preview ? previewSpans(ctx.preview) : []), [ctx.preview]);
  const preview = useMemo(() => spans.map((s) => s.text).join(''), [spans]);
  const extraLinks = useMemo(() => cardLinks(item).filter((link) => link.index > 0), [item]);
  const media = useMemo(() => cardMedia(item), [item]);
  const embed = item.embed?.type === 'x_post' ? item.embed : undefined;
  const video = useMemo(() => (embed ? null : cardVideo(item)), [embed, item]);
  const carousel = !embed && !video && media.length > 1;
  // The column beside the icon, where the media sits.
  const column = screen - Spacing.three * 2 - AVATAR - GAP;

  const open = () => onOpen(item);
  const more = () => {
    setExpanded(true);
    onExpand(item);
  };
  const share = () => onShare(item, embed?.text || preview);
  const save = () => onSave(item, 'button');
  const like = () => onLike(item, 'button');
  const browseUrl = (url: string, via: 'post_link' | 'body_link' = 'post_link') =>
    onBrowse(item, { url, label: url, site: '', index: -1 }, via);
  const reportMedia = (a: MediaAction) => onMedia(item, a);
  const profile = embed ? profileUrl(embed) : '';
  const openProfile = profile ? () => onBrowse(item, { url: profile, label: `@${embed?.author_handle}`, site: 'x.com', index: -1 }) : undefined;
  const hn = ctx.hn;
  const openHn = hn?.url ? () => onBrowse(item, { url: hn.url, label: 'Hacker News discussion', site: 'news.ycombinator.com', index: -1 }) : undefined;
  const lines = (n: number) => {
    if (!expanded && n >= (embed ? POST_LINES : PREVIEW_LINES)) setTruncated(true);
  };

  let content: React.ReactNode;
  if (embed) {
    content = (
      <Pressable onPress={open} style={({ pressed }) => [styles.pad, pressed && styles.pressed]}>
        {item.title ? <ContextLine label={item.title} inset={AVATAR + GAP - 18} /> : null}
        <View style={styles.row}>
          <Pressable onPress={openProfile} disabled={!openProfile} accessibilityLabel={`Open @${embed.author_handle} on X`}>
            <PostAvatar embed={embed} size={AVATAR + 4} />
          </Pressable>
          <View style={styles.main}>
            <View style={styles.head}>
              <Pressable onPress={openProfile} disabled={!openProfile} style={styles.flexShrink}>
                <PostByline embed={embed} />
              </Pressable>
              <Pressable hitSlop={12} onPress={() => onMenu(item)} accessibilityLabel="More">
                <SymbolView name="ellipsis" tintColor={theme.textSecondary} size={16} />
              </Pressable>
            </View>
            {embed.replying_to ? (
              <ThemedText style={[styles.replying, { color: theme.textSecondary }]}>
                Replying to <ThemedText style={{ color: ACCENT }}>@{embed.replying_to}</ThemedText>
              </ThemedText>
            ) : null}
            {embed.text ? (
              <PostText
                text={embed.text}
                style={styles.postText}
                numberOfLines={expanded ? undefined : POST_LINES}
                onLink={(url) => browseUrl(url)}
                onTextLayout={lines}
              />
            ) : null}
            {truncated && !expanded ? (
              <Pressable hitSlop={8} onPress={more}>
                <ThemedText style={styles.showMore}>Show more</ThemedText>
              </Pressable>
            ) : null}
            {embed.media?.length ? (
              <View style={styles.media}>
                <MediaGrid media={embed.media} onMedia={reportMedia} recyclingKey={item.item_id} />
              </View>
            ) : null}
            {embed.quote ? (
              <QuotePost embed={embed.quote} onPress={open} onLink={(url) => browseUrl(url)} onMedia={reportMedia} playerKey={`${item.item_id}:quote`} />
            ) : null}
            {embed.thread?.length ? (
              <ThemedText style={[styles.thread, { color: ACCENT }]}>Show this thread ({embed.thread.length + 1} posts)</ThemedText>
            ) : null}
            <PostMetrics embed={embed} onShare={share} onSave={save} saved={saved} onLike={like} liked={liked} />
          </View>
        </View>
      </Pressable>
    );
  } else {
    const name = item.site || item.source;
    const age = shortAge(item.published_at ?? item.created_at);
    // A resurfaced card is one he saw and passed over, back because the
    // unseen ones ran out (server Store.Feed).
    const sub = [item.title ? name : '', ctx.author, item.site && item.source !== item.site ? item.source : '', item.resurfaced ? 'Seen before' : '']
      .filter(Boolean)
      .join(' · ');
    content = (
      <View style={styles.pad}>
        {ctx.label ? <ContextLine label={ctx.label} inset={AVATAR + GAP - 18} /> : null}
        <View style={styles.row}>
          <Pressable onPress={item.url ? () => onBrowse(item) : open} accessibilityLabel={`Open ${name}`} style={styles.avatar}>
            <SourceAvatar name={name} url={item.url} size={AVATAR} />
          </Pressable>
          <View style={styles.main}>
            <Pressable onPress={open} style={({ pressed }) => pressed && styles.pressed}>
              <View style={styles.head}>
                <ThemedText style={styles.title} numberOfLines={3}>
                  {item.title || name}
                </ThemedText>
                <View style={styles.meta}>
                  {age ? <ThemedText style={[styles.age, { color: theme.textSecondary }]}>{age}</ThemedText> : null}
                  <Pressable hitSlop={12} onPress={() => onMenu(item)} accessibilityLabel="More">
                    <SymbolView name="ellipsis" tintColor={theme.textSecondary} size={16} />
                  </Pressable>
                </View>
              </View>
              {sub ? (
                <ThemedText style={[styles.sub, { color: theme.textSecondary }]} numberOfLines={1}>
                  {sub}
                </ThemedText>
              ) : null}
              {spans.length ? (
                <ThemedText
                  style={styles.body}
                  numberOfLines={expanded ? undefined : PREVIEW_LINES}
                  onTextLayout={(e) => lines(e.nativeEvent.lines.length)}>
                  {spans.map((s, i) =>
                    s.url ? (
                      <ThemedText key={i} style={[styles.body, styles.link]} onPress={() => browseUrl(s.url as string, 'body_link')}>
                        {s.text}
                      </ThemedText>
                    ) : s.bold ? (
                      <ThemedText key={i} style={[styles.body, styles.bold]}>
                        {s.text}
                      </ThemedText>
                    ) : (
                      s.text
                    ),
                  )}
                </ThemedText>
              ) : null}
            </Pressable>
            {truncated && !expanded ? (
              <Pressable hitSlop={8} onPress={more}>
                <ThemedText style={[styles.more, { color: theme.textSecondary }]}>more</ThemedText>
              </Pressable>
            ) : null}
            {video ? (
              <View style={styles.media}>
                <InlineVideo
                  source={video}
                  playerKey={item.item_id}
                  aspect={video.kind === 'file' ? 1 : 16 / 9}
                  durationSec={media.find((m) => m.type !== 'image')?.duration_sec}
                  onPlay={() => reportMedia({ action: 'play', index: 0, inline: true })}
                  onStop={(ms) => reportMedia({ action: 'stop', index: 0, inline: true, dwellMs: ms })}
                />
              </View>
            ) : media.length ? (
              <View style={styles.media}>
                <MediaCarousel media={media} onPress={open} onMedia={reportMedia} recyclingKey={item.item_id} width={column} radius={14} />
              </View>
            ) : null}
            {extraLinks.length ? (
              <View style={styles.chips}>
                {extraLinks.map((link) => (
                  <Pressable
                    key={link.index}
                    onPress={() => onBrowse(item, link)}
                    accessibilityLabel={`Open ${link.label}`}
                    style={({ pressed }) => [styles.chip, { backgroundColor: theme.backgroundElement }, pressed && styles.pressed]}>
                    <SymbolView name="link" tintColor={ACCENT} size={13} />
                    <ThemedText style={styles.chipText} numberOfLines={1}>
                      {link.label}
                    </ThemedText>
                  </Pressable>
                ))}
              </View>
            ) : null}
            <View style={styles.actions}>
              <LikeButton liked={liked} onPress={like} />
              <Pressable hitSlop={10} onPress={open} accessibilityLabel="Details" style={[styles.action, styles.details]}>
                <SymbolView name="text.bubble" tintColor={theme.textSecondary} size={19} />
                <ThemedText style={[styles.actionText, { color: theme.textSecondary }]}>Details</ThemedText>
              </Pressable>
              {item.url ? (
                <Pressable hitSlop={10} onPress={() => onBrowse(item)} accessibilityLabel="Open page" style={styles.action}>
                  <SymbolView name="safari" tintColor={theme.textSecondary} size={19} />
                </Pressable>
              ) : null}
              <Pressable hitSlop={10} onPress={share} accessibilityLabel="Share" style={styles.action}>
                <SymbolView name="paperplane" tintColor={theme.textSecondary} size={19} />
              </Pressable>
              <Pressable hitSlop={10} onPress={save} accessibilityLabel={saved ? 'Remove from Saved' : 'Save for later'} style={styles.action}>
                <SymbolView name={saved ? 'bookmark.fill' : 'bookmark'} tintColor={saved ? ACCENT : theme.textSecondary} size={19} />
              </Pressable>
              <View style={styles.flex} />
              {hn ? <HnCounts hn={hn} onPress={openHn} /> : null}
            </View>
          </View>
        </View>
      </View>
    );
  }

  return (
    <ReanimatedSwipeable
      ref={swipeable}
      enabled={!carousel}
      friction={1.5}
      rightThreshold={80}
      leftThreshold={60}
      overshootRight={false}
      overshootLeft={false}
      renderRightActions={() => <HideAction />}
      renderLeftActions={() => <SaveAction saved={saved} />}
      onSwipeableOpenStartDrag={(direction) => onSwipeStart?.(item, direction === SwipeDirection.RIGHT ? 'save' : 'dismiss')}
      onSwipeableOpen={(direction) => {
        swipeable.current?.close();
        // The direction the card moved: right reveals the left (save) action.
        if (direction === SwipeDirection.RIGHT) onSave(item, 'swipe');
        else onDismiss(item, 'swipe');
      }}>
      <View
        style={[styles.wrap, { borderBottomColor: theme.backgroundSelected, backgroundColor: theme.background }]}
        onLayout={(e: LayoutChangeEvent) => onLayout(item.item_id, e.nativeEvent.layout.height)}>
        {content}
      </View>
    </ReanimatedSwipeable>
  );
}

export const FeedCard = memo(FeedCardImpl);

const styles = StyleSheet.create({
  wrap: { paddingTop: 14, paddingBottom: 6, borderBottomWidth: StyleSheet.hairlineWidth },
  pad: { paddingHorizontal: Spacing.three },
  row: { flexDirection: 'row', gap: GAP },
  avatar: { paddingTop: 2 },
  main: { flex: 1, minWidth: 0 },
  head: { flexDirection: 'row', alignItems: 'flex-start', justifyContent: 'space-between', gap: 8 },
  meta: { flexDirection: 'row', alignItems: 'center', gap: 10, paddingTop: 2 },
  flex: { flex: 1 },
  flexShrink: { flexShrink: 1 },
  pressed: { opacity: 0.8 },
  title: { flex: 1, fontSize: 16, lineHeight: 21, fontWeight: '600' },
  age: { fontSize: 13, lineHeight: 17 },
  sub: { fontSize: 13, lineHeight: 17, marginTop: 1 },
  body: { fontSize: 15, lineHeight: 21, marginTop: 4 },
  link: { color: ACCENT, marginTop: 0 },
  bold: { fontWeight: '700', marginTop: 0 },
  more: { paddingTop: 2, fontSize: 15, fontWeight: '600' },
  media: { marginTop: 10 },
  chips: { flexDirection: 'row', flexWrap: 'wrap', gap: 6, paddingTop: Spacing.two },
  chip: { flexDirection: 'row', alignItems: 'center', gap: 5, paddingHorizontal: 10, paddingVertical: 6, borderRadius: 14, maxWidth: '100%' },
  chipText: { fontSize: 13, fontWeight: '600', color: ACCENT, flexShrink: 1, lineHeight: 17 },
  actions: { flexDirection: 'row', alignItems: 'center', marginLeft: -Spacing.two, paddingTop: 4 },
  action: { padding: Spacing.two, flexDirection: 'row', alignItems: 'center' },
  details: { gap: 5 },
  actionText: { fontSize: 13, fontWeight: '600', lineHeight: 17 },
  context: { flexDirection: 'row', alignItems: 'center', gap: 6, paddingBottom: 6 },
  contextText: { fontSize: 13, fontWeight: '600', flexShrink: 1, lineHeight: 17 },
  replying: { fontSize: 14, lineHeight: 19 },
  postText: { fontSize: 15, lineHeight: 20, marginTop: 2 },
  showMore: { fontSize: 15, color: ACCENT, marginTop: 2 },
  thread: { fontSize: 14, marginTop: Spacing.two },
  swipe: { width: 96, alignItems: 'center', justifyContent: 'center', gap: Spacing.one },
  swipeLabel: { color: '#fff', fontSize: 13, fontWeight: '600' },
  hide: { backgroundColor: '#FF3B30' },
  save: { backgroundColor: '#FF9500' },
  hn: { flexDirection: 'row', alignItems: 'center', gap: 5, paddingVertical: Spacing.one },
  hnText: { fontSize: 13, fontWeight: '600', marginRight: 6 },
});
