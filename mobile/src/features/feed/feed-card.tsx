import { SymbolView } from 'expo-symbols';
import { memo, useMemo, useRef, useState } from 'react';
import { Pressable, StyleSheet, View, type LayoutChangeEvent } from 'react-native';
import ReanimatedSwipeable, { SwipeDirection, type SwipeableMethods } from 'react-native-gesture-handler/ReanimatedSwipeable';

import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';

import type { FeedItem } from './api';
import { cardLinks, type CardLink } from './links';
import { plainText } from './markdown';
import { shortAge, type FeedMedia } from './rich';
import {
  ACCENT,
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

const PREVIEW_LINES = 3;
const POST_LINES = 8;

// Three shapes of card, after the feeds he scrolls most:
// - an embedded X post, laid out like X (avatar, name, text, media grid,
//   quoted post, counts), read in place rather than opened in X;
// - a media card, laid out like Instagram (source row, full-bleed photo or
//   swipeable carousel, then the headline and a short caption);
// - a link card (source row, headline, a few lines, the extra links).
// A tap anywhere opens the detail view (a photo in a post opens full
// screen); the compass opens the page itself.
// Swiping left shares and swiping right saves it for later (not on a
// carousel, whose swipe pages the photos); the bookmark does the same.
type Props = {
  item: FeedItem;
  saved: boolean;
  onSave: (item: FeedItem, via: string) => void;
  onLayout: (itemId: string, height: number) => void;
  onOpen: (item: FeedItem) => void;
  onBrowse: (item: FeedItem, link?: CardLink) => void;
  onMedia: (item: FeedItem, action: MediaAction) => void;
  onExpand: (item: FeedItem) => void;
  onMenu: (item: FeedItem) => void;
  onShare: (item: FeedItem, preview: string) => void;
  onSwipeStart?: (item: FeedItem, toward: 'save' | 'share') => void;
};

function ShareAction() {
  return (
    <View style={styles.share}>
      <SymbolView name="paperplane" tintColor="#fff" size={22} />
      <ThemedText style={styles.shareLabel}>Share</ThemedText>
    </View>
  );
}

function SaveAction({ saved }: { saved: boolean }) {
  return (
    <View style={[styles.share, styles.save]}>
      <SymbolView name={saved ? 'bookmark.slash' : 'bookmark.fill'} tintColor="#fff" size={22} />
      <ThemedText style={styles.shareLabel}>{saved ? 'Unsave' : 'Save'}</ThemedText>
    </View>
  );
}

export function cardMedia(item: FeedItem): FeedMedia[] {
  if (item.media?.length) return item.media;
  return item.image_url ? [{ type: 'image', url: item.image_url }] : [];
}

function SourceRow({ item, onMenu }: { item: FeedItem; onMenu: () => void }) {
  const theme = useTheme();
  const name = item.site || item.source;
  const age = shortAge(item.published_at ?? item.created_at);
  // A resurfaced card is one he saw and passed over, back because the
  // unseen ones ran out (server Store.Feed).
  const sub = [item.resurfaced ? 'Seen before' : '', item.author, item.site && item.source !== item.site ? item.source : '']
    .filter(Boolean)
    .join(' · ');
  return (
    <View style={styles.sourceRow}>
      <SourceAvatar name={name} url={item.url} size={34} />
      <View style={styles.sourceText}>
        <ThemedText style={styles.sourceName} numberOfLines={1}>
          {name}
          <ThemedText style={[styles.sourceAge, { color: theme.textSecondary }]}>{age ? `  ${age}` : ''}</ThemedText>
        </ThemedText>
        {sub ? (
          <ThemedText style={[styles.sourceSub, { color: theme.textSecondary }]} numberOfLines={1}>
            {sub}
          </ThemedText>
        ) : null}
      </View>
      <Pressable hitSlop={12} onPress={onMenu} accessibilityLabel="Hide" style={styles.menu}>
        <SymbolView name="ellipsis" tintColor={theme.text} size={18} />
      </Pressable>
    </View>
  );
}

function FeedCardImpl({ item, saved, onSave, onLayout, onOpen, onBrowse, onMedia, onExpand, onMenu, onShare, onSwipeStart }: Props) {
  const theme = useTheme();
  const swipeable = useRef<SwipeableMethods>(null);
  const [expanded, setExpanded] = useState(false);
  const [truncated, setTruncated] = useState(false);
  const preview = useMemo(() => (item.body ? plainText(item.body) : ''), [item.body]);
  const extraLinks = useMemo(() => cardLinks(item).filter((link) => link.index > 0), [item]);
  const media = useMemo(() => cardMedia(item), [item]);
  const embed = item.embed?.type === 'x_post' ? item.embed : undefined;
  const carousel = !embed && media.length > 1;

  const open = () => onOpen(item);
  const more = () => {
    setExpanded(true);
    onExpand(item);
  };
  const share = () => onShare(item, embed?.text || preview);
  const save = () => onSave(item, 'button');
  const browseUrl = (url: string) => onBrowse(item, { url, label: url, site: '', index: -1 });
  const reportMedia = (a: MediaAction) => onMedia(item, a);
  const lines = (n: number) => {
    if (!expanded && n >= (embed ? POST_LINES : PREVIEW_LINES)) setTruncated(true);
  };

  let content: React.ReactNode;
  if (embed) {
    content = (
      <Pressable onPress={open} style={({ pressed }) => [styles.post, pressed && styles.pressed]}>
        {item.title ? (
          <View style={styles.context}>
            <SymbolView name="sparkles" tintColor={theme.textSecondary} size={13} />
            <ThemedText style={[styles.contextText, { color: theme.textSecondary }]} numberOfLines={2}>
              {item.title}
            </ThemedText>
          </View>
        ) : null}
        <View style={styles.postRow}>
          <PostAvatar embed={embed} />
          <View style={styles.postMain}>
            <View style={styles.postHead}>
              <PostByline embed={embed} />
              <Pressable hitSlop={12} onPress={() => onMenu(item)} accessibilityLabel="Hide">
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
                onLink={browseUrl}
                onTextLayout={lines}
              />
            ) : null}
            {truncated && !expanded ? (
              <Pressable hitSlop={8} onPress={more}>
                <ThemedText style={styles.showMore}>Show more</ThemedText>
              </Pressable>
            ) : null}
            {embed.media?.length ? (
              <View style={styles.postMedia}>
                <MediaGrid media={embed.media} onMedia={reportMedia} recyclingKey={item.item_id} />
              </View>
            ) : null}
            {embed.quote ? <QuotePost embed={embed.quote} onPress={open} onLink={browseUrl} onMedia={reportMedia} /> : null}
            {embed.thread?.length ? (
              <ThemedText style={[styles.thread, { color: ACCENT }]}>Show this thread ({embed.thread.length + 1} posts)</ThemedText>
            ) : null}
            <PostMetrics embed={embed} onShare={share} onSave={save} saved={saved} />
          </View>
        </View>
      </Pressable>
    );
  } else {
    content = (
      <>
        <SourceRow item={item} onMenu={() => onMenu(item)} />
        {media.length ? (
          <MediaCarousel media={media} onPress={open} onMedia={reportMedia} recyclingKey={item.item_id} />
        ) : null}
        <Pressable onPress={open} style={({ pressed }) => [styles.textBlock, pressed && styles.pressed]}>
          {item.title ? <ThemedText style={[styles.title, media.length ? styles.titleUnderMedia : null]}>{item.title}</ThemedText> : null}
          {preview ? (
            <ThemedText
              style={[styles.body, { color: theme.textSecondary }]}
              numberOfLines={expanded ? undefined : PREVIEW_LINES}
              onTextLayout={(e) => lines(e.nativeEvent.lines.length)}>
              {preview}
            </ThemedText>
          ) : null}
        </Pressable>
        {truncated && !expanded ? (
          <Pressable hitSlop={8} onPress={more}>
            <ThemedText style={styles.more}>more</ThemedText>
          </Pressable>
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
          {item.url ? (
            <Pressable hitSlop={10} onPress={() => onBrowse(item)} accessibilityLabel="Open page" style={styles.action}>
              <SymbolView name="safari" tintColor={theme.text} size={22} />
            </Pressable>
          ) : null}
          <Pressable hitSlop={10} onPress={open} accessibilityLabel="Details" style={styles.action}>
            <SymbolView name="text.alignleft" tintColor={theme.text} size={21} />
          </Pressable>
          <Pressable hitSlop={10} onPress={share} accessibilityLabel="Share" style={styles.action}>
            <SymbolView name="paperplane" tintColor={theme.text} size={21} />
          </Pressable>
          <Pressable hitSlop={10} onPress={save} accessibilityLabel={saved ? 'Remove from Saved' : 'Save for later'} style={styles.action}>
            <SymbolView name={saved ? 'bookmark.fill' : 'bookmark'} tintColor={saved ? ACCENT : theme.text} size={21} />
          </Pressable>
          <View style={styles.flex} />
          {item.tags.length ? (
            <ThemedText style={[styles.tags, { color: theme.textSecondary }]} numberOfLines={1}>
              {item.tags.slice(0, 3).map((t) => `#${t}`).join(' ')}
            </ThemedText>
          ) : null}
        </View>
      </>
    );
  }

  return (
    <ReanimatedSwipeable
      ref={swipeable}
      enabled={!carousel}
      friction={1.5}
      rightThreshold={60}
      leftThreshold={60}
      overshootRight={false}
      overshootLeft={false}
      renderRightActions={() => <ShareAction />}
      renderLeftActions={() => <SaveAction saved={saved} />}
      onSwipeableOpenStartDrag={(direction) => onSwipeStart?.(item, direction === SwipeDirection.RIGHT ? 'save' : 'share')}
      onSwipeableOpen={(direction) => {
        swipeable.current?.close();
        // The direction the card moved: right reveals the left (save) action.
        if (direction === SwipeDirection.RIGHT) onSave(item, 'swipe');
        else share();
      }}>
      <View
        style={[styles.wrap, { borderBottomColor: theme.backgroundElement, backgroundColor: theme.background }]}
        onLayout={(e: LayoutChangeEvent) => onLayout(item.item_id, e.nativeEvent.layout.height)}>
        {content}
      </View>
    </ReanimatedSwipeable>
  );
}

export const FeedCard = memo(FeedCardImpl);

const styles = StyleSheet.create({
  wrap: { paddingTop: Spacing.two, paddingBottom: Spacing.one, borderBottomWidth: 6 },
  flex: { flex: 1 },
  pressed: { opacity: 0.8 },
  sourceRow: { flexDirection: 'row', alignItems: 'center', gap: 10, paddingHorizontal: Spacing.three, paddingBottom: Spacing.two },
  sourceText: { flex: 1 },
  sourceName: { fontSize: 14, fontWeight: '700', lineHeight: 18 },
  sourceAge: { fontSize: 13, fontWeight: '400' },
  sourceSub: { fontSize: 12, lineHeight: 16 },
  menu: { padding: 4 },
  textBlock: { paddingHorizontal: Spacing.three },
  title: { fontSize: 18, lineHeight: 24, fontWeight: '700' },
  titleUnderMedia: { fontSize: 16, lineHeight: 21, marginTop: Spacing.two },
  body: { fontSize: 15, lineHeight: 21, marginTop: 4 },
  more: { paddingHorizontal: Spacing.three, paddingTop: 2, fontSize: 15, fontWeight: '600', color: '#8E8E93' },
  chips: { flexDirection: 'row', flexWrap: 'wrap', gap: 6, paddingHorizontal: Spacing.three, paddingTop: Spacing.two },
  chip: { flexDirection: 'row', alignItems: 'center', gap: 5, paddingHorizontal: 10, paddingVertical: 6, borderRadius: 14, maxWidth: '100%' },
  chipText: { fontSize: 13, fontWeight: '600', color: ACCENT, flexShrink: 1, lineHeight: 17 },
  actions: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: Spacing.two, paddingTop: Spacing.one, gap: 2 },
  action: { padding: Spacing.two },
  tags: { fontSize: 13, maxWidth: 170, marginRight: Spacing.two },
  post: { paddingHorizontal: Spacing.three, paddingBottom: Spacing.two },
  context: { flexDirection: 'row', alignItems: 'center', gap: 6, paddingLeft: 26, paddingBottom: 6 },
  contextText: { fontSize: 13, fontWeight: '600', flexShrink: 1, lineHeight: 17 },
  postRow: { flexDirection: 'row', gap: 10 },
  postMain: { flex: 1 },
  postHead: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: 8 },
  replying: { fontSize: 14, lineHeight: 19 },
  postText: { fontSize: 15, lineHeight: 20, marginTop: 2 },
  showMore: { fontSize: 15, color: ACCENT, marginTop: 2 },
  postMedia: { marginTop: Spacing.two },
  thread: { fontSize: 14, marginTop: Spacing.two },
  share: { width: 96, alignItems: 'center', justifyContent: 'center', gap: Spacing.one, backgroundColor: ACCENT },
  shareLabel: { color: '#fff', fontSize: 13, fontWeight: '600' },
  save: { backgroundColor: '#FF9500' },
});
