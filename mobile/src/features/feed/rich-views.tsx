import { Image } from 'expo-image';
import { SymbolView } from 'expo-symbols';
import { useState } from 'react';
import { Pressable, ScrollView, StyleSheet, View, useWindowDimensions, type NativeScrollEvent, type NativeSyntheticEvent } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';

import {
  avatarColor,
  compactCount,
  hostOf,
  mediaAspect,
  mediaGrid,
  postSegments,
  shortAge,
  videoLength,
  type FeedEmbed,
  type FeedMedia,
} from './rich';

export const ACCENT = '#1D9BF0';

// What the views report up so engagement can count it: a swipe to another
// photo, a tap to play a video, a tap on a link inside a post.
export type MediaAction = { action: 'swipe' | 'play'; index: number; url?: string };

// A round avatar: the site's icon over a colored letter, so a missing icon
// still reads as that source.
export function SourceAvatar({ name, url, size = 36, ring = false }: { name: string; url?: string; size?: number; ring?: boolean }) {
  const [failed, setFailed] = useState(false);
  const host = hostOf(url);
  const letter = (name.trim()[0] ?? '?').toUpperCase();
  const inner = ring ? size - 6 : size;
  const face = (
    <View style={[styles.avatar, { width: inner, height: inner, borderRadius: inner / 2, backgroundColor: avatarColor(name) }]}>
      <ThemedText style={[styles.avatarLetter, { fontSize: inner * 0.45, lineHeight: inner * 0.6 }]}>{letter}</ThemedText>
      {host && !failed ? (
        <Image
          source={{ uri: `https://icons.duckduckgo.com/ip3/${host}.ico` }}
          style={[StyleSheet.absoluteFill, { borderRadius: inner / 2, backgroundColor: '#fff' }]}
          contentFit="contain"
          onError={() => setFailed(true)}
        />
      ) : null}
    </View>
  );
  if (!ring) return face;
  return <View style={[styles.ring, { width: size, height: size, borderRadius: size / 2 }]}>{face}</View>;
}

export function PostAvatar({ embed, size = 40 }: { embed: FeedEmbed; size?: number }) {
  const name = embed.author_name || embed.author_handle || 'X';
  if (!embed.author_avatar_url) return <SourceAvatar name={name} size={size} />;
  return (
    <Image
      source={{ uri: embed.author_avatar_url }}
      style={{ width: size, height: size, borderRadius: size / 2, backgroundColor: avatarColor(name) }}
      contentFit="cover"
      transition={150}
    />
  );
}

// Post text with urls, @mentions and #tags in the accent color, tappable.
export function PostText({
  text,
  style,
  numberOfLines,
  onLink,
  onTextLayout,
}: {
  text: string;
  style?: object;
  numberOfLines?: number;
  onLink: (url: string) => void;
  onTextLayout?: (lines: number) => void;
}) {
  return (
    <ThemedText style={style} numberOfLines={numberOfLines} onTextLayout={onTextLayout ? (e) => onTextLayout(e.nativeEvent.lines.length) : undefined}>
      {postSegments(text).map((seg, i) =>
        seg.href ? (
          <ThemedText key={i} style={[style, { color: ACCENT }]} onPress={() => onLink(seg.href as string)}>
            {seg.text}
          </ThemedText>
        ) : (
          seg.text
        ),
      )}
    </ThemedText>
  );
}

function VideoBadge({ media }: { media: FeedMedia }) {
  const length = videoLength(media.duration_sec);
  return (
    <>
      <View style={styles.playWrap} pointerEvents="none">
        <View style={styles.play}>
          <SymbolView name="play.fill" tintColor="#fff" size={22} />
        </View>
      </View>
      {length || media.type === 'gif' ? (
        <View style={styles.duration} pointerEvents="none">
          <ThemedText style={styles.durationText}>{media.type === 'gif' ? 'GIF' : length}</ThemedText>
        </View>
      ) : null}
    </>
  );
}

function Still({ media, recyclingKey }: { media: FeedMedia; recyclingKey?: string }) {
  const theme = useTheme();
  const still = media.type === 'image' ? media.url : media.thumbnail_url;
  return (
    <>
      {still ? (
        <Image
          source={{ uri: still }}
          style={[StyleSheet.absoluteFill, { backgroundColor: theme.backgroundElement }]}
          contentFit="cover"
          transition={150}
          recyclingKey={recyclingKey}
          accessibilityLabel={media.alt || undefined}
          accessibilityIgnoresInvertColors
        />
      ) : (
        <View style={[StyleSheet.absoluteFill, { backgroundColor: theme.backgroundElement }]} />
      )}
      {media.type !== 'image' ? <VideoBadge media={media} /> : null}
    </>
  );
}

// X's media block: 1 to 4 photos or videos in a rounded grid. A tap on a
// photo opens the detail view; a tap on a video plays it.
export function MediaGrid({
  media,
  onPress,
  onMedia,
  recyclingKey,
}: {
  media: FeedMedia[];
  onPress?: () => void;
  onMedia: (a: MediaAction) => void;
  recyclingKey?: string;
}) {
  const theme = useTheme();
  const rows = mediaGrid(media.length);
  if (!rows.length) return null;
  const aspect = media.length === 1 ? mediaAspect(media[0], 16 / 9) : 16 / 9;
  const press = (i: number) => {
    const m = media[i];
    if (m.type !== 'image') onMedia({ action: 'play', index: i, url: m.url });
    else onPress?.();
  };
  return (
    <View style={[styles.grid, { aspectRatio: aspect, borderColor: theme.backgroundSelected }]}>
      {rows.map((row, r) => (
        <View key={r} style={styles.gridRow}>
          {row.map((col, c) => (
            <View key={c} style={styles.gridCol}>
              {col.map((i) => (
                <Pressable key={i} style={styles.gridCell} onPress={() => press(i)}>
                  <Still media={media[i]} recyclingKey={recyclingKey ? `${recyclingKey}:${i}` : undefined} />
                </Pressable>
              ))}
            </View>
          ))}
        </View>
      ))}
    </View>
  );
}

// Instagram's carousel: full-bleed pages, a 1/5 counter and dots. A swipe
// to a new page is reported once per page.
export function MediaCarousel({
  media,
  onPress,
  onMedia,
  recyclingKey,
  inset = 0,
}: {
  media: FeedMedia[];
  onPress?: () => void;
  onMedia: (a: MediaAction) => void;
  recyclingKey?: string;
  inset?: number;
}) {
  const { width: screen } = useWindowDimensions();
  const width = screen - inset * 2;
  const [page, setPage] = useState(0);
  const [reported] = useState(() => new Set<number>([0]));
  // A link preview (og:image) with no size is 1.91:1; a known photo keeps its shape.
  const height = width / mediaAspect(media[0], 1.91);
  const onEnd = (e: NativeSyntheticEvent<NativeScrollEvent>) => {
    const next = Math.round(e.nativeEvent.contentOffset.x / width);
    setPage(next);
    if (!reported.has(next)) {
      reported.add(next);
      onMedia({ action: 'swipe', index: next });
    }
  };
  if (media.length === 1) {
    const m = media[0];
    return (
      <Pressable style={{ width, height }} onPress={() => (m.type === 'image' ? onPress?.() : onMedia({ action: 'play', index: 0, url: m.url }))}>
        <Still media={m} recyclingKey={recyclingKey} />
      </Pressable>
    );
  }
  return (
    <View>
      <ScrollView horizontal pagingEnabled showsHorizontalScrollIndicator={false} onMomentumScrollEnd={onEnd} style={{ width, height }}>
        {media.map((m, i) => (
          <Pressable
            key={i}
            style={{ width, height }}
            onPress={() => (m.type === 'image' ? onPress?.() : onMedia({ action: 'play', index: i, url: m.url }))}>
            <Still media={m} recyclingKey={recyclingKey ? `${recyclingKey}:${i}` : undefined} />
          </Pressable>
        ))}
      </ScrollView>
      <View style={styles.counter} pointerEvents="none">
        <ThemedText style={styles.counterText}>
          {page + 1}/{media.length}
        </ThemedText>
      </View>
      <View style={styles.dots} pointerEvents="none">
        {media.slice(0, 10).map((_, i) => (
          <View key={i} style={[styles.dot, { backgroundColor: i === page ? ACCENT : '#A0A0A0', opacity: i === page ? 1 : 0.5 }]} />
        ))}
      </View>
    </View>
  );
}

function Verified() {
  return <SymbolView name="checkmark.seal.fill" tintColor={ACCENT} size={15} />;
}

// Name, badge, @handle and age on one line, like an X post header.
export function PostByline({ embed, big = false }: { embed: FeedEmbed; big?: boolean }) {
  const theme = useTheme();
  return (
    <View style={styles.bylineRow}>
      <ThemedText style={[styles.name, big && styles.nameBig]} numberOfLines={1}>
        {embed.author_name || embed.author_handle}
      </ThemedText>
      {embed.author_verified ? <Verified /> : null}
      <ThemedText style={[styles.handle, { color: theme.textSecondary }]} numberOfLines={1}>
        {embed.author_handle ? `@${embed.author_handle}` : ''}
        {!big && embed.created_at ? ` · ${shortAge(embed.created_at)}` : ''}
      </ThemedText>
    </View>
  );
}

export function QuotePost({ embed, onPress, onLink, onMedia }: { embed: FeedEmbed; onPress?: () => void; onLink: (url: string) => void; onMedia: (a: MediaAction) => void }) {
  const theme = useTheme();
  return (
    <Pressable onPress={onPress} style={[styles.quote, { borderColor: theme.backgroundSelected }]}>
      <View style={styles.quoteHead}>
        <PostAvatar embed={embed} size={20} />
        <PostByline embed={embed} />
      </View>
      {embed.text ? <PostText text={embed.text} style={styles.quoteText} numberOfLines={6} onLink={onLink} /> : null}
      {embed.media?.length ? (
        <View style={styles.quoteMedia}>
          <MediaGrid media={embed.media} onPress={onPress} onMedia={onMedia} />
        </View>
      ) : null}
    </Pressable>
  );
}

// Replies, reposts, likes and views as X shows them. They are the post's
// numbers on X, not buttons: the feed measures, it does not ask.
export function PostMetrics({ embed, onShare }: { embed: FeedEmbed; onShare?: () => void }) {
  const theme = useTheme();
  const cells: [string, number | undefined][] = [
    ['bubble.left', embed.replies],
    ['arrow.2.squarepath', embed.reposts],
    ['heart', embed.likes],
    ['chart.bar', embed.views],
  ];
  return (
    <View style={styles.metrics}>
      {cells.map(([icon, n]) => (
        <View key={icon} style={styles.metric}>
          <SymbolView name={icon as never} tintColor={theme.textSecondary} size={16} />
          <ThemedText style={[styles.metricText, { color: theme.textSecondary }]}>{compactCount(n)}</ThemedText>
        </View>
      ))}
      {onShare ? (
        <Pressable hitSlop={10} onPress={onShare} accessibilityLabel="Share">
          <SymbolView name="square.and.arrow.up" tintColor={theme.textSecondary} size={16} />
        </Pressable>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  avatar: { alignItems: 'center', justifyContent: 'center', overflow: 'hidden' },
  avatarLetter: { color: '#fff', fontWeight: '700' },
  ring: { borderWidth: 2, borderColor: '#E4405F', alignItems: 'center', justifyContent: 'center' },
  grid: { width: '100%', borderRadius: 16, overflow: 'hidden', borderWidth: StyleSheet.hairlineWidth, gap: 2 },
  gridRow: { flex: 1, flexDirection: 'row', gap: 2 },
  gridCol: { flex: 1, gap: 2 },
  gridCell: { flex: 1, overflow: 'hidden' },
  playWrap: { position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, alignItems: 'center', justifyContent: 'center' },
  play: { width: 52, height: 52, borderRadius: 26, backgroundColor: 'rgba(29,155,240,0.92)', alignItems: 'center', justifyContent: 'center', paddingLeft: 3 },
  duration: { position: 'absolute', left: 8, bottom: 8, backgroundColor: 'rgba(0,0,0,0.7)', borderRadius: 4, paddingHorizontal: 5, paddingVertical: 1 },
  durationText: { color: '#fff', fontSize: 12, fontWeight: '700', lineHeight: 16 },
  counter: { position: 'absolute', top: 12, right: 12, backgroundColor: 'rgba(0,0,0,0.65)', borderRadius: 12, paddingHorizontal: 8, paddingVertical: 2 },
  counterText: { color: '#fff', fontSize: 12, fontWeight: '600', lineHeight: 17 },
  dots: { flexDirection: 'row', justifyContent: 'center', gap: 4, paddingTop: Spacing.two },
  dot: { width: 6, height: 6, borderRadius: 3 },
  bylineRow: { flexDirection: 'row', alignItems: 'center', gap: 4, flexShrink: 1 },
  name: { fontSize: 15, fontWeight: '700', flexShrink: 1, lineHeight: 20 },
  nameBig: { fontSize: 16 },
  handle: { fontSize: 15, flexShrink: 2, lineHeight: 20 },
  quote: { borderWidth: 1, borderRadius: 16, overflow: 'hidden', marginTop: Spacing.two },
  quoteHead: { flexDirection: 'row', alignItems: 'center', gap: 6, paddingHorizontal: Spacing.three, paddingTop: Spacing.two },
  quoteText: { fontSize: 15, lineHeight: 20, paddingHorizontal: Spacing.three, paddingTop: 4, paddingBottom: Spacing.two },
  quoteMedia: { paddingHorizontal: 0 },
  metrics: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', paddingTop: Spacing.two, paddingRight: Spacing.two },
  metric: { flexDirection: 'row', alignItems: 'center', gap: 5, minWidth: 52 },
  metricText: { fontSize: 13, lineHeight: 17 },
});
