import { Image } from 'expo-image';
import { useRef, useState } from 'react';
import { Pressable, ScrollView, StyleSheet, View, useWindowDimensions, type NativeScrollEvent, type NativeSyntheticEvent } from 'react-native';

import { Icon } from '@/components/icon';
import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { IconName } from '@/lib/icons';

import { startTimer } from './engagement';
import { PhotoViewer } from './photo-viewer';
import { mediaVideo } from './video';
import { InlineVideo } from './video-player';
import {
  avatarColor,
  compactCount,
  hostOf,
  mediaAspect,
  mediaGrid,
  postSegments,
  shortAge,
  videoLength,
  viewerPhotos,
  type FeedEmbed,
  type FeedMedia,
} from './rich';

export const ACCENT = '#1D9BF0';
export const LIKE_RED = '#F91880';

// What the views report up so engagement can count it: a swipe to another
// photo, a tap to play a video (inline when it plays in the card, and stop
// with dwellMs = watch time when it ends), a photo opened full screen (view), a swipe
// to another photo there (view_swipe), one photo shared from it (share), a
// zoom into a photo there (zoom), and the viewer closed (view_close, with
// dwellMs = time in the viewer). index is the photo's place in the card's
// media.
export type MediaAction = {
  action: 'swipe' | 'play' | 'stop' | 'view' | 'view_swipe' | 'share' | 'zoom' | 'view_close';
  index: number;
  inline?: boolean;
  // A play the feed started because the card scrolled into view.
  auto?: boolean;
  url?: string;
  dwellMs?: number;
};

// Opens a tapped photo in the full-screen viewer and reports what he does
// there; index in and out is the photo's place in media.
function usePhotoViewer(media: FeedMedia[], onMedia: (a: MediaAction) => void) {
  const [open, setOpen] = useState<{ photos: FeedMedia[]; start: number }>();
  // When the viewer opened and the page on screen, for view_close.
  const viewing = useRef<{ stop?: () => { dwell_ms: number }; page: number }>({ page: 0 });
  const mediaIndex = (page: number) => (open ? media.indexOf(open.photos[page]) : page);
  const show = (tapped: number) => {
    const v = viewerPhotos(media, tapped);
    if (!v.photos.length) return;
    viewing.current = { stop: startTimer(), page: v.start };
    setOpen(v);
    onMedia({ action: 'view', index: tapped });
  };
  const viewer = open ? (
    <PhotoViewer
      photos={open.photos}
      start={open.start}
      onClose={() => {
        onMedia({ action: 'view_close', index: mediaIndex(viewing.current.page), dwellMs: viewing.current.stop?.().dwell_ms ?? 0 });
        setOpen(undefined);
      }}
      onPage={(page) => {
        viewing.current.page = page;
        onMedia({ action: 'view_swipe', index: mediaIndex(page) });
      }}
      onZoom={(page) => onMedia({ action: 'zoom', index: mediaIndex(page) })}
      onShare={(page) => onMedia({ action: 'share', index: mediaIndex(page) })}
    />
  ) : null;
  return { show, viewer };
}

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
          <Icon name="play.fill" tintColor="#fff" size={22} />
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
// photo opens it full screen, as X does; a lone video plays in place, one
// among photos in the system player. playerKey names the inline player
// (unique per screen), defaulting to recyclingKey.
export function MediaGrid({
  media,
  onMedia,
  recyclingKey,
  playerKey,
}: {
  media: FeedMedia[];
  onMedia: (a: MediaAction) => void;
  recyclingKey?: string;
  playerKey?: string;
}) {
  const theme = useTheme();
  const photos = usePhotoViewer(media, onMedia);
  const rows = mediaGrid(media.length);
  const lone = media.length === 1 ? mediaVideo(media[0]) : null;
  if (lone) {
    return (
      <InlineVideo
        source={lone}
        playerKey={playerKey ?? recyclingKey ?? media[0].url}
        aspect={mediaAspect(media[0], 16 / 9)}
        durationSec={media[0].duration_sec}
        radius={16}
        onPlay={(auto) => onMedia({ action: 'play', index: 0, inline: true, auto })}
        onStop={(ms) => onMedia({ action: 'stop', index: 0, inline: true, dwellMs: ms })}
      />
    );
  }
  if (!rows.length) return null;
  const aspect = media.length === 1 ? mediaAspect(media[0], 16 / 9) : 16 / 9;
  const press = (i: number) => {
    const m = media[i];
    if (m.type !== 'image') onMedia({ action: 'play', index: i, url: m.url });
    else photos.show(i);
  };
  return (
    <View style={[styles.grid, { aspectRatio: aspect, borderColor: theme.backgroundSelected }]}>
      {photos.viewer}
      {rows.map((row, r) => (
        <View key={r} style={styles.gridRow}>
          {row.map((col, c) => (
            <View key={c} style={styles.gridCol}>
              {col.map((i) => (
                <Pressable key={i} style={styles.gridCell} onPress={() => press(i)} accessibilityRole={media[i].type === 'image' ? 'imagebutton' : 'button'}>
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
// to a new page is reported once per page. A tap on a photo calls onPress
// (the feed opens the detail view) or, with none, opens it full screen.
export function MediaCarousel({
  media,
  onPress,
  onMedia,
  recyclingKey,
  inset = 0,
  width: fixed,
  radius = 0,
}: {
  media: FeedMedia[];
  onPress?: () => void;
  onMedia: (a: MediaAction) => void;
  recyclingKey?: string;
  inset?: number;
  // A set width (a card's text column) instead of the screen less inset.
  width?: number;
  radius?: number;
}) {
  const { width: screen } = useWindowDimensions();
  const width = fixed ?? screen - inset * 2;
  const [page, setPage] = useState(0);
  const [reported] = useState(() => new Set<number>([0]));
  const photos = usePhotoViewer(media, onMedia);
  const press = (i: number) => {
    const m = media[i];
    if (m.type !== 'image') onMedia({ action: 'play', index: i, url: m.url });
    else if (onPress) onPress();
    else photos.show(i);
  };
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
  const lone = media.length === 1 ? mediaVideo(media[0]) : null;
  if (lone) {
    return (
      <View style={{ width }}>
        <InlineVideo
          source={lone}
          playerKey={recyclingKey ?? media[0].url}
          aspect={mediaAspect(media[0], 16 / 9)}
          durationSec={media[0].duration_sec}
          radius={radius}
          onPlay={(auto) => onMedia({ action: 'play', index: 0, inline: true, auto })}
          onStop={(ms) => onMedia({ action: 'stop', index: 0, inline: true, dwellMs: ms })}
        />
      </View>
    );
  }
  if (media.length === 1) {
    return (
      <Pressable style={{ width, height, borderRadius: radius, overflow: 'hidden' }} onPress={() => press(0)}>
        <Still media={media[0]} recyclingKey={recyclingKey} />
        {photos.viewer}
      </Pressable>
    );
  }
  return (
    <View>
      {photos.viewer}
      <ScrollView
        horizontal
        pagingEnabled
        showsHorizontalScrollIndicator={false}
        onMomentumScrollEnd={onEnd}
        style={{ width, height, borderRadius: radius, overflow: 'hidden' }}>
        {media.map((m, i) => (
          <Pressable
            key={i}
            style={{ width, height }}
            onPress={() => press(i)}>
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
  return <Icon name="checkmark.seal.fill" tintColor={ACCENT} size={15} />;
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

export function QuotePost({
  embed,
  onPress,
  onLink,
  onMedia,
  playerKey,
}: {
  embed: FeedEmbed;
  onPress?: () => void;
  onLink: (url: string) => void;
  onMedia: (a: MediaAction) => void;
  playerKey?: string;
}) {
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
          <MediaGrid media={embed.media} onMedia={onMedia} playerKey={playerKey} />
        </View>
      ) : null}
    </Pressable>
  );
}

// Replies, reposts, likes and views as X shows them: the post's numbers on
// X. The heart is also his like button, as on X (onLike); the rest are
// counts, not buttons.
export function PostMetrics({
  embed,
  onShare,
  onSave,
  saved,
  onLike,
  liked = false,
}: {
  embed: FeedEmbed;
  onShare?: () => void;
  onSave?: () => void;
  saved?: boolean;
  onLike?: () => void;
  liked?: boolean;
}) {
  const theme = useTheme();
  const cells: [IconName, number | undefined][] = [
    ['bubble.left', embed.replies],
    ['arrow.2.squarepath', embed.reposts],
    ['heart', embed.likes],
    ['chart.bar', embed.views],
  ];
  return (
    <View style={styles.metrics}>
      {cells.map(([icon, n]) =>
        icon === 'heart' && onLike ? (
          <Pressable key={icon} hitSlop={10} onPress={onLike} style={styles.metric} accessibilityLabel={liked ? 'Unlike' : 'Like'} accessibilityState={{ selected: liked }}>
            <Icon name={liked ? 'heart.fill' : 'heart'} tintColor={liked ? LIKE_RED : theme.textSecondary} size={16} />
            <ThemedText style={[styles.metricText, { color: liked ? LIKE_RED : theme.textSecondary }]}>{compactCount(n)}</ThemedText>
          </Pressable>
        ) : (
          <View key={icon} style={styles.metric}>
            <Icon name={icon} tintColor={theme.textSecondary} size={16} />
            <ThemedText style={[styles.metricText, { color: theme.textSecondary }]}>{compactCount(n)}</ThemedText>
          </View>
        ),
      )}
      {onSave ? (
        <Pressable hitSlop={10} onPress={onSave} accessibilityLabel={saved ? 'Remove from Saved' : 'Save for later'}>
          <Icon name={saved ? 'bookmark.fill' : 'bookmark'} tintColor={saved ? ACCENT : theme.textSecondary} size={16} />
        </Pressable>
      ) : null}
      {onShare ? (
        <Pressable hitSlop={10} onPress={onShare} accessibilityLabel="Share">
          <Icon name="square.and.arrow.up" tintColor={theme.textSecondary} size={16} />
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
