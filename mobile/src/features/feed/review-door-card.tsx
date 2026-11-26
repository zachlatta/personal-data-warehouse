import { Image } from 'expo-image';
import { useEffect, useRef, useState } from 'react';
import { Pressable, StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { AppConfig } from '@/lib/config';

import { fetchFeed } from './api';
import { recordUi } from './engagement';
import { FEED_ENABLED } from './index';
import { requestFeedJump, reviewDoorPreview, type ReviewDoorPreview } from './review-door';

type Props = {
  config: AppConfig;
  // Incremented on each new screen paint (initial mount, later focus,
  // foreground resume, pull): refetches and allows one new `shown` event.
  paintKey?: number;
  // Incremented on silent background refreshes (e.g. 60s periodic tick):
  // refetches to keep count and card current without logging another `shown`.
  refreshKey?: number;
  onOpenFeed: () => void;
};

// Compact door to the Feed tab shown under "All caught up." in Review.
// Fetches asynchronously so it never delays the Review list, hides itself on
// error or 0 unseen, and never records a feed impression for the previewed card.
export function ReviewToFeedDoor({ config, paintKey = 0, refreshKey = 0, onOpenFeed }: Props) {
  const theme = useTheme();
  const [paint, setPaint] = useState<{ paintKey: number; preview: ReviewDoorPreview | null }>({ paintKey: -1, preview: null });
  const loggedPaintKey = useRef(-1);

  useEffect(() => {
    if (!FEED_ENABLED) return;
    let cancelled = false;
    fetchFeed(config)
      .then((page) => {
        if (!cancelled) setPaint({ paintKey, preview: reviewDoorPreview(page) });
      })
      .catch(() => {
        if (!cancelled) setPaint({ paintKey, preview: null });
      });
    return () => {
      cancelled = true;
    };
  }, [config, paintKey, refreshKey]);

  useEffect(() => {
    if (!paint.preview || loggedPaintKey.current === paint.paintKey) return;
    loggedPaintKey.current = paint.paintKey;
    recordUi('review_to_feed', {
      metadata: { event: 'shown', item_id: paint.preview.itemId, unseen: paint.preview.unseen },
    });
  }, [paint]);

  const preview = paint.preview;
  if (!FEED_ENABLED || !preview) return null;

  const onPress = () => {
    recordUi('review_to_feed', {
      metadata: { event: 'tapped', item_id: preview.itemId },
    });
    requestFeedJump(preview.itemId);
    onOpenFeed();
  };

  return (
    <Pressable
      accessibilityRole="button"
      onPress={onPress}
      style={({ pressed }) => [
        styles.card,
        { backgroundColor: theme.surface, borderColor: theme.surfaceBorder, opacity: pressed ? 0.85 : 1 },
      ]}>
      <View style={styles.textColumn}>
        <ThemedText type="smallBold" themeColor="textSecondary">
          {`${preview.unseen} new in Feed`}
        </ThemedText>
        <ThemedText numberOfLines={2} style={styles.title}>
          {preview.title}
        </ThemedText>
        <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>
          {preview.source}
        </ThemedText>
      </View>
      {preview.imageUrl ? (
        <Image source={{ uri: preview.imageUrl }} contentFit="cover" style={styles.thumb} />
      ) : null}
    </Pressable>
  );
}

const styles = StyleSheet.create({
  card: {
    marginLeft: 26,
    marginRight: Spacing.three,
    marginBottom: Spacing.three,
    padding: Spacing.three,
    borderRadius: 12,
    borderWidth: StyleSheet.hairlineWidth,
    flexDirection: 'row',
    alignItems: 'center',
    gap: Spacing.three,
  },
  textColumn: {
    flex: 1,
    gap: Spacing.one,
  },
  title: {
    fontSize: 15,
    fontWeight: '600',
    lineHeight: 20,
  },
  thumb: {
    width: 64,
    height: 64,
    borderRadius: 8,
  },
});
