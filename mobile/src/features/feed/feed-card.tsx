import { Image } from 'expo-image';
import { SymbolView } from 'expo-symbols';
import { memo, useMemo, useState } from 'react';
import { Pressable, StyleSheet, View, type LayoutChangeEvent } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { formatWhen } from '@/lib/format';

import type { FeedItem } from './api';
import { cardLinks, type CardLink } from './links';
import { plainText } from './markdown';

const PREVIEW_LINES = 4;
const LINK_COLOR = '#208AEF';

// There is no like button: what he looks at, opens and reads is measured.
type Props = {
  item: FeedItem;
  onLayout: (itemId: string, height: number) => void;
  onOpen: (item: FeedItem) => void;
  onRead: (item: FeedItem) => void;
  onOpenLink: (item: FeedItem, link: CardLink) => void;
  onExpand: (item: FeedItem) => void;
  onMenu: (item: FeedItem) => void;
};

function FeedCardImpl({ item, onLayout, onOpen, onRead, onOpenLink, onExpand, onMenu }: Props) {
  const theme = useTheme();
  const [expanded, setExpanded] = useState(false);
  const [truncated, setTruncated] = useState(false);
  const preview = useMemo(() => (item.body ? plainText(item.body) : ''), [item.body]);
  // The main url is the card itself; the rows list the extra links.
  const extraLinks = useMemo(() => cardLinks(item).filter((link) => link.index > 0), [item]);
  const byline = [item.site || item.source, item.author, formatWhen(item.published_at ?? item.created_at)]
    .filter(Boolean)
    .join(' · ');

  return (
    <View
      style={[styles.wrap, { borderBottomColor: theme.backgroundElement }]}
      onLayout={(e: LayoutChangeEvent) => onLayout(item.item_id, e.nativeEvent.layout.height)}>
      <Pressable onPress={() => onOpen(item)} style={({ pressed }) => [pressed && { opacity: 0.85 }]}>
        <View style={styles.header}>
          <ThemedText style={[styles.byline, { color: theme.textSecondary }]} numberOfLines={1}>
            {byline}
          </ThemedText>
        </View>
        {item.title ? <ThemedText style={styles.title}>{item.title}</ThemedText> : null}
        {item.image_url ? (
          <Image
            source={{ uri: item.image_url }}
            style={[styles.image, { backgroundColor: theme.backgroundElement }]}
            contentFit="cover"
            transition={200}
            recyclingKey={item.item_id}
            accessibilityIgnoresInvertColors
          />
        ) : null}
        {preview ? (
          <ThemedText
            style={[styles.body, { color: theme.textSecondary }]}
            numberOfLines={expanded ? undefined : PREVIEW_LINES}
            onTextLayout={(e) => {
              if (!expanded && e.nativeEvent.lines.length >= PREVIEW_LINES) setTruncated(true);
            }}>
            {preview}
          </ThemedText>
        ) : null}
      </Pressable>
      {truncated && !expanded ? (
        <Pressable
          hitSlop={8}
          onPress={() => {
            setExpanded(true);
            onExpand(item);
          }}>
          <ThemedText style={styles.more}>More</ThemedText>
        </Pressable>
      ) : null}
      {extraLinks.length ? (
        <View style={[styles.links, { borderColor: theme.backgroundElement }]}>
          {extraLinks.map((link) => (
            <Pressable
              key={link.index}
              onPress={() => onOpenLink(item, link)}
              accessibilityLabel={`Open ${link.label}`}
              style={({ pressed }) => [styles.linkRow, { borderTopColor: theme.backgroundElement }, pressed && { opacity: 0.6 }]}>
              <SymbolView name="link" tintColor={LINK_COLOR} size={15} />
              <ThemedText style={styles.linkLabel} numberOfLines={1}>
                {link.label}
              </ThemedText>
              {link.label !== link.site ? (
                <ThemedText style={[styles.linkSite, { color: theme.textSecondary }]} numberOfLines={1}>
                  {link.site}
                </ThemedText>
              ) : null}
            </Pressable>
          ))}
        </View>
      ) : null}
      <View style={styles.actions}>
        {item.url ? (
          <Pressable hitSlop={10} onPress={() => onOpen(item)} accessibilityLabel="Open link" style={styles.action}>
            <SymbolView name="safari" tintColor={theme.textSecondary} size={21} />
          </Pressable>
        ) : null}
        {item.body ? (
          <Pressable hitSlop={10} onPress={() => onRead(item)} accessibilityLabel="Read" style={styles.action}>
            <SymbolView name="doc.text" tintColor={theme.textSecondary} size={20} />
          </Pressable>
        ) : null}
        <View style={{ flex: 1 }} />
        {item.tags.length ? (
          <ThemedText style={[styles.tags, { color: theme.textSecondary }]} numberOfLines={1}>
            {item.tags.slice(0, 3).map((t) => `#${t}`).join(' ')}
          </ThemedText>
        ) : null}
        <Pressable hitSlop={10} onPress={() => onMenu(item)} accessibilityLabel="Dismiss" style={styles.action}>
          <SymbolView name="xmark" tintColor={theme.textSecondary} size={17} />
        </Pressable>
      </View>
    </View>
  );
}

export const FeedCard = memo(FeedCardImpl);

const styles = StyleSheet.create({
  wrap: { paddingTop: Spacing.three, borderBottomWidth: 8 },
  header: { paddingHorizontal: Spacing.three, marginBottom: Spacing.one },
  byline: { fontSize: 13, lineHeight: 17 },
  title: { paddingHorizontal: Spacing.three, fontSize: 19, lineHeight: 25, fontWeight: '700', marginBottom: Spacing.two },
  image: { width: '100%', aspectRatio: 1.91, marginBottom: Spacing.two },
  body: { paddingHorizontal: Spacing.three, fontSize: 15, lineHeight: 21 },
  more: { paddingHorizontal: Spacing.three, paddingTop: Spacing.one, fontSize: 15, color: LINK_COLOR, fontWeight: '600' },
  links: { marginTop: Spacing.two, marginHorizontal: Spacing.three, borderWidth: 1, borderRadius: 10, overflow: 'hidden' },
  linkRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, paddingHorizontal: Spacing.three, paddingVertical: 10, borderTopWidth: StyleSheet.hairlineWidth },
  linkLabel: { flexShrink: 1, fontSize: 15, fontWeight: '600', color: LINK_COLOR },
  linkSite: { flexShrink: 2, fontSize: 13 },
  actions: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: Spacing.two, paddingVertical: Spacing.two, gap: Spacing.one },
  action: { padding: Spacing.two },
  tags: { fontSize: 13, maxWidth: 170, marginRight: Spacing.one },
});
