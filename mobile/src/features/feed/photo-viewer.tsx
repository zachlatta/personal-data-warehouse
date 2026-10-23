import { Directory, File, Paths } from 'expo-file-system';
import { Image } from 'expo-image';
import * as Sharing from 'expo-sharing';
import { useEffect, useRef, useState } from 'react';
import {
  ActivityIndicator,
  Alert,
  Modal,
  Pressable,
  ScrollView,
  StyleSheet,
  View,
  useWindowDimensions,
  type NativeScrollEvent,
  type NativeSyntheticEvent,
} from 'react-native';
import { SafeAreaProvider } from 'react-native-safe-area-context';

import { Icon } from '@/components/icon';
import { ThemedText } from '@/components/themed-text';
import { useModalInsets } from '@/hooks/use-modal-insets';

import { fullSizeUrl, photoFilename, type FeedMedia } from './rich';

// Downloads one photo at full size into the cache and opens the share sheet
// for that file alone, so Save Image, Messages and AirDrop get the photo
// itself rather than a link to the post.
async function sharePhoto(media: FeedMedia, index: number) {
  if (!(await Sharing.isAvailableAsync())) throw new Error('Sharing is not available on this device.');
  const dir = new Directory(Paths.cache, 'feed-photos');
  dir.create({ idempotent: true, intermediates: true });
  const target = new File(dir, photoFilename(media.url, index));
  let file: File;
  try {
    file = await File.downloadFileAsync(fullSizeUrl(media.url), target, { idempotent: true });
  } catch {
    // Not every host serves an original; the size the card showed will do.
    file = await File.downloadFileAsync(media.url, target, { idempotent: true });
  }
  await Sharing.shareAsync(file.uri, { dialogTitle: media.alt || 'Photo' });
}

type PhotoViewerProps = {
  photos: FeedMedia[];
  start: number;
  onClose: () => void;
  onPage?: (index: number) => void;
  onShare?: (index: number) => void;
  onZoom?: (index: number) => void;
};

// X's photo viewer: full screen on black, swipe between the post's photos,
// pinch or double-tap to zoom, and share the one on screen.
export function PhotoViewer(props: PhotoViewerProps) {
  return (
    <Modal visible animationType="fade" presentationStyle="overFullScreen" transparent statusBarTranslucent onRequestClose={props.onClose}>
      <SafeAreaProvider>
        <PhotoViewerScreen {...props} />
      </SafeAreaProvider>
    </Modal>
  );
}

function PhotoViewerScreen({ photos, start, onClose, onPage, onShare, onZoom }: PhotoViewerProps) {
  const { width, height } = useWindowDimensions();
  const insets = useModalInsets();
  const [page, setPage] = useState(start);
  const [sharing, setSharing] = useState(false);
  const [chrome, setChrome] = useState(true);

  const onEnd = (e: NativeSyntheticEvent<NativeScrollEvent>) => {
    const next = Math.round(e.nativeEvent.contentOffset.x / width);
    if (next !== page) {
      setPage(next);
      onPage?.(next);
    }
  };
  const share = async () => {
    if (sharing) return;
    setSharing(true);
    try {
      onShare?.(page);
      await sharePhoto(photos[page], page);
    } catch (e) {
      Alert.alert('Could not share this photo', e instanceof Error ? e.message : undefined);
    } finally {
      setSharing(false);
    }
  };

  // Touches bubble up the React tree, not the screen: claim any the viewer
  // does not handle so they never reach the card behind it.
  return (
    <View style={styles.screen} onStartShouldSetResponder={() => true}>
      <ScrollView
        horizontal
        pagingEnabled
        showsHorizontalScrollIndicator={false}
        contentOffset={{ x: start * width, y: 0 }}
        onMomentumScrollEnd={onEnd}>
        {photos.map((m, i) => (
          <ZoomablePhoto key={i} media={m} width={width} height={height} onTap={() => setChrome((c) => !c)} onZoom={() => onZoom?.(i)} />
        ))}
      </ScrollView>
      {chrome ? (
        <View style={[styles.bar, { paddingTop: insets.top, paddingLeft: 12 + insets.left, paddingRight: 12 + insets.right }]} pointerEvents="box-none">
          <Pressable hitSlop={12} onPress={onClose} accessibilityLabel="Close" style={styles.button}>
            <Icon name="xmark" tintColor="#fff" size={18} />
          </Pressable>
          {photos.length > 1 ? (
            <ThemedText style={styles.counter}>
              {page + 1}/{photos.length}
            </ThemedText>
          ) : (
            <View />
          )}
          <Pressable hitSlop={12} onPress={() => void share()} disabled={sharing} accessibilityLabel="Share photo" style={styles.button}>
            {sharing ? <ActivityIndicator color="#fff" /> : <Icon name="square.and.arrow.up" tintColor="#fff" size={18} />}
          </Pressable>
        </View>
      ) : null}
      {chrome && photos[page]?.alt ? (
        <View style={[styles.altWrap, { paddingBottom: 12 + insets.bottom }]} pointerEvents="none">
          <ThemedText style={styles.alt} numberOfLines={4}>
            {photos[page].alt}
          </ThemedText>
        </View>
      ) : null}
    </View>
  );
}

// One page: an iOS zoom view around the photo. A single tap hides the
// controls; a double tap zooms in to 2.5x or back out.
function ZoomablePhoto({
  media,
  width,
  height,
  onTap,
  onZoom,
}: {
  media: FeedMedia;
  width: number;
  height: number;
  onTap: () => void;
  onZoom?: () => void;
}) {
  const scroller = useRef<ScrollView>(null);
  const zoomed = useRef(false);
  const lastTap = useRef(0);
  const pending = useRef<ReturnType<typeof setTimeout>>(undefined);
  useEffect(() => () => clearTimeout(pending.current), []);

  const tap = (x: number, y: number) => {
    const now = Date.now();
    if (now - lastTap.current < 280) {
      clearTimeout(pending.current);
      lastTap.current = 0;
      const scale = zoomed.current ? 1 : 2.5;
      const w = width / scale;
      const h = height / scale;
      const at = zoomed.current ? { x: 0, y: 0 } : { x: x - w / 2, y: y - h / 2 };
      scroller.current?.scrollResponderZoomTo({ ...at, width: w, height: h, animated: true });
      return;
    }
    lastTap.current = now;
    pending.current = setTimeout(onTap, 280);
  };

  return (
    <ScrollView
      ref={scroller}
      style={{ width, height }}
      contentContainerStyle={{ width, height }}
      maximumZoomScale={4}
      minimumZoomScale={1}
      bouncesZoom
      centerContent
      showsHorizontalScrollIndicator={false}
      showsVerticalScrollIndicator={false}
      onScroll={(e) => {
        const now = e.nativeEvent.zoomScale > 1.01;
        if (now && !zoomed.current) onZoom?.();
        zoomed.current = now;
      }}
      scrollEventThrottle={100}>
      <Pressable style={{ width, height }} onPress={(e) => tap(e.nativeEvent.locationX, e.nativeEvent.locationY)}>
        <Image
          source={{ uri: fullSizeUrl(media.url) }}
          placeholder={{ uri: media.url }}
          style={{ width, height }}
          contentFit="contain"
          transition={150}
          accessibilityLabel={media.alt || undefined}
          accessibilityIgnoresInvertColors
        />
      </Pressable>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: '#000' },
  bar: { position: 'absolute', top: 0, left: 0, right: 0, flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', paddingHorizontal: 12 },
  button: { width: 36, height: 36, borderRadius: 18, backgroundColor: 'rgba(0,0,0,0.55)', alignItems: 'center', justifyContent: 'center', margin: 4 },
  counter: { color: '#fff', fontSize: 15, fontWeight: '600', lineHeight: 20 },
  altWrap: { position: 'absolute', left: 0, right: 0, bottom: 0, paddingHorizontal: 16, paddingBottom: 12, backgroundColor: 'rgba(0,0,0,0.55)' },
  alt: { color: '#fff', fontSize: 14, lineHeight: 19, paddingTop: 10 },
});
