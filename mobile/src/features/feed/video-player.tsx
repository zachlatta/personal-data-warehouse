import { Image } from 'expo-image';
import { useFocusEffect } from 'expo-router';
import * as WebBrowser from 'expo-web-browser';
import { useCallback, useEffect, useRef, useSyncExternalStore } from 'react';
import { Pressable, StyleSheet, View } from 'react-native';
import { WebView } from 'react-native-webview';

import { Icon } from '@/components/icon';
import { ThemedText } from '@/components/themed-text';
import { useTheme } from '@/hooks/use-theme';

import { startTimer } from './engagement';
import { videoLength } from './rich';
import { EMBED_BASE_URL, playerHtml, type VideoSource } from './video';

// One video plays at a time: starting another stops the last, as in X and
// Instagram. The playing player's key, shared by the feed and detail view.
let playing: string | null = null;
const listeners = new Set<() => void>();
function setPlaying(key: string | null) {
  if (playing === key) return;
  playing = key;
  for (const l of listeners) l();
}
function subscribe(l: () => void) {
  listeners.add(l);
  return () => listeners.delete(l);
}

const LABEL: Record<VideoSource['kind'], string> = { youtube: 'YouTube', vimeo: 'Vimeo', file: '' };

// A video that plays inside the card: the poster with a play button, then,
// on a tap, the site's own player (YouTube, Vimeo) or the file itself, in
// place. onPlay fires on the tap; onStop with the watch time (active time:
// not while the app is in the background) when it stops
// (another video started, the card scrolled away or the screen changed).
export function InlineVideo({
  source,
  playerKey,
  aspect = 16 / 9,
  durationSec,
  radius = 14,
  onPlay,
  onStop,
}: {
  source: VideoSource;
  playerKey: string;
  aspect?: number;
  durationSec?: number;
  radius?: number;
  onPlay: () => void;
  onStop: (watchedMs: number) => void;
}) {
  const theme = useTheme();
  const active = useSyncExternalStore(subscribe, () => playing) === playerKey;
  const stopRef = useRef(onStop);
  useEffect(() => {
    stopRef.current = onStop;
  }, [onStop]);

  useEffect(() => {
    if (!active) return undefined;
    const stop = startTimer(true);
    return () => stopRef.current(stop().dwell_ms);
  }, [active]);

  // Off screen (a detail view opened, another tab) stops it.
  useFocusEffect(
    useCallback(
      () => () => {
        if (playing === playerKey) setPlaying(null);
      },
      [playerKey],
    ),
  );
  useEffect(
    () => () => {
      if (playing === playerKey) setPlaying(null);
    },
    [playerKey],
  );

  const frame = [styles.frame, { aspectRatio: aspect, borderRadius: radius, backgroundColor: theme.backgroundElement }];
  if (active) {
    return (
      <View style={[frame, styles.black]}>
        <WebView
          source={{ html: playerHtml(source), baseUrl: EMBED_BASE_URL }}
          originWhitelist={['*']}
          allowsInlineMediaPlayback
          mediaPlaybackRequiresUserAction={false}
          allowsFullscreenVideo
          allowsPictureInPictureMediaPlayback
          scrollEnabled={false}
          bounces={false}
          style={styles.black}
          onShouldStartLoadWithRequest={(req) => {
            // The player page and everything inside it load here; a tap
            // that leaves the player (the YouTube logo) opens the browser.
            if (!req.isTopFrame || req.url === 'about:blank' || req.url.startsWith(EMBED_BASE_URL)) return true;
            void WebBrowser.openBrowserAsync(req.url, { dismissButtonStyle: 'close' });
            return false;
          }}
        />
      </View>
    );
  }
  const length = videoLength(durationSec);
  return (
    <Pressable
      style={frame}
      onPress={() => {
        setPlaying(playerKey);
        onPlay();
      }}
      accessibilityRole="button"
      accessibilityLabel="Play video">
      {source.poster ? (
        <Image source={{ uri: source.poster }} style={StyleSheet.absoluteFill} contentFit="cover" transition={150} recyclingKey={playerKey} />
      ) : (
        <View style={[StyleSheet.absoluteFill, styles.black]} />
      )}
      <View style={styles.center} pointerEvents="none">
        <View style={styles.play}>
          <Icon name="play.fill" tintColor="#fff" size={24} />
        </View>
      </View>
      {length || LABEL[source.kind] ? (
        <View style={styles.badge} pointerEvents="none">
          <ThemedText style={styles.badgeText}>{[LABEL[source.kind], length].filter(Boolean).join(' · ')}</ThemedText>
        </View>
      ) : null}
    </Pressable>
  );
}

const styles = StyleSheet.create({
  frame: { width: '100%', overflow: 'hidden' },
  black: { flex: 1, backgroundColor: '#000' },
  center: { position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, alignItems: 'center', justifyContent: 'center' },
  play: { width: 56, height: 56, borderRadius: 28, backgroundColor: 'rgba(0,0,0,0.6)', alignItems: 'center', justifyContent: 'center', paddingLeft: 3 },
  badge: { position: 'absolute', left: 8, bottom: 8, backgroundColor: 'rgba(0,0,0,0.7)', borderRadius: 4, paddingHorizontal: 6, paddingVertical: 1 },
  badgeText: { color: '#fff', fontSize: 12, fontWeight: '700', lineHeight: 16 },
});
