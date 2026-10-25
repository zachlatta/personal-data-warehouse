import { Image } from 'expo-image';
import * as WebBrowser from 'expo-web-browser';
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import { ActivityIndicator, Platform, Pressable, StyleSheet, View } from 'react-native';
import { Gesture, GestureDetector, type GestureType } from 'react-native-gesture-handler';
import type { SharedValue } from 'react-native-reanimated';
import { WebView } from 'react-native-webview';

import { Icon } from '@/components/icon';
import { ThemedText } from '@/components/themed-text';
import { useTheme } from '@/hooks/use-theme';

import { startTimer } from './engagement';
import { useFeedHost } from './feed-host';
import { videoLength } from './rich';
import { EMBED_BASE_URL, instagramEmbedUrl, instagramPlayerScript, playerHtml, setMutedScript, type VideoSource } from './video';

// One video plays at a time: starting another stops the last, as in X and
// Instagram. The playing player's key, shared by the feed and detail view.
// autoplayed: the feed started it on a scroll (muted), not a tap.
// muted: the sound button's state, which starts as autoplayed.
let playing: string | null = null;
let autoplayed = false;
let muted = false;
// The player that is full screen, if any. The feed's autoplay leaves it
// alone: turning the phone sideways for it lays the feed out again, and
// stopping the video then would end full screen and turn the phone back.
let fullscreen: string | null = null;
const listeners = new Set<() => void>();
function setPlaying(key: string | null, auto = false) {
  if (playing === key) return;
  playing = key;
  fullscreen = null;
  autoplayed = auto;
  muted = auto;
  for (const l of listeners) l();
}
function setMuted(m: boolean) {
  muted = m;
  for (const l of listeners) l();
}

/** The key of the video playing now, if any. */
export function playingVideo(): string | null {
  return playing;
}

/**
 * Plays a video because its card scrolled into view (Zach, voice memo
 * 2026-10-24: "I want videos to start autoplaying when I pass them, just
 * like on the better feeds"), muted; null stops whatever plays.
 */
export function autoplayVideo(key: string | null) {
  if (fullscreen !== null && fullscreen === playing) return;
  setPlaying(key, key !== null);
}

// Android's WebView engine starts cold, which made the first video take
// seconds to begin. The feed mounts this empty one up front so that cost is
// paid before a video needs it.
export function VideoWarmup() {
  return (
    <View style={styles.warmup} pointerEvents="none">
      <WebView source={{ html: '<html></html>' }} style={styles.warmup} />
    </View>
  );
}

/**
 * A touch on a playing player, for the swipeable card around it to wait on
 * (Zach, voice memo 2026-10-24: seeking in a YouTube video "scrolls the whole
 * card instead of just letting me seek"). It begins on the finger down and
 * never takes the touch itself, so the player still gets it, and it ends on
 * the finger up, which cancels the card's swipe instead of letting it go. On
 * a video that is not playing (live false) it fails at once and the card
 * swipes as before. The card makes it once and its videos are wrapped in it
 * from the start: a gesture that mounts later is missing from the card's.
 */
export function playerTouchGesture(live: SharedValue<boolean>) {
  return Gesture.Manual()
    .onTouchesDown((_e, manager) => {
      if (!live.value) manager.fail();
    })
    .onTouchesUp((_e, manager) => {
      manager.end();
    })
    .onTouchesCancelled((_e, manager) => {
      manager.end();
    });
}
export const PlayerTouch = createContext<GestureType | null>(null);

/** Whether the card's own video (key) or its quoted post's (key:quote) plays. */
export function usePlayingIn(key: string): boolean {
  return useSyncExternalStore(subscribe, () => playing === key || playing === `${key}:quote`);
}

function subscribe(l: () => void) {
  listeners.add(l);
  return () => listeners.delete(l);
}

// TikTok's player shows a "Watch this video on TikTok" gate instead of the
// video to Android's WebView (the "; wv" in its user agent), so it is told
// it runs in Chrome. iOS's WebView plays it as is.
const TIKTOK_ANDROID_UA =
  'Mozilla/5.0 (Linux; Android 16; Pixel) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36';

const LABEL: Record<VideoSource['kind'], string> = { youtube: 'YouTube', vimeo: 'Vimeo', tiktok: 'TikTok', instagram: 'Instagram', file: '' };

// A video that plays inside the card: the poster with a play button, then,
// on a tap or when the feed autoplays it, the site's own player (YouTube,
// Vimeo) or the file itself, in place, over the poster and a spinner until it
// draws. onPlay fires when it starts (auto when the feed started it); onStop with the watch time (active time:
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
  onPlay: (auto: boolean) => void;
  onStop: (watchedMs: number) => void;
}) {
  const theme = useTheme();
  const touch = useContext(PlayerTouch);
  const active = useSyncExternalStore(subscribe, () => playing) === playerKey;
  const [drawn, setDrawn] = useState(false);
  const soundOff = useSyncExternalStore(subscribe, () => muted);
  const webRef = useRef<WebView>(null);
  const stopRef = useRef(onStop);
  const playRef = useRef(onPlay);
  useEffect(() => {
    stopRef.current = onStop;
    playRef.current = onPlay;
  }, [onStop, onPlay]);

  // A tap reports its play itself; an autoplay is reported here.
  useEffect(() => {
    if (!active) return undefined;
    if (autoplayed) playRef.current(true);
    const stop = startTimer(true);
    return () => {
      setDrawn(false);
      stopRef.current(stop().dwell_ms);
    };
  }, [active]);
  // Muted is fixed when the player starts: an autoplay stays muted until he
  // turns the sound on with the sound button (or in the player).
  const html = useMemo(() => (active && source.kind !== 'instagram' ? playerHtml(source, { muted: autoplayed }) : ''), [active, source]);
  // Instagram's embed cannot sit in a frame, so it is the page itself.
  const igPage = source.kind === 'instagram' ? instagramEmbedUrl(source) : '';
  const igScript = useMemo(() => (active && igPage ? instagramPlayerScript({ muted: autoplayed }) : undefined), [active, igPage]);

  // Off screen (a detail view opened, another tab) stops it.
  useFeedHost().useFocusEffect(
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

  const wrap = (body: React.ReactElement) =>
    touch ? (
      <GestureDetector gesture={touch}>
        <View collapsable={false}>{body}</View>
      </GestureDetector>
    ) : (
      body
    );
  const frame = [styles.frame, { aspectRatio: aspect, borderRadius: radius, backgroundColor: theme.backgroundElement }];
  if (active) {
    return wrap(
      <View style={[frame, styles.black]}>
        {source.poster ? <Image source={{ uri: source.poster }} style={StyleSheet.absoluteFill} contentFit="cover" recyclingKey={playerKey} /> : null}
        {!drawn ? (
          <View style={styles.center} pointerEvents="none">
            <ActivityIndicator color="#fff" size="large" />
          </View>
        ) : null}
        <WebView
          ref={webRef}
          source={igPage ? { uri: igPage } : { html, baseUrl: EMBED_BASE_URL }}
          injectedJavaScript={igScript}
          userAgent={source.kind === 'tiktok' && Platform.OS === 'android' ? TIKTOK_ANDROID_UA : undefined}
          originWhitelist={['*']}
          allowsInlineMediaPlayback
          mediaPlaybackRequiresUserAction={false}
          allowsFullscreenVideo
          allowsPictureInPictureMediaPlayback
          scrollEnabled={false}
          bounces={false}
          style={styles.clear}
          containerStyle={styles.clear}
          // A file or Instagram says when it draws; a YouTube, Vimeo or
          // TikTok page draws its own poster as soon as it loads.
          onMessage={(e) => {
            const msg = e.nativeEvent.data;
            if (msg === 'playing') setDrawn(true);
            else if (msg === 'fullscreen') fullscreen = playerKey;
            else if (msg === 'inline' && fullscreen === playerKey) fullscreen = null;
          }}
          onLoadEnd={() => source.kind !== 'file' && source.kind !== 'instagram' && setDrawn(true)}
          onShouldStartLoadWithRequest={(req) => {
            // The player page and everything inside it load here; a tap
            // that leaves the player (the YouTube logo) opens the browser.
            if (!req.isTopFrame || req.url === 'about:blank' || req.url.startsWith(EMBED_BASE_URL)) return true;
            if (igPage && req.url.split('?')[0] === igPage) return true;
            void WebBrowser.openBrowserAsync(req.url, { dismissButtonStyle: 'close' });
            return false;
          }}
        />
        <Pressable
          style={styles.sound}
          hitSlop={10}
          onPress={() => {
            webRef.current?.injectJavaScript(setMutedScript(!soundOff));
            setMuted(!soundOff);
          }}
          accessibilityRole="button"
          accessibilityLabel={soundOff ? 'Turn sound on' : 'Turn sound off'}>
          <Icon name={soundOff ? 'speaker.slash.fill' : 'speaker.wave.2.fill'} tintColor="#fff" size={16} />
        </Pressable>
      </View>
    );
  }
  const length = videoLength(durationSec);
  return wrap(
    <Pressable
      style={frame}
      onPress={() => {
        setPlaying(playerKey);
        onPlay(false);
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
  clear: { flex: 1, backgroundColor: 'transparent' },
  warmup: { position: 'absolute', width: 1, height: 1, opacity: 0 },
  center: { position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, alignItems: 'center', justifyContent: 'center' },
  play: { width: 56, height: 56, borderRadius: 28, backgroundColor: 'rgba(0,0,0,0.6)', alignItems: 'center', justifyContent: 'center', paddingLeft: 3 },
  sound: { position: 'absolute', top: 8, right: 8, width: 32, height: 32, borderRadius: 16, backgroundColor: 'rgba(0,0,0,0.6)', alignItems: 'center', justifyContent: 'center' },
  badge: { position: 'absolute', left: 8, bottom: 8, backgroundColor: 'rgba(0,0,0,0.7)', borderRadius: 4, paddingHorizontal: 6, paddingVertical: 1 },
  badgeText: { color: '#fff', fontSize: 12, fontWeight: '700', lineHeight: 16 },
});
