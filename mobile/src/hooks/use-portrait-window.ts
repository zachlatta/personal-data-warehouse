import { Dimensions, useWindowDimensions } from 'react-native';

/**
 * The window as the app lays out in it: portrait. The app is locked to
 * portrait (app.json), but a full-screen video unlocks rotation on Android
 * (react-native-webview), and the window behind the video turns landscape.
 * Laying the feed out again at landscape width moved the playing video's
 * card off screen, which stopped the video and so snapped the phone back to
 * portrait (Zach, voice memo 2026-10-24: "when I try to rotate a video
 * landscape in the PDW feed, it like freaks out and rotates back").
 * sideways is true while the window is landscape.
 */
export function usePortraitWindow() {
  const { width, height } = useWindowDimensions();
  return { width: Math.min(width, height), height: Math.max(width, height), sideways: width > height };
}

/** Whether the window is landscape now, outside render. */
export function isSideways(): boolean {
  const { width, height } = Dimensions.get('window');
  return width > height;
}
