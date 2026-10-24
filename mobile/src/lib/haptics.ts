import { Platform, Vibration } from 'react-native';

// Superhuman's two buzzes (decompiled 2.5.0, RedesignView_Haptic): a light
// tick when a swipe starts to commit (GESTURE_START) and a firmer one when
// an action lands (CONFIRM). The app has no haptics module yet, so Android
// uses a very short vibration; iOS's Vibration API only knows a long buzz,
// so it stays silent there rather than feel wrong.
export function tick(): void {
  if (Platform.OS === 'android') Vibration.vibrate(8);
}

export function confirm(): void {
  if (Platform.OS === 'android') Vibration.vibrate(18);
}
