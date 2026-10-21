import { initialWindowMetrics, useSafeAreaInsets } from 'react-native-safe-area-context';

// A full-screen Modal is its own native window, outside the app's
// SafeAreaProvider, so a SafeAreaView inside it read zero insets and the
// viewers' buttons sat under the status bar. Call this below a
// SafeAreaProvider placed inside the Modal; the launch metrics are the floor
// in case that provider has not measured yet.
export function useModalInsets() {
  const insets = useSafeAreaInsets();
  const launch = initialWindowMetrics?.insets;
  return {
    top: Math.max(insets.top, launch?.top ?? 0),
    bottom: Math.max(insets.bottom, launch?.bottom ?? 0),
    left: Math.max(insets.left, launch?.left ?? 0),
    right: Math.max(insets.right, launch?.right ?? 0),
  };
}
