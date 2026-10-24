import { useSafeAreaInsets } from 'react-native-safe-area-context';

// The floating pill tab bar sits over the page, Superhuman-style, so content
// scrolls under it. Lists on a tab pad their end by this much so the last row
// can scroll clear of it, and toasts sit just above it.
export const TAB_PILL_HEIGHT = 52;
const GAP_ABOVE = 8;

export function tabBarSpace(bottomInset: number): number {
  return TAB_PILL_HEIGHT + GAP_ABOVE + Math.max(bottomInset, 10);
}

export function useTabBarSpace(): number {
  return tabBarSpace(useSafeAreaInsets().bottom);
}
