// The Feed tab (docs/feed.md). Everything the app needs is in this
// directory; it hooks in through two route files that re-export the screens
// and one line each in src/app/(tabs)/_layout.tsx and src/app/_layout.tsx.
import { SymbolView } from 'expo-symbols';
import type { ColorValue } from 'react-native';

// The off switch: false hides the tab (the route stays reachable by URL only).
export const FEED_ENABLED = true;

export const feedTabOptions = {
  title: 'Feed',
  href: FEED_ENABLED ? undefined : null,
  tabBarIcon: ({ color, size }: { color: ColorValue; size: number }) => <SymbolView name="newspaper" tintColor={color} size={size} />,
};
