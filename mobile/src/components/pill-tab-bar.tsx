import type { BottomTabBarProps } from 'expo-router/tabs';
import { Pressable, StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { Palette } from '@/constants/theme';
import { TAB_PILL_HEIGHT } from '@/hooks/use-tab-bar-space';
import { useTheme } from '@/hooks/use-theme';

// Superhuman's bottom navigation: one rounded pill floating above the page,
// icons only, the current tab sitting in a lighter pill of its own, and the
// account (here Settings) as a round avatar at the far end. A route given
// `href: null` stays routable and off the bar. A tab's `tabBarBadge` shows as
// a count on the icon's top right (Mutations: requests waiting on review).
export function PillTabBar({ state, descriptors, navigation, insets }: BottomTabBarProps) {
  const theme = useTheme();
  const routes = state.routes.filter((route) => StyleSheet.flatten(descriptors[route.key].options.tabBarItemStyle)?.display !== 'none');
  const tabs = routes.filter((route) => route.name !== 'settings');
  const account = routes.find((route) => route.name === 'settings');
  const focusedKey = state.routes[state.index]?.key;

  const press = (key: string, name: string) => {
    const event = navigation.emit({ type: 'tabPress', target: key, canPreventDefault: true });
    if (focusedKey !== key && !event.defaultPrevented) navigation.navigate(name);
  };

  return (
    // Floats over the page (position absolute), so the list scrolls under it
    // as in Superhuman; screens pad by useTabBarSpace(). A fade in the page
    // colour sits behind the pill so rows dissolve under it instead of
    // colliding with it (no gradient library: stacked bands).
    <View pointerEvents="box-none" style={[styles.wrap, { paddingBottom: Math.max(insets.bottom, 10) }]}>
      <View pointerEvents="none" style={styles.scrim}>
        {SCRIM.map((opacity, i) => (
          <View key={i} style={{ flex: 1, backgroundColor: theme.background, opacity }} />
        ))}
      </View>
      <View style={[styles.pill, styles.shadow, { backgroundColor: theme.surface, borderColor: theme.surfaceBorder }]}>
        {tabs.map((route) => {
          const { options } = descriptors[route.key];
          const focused = route.key === focusedKey;
          const color = focused ? theme.text : theme.textSecondary;
          return (
            <Pressable
              key={route.key}
              accessibilityRole="tab"
              accessibilityState={{ selected: focused }}
              accessibilityLabel={options.tabBarBadge != null ? `${options.title ?? route.name}, ${options.tabBarBadge} waiting` : (options.title ?? route.name)}
              onPress={() => press(route.key, route.name)}
              onLongPress={() => navigation.emit({ type: 'tabLongPress', target: route.key })}
              style={[styles.item, focused && { backgroundColor: theme.backgroundSelected }]}>
              {options.tabBarIcon?.({ focused, color, size: 22 })}
              {options.tabBarBadge != null ? (
                <View style={[styles.badge, { backgroundColor: Palette.destructive, borderColor: theme.surface }]}>
                  <ThemedText style={styles.badgeText}>{options.tabBarBadge}</ThemedText>
                </View>
              ) : null}
            </Pressable>
          );
        })}
      </View>
      {account ? (
        <Pressable
          accessibilityRole="tab"
          accessibilityLabel="Settings"
          accessibilityState={{ selected: account.key === focusedKey }}
          onPress={() => press(account.key, account.name)}
          style={[styles.avatar, styles.shadow, { backgroundColor: theme.surface, borderColor: account.key === focusedKey ? theme.text : theme.surfaceBorder }]}>
          <ThemedText style={styles.avatarText}>Z</ThemedText>
        </Pressable>
      ) : null}
    </View>
  );
}

// Band opacities from the top of the fade down; the lower half is near solid.
const SCRIM = [0, 0.2, 0.45, 0.65, 0.8, 0.9, 0.96, 1, 1, 1];

const styles = StyleSheet.create({
  scrim: { position: 'absolute', left: 0, right: 0, top: -18, bottom: 0 },
  wrap: { position: 'absolute', left: 0, right: 0, bottom: 0, flexDirection: 'row', alignItems: 'center', gap: 10, paddingHorizontal: 14, paddingTop: 8 },
  // A soft lift so the pill reads above the rows scrolling beneath it.
  shadow: { elevation: 8, shadowColor: '#000', shadowOpacity: 0.25, shadowRadius: 12, shadowOffset: { width: 0, height: 4 } },
  pill: { flex: 1, flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', height: TAB_PILL_HEIGHT, borderRadius: TAB_PILL_HEIGHT / 2, borderWidth: StyleSheet.hairlineWidth, paddingHorizontal: 5 },
  item: { flex: 1, height: 42, borderRadius: 21, alignItems: 'center', justifyContent: 'center' },
  badge: { position: 'absolute', top: 2, left: '50%', marginLeft: 4, minWidth: 20, height: 20, borderRadius: 10, borderWidth: 2, paddingHorizontal: 4, alignItems: 'center', justifyContent: 'center' },
  badgeText: { color: '#FFFFFF', fontSize: 11, lineHeight: 14, fontWeight: '700' },
  avatar: { width: TAB_PILL_HEIGHT, height: TAB_PILL_HEIGHT, borderRadius: TAB_PILL_HEIGHT / 2, borderWidth: 1, alignItems: 'center', justifyContent: 'center' },
  avatarText: { fontSize: 18, fontWeight: '600' },
});
