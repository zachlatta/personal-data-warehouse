import type { BottomTabBarProps } from 'expo-router/tabs';
import { Pressable, StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { useTheme } from '@/hooks/use-theme';

// Superhuman's bottom navigation: one rounded pill floating above the page,
// icons only, the current tab sitting in a lighter pill of its own, and the
// account (here Settings) as a round avatar at the far end. A route given
// `href: null` stays routable and off the bar.
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
    <View style={[styles.wrap, { paddingBottom: Math.max(insets.bottom, 10), backgroundColor: theme.background }]}>
      <View style={[styles.pill, { backgroundColor: theme.surface, borderColor: theme.surfaceBorder }]}>
        {tabs.map((route) => {
          const { options } = descriptors[route.key];
          const focused = route.key === focusedKey;
          const color = focused ? theme.text : theme.textSecondary;
          return (
            <Pressable
              key={route.key}
              accessibilityRole="tab"
              accessibilityState={{ selected: focused }}
              accessibilityLabel={options.title ?? route.name}
              onPress={() => press(route.key, route.name)}
              onLongPress={() => navigation.emit({ type: 'tabLongPress', target: route.key })}
              style={[styles.item, focused && { backgroundColor: theme.backgroundSelected }]}>
              {options.tabBarIcon?.({ focused, color, size: 22 })}
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
          style={[styles.avatar, { backgroundColor: theme.surface, borderColor: account.key === focusedKey ? theme.text : theme.surfaceBorder }]}>
          <ThemedText style={styles.avatarText}>Z</ThemedText>
        </Pressable>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: { flexDirection: 'row', alignItems: 'center', gap: 10, paddingHorizontal: 14, paddingTop: 8 },
  pill: { flex: 1, flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', height: 52, borderRadius: 26, borderWidth: StyleSheet.hairlineWidth, paddingHorizontal: 5 },
  item: { flex: 1, height: 42, borderRadius: 21, alignItems: 'center', justifyContent: 'center' },
  avatar: { width: 52, height: 52, borderRadius: 26, borderWidth: 1, alignItems: 'center', justifyContent: 'center' },
  avatarText: { fontSize: 18, fontWeight: '600' },
});
