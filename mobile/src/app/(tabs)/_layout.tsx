import { Tabs, useRouter } from 'expo-router';
import { Pressable, StyleSheet } from 'react-native';

import { Icon } from '@/components/icon';
import { PillTabBar } from '@/components/pill-tab-bar';
import { feedTabOptions } from '@/features/feed';
import { usePendingMutationCount } from '@/hooks/use-pending-mutation-count';
import { useTheme } from '@/hooks/use-theme';

// A round toolbar button in the header, Superhuman's top-right search and
// compose buttons.
function HeaderButton({ icon, label, onPress }: { icon: 'magnifyingglass'; label: string; onPress: () => void }) {
  const theme = useTheme();
  return (
    <Pressable accessibilityRole="button" accessibilityLabel={label} hitSlop={6} onPress={onPress} style={[styles.headerButton, { backgroundColor: theme.surface, borderColor: theme.surfaceBorder }]}>
      <Icon name={icon} tintColor={theme.text} size={20} />
    </Pressable>
  );
}

export default function TabsLayout() {
  const router = useRouter();
  const pendingMutations = usePendingMutationCount();
  return (
    <Tabs
      tabBar={(props) => <PillTabBar {...props} />}
      screenOptions={{
        headerShadowVisible: false,
        headerTitleAlign: 'left',
        headerTitleStyle: { fontSize: 22, fontWeight: '600' },
      }}>
      <Tabs.Screen
        name="index"
        options={{
          title: 'Timeline',
          tabBarIcon: ({ color, size }) => <Icon name="clock" tintColor={color} size={size} />,
          headerRight: () => <HeaderButton icon="magnifyingglass" label="Search" onPress={() => router.navigate('/search')} />,
        }}
      />
      <Tabs.Screen name="feed" options={feedTabOptions} />
      {/* Off the tab bar but still routable: Settings opens Alerts, and pushes keep coming. */}
      <Tabs.Screen name="search" options={{ title: 'Search', href: null }} />
      <Tabs.Screen
        name="mutations"
        options={{
          title: 'Mutations',
          tabBarBadge: pendingMutations > 0 ? (pendingMutations > 99 ? '99+' : pendingMutations) : undefined,
          tabBarIcon: ({ color, size }) => <Icon name="checkmark.seal" tintColor={color} size={size} />,
        }}
      />
      <Tabs.Screen name="notifications" options={{ title: 'Alerts', href: null }} />
      <Tabs.Screen
        name="settings"
        options={{
          title: 'Settings',
          tabBarIcon: ({ color, size }) => <Icon name="gearshape" tintColor={color} size={size} />,
        }}
      />
    </Tabs>
  );
}

const styles = StyleSheet.create({
  headerButton: { width: 40, height: 40, borderRadius: 20, borderWidth: StyleSheet.hairlineWidth, alignItems: 'center', justifyContent: 'center', marginRight: 14 },
});
