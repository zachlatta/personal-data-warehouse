import { Tabs } from 'expo-router';
import { SymbolView } from 'expo-symbols';

import { feedTabOptions } from '@/features/feed';

export default function TabsLayout() {
  return (
    <Tabs screenOptions={{ tabBarActiveTintColor: '#208AEF' }}>
      <Tabs.Screen
        name="index"
        options={{
          title: 'Timeline',
          tabBarIcon: ({ color, size }) => <SymbolView name="clock" tintColor={color} size={size} />,
        }}
      />
      <Tabs.Screen name="feed" options={feedTabOptions} />
      {/* Off the tab bar but still routable: Settings opens Alerts, and pushes keep coming. */}
      <Tabs.Screen name="search" options={{ title: 'Search', href: null }} />
      <Tabs.Screen
        name="mutations"
        options={{
          title: 'Mutations',
          tabBarIcon: ({ color, size }) => <SymbolView name="checkmark.seal" tintColor={color} size={size} />,
        }}
      />
      <Tabs.Screen name="notifications" options={{ title: 'Alerts', href: null }} />
      <Tabs.Screen
        name="settings"
        options={{
          title: 'Settings',
          tabBarIcon: ({ color, size }) => <SymbolView name="gearshape" tintColor={color} size={size} />,
        }}
      />
    </Tabs>
  );
}
