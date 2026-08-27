import { Tabs } from 'expo-router';
import { SymbolView } from 'expo-symbols';

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
      <Tabs.Screen
        name="search"
        options={{
          title: 'Search',
          tabBarIcon: ({ color, size }) => <SymbolView name="magnifyingglass" tintColor={color} size={size} />,
        }}
      />
      <Tabs.Screen
        name="mutations"
        options={{
          title: 'Mutations',
          tabBarIcon: ({ color, size }) => <SymbolView name="checkmark.seal" tintColor={color} size={size} />,
        }}
      />
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
