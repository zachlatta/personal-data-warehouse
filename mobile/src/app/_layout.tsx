import { DarkTheme, DefaultTheme, ThemeProvider, useRouter, Stack } from 'expo-router';
import * as Notifications from 'expo-notifications';
import * as SplashScreen from 'expo-splash-screen';
import { useEffect } from 'react';
import { useColorScheme } from 'react-native';

import { routeFromNotification } from '@/lib/push';
import { SessionProvider, useSession } from '@/lib/session';

SplashScreen.preventAutoHideAsync();

function Root() {
  const { ready, config } = useSession();
  const router = useRouter();

  useEffect(() => {
    if (ready) SplashScreen.hideAsync();
  }, [ready]);

  // Notification taps: the one that launched the app, and any while running.
  useEffect(() => {
    if (!ready || !config) return;
    let cancelled = false;
    Notifications.getLastNotificationResponseAsync().then((response) => {
      const route = routeFromNotification(response);
      if (route && !cancelled) router.push(route as never);
    });
    const sub = Notifications.addNotificationResponseReceivedListener((response) => {
      const route = routeFromNotification(response);
      if (route) router.push(route as never);
    });
    return () => {
      cancelled = true;
      sub.remove();
    };
  }, [ready, config, router]);

  if (!ready) return null;
  return (
    <Stack>
      <Stack.Protected guard={config !== null}>
        <Stack.Screen name="(tabs)" options={{ headerShown: false }} />
        <Stack.Screen name="timeline/[adapter]/[eventId]" options={{ title: 'Event', headerBackTitle: 'Timeline' }} />
        <Stack.Screen name="mutations/[id]" options={{ title: 'Review', headerBackTitle: 'Mutations' }} />
      </Stack.Protected>
      <Stack.Protected guard={config === null}>
        <Stack.Screen name="login" options={{ title: 'Connect to PDW', headerShown: false }} />
      </Stack.Protected>
    </Stack>
  );
}

export default function RootLayout() {
  const colorScheme = useColorScheme();
  return (
    <ThemeProvider value={colorScheme === 'dark' ? DarkTheme : DefaultTheme}>
      <SessionProvider>
        <Root />
      </SessionProvider>
    </ThemeProvider>
  );
}
