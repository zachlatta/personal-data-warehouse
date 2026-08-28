import { DarkTheme, DefaultTheme, ThemeProvider, useRouter, Stack } from 'expo-router';
import * as Notifications from 'expo-notifications';
import * as SplashScreen from 'expo-splash-screen';
import { useEffect } from 'react';
import { Alert, useColorScheme } from 'react-native';

import { handleNotificationResponse, syncNotificationCategories } from '@/lib/push';
import { applyUpdateNow } from '@/lib/updates';
import { SessionProvider, useSession } from '@/lib/session';

SplashScreen.preventAutoHideAsync();

function Root() {
  const { ready, config } = useSession();
  const router = useRouter();

  useEffect(() => {
    if (ready) SplashScreen.hideAsync();
  }, [ready]);

  // Apply a newer OTA bundle on this launch rather than the next one.
  useEffect(() => {
    void applyUpdateNow(() => {});
  }, []);

  // Categories give alerts their action buttons; the list is the server's.
  useEffect(() => {
    if (!ready || !config) return;
    void syncNotificationCategories(config);
  }, [ready, config]);

  // Notification taps and action buttons: the one that launched the app,
  // and any while running (including background actions like Approve).
  useEffect(() => {
    if (!ready || !config) return;
    let cancelled = false;
    const act = async (response: Notifications.NotificationResponse | null | undefined) => {
      if (!response) return;
      const outcome = await handleNotificationResponse(config, response);
      if (cancelled) return;
      if (outcome.message) Alert.alert('PDW', outcome.message);
      if (outcome.route) router.push(outcome.route as never);
    };
    Notifications.getLastNotificationResponseAsync().then(act);
    const sub = Notifications.addNotificationResponseReceivedListener((response) => void act(response));
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
