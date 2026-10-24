import { DarkTheme, DefaultTheme, ThemeProvider, useRouter, Stack } from 'expo-router';
import * as Notifications from 'expo-notifications';
import * as SplashScreen from 'expo-splash-screen';
import { StatusBar } from 'expo-status-bar';
import { useEffect, useRef } from 'react';
import { Alert, AppState, useColorScheme } from 'react-native';
import { GestureHandlerRootView } from 'react-native-gesture-handler';

import { Colors } from '@/constants/theme';
import { handleNotificationResponse, seedFromNotification, syncNotificationCategories, flushNotificationOpens } from '@/lib/push';
import { decisions } from '@/lib/undo-decision';
import { applyUpdateNow } from '@/lib/updates';
import { SessionProvider, useSession } from '@/lib/session';
import { TrackedBrowserHost } from '@/features/feed/tracked-browser';

SplashScreen.preventAutoHideAsync();

// Navigation chrome (headers, cards, the tab scene) in the app's own palette,
// so a header is the page's colour with no bar or shadow, as in Superhuman.
const navTheme = (dark: boolean) => {
  const base = dark ? DarkTheme : DefaultTheme;
  const c = dark ? Colors.dark : Colors.light;
  return { ...base, colors: { ...base.colors, primary: c.accent, background: c.background, card: c.background, text: c.text, border: c.separator } };
};
const NAV_THEMES = { dark: navTheme(true), light: navTheme(false) };

function Root() {
  const { ready, config } = useSession();
  const router = useRouter();
  const handled = useRef(new Set<string>());

  useEffect(() => {
    if (ready) SplashScreen.hideAsync();
  }, [ready]);

  // Apply a newer OTA bundle on this launch rather than the next one.
  useEffect(() => {
    void applyUpdateNow(() => {});
  }, []);

  // A decision still inside its undo window is sent when the app leaves the
  // foreground: the reviewer has moved on, and a suspended or killed app
  // would otherwise leave it unsent with no one watching.
  useEffect(() => {
    const sub = AppState.addEventListener('change', (state) => {
      if (state === 'background') void decisions.flush();
    });
    return () => sub.remove();
  }, []);

  // Categories give alerts their action buttons; the list is the server's.
  useEffect(() => {
    if (!ready || !config) return;
    void syncNotificationCategories(config);
    void flushNotificationOpens().catch(error => console.warn('notification open will retry', error));
    const sub = AppState.addEventListener('change', (state) => { if (state === 'active') void flushNotificationOpens().catch(error => console.warn('notification open will retry', error)); });
    return () => sub.remove();
  }, [ready, config]);

  // Notification taps and action buttons: the one that launched the app,
  // and any while running (including background actions like Approve).
  useEffect(() => {
    if (!ready || !config) return;
    let cancelled = false;
    const act = async (response: Notifications.NotificationResponse | null | undefined) => {
      if (!response) return;
      const key = `${response.notification.request.identifier}:${response.actionIdentifier}`;
      if (handled.current.has(key)) return;
      handled.current.add(key);
      try {
        const outcome = await handleNotificationResponse(config, response);
        if (cancelled) return;
        if (outcome.message) Alert.alert('PDW', outcome.message);
        if (outcome.route) router.push(outcome.route as never);
        if (outcome.recorded && !await outcome.recorded) throw new Error('notification open was not durably saved');
        const last = await Notifications.getLastNotificationResponseAsync();
        if (last?.notification.request.identifier === response.notification.request.identifier) await Notifications.clearLastNotificationResponseAsync();
      } catch (error) {
        // Keep the OS launch response available if durable enqueue failed.
        handled.current.delete(key);
        console.warn('notification response will retry on next launch', error);
      } finally {
        // Dedupe the cold-start/listener race, not a deliberate later tap.
        setTimeout(() => handled.current.delete(key), 2000);
      }
    };
    void Notifications.getLastNotificationResponseAsync().then(act).catch(error => console.warn('notification launch response unavailable', error));
    const sub = Notifications.addNotificationResponseReceivedListener((response) => void act(response));
    // An alert that arrives while the app is open seeds the review it carries,
    // so opening it from the list is instant too.
    const received = Notifications.addNotificationReceivedListener((notification) => seedFromNotification(notification));
    return () => {
      cancelled = true;
      sub.remove();
      received.remove();
    };
  }, [ready, config, router]);

  if (!ready) return null;
  return (
    <Stack screenOptions={{ headerShadowVisible: false, headerTitleStyle: { fontSize: 17, fontWeight: '600' } }}>
      <Stack.Protected guard={config !== null}>
        <Stack.Screen name="(tabs)" options={{ headerShown: false }} />
        <Stack.Screen name="timeline/[adapter]/[eventId]" options={{ title: 'Event', headerBackTitle: 'Timeline' }} />
        <Stack.Screen name="mutations/[id]" options={{ title: 'Review', headerBackTitle: 'Mutations' }} />
        <Stack.Screen name="feed/[id]" options={{ title: '', headerBackTitle: 'Feed' }} />
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
    <GestureHandlerRootView style={{ flex: 1 }}>
      <ThemeProvider value={colorScheme === 'dark' ? NAV_THEMES.dark : NAV_THEMES.light}>
        <SessionProvider>
          <Root />
          <TrackedBrowserHost />
        </SessionProvider>
        {/* Android draws edge to edge: without this the status bar kept
            light icons over the light theme and the clock disappeared. */}
        <StatusBar style="auto" />
      </ThemeProvider>
    </GestureHandlerRootView>
  );
}
