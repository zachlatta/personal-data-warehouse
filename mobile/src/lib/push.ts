import Constants from 'expo-constants';
import * as Device from 'expo-device';
import * as Notifications from 'expo-notifications';
import { Platform } from 'react-native';

import { registerPushDevice } from './api';
import type { AppConfig } from './config';

// Foreground notifications still show a banner; the default would swallow them.
Notifications.setNotificationHandler({
  handleNotification: async () => ({
    shouldShowBanner: true,
    shouldShowList: true,
    shouldPlaySound: true,
    shouldSetBadge: false,
  }),
});

export type PushStatus =
  | { state: 'unsupported'; reason: string }
  | { state: 'denied' }
  | { state: 'registered'; token: string }
  | { state: 'error'; message: string };

function projectId(): string | undefined {
  const fromEas = Constants.expoConfig?.extra?.eas?.projectId as string | undefined;
  return fromEas ?? (Constants.easConfig?.projectId as string | undefined);
}

// Ask iOS for permission, get the Expo push token, and tell the warehouse
// about it. A simulator has no APNs, so this reports `unsupported` there
// rather than failing; the rest of the app does not depend on it.
export async function registerForPush(config: AppConfig): Promise<PushStatus> {
  if (!Device.isDevice) {
    return { state: 'unsupported', reason: 'Push notifications need a physical device (simulators have no APNs).' };
  }
  try {
    let { status } = await Notifications.getPermissionsAsync();
    if (status !== 'granted') {
      status = (await Notifications.requestPermissionsAsync()).status;
    }
    if (status !== 'granted') return { state: 'denied' };
    const id = projectId();
    if (!id) return { state: 'error', message: 'No EAS projectId in app config.' };
    const token = (await Notifications.getExpoPushTokenAsync({ projectId: id })).data;
    await registerPushDevice(config, {
      expo_push_token: token,
      device_name: Device.deviceName ?? `${Device.manufacturer ?? ''} ${Device.modelName ?? ''}`.trim(),
      platform: Platform.OS,
      app_version: Constants.expoConfig?.version ?? '',
    });
    return { state: 'registered', token };
  } catch (error) {
    return { state: 'error', message: error instanceof Error ? error.message : String(error) };
  }
}

// The server puts the screen to open under data.route (see mutationNotification
// in the Go app).
export function routeFromNotification(response: Notifications.NotificationResponse | null | undefined): string | null {
  const data = response?.notification.request.content.data as { route?: unknown } | undefined;
  return typeof data?.route === 'string' && data.route.startsWith('/') ? data.route : null;
}
