import Constants from 'expo-constants';
import * as Device from 'expo-device';
import * as Notifications from 'expo-notifications';
import { Platform } from 'react-native';

import { approveMutationRequest, fetchPushCategories, registerPushDevice, rejectMutationRequest } from './api';
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

// Register the server's notification categories so its categoryId values
// put action buttons on alerts. Failure is logged, not fatal: the alert still
// shows, only without buttons.
export async function syncNotificationCategories(config: AppConfig): Promise<void> {
  try {
    const categories = await fetchPushCategories(config);
    await Promise.all(
      categories.map((category) =>
        Notifications.setNotificationCategoryAsync(
          category.id,
          category.actions.map((action) => ({
            identifier: action.id,
            buttonTitle: action.title,
            textInput: action.text_input
              ? { placeholder: action.text_input.placeholder, submitButtonTitle: action.text_input.submit_title }
              : undefined,
            options: {
              isDestructive: action.destructive,
              opensAppToForeground: action.opens_app,
              isAuthenticationRequired: false,
            },
          })),
        ),
      ),
    );
  } catch (error) {
    console.warn('notification categories not registered', error);
  }
}

type NotificationData = { route?: unknown; request_id?: unknown; kind?: unknown };

function dataOf(response: Notifications.NotificationResponse | null | undefined): NotificationData {
  return (response?.notification.request.content.data as NotificationData | undefined) ?? {};
}

// The server puts the screen to open under data.route (see mutationNotification
// in the Go app).
export function routeFromNotification(response: Notifications.NotificationResponse | null | undefined): string | null {
  const data = dataOf(response);
  return typeof data.route === 'string' && data.route.startsWith('/') ? data.route : null;
}

export type NotificationOutcome = { route: string | null; message?: string };

// What to do with a tap or an action button. Approve/Deny on a mutation
// alert call the review API directly (the button does not open the app);
// everything else opens the notification's route. A reply action's text
// comes back in response.userText; it has no server endpoint yet, so it
// opens the route with the text reported, rather than being dropped.
export async function handleNotificationResponse(
  config: AppConfig,
  response: Notifications.NotificationResponse,
): Promise<NotificationOutcome> {
  const data = dataOf(response);
  const route = routeFromNotification(response);
  const action = response.actionIdentifier;
  const requestId = typeof data.request_id === 'string' ? data.request_id : null;
  if (requestId && (action === 'approve' || action === 'deny')) {
    try {
      if (action === 'approve') {
        await approveMutationRequest(config, requestId);
        return { route: null, message: 'Approved from the notification.' };
      }
      await rejectMutationRequest(config, requestId, 'Denied from the notification.');
      return { route: null, message: 'Denied from the notification.' };
    } catch (error) {
      // The decision did not land; open the review screen so it can be made there.
      return { route: route ?? `/mutations/${requestId}`, message: error instanceof Error ? error.message : String(error) };
    }
  }
  if (action === 'reply' && typeof response.userText === 'string') {
    return { route, message: `Reply captured: ${response.userText}` };
  }
  return { route };
}
