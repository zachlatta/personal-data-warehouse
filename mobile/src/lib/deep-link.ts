import { Linking, Platform } from 'react-native';

import { openAndroidChat } from '../../modules/pdw-chat-launcher';
import { linkCanOpen, openOrder } from './android-chat.ts';
import type { TimelineDeepLink } from './api';

// Open a timeline item in its source app, trying each step of openOrder in
// turn: on Android a chat goes straight into BlueBubbles or Google Messages
// (modules/pdw-chat-launcher); then the native scheme (it lands inside
// Slack/Messages/Notes instead of in a browser); then the web URL. An
// unhandled scheme rejects and the next step runs. canOpenURL is deliberately
// not used: on iOS it needs every scheme declared in the native Info.plist,
// which would turn each new source into a native rebuild.
export async function openDeepLink(link: TimelineDeepLink): Promise<void> {
  let lastError: unknown = null;
  for (const step of openOrder(link, Platform.OS)) {
    try {
      if (step.kind === 'android') {
        if (await openAndroidChat(step.chat)) return;
        continue;
      }
      await Linking.openURL(step.url);
      return;
    } catch (e) {
      lastError = e;
    }
  }
  throw lastError instanceof Error ? lastError : new Error(`Could not open ${link.label}`);
}

// Whether this phone has anything to open for a link: a group chat's link
// carries only an Android target, so iOS shows no button for it.
export function canOpenDeepLink(link: TimelineDeepLink | null | undefined): link is TimelineDeepLink {
  return linkCanOpen(link, Platform.OS);
}
