import { requireOptionalNativeModule } from 'expo';

import type { AndroidChat } from '@/lib/android-chat';

// android/src/main/java/expo/modules/pdwchatlauncher/PdwChatLauncherModule.kt.
// Android only; null on iOS, the web, and an APK built before this module.
type PdwChatLauncher = { openChat(app: string, chatGuid: string, recipients: string): Promise<boolean> };

const native = requireOptionalNativeModule<PdwChatLauncher>('PdwChatLauncher');

// Opens the chat in BlueBubbles or Google Messages; false when the module or
// the app is missing, so the caller can fall back to the link's URL.
export async function openAndroidChat(chat: AndroidChat): Promise<boolean> {
  if (!native) return false;
  return chat.app === 'bluebubbles'
    ? native.openChat(chat.app, chat.chat_guid, '')
    : native.openChat(chat.app, '', chat.recipients);
}
