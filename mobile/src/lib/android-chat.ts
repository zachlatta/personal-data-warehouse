// A chat the Android app opens directly in its messaging app: BlueBubbles
// for iMessage (by chat guid, which also opens group chats), Google Messages
// for texts (by members, or its conversation list for an opaque RCS group).
// The server's side is AndroidChat in app/internal/deeplink/android.go.
//
// Pure (no React Native import) so the node tests can run it. The payload
// comes from a push, so it is validated here and again natively
// (modules/pdw-chat-launcher): only these two apps, only these shapes.

export type AndroidChat =
  | { app: 'bluebubbles'; chat_guid: string }
  | { app: 'google_messages'; recipients: string };

type LinkLike = { url?: unknown; app_url?: unknown; android?: unknown } | null | undefined;

// chat.db's guid, "<service>;<-|+>;<identifier>": BlueBubbles' chat guid.
const CHAT_GUID = /^[A-Za-z]{2,16};[-+];[^\x00-\x1f\x7f;]{1,200}$/;
const PHONE = /^\+?[0-9]{3,20}$/;
const EMAIL = /^[A-Za-z0-9._+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$/;
const MAX_RECIPIENTS = 20;

function validRecipients(value: string): boolean {
  if (value === '') return true;
  const members = value.split(',');
  return members.length <= MAX_RECIPIENTS && members.every((m) => PHONE.test(m) || EMAIL.test(m));
}

export function androidChatOf(value: unknown): AndroidChat | null {
  if (!value || typeof value !== 'object') return null;
  const v = value as Record<string, unknown>;
  if (v.app === 'bluebubbles') {
    return typeof v.chat_guid === 'string' && CHAT_GUID.test(v.chat_guid) ? { app: 'bluebubbles', chat_guid: v.chat_guid } : null;
  }
  if (v.app === 'google_messages') {
    const recipients = v.recipients ?? '';
    return typeof recipients === 'string' && validRecipients(recipients) ? { app: 'google_messages', recipients } : null;
  }
  return null;
}

export type OpenStep = { kind: 'android'; chat: AndroidChat } | { kind: 'url'; url: string };

// What a phone tries, in order, to open a link. On Android the chat target
// comes first: it lands in the conversation itself, where a URL scheme lands
// in whichever app claims it (sms: is Google Messages even for an iMessage).
// The native scheme is next (it opens the app rather than a browser), and the
// web URL is the fallback; a browser has only the URL.
export function openOrder(link: LinkLike, platform: string): OpenStep[] {
  if (!link) return [];
  const steps: OpenStep[] = [];
  if (platform === 'android') {
    const chat = androidChatOf(link.android);
    if (chat) steps.push({ kind: 'android', chat });
  }
  const urls = platform === 'web' ? [link.url] : [link.app_url, link.url];
  for (const url of urls) {
    if (typeof url === 'string' && url && !steps.some((s) => s.kind === 'url' && s.url === url)) steps.push({ kind: 'url', url });
  }
  return steps;
}

// Whether this platform has anything to open: a group chat's link carries
// only an Android target, so iOS and the web show no button for it.
export function linkCanOpen(link: LinkLike, platform: string): boolean {
  return openOrder(link, platform).length > 0;
}
