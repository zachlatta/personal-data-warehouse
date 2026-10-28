// What this build's native code can render, sent with the push registration
// (push.CapabilityAndroidMessagingStyle in app/internal/push). It comes from
// app.json `extra.pushCapabilities`, which ships with the native build that
// has plugins/with-messaging-notifications.js: the plugin changes the
// fingerprint runtime version, so no older build can receive config that
// claims it. iOS never claims an Android capability.
export function pushCapabilities(extra: unknown, platform: string): string[] {
  const declared = (extra as { pushCapabilities?: unknown } | null | undefined)?.pushCapabilities;
  if (!Array.isArray(declared)) return [];
  return declared.filter((value): value is string => typeof value === 'string' && (platform === 'android' || !value.startsWith('android_')));
}
