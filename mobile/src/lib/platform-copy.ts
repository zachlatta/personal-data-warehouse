// Words that differ by phone. The platform is passed in (Platform.OS) so the
// copy stays testable without React Native.
type PhonePlatform = string;

export function secureStoreName(platform: PhonePlatform): string {
  return platform === 'android' ? 'the Android Keystore' : 'the iOS Keychain';
}

export function notificationSettingsName(platform: PhonePlatform): string {
  return platform === 'android' ? 'Android Settings → Apps → PDW → Notifications' : 'iOS Settings';
}

// The client name is the review actor (app:<client>), so it says which phone acted.
export function defaultClientName(platform: PhonePlatform): string {
  return platform === 'android' ? 'android' : 'iphone';
}
