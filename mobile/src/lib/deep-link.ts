import { Linking, Platform } from 'react-native';

import type { TimelineDeepLink } from './api';

// Open a timeline item in its source app. On a phone the native scheme is
// tried first (it lands inside Slack/Messages/Notes instead of in Safari);
// an unhandled scheme rejects, and the web URL is the fallback. canOpenURL
// is deliberately not used: on iOS it needs every scheme declared in the
// native Info.plist, which would turn each new source into a native rebuild.
export async function openDeepLink(link: TimelineDeepLink): Promise<void> {
  const candidates = Platform.OS === 'web' ? [link.url] : [link.app_url, link.url].filter((u): u is string => !!u);
  let lastError: unknown = null;
  for (const url of candidates) {
    try {
      await Linking.openURL(url);
      return;
    } catch (e) {
      lastError = e;
    }
  }
  throw lastError instanceof Error ? lastError : new Error(`Could not open ${link.label}`);
}
