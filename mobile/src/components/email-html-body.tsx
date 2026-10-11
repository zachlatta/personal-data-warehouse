import * as WebBrowser from 'expo-web-browser';
import { useMemo, useState } from 'react';
import { Linking, Pressable, StyleSheet, View } from 'react-native';
import { WebView, type WebViewMessageEvent } from 'react-native-webview';

import { ThemedText } from '@/components/themed-text';
import { EMAIL_BODY_COLLAPSED_HEIGHT, EMAIL_SIZER_SCRIPT, emailHtmlDocument, emailNavigation } from '@/lib/email-html';

async function openOutside(url: string) {
  try {
    if (/^https?:/i.test(url)) await WebBrowser.openBrowserAsync(url);
    else await Linking.openURL(url);
  } catch {
    // A link the phone cannot open is not worth an alert in the middle of a review.
  }
}

// An email's HTML part, rendered as the sender laid it out: on white, scaled
// to the phone's width, as tall as the message up to a cap, and never
// scrollable inside the list. The mail's own scripts never run (see
// lib/email-html.ts); a tapped link opens in Safari, not in the review.
export function EmailHtmlBody({ html }: { html: string }) {
  const document = useMemo(() => emailHtmlDocument(html), [html]);
  const [height, setHeight] = useState(0);
  const [full, setFull] = useState(false);
  const capped = !full && height > EMAIL_BODY_COLLAPSED_HEIGHT;
  const shown = height ? (capped ? EMAIL_BODY_COLLAPSED_HEIGHT : height) : 160;

  const onMessage = (event: WebViewMessageEvent) => {
    try {
      const data = JSON.parse(event.nativeEvent.data) as { type?: string; height?: number };
      if (data.type === 'height' && typeof data.height === 'number' && data.height > 0) setHeight(data.height);
    } catch {
      // Only the injected sizing script posts, and it posts JSON.
    }
  };

  return (
    <View style={styles.frame}>
      <View style={{ height: shown, overflow: 'hidden' }}>
        <WebView
          source={{ html: document }}
          // Every navigation goes through onShouldStartLoadWithRequest; a
          // narrower whitelist would hand links to Linking before it ran.
          originWhitelist={['*']}
          injectedJavaScript={EMAIL_SIZER_SCRIPT}
          onMessage={onMessage}
          onShouldStartLoadWithRequest={(request) => {
            const action = emailNavigation(request.url);
            if (action === 'load') return true;
            if (action === 'open') void openOutside(request.url);
            return false;
          }}
          scrollEnabled={false}
          bounces={false}
          javaScriptCanOpenWindowsAutomatically={false}
          allowsLinkPreview={false}
          dataDetectorTypes="none"
          cacheEnabled
          incognito
          style={styles.web}
        />
      </View>
      {capped ? (
        <Pressable accessibilityRole="button" onPress={() => setFull(true)} style={styles.more} hitSlop={6}>
          <ThemedText type="smallBold" style={styles.moreText}>Show full email</ThemedText>
        </Pressable>
      ) : full && height > EMAIL_BODY_COLLAPSED_HEIGHT ? (
        <Pressable accessibilityRole="button" onPress={() => setFull(false)} style={styles.more} hitSlop={6}>
          <ThemedText type="smallBold" style={styles.moreText}>Show less</ThemedText>
        </Pressable>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  frame: { marginTop: 8, borderRadius: 10, overflow: 'hidden', backgroundColor: '#FFFFFF' },
  web: { backgroundColor: '#FFFFFF' },
  more: { minHeight: 40, alignItems: 'center', justifyContent: 'center', borderTopWidth: StyleSheet.hairlineWidth, borderTopColor: '#E5E7EB', backgroundColor: '#F9FAFB' },
  moreText: { color: '#2563EB' },
});
