import * as WebBrowser from 'expo-web-browser';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Linking, Platform, Pressable, StyleSheet, View } from 'react-native';
import { WebView, type WebViewMessageEvent } from 'react-native-webview';

import { ThemedText } from '@/components/themed-text';
import { useTheme } from '@/hooks/use-theme';
import { EMAIL_EDITOR_SCRIPT, emailEditorDocument, emailEditorHtmlFromMessage, type EmailEditorMessage } from '@/lib/email-editor';
import { emailNavigation, emailWebViewPlatformProps } from '@/lib/email-html';

export const EMAIL_TOOLBAR_COMMANDS = [
  { label: 'B', command: 'bold', title: 'Bold' },
  { label: 'I', command: 'italic', title: 'Italic' },
  { label: '•', command: 'insertUnorderedList', title: 'Bulleted list' },
  { label: '1.', command: 'insertOrderedList', title: 'Numbered list' },
  { label: '↶', command: 'undo', title: 'Undo' },
] as const;

export function EmailFormatToolbar({ onCommand }: { onCommand: (command: string) => void }) {
  const theme = useTheme();
  return (
    <View style={[styles.pinnedToolbar, { backgroundColor: theme.backgroundElement, borderTopColor: theme.backgroundSelected, borderBottomColor: theme.backgroundSelected }]}>
      {EMAIL_TOOLBAR_COMMANDS.map((item) => (
        <Pressable
          key={item.command}
          accessibilityRole="button"
          accessibilityLabel={item.title}
          onPress={() => onCommand(item.command)}
          style={[styles.tbButton, { backgroundColor: theme.background }]}>
          <ThemedText type="smallBold" style={item.command === 'italic' ? styles.italic : undefined}>
            {item.label}
          </ThemedText>
        </Pressable>
      ))}
    </View>
  );
}

async function openOutside(url: string) {
  try {
    if (/^https?:/i.test(url)) await WebBrowser.openBrowserAsync(url);
    else await Linking.openURL(url);
  } catch {
    // Ignore unopenable links during review.
  }
}

// Rich WYSIWYG email body editor backed by a CSP-protected WebView on a white
// surface (matching EmailHtmlBody so styled emails stay readable in dark theme).
// When focused, reports its command handler up via onToolbarCommandChange so
// the screen pins the formatting toolbar directly above the keyboard/footer.
export function EmailRichEditor({
  initialHtml,
  resetKey,
  editable,
  locked,
  onChangeHtml,
  onFocus,
  onBlur,
  onToolbarCommandChange,
  onRegisterSnapshotReader,
}: {
  initialHtml: string;
  resetKey: string;
  editable: boolean;
  locked?: boolean;
  onChangeHtml: (html: string) => void;
  onFocus: () => void;
  onBlur: () => void;
  onToolbarCommandChange?: (exec: ((command: string) => void) | null) => void;
  onRegisterSnapshotReader?: (read: (() => Promise<string>) | null) => void;
}) {
  const theme = useTheme();
  const webViewRef = useRef<WebView>(null);
  const [height, setHeight] = useState(160);
  const [focused, setFocused] = useState(false);

  // Capture the exact HTML the WebView document was built from (never the live
  // draft prop) so 'revert' and 'snapshot' with isBaseline restore that exact
  // HTML (server copy or reopened unsaved draft).
  const { document, builtHtml } = useMemo(
    () => ({ document: emailEditorDocument(initialHtml, editable), builtHtml: initialHtml }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [resetKey],
  );
  const builtHtmlRef = useRef(builtHtml);
  const latestHtmlRef = useRef(builtHtml);
  useEffect(() => {
    builtHtmlRef.current = builtHtml;
    latestHtmlRef.current = builtHtml;
  }, [builtHtml, resetKey]);

  const pendingSnapshots = useRef<Map<number, (html: string) => void>>(new Map());
  const nextSnapshotId = useRef(1);

  const readSnapshot = useCallback((): Promise<string> => {
    const view = webViewRef.current;
    if (!view) return Promise.resolve(latestHtmlRef.current);
    const id = nextSnapshotId.current++;
    return new Promise<string>((resolve) => {
      const timer = setTimeout(() => {
        if (pendingSnapshots.current.delete(id)) resolve(latestHtmlRef.current);
      }, 350);
      pendingSnapshots.current.set(id, (html) => {
        clearTimeout(timer);
        resolve(html);
      });
      view.injectJavaScript(`window.__pdwReadSnapshot && window.__pdwReadSnapshot(${id}); true;`);
    });
  }, []);

  const snapshotRegRef = useRef(onRegisterSnapshotReader);
  useEffect(() => {
    snapshotRegRef.current = onRegisterSnapshotReader;
  }, [onRegisterSnapshotReader]);
  useEffect(() => {
    snapshotRegRef.current?.(readSnapshot);
    const reg = snapshotRegRef;
    return () => reg.current?.(null);
  }, [readSnapshot]);

  useEffect(() => {
    webViewRef.current?.injectJavaScript(`window.__pdwSetEditable && window.__pdwSetEditable(${editable ? 'true' : 'false'}); true;`);
  }, [editable]);

  const runCommand = useCallback((command: string) => {
    webViewRef.current?.injectJavaScript(`window.__pdwExec && window.__pdwExec(${JSON.stringify(command)}); true;`);
  }, []);

  const toolbarRef = useRef(onToolbarCommandChange);
  useEffect(() => {
    toolbarRef.current = onToolbarCommandChange;
  }, [onToolbarCommandChange]);

  useEffect(() => {
    toolbarRef.current?.(editable && focused ? runCommand : null);
  }, [editable, focused, runCommand]);

  useEffect(() => {
    const reporter = toolbarRef;
    return () => reporter.current?.(null);
  }, []);

  const onMessage = (event: WebViewMessageEvent) => {
    try {
      const data = JSON.parse(event.nativeEvent.data) as EmailEditorMessage;
      if (data.type === 'height' && typeof data.height === 'number' && data.height > 0) {
        setHeight(Math.max(140, data.height));
      } else if (data.type === 'snapshot' && typeof data.id === 'number') {
        const html = emailEditorHtmlFromMessage(data, builtHtmlRef.current) ?? latestHtmlRef.current;
        latestHtmlRef.current = html;
        const resolve = pendingSnapshots.current.get(data.id);
        if (resolve) {
          pendingSnapshots.current.delete(data.id);
          resolve(html);
        }
      } else if (data.type === 'change' || data.type === 'revert') {
        const html = emailEditorHtmlFromMessage(data, builtHtmlRef.current);
        if (html !== null) {
          latestHtmlRef.current = html;
          onChangeHtml(html);
        }
      } else if (data.type === 'focus') {
        setFocused(true);
        onFocus();
      } else if (data.type === 'blur') {
        setFocused(false);
        onBlur();
      }
    } catch {
      // Only our injected script posts JSON messages.
    }
  };

  return (
    <View style={[styles.wrap, { borderColor: theme.backgroundSelected }]}>
      {!onToolbarCommandChange && editable && focused ? <EmailFormatToolbar onCommand={runCommand} /> : null}
      <View pointerEvents={locked && !focused ? 'none' : 'auto'} style={{ height }}>
        <WebView
          key={resetKey}
          ref={webViewRef}
          source={{ html: document }}
          originWhitelist={['*']}
          injectedJavaScript={EMAIL_EDITOR_SCRIPT}
          onMessage={onMessage}
          onShouldStartLoadWithRequest={(request) => {
            const action = emailNavigation(request.url);
            if (action === 'load') return true;
            if (action === 'open' && !editable) void openOutside(request.url);
            return false;
          }}
          scrollEnabled={false}
          bounces={false}
          nestedScrollEnabled={false}
          keyboardDisplayRequiresUserAction={false}
          hideKeyboardAccessoryView={false}
          javaScriptCanOpenWindowsAutomatically={false}
          allowFileAccess={false}
          {...emailWebViewPlatformProps(Platform.OS)}
          incognito
          style={styles.web}
        />
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    minHeight: 140,
    marginVertical: 8,
    borderRadius: 8,
    overflow: 'hidden',
    backgroundColor: '#FFFFFF',
    borderWidth: StyleSheet.hairlineWidth,
  },
  pinnedToolbar: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    paddingHorizontal: 14,
    paddingVertical: 7,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  tbButton: {
    minWidth: 38,
    minHeight: 34,
    paddingHorizontal: 11,
    borderRadius: 7,
    alignItems: 'center',
    justifyContent: 'center',
  },
  italic: { fontStyle: 'italic' },
  web: { flex: 1, backgroundColor: '#FFFFFF' },
});
