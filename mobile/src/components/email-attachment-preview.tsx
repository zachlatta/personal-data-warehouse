import { Directory, File, Paths } from 'expo-file-system';
import { Image } from 'expo-image';
import * as Sharing from 'expo-sharing';
import { useEffect, useState } from 'react';
import { ActivityIndicator, Modal, Platform, Pressable, ScrollView, StyleSheet, View } from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { WebView } from 'react-native-webview';

import { ThemedText } from '@/components/themed-text';
import { useTheme } from '@/hooks/use-theme';
import { attachmentPreviewKind, prepareAttachment } from '@/lib/email-attachment';
import type { GmailEmailAttachment } from '@/lib/mutation-review';

function AttachmentViewer({ attachment, onClose }: { attachment: GmailEmailAttachment; onClose: () => void }) {
  const theme = useTheme();
  const [file, setFile] = useState<{ uri: string; previewUri: string; text?: string }>();
  const [error, setError] = useState('');
  const [sharing, setSharing] = useState(false);
  const kind = attachmentPreviewKind(attachment.content_type, Platform.OS);

  useEffect(() => {
    const directory = new Directory(Paths.cache, `email-review-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    let prepared: ReturnType<typeof prepareAttachment> | undefined;
    const frame = requestAnimationFrame(() => {
      try {
        prepared = prepareAttachment(attachment, {
          write(filename, base64) {
            directory.create();
            const target = new File(directory, filename);
            target.write(base64, { encoding: 'base64' });
            return target.uri;
          },
          remove() { if (directory.exists) directory.delete(); },
        });
        const target = new File(prepared.uri);
        // WKWebView uses the extension to select its PDF viewer, even if the
        // proposed filename omitted it. Never render arbitrary HTML attachments.
        setFile({ uri: target.uri, previewUri: target.uri });
        let previewUri = target.uri;
        if (kind === 'pdf') {
          if (!attachment.data_base64.startsWith('JVBERi0')) throw new Error('This file is not a valid PDF. Use Share / Save to open it in another app.');
          const pdf = new File(directory, 'preview.pdf');
          if (pdf.uri !== target.uri) target.copy(pdf);
          previewUri = pdf.uri;
        }
        setFile({ uri: target.uri, previewUri, text: kind === 'text' ? target.textSync() : undefined });
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Could not open attachment.');
      }
    });
    return () => { cancelAnimationFrame(frame); prepared?.dispose(); };
  }, [attachment, kind]);

  const share = async () => {
    if (!file || sharing) return;
    setSharing(true);
    try {
      if (!await Sharing.isAvailableAsync()) throw new Error('File sharing is not available on this device.');
      await Sharing.shareAsync(file.uri, { mimeType: attachment.content_type, dialogTitle: attachment.filename });
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not share attachment.');
    } finally {
      setSharing(false);
    }
  };

  return (
    <Modal animationType="slide" presentationStyle="fullScreen" onRequestClose={() => { if (!sharing) onClose(); }}>
      <SafeAreaView style={[styles.screen, { backgroundColor: theme.background }]}>
        <View style={styles.header}>
          <Pressable accessibilityRole="button" onPress={onClose} disabled={sharing} style={styles.button}><ThemedText style={styles.link}>Done</ThemedText></Pressable>
          <ThemedText type="smallBold" numberOfLines={2} style={styles.title}>{attachment.filename || 'Attachment'}</ThemedText>
          <Pressable accessibilityRole="button" onPress={() => void share()} disabled={!file || sharing} style={styles.button}><ThemedText style={styles.link}>{sharing ? 'Sharing…' : 'Share / Save'}</ThemedText></Pressable>
        </View>
        {error ? <ThemedText accessibilityRole="alert" style={styles.error}>{error}</ThemedText> : null}
        {!file && !error ? <ActivityIndicator accessibilityLabel="Loading attachment" /> : null}
        {file && kind === 'image' ? <Image source={{ uri: file.uri }} contentFit="contain" style={styles.screen} onError={() => setError('Could not preview this image. Use Share / Save to open it in another app.')} /> : null}
        {file && kind === 'text' ? <ScrollView contentContainerStyle={styles.text}><ThemedText selectable>{file.text}</ThemedText></ScrollView> : null}
        {file && kind === 'pdf' && !error ? <WebView
          source={{ uri: file.previewUri }}
          style={styles.screen}
          javaScriptEnabled={false}
          domStorageEnabled={false}
          incognito
          originWhitelist={['*']}
          onShouldStartLoadWithRequest={(request) => request.url === file.previewUri}
          onError={() => setError('Could not preview this PDF. Use Share / Save to open it in another app.')}
        /> : null}
        {file && kind === 'external' ? <ThemedText style={styles.text}>Use Share / Save to view this file in another app or save it to Files.</ThemedText> : null}
      </SafeAreaView>
    </Modal>
  );
}

export function EmailAttachments({ attachments }: { attachments: GmailEmailAttachment[] }) {
  const [selected, setSelected] = useState<GmailEmailAttachment>();
  return <View style={styles.list}>
    <ThemedText type="smallBold">Attachments ({attachments.length})</ThemedText>
    {attachments.map((attachment, index) => <Pressable
      key={index} accessibilityRole="button" accessibilityLabel={`View attachment ${attachment.filename}`}
      onPress={() => setSelected(attachment)} style={styles.attachment}>
      <ThemedText type="smallBold" style={styles.link}>{attachment.filename || 'Attachment'}</ThemedText>
      <ThemedText type="small" themeColor="textSecondary">{attachment.content_type} · Tap to view</ThemedText>
    </Pressable>)}
    <ThemedText type="small" themeColor="textSecondary">Email edits keep these files. Use the web review to change attachments.</ThemedText>
    {selected ? <AttachmentViewer attachment={selected} onClose={() => setSelected(undefined)} /> : null}
  </View>;
}

const styles = StyleSheet.create({
  screen: { flex: 1 },
  header: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 12, gap: 8 },
  title: { flex: 1 },
  button: { minHeight: 44, justifyContent: 'center' },
  attachment: { minHeight: 48, justifyContent: 'center', gap: 4 },
  list: { gap: 6 },
  link: { color: '#3c87f7' },
  error: { color: '#D0342C', padding: 16 },
  text: { padding: 16 },
});
