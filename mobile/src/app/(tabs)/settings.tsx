import Constants from 'expo-constants';
import { useState } from 'react';
import { Alert, Pressable, ScrollView, StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { sendTestPush } from '@/lib/api';
import { useConfig, useSession } from '@/lib/session';
import { applyUpdateNow, describeInstalledUpdate, type UpdateState } from '@/lib/updates';

function describePush(push: ReturnType<typeof useSession>['push']): string {
  if (!push) return 'Registering…';
  switch (push.state) {
    case 'registered':
      return `Registered (${push.token})`;
    case 'denied':
      return 'Permission denied — enable notifications for PDW in iOS Settings.';
    case 'unsupported':
      return push.reason;
    case 'error':
      return `Error: ${push.message}`;
  }
}

export default function SettingsScreen() {
  const theme = useTheme();
  const config = useConfig();
  const { push, refreshPush, signOut } = useSession();
  const [testing, setTesting] = useState(false);
  const [update, setUpdate] = useState<UpdateState | null>(null);
  const [testResult, setTestResult] = useState<string | null>(null);

  const test = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const report = await sendTestPush(config);
      setTestResult(`${report.sent} sent, ${report.failed} failed, ${report.devices} device${report.devices === 1 ? '' : 's'} registered.`);
    } catch (e) {
      setTestResult(e instanceof Error ? e.message : String(e));
    } finally {
      setTesting(false);
    }
  };

  const confirmSignOut = () =>
    Alert.alert('Disconnect?', 'The stored token is removed from this device.', [
      { text: 'Cancel', style: 'cancel' },
      { text: 'Disconnect', style: 'destructive', onPress: () => void signOut() },
    ]);

  const card = [styles.card, { backgroundColor: theme.backgroundElement }];
  return (
    <ThemedView style={styles.container}>
      <ScrollView contentContainerStyle={styles.content}>
        <View style={card}>
          <ThemedText type="smallBold" themeColor="textSecondary">
            Server
          </ThemedText>
          <ThemedText selectable>{config.baseUrl}</ThemedText>
          <ThemedText type="small" themeColor="textSecondary">
            client {config.clientName} · token stored in Keychain
          </ThemedText>
        </View>
        <View style={card}>
          <ThemedText type="smallBold" themeColor="textSecondary">
            Push notifications
          </ThemedText>
          <ThemedText type="small" selectable>
            {describePush(push)}
          </ThemedText>
          <View style={styles.rowButtons}>
            <Pressable onPress={() => void refreshPush()} style={styles.secondary}>
              <ThemedText type="smallBold">Re-register</ThemedText>
            </Pressable>
            <Pressable onPress={test} disabled={testing} style={[styles.secondary, testing && styles.disabled]}>
              <ThemedText type="smallBold">Send test push</ThemedText>
            </Pressable>
          </View>
          {testResult ? (
            <ThemedText type="small" themeColor="textSecondary">
              {testResult}
            </ThemedText>
          ) : null}
        </View>
        <View style={card}>
          <ThemedText type="smallBold" themeColor="textSecondary">
            App
          </ThemedText>
          <ThemedText type="small">
            PDW {Constants.expoConfig?.version ?? ''} · EAS project {(Constants.expoConfig?.extra?.eas?.projectId as string | undefined) ?? 'none'}
          </ThemedText>
          <ThemedText type="small" themeColor="textSecondary" selectable>
            {describeInstalledUpdate()}
          </ThemedText>
          <View style={styles.rowButtons}>
            <Pressable onPress={() => void applyUpdateNow(setUpdate)} disabled={update?.state === 'checking' || update?.state === 'downloading'} style={styles.secondary}>
              <ThemedText type="smallBold">Check for update</ThemedText>
            </Pressable>
          </View>
          {update ? (
            <ThemedText type="small" themeColor="textSecondary">
              {update.state === 'current' ? 'Up to date.' : update.state === 'error' ? `Update error: ${update.message}` : update.state === 'disabled' ? 'Updates are disabled in this build.' : `${update.state}…`}
            </ThemedText>
          ) : null}
        </View>
        <Pressable onPress={confirmSignOut} style={styles.danger}>
          <ThemedText style={styles.dangerText}>Disconnect</ThemedText>
        </Pressable>
      </ScrollView>
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  content: { padding: Spacing.three, gap: Spacing.three },
  card: { borderRadius: 12, padding: Spacing.three, gap: Spacing.two },
  rowButtons: { flexDirection: 'row', gap: Spacing.two, marginTop: Spacing.one },
  secondary: { borderRadius: 10, paddingHorizontal: 12, paddingVertical: 8, borderWidth: 1, borderColor: '#208AEF' },
  disabled: { opacity: 0.6 },
  danger: { borderRadius: 12, paddingVertical: 14, alignItems: 'center', borderWidth: 1, borderColor: '#DC2626' },
  dangerText: { color: '#DC2626', fontWeight: '600' },
});
