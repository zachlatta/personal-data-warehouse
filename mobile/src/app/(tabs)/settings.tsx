import Constants from 'expo-constants';
import { useState, type ReactNode } from 'react';
import { useRouter } from 'expo-router';
import { Alert, Platform, Pressable, ScrollView, StyleSheet, View } from 'react-native';

import { Icon } from '@/components/icon';
import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Palette, Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { sendTestPush } from '@/lib/api';
import { notificationSettingsName, secureStoreName } from '@/lib/platform-copy';
import { useConfig, useSession } from '@/lib/session';
import { applyUpdateNow, describeInstalledUpdate, describeUpdateAge, type UpdateState } from '@/lib/updates';
import { useTabBarSpace } from '@/hooks/use-tab-bar-space';

function describePush(push: ReturnType<typeof useSession>['push']): string {
  if (!push) return 'Registering…';
  switch (push.state) {
    case 'registered':
      return 'On for this phone';
    case 'denied':
      return `Permission denied — enable notifications for PDW in ${notificationSettingsName(Platform.OS)}.`;
    case 'unsupported':
      return push.reason;
    case 'error':
      return `Error: ${push.message}`;
  }
}

export default function SettingsScreen() {
  const tabSpace = useTabBarSpace();
  const config = useConfig();
  const router = useRouter();
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

  const pushToken = push?.state === 'registered' ? push.token : null;
  const updateBusy = update?.state === 'checking' || update?.state === 'downloading';
  return (
    <ThemedView style={styles.container}>
      <ScrollView contentContainerStyle={[styles.content, { paddingBottom: tabSpace }]}>
        <Section title="Notifications">
          <Row title={pushToken ? 'On for this phone' : 'Push'} detail={pushToken ? undefined : describePush(push)}>
            {pushToken ? (
              <ThemedText type="small" themeColor="textSecondary" numberOfLines={1} ellipsizeMode="middle" selectable>
                {pushToken}
              </ThemedText>
            ) : null}
            <View style={styles.links}>
              <Link label="Send test push" onPress={test} disabled={testing} />
              <Link label="Re-register" onPress={() => void refreshPush()} />
            </View>
            {testResult ? (
              <ThemedText type="small" themeColor="textSecondary">
                {testResult}
              </ThemedText>
            ) : null}
          </Row>
          <Row title="Timeline alerts" detail="The on/off switch and everything sent" onPress={() => router.navigate('/notifications')} />
        </Section>
        <Section title="Data">
          <Row title="MCP connections" detail="Add, sign in to, share and turn off servers" onPress={() => router.push('/connections')} />
          <Row title="Server" detail={config.baseUrl}>
            <ThemedText type="small" themeColor="textSecondary">
              client {config.clientName} · token stored in {secureStoreName(Platform.OS)}
            </ThemedText>
          </Row>
        </Section>
        <Section title="App">
          <Row title={describeUpdateAge()} detail={`PDW ${Constants.expoConfig?.version ?? ''}`}>
            <ThemedText type="small" themeColor="textSecondary" selectable>
              {describeInstalledUpdate()}
            </ThemedText>
            <View style={styles.links}>
              <Link label={updateBusy ? `${update?.state === 'checking' ? 'Checking' : 'Downloading'}…` : 'Check for update'} onPress={() => void applyUpdateNow(setUpdate)} disabled={updateBusy} />
            </View>
            {update && !updateBusy ? (
              <ThemedText type="small" themeColor="textSecondary">
                {update.state === 'current' ? 'Up to date.' : update.state === 'error' ? `Update error: ${update.message}` : 'Updates are off in this build.'}
              </ThemedText>
            ) : null}
          </Row>
        </Section>
        <Pressable onPress={confirmSignOut} hitSlop={8} style={styles.danger}>
          <ThemedText style={styles.dangerText}>Disconnect</ThemedText>
        </Pressable>
      </ScrollView>
    </ThemedView>
  );
}

// Superhuman's settings are a plain grouped list: a small heading, then rows
// split by hairlines, no boxes.
function Section({ title, children }: { title: string; children: ReactNode }) {
  const theme = useTheme();
  return (
    <View>
      <ThemedText type="smallBold" themeColor="textSecondary" style={styles.sectionTitle}>
        {title}
      </ThemedText>
      <View style={[styles.section, { borderColor: theme.separator }]}>{children}</View>
    </View>
  );
}

function Row({ title, detail, onPress, children }: { title: string; detail?: string; onPress?: () => void; children?: ReactNode }) {
  const theme = useTheme();
  const body = (
    <>
      <View style={styles.rowText}>
        <ThemedText>{title}</ThemedText>
        {detail ? (
          <ThemedText type="small" themeColor="textSecondary" selectable={!onPress}>
            {detail}
          </ThemedText>
        ) : null}
        {children}
      </View>
      {onPress ? <Icon name="chevron.right" tintColor={theme.textSecondary} size={18} /> : null}
    </>
  );
  const style = [styles.row, { borderBottomColor: theme.separator }];
  if (!onPress) return <View style={style}>{body}</View>;
  return (
    <Pressable accessibilityRole="button" onPress={onPress} android_ripple={{ color: theme.separator }} style={({ pressed }) => [...style, pressed && { backgroundColor: theme.backgroundElement }]}>
      {body}
    </Pressable>
  );
}

function Link({ label, onPress, disabled }: { label: string; onPress: () => void; disabled?: boolean }) {
  return (
    <Pressable accessibilityRole="button" onPress={onPress} disabled={disabled} hitSlop={8} style={disabled && styles.disabled}>
      <ThemedText type="smallBold" themeColor="accent">
        {label}
      </ThemedText>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  content: { paddingTop: Spacing.two, gap: Spacing.four },
  sectionTitle: { paddingHorizontal: Spacing.three, paddingBottom: Spacing.one },
  section: { borderTopWidth: StyleSheet.hairlineWidth },
  row: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, paddingHorizontal: Spacing.three, paddingVertical: 12, borderBottomWidth: StyleSheet.hairlineWidth },
  rowText: { flex: 1, gap: 2 },
  links: { flexDirection: 'row', gap: Spacing.four, marginTop: Spacing.two },
  disabled: { opacity: 0.5 },
  danger: { alignSelf: 'center', paddingVertical: Spacing.three },
  dangerText: { color: Palette.destructive, fontWeight: '600' },
});
