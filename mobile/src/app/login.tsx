import { useState } from 'react';
import { ActivityIndicator, KeyboardAvoidingView, Platform, Pressable, ScrollView, StyleSheet, TextInput } from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { probe } from '@/lib/api';
import { DEFAULT_BASE_URL, normalizeBaseUrl, normalizeClientName } from '@/lib/config';
import { useSession } from '@/lib/session';

export default function LoginScreen() {
  const theme = useTheme();
  const { signIn } = useSession();
  const [baseUrl, setBaseUrl] = useState(DEFAULT_BASE_URL);
  const [clientName, setClientName] = useState('iphone');
  const [token, setToken] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async () => {
    setError(null);
    const config = { baseUrl: normalizeBaseUrl(baseUrl), clientName: normalizeClientName(clientName), token: token.trim() };
    if (!config.baseUrl || !config.token) {
      setError('Server URL and token are both required.');
      return;
    }
    setBusy(true);
    try {
      await probe(config);
      await signIn(config);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      // A rejected token is nearly always a mangled paste; the length says so
      // without ever showing the value.
      setError(message.includes('token') ? `${message} (token length ${config.token.length})` : message);
    } finally {
      setBusy(false);
    }
  };

  const inputStyle = [styles.input, { backgroundColor: theme.backgroundElement, color: theme.text }];
  return (
    <ThemedView style={styles.container}>
      <SafeAreaView style={styles.safe}>
        <KeyboardAvoidingView behavior={Platform.OS === 'ios' ? 'padding' : undefined} style={styles.flex}>
          <ScrollView contentContainerStyle={styles.content} keyboardShouldPersistTaps="handled">
            <ThemedText type="title">PDW</ThemedText>
            <ThemedText themeColor="textSecondary">
              Same credentials as <ThemedText type="code">pdw login</ThemedText>: the app URL, a client name, and PDW_SECRET_TOKEN. The
              token is kept in the iOS Keychain.
            </ThemedText>

            <ThemedText type="smallBold">Server URL</ThemedText>
            <TextInput style={inputStyle} value={baseUrl} onChangeText={setBaseUrl} autoCapitalize="none" autoCorrect={false} keyboardType="url" />

            <ThemedText type="smallBold">Client name</ThemedText>
            <TextInput style={inputStyle} value={clientName} onChangeText={setClientName} autoCapitalize="none" autoCorrect={false} />

            <ThemedText type="smallBold">Secret token</ThemedText>
            <TextInput
              style={inputStyle}
              value={token}
              onChangeText={setToken}
              autoCapitalize="none"
              autoCorrect={false}
              secureTextEntry
              textContentType="password"
              onSubmitEditing={submit}
            />

            {error ? <ThemedText style={styles.error}>{error}</ThemedText> : null}

            <Pressable onPress={submit} disabled={busy} style={[styles.button, busy && styles.buttonDisabled]}>
              {busy ? <ActivityIndicator color="#fff" /> : <ThemedText style={styles.buttonText}>Connect</ThemedText>}
            </Pressable>
          </ScrollView>
        </KeyboardAvoidingView>
      </SafeAreaView>
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  safe: { flex: 1 },
  flex: { flex: 1 },
  content: { padding: Spacing.four, gap: Spacing.three },
  input: { borderRadius: 10, paddingHorizontal: Spacing.three, paddingVertical: 12, fontSize: 16 },
  button: { backgroundColor: '#208AEF', borderRadius: 12, paddingVertical: 14, alignItems: 'center', marginTop: Spacing.two },
  buttonDisabled: { opacity: 0.6 },
  buttonText: { color: '#fff', fontWeight: '600', fontSize: 16 },
  error: { color: '#D0342C' },
});
