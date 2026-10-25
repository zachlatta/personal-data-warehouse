import { useFocusEffect, useLocalSearchParams, useRouter } from 'expo-router';
import * as WebBrowser from 'expo-web-browser';
import { useCallback, useState } from 'react';
import { Alert, KeyboardAvoidingView, Pressable, RefreshControl, ScrollView, StyleSheet, Switch, TextInput, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Palette, Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import {
  beginConnectionAuth,
  changeConnection,
  createConnection,
  fetchConnections,
  refreshConnection,
  type McpConnection,
} from '@/lib/api';
import {
  accessLabel,
  authMessage,
  authResultFromUrl,
  clientNames,
  connectionState,
  scopeList,
  validConnectionName,
  type ConnectionTone,
} from '@/lib/connections';
import { useConfig } from '@/lib/session';

const TONE_COLORS: Record<ConnectionTone, string> = {
  good: Palette.success,
  neutral: Palette.link,
  muted: Palette.neutral,
  bad: Palette.danger,
};

const SHARING_WARNING =
  'Sharing includes write tools. Calls run with your upstream account and its permissions, without PDW mutation review.';

function errorText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function ConnectionCard({ connection, onChanged, onMessage }: { connection: McpConnection; onChanged: () => Promise<void>; onMessage: (text: string) => void }) {
  const config = useConfig();
  const theme = useTheme();
  const [busy, setBusy] = useState(false);
  const [editingAccess, setEditingAccess] = useState(false);
  const [names, setNames] = useState((connection.clients ?? []).join(', '));
  const [everyone, setEveryone] = useState(connection.all_clients);
  const state = connectionState(connection);
  const color = TONE_COLORS[state.tone];

  const run = async (fn: () => Promise<void>) => {
    setBusy(true);
    try {
      await fn();
    } catch (e) {
      onMessage(errorText(e));
    } finally {
      setBusy(false);
    }
  };

  // The provider signs in through the system browser (Google and others refuse
  // embedded web views); the callback returns to pdw://connections.
  const connect = () =>
    run(async () => {
      const startUrl = await beginConnectionAuth(config, connection.name);
      const result = await WebBrowser.openAuthSessionAsync(startUrl, 'pdw://connections');
      const auth = result.type === 'success' ? authResultFromUrl(result.url) : null;
      if (auth) onMessage(authMessage(auth));
      await onChanged();
    });

  const refresh = () =>
    run(async () => {
      await refreshConnection(config, connection.name);
      onMessage(`${connection.name}: tools refreshed.`);
      await onChanged();
    });

  const toggle = (enabled: boolean) =>
    run(async () => {
      await changeConnection(config, connection, enabled ? 'enable' : 'disable');
      await onChanged();
    });

  const remove = () =>
    Alert.alert(`Remove ${connection.name}?`, 'Its stored credentials are deleted. This does not revoke the authorization at the upstream provider.', [
      { text: 'Cancel', style: 'cancel' },
      {
        text: 'Remove',
        style: 'destructive',
        onPress: () =>
          void run(async () => {
            await changeConnection(config, connection, 'remove');
            await onChanged();
          }),
      },
    ]);

  const saveAccess = () =>
    run(async () => {
      await changeConnection(config, connection, 'access', { all_clients: everyone, clients: clientNames(names) });
      setEditingAccess(false);
      await onChanged();
    });

  const button = [styles.secondary, { backgroundColor: theme.backgroundSelected }, busy && styles.disabled];
  return (
    <View style={[styles.card, { backgroundColor: theme.backgroundElement }]}>
      <View style={styles.titleRow}>
        <View style={styles.titleText}>
          <ThemedText type="subtitle">{connection.name}</ThemedText>
          <ThemedText type="small" themeColor="textSecondary" selectable numberOfLines={1}>
            {connection.url}
          </ThemedText>
        </View>
        <Switch value={connection.enabled} disabled={busy} onValueChange={toggle} />
      </View>
      <View style={styles.metaRow}>
        <View style={[styles.pill, { borderColor: color, backgroundColor: `${color}22` }]}>
          <ThemedText style={[styles.pillText, { color }]}>{state.label}</ThemedText>
        </View>
        <ThemedText type="small" themeColor="textSecondary">
          {connection.tool_count} tool{connection.tool_count === 1 ? '' : 's'} · {connection.name}__
        </ThemedText>
      </View>
      <ThemedText type="small" themeColor="textSecondary">
        Access: {accessLabel(connection)}
      </ThemedText>
      <View style={styles.rowButtons}>
        <Pressable onPress={connect} disabled={busy} style={[styles.primary, { backgroundColor: theme.text }, busy && styles.disabled]}>
          <ThemedText type="smallBold" style={{ color: theme.background }}>
            {connection.authenticated ? 'Reconnect' : 'Connect'}
          </ThemedText>
        </Pressable>
        <Pressable onPress={refresh} disabled={busy} style={button}>
          <ThemedText type="smallBold">Refresh tools</ThemedText>
        </Pressable>
        <Pressable onPress={() => setEditingAccess((v) => !v)} disabled={busy} style={button}>
          <ThemedText type="smallBold">Access</ThemedText>
        </Pressable>
      </View>
      {editingAccess ? (
        <View style={styles.access}>
          <ThemedText type="smallBold">Allowed client names</ThemedText>
          <TextInput
            style={[styles.input, { backgroundColor: theme.background, color: theme.text }]}
            value={names}
            onChangeText={setNames}
            placeholder="codex, claude, web"
            placeholderTextColor={theme.textSecondary}
            autoCapitalize="none"
            autoCorrect={false}
          />
          <View style={styles.switchRow}>
            <ThemedText type="small" style={styles.switchLabel}>
              Share with every authenticated PDW client
            </ThemedText>
            <Switch value={everyone} onValueChange={setEveryone} />
          </View>
          <ThemedText type="small" themeColor="textSecondary">
            {SHARING_WARNING}
          </ThemedText>
          <View style={styles.rowButtons}>
            <Pressable onPress={saveAccess} disabled={busy} style={button}>
              <ThemedText type="smallBold">Save access</ThemedText>
            </Pressable>
            <Pressable onPress={remove} disabled={busy} style={button}>
              <ThemedText type="smallBold" style={{ color: Palette.danger }}>
                Remove
              </ThemedText>
            </Pressable>
          </View>
        </View>
      ) : null}
    </View>
  );
}

function AddConnection({ onAdded, onMessage }: { onAdded: () => Promise<void>; onMessage: (text: string) => void }) {
  const config = useConfig();
  const theme = useTheme();
  const [name, setName] = useState('');
  const [url, setUrl] = useState('');
  const [advanced, setAdvanced] = useState(false);
  const [token, setToken] = useState('');
  const [clientId, setClientId] = useState('');
  const [clientSecret, setClientSecret] = useState('');
  const [scopes, setScopes] = useState('');
  const [clients, setClients] = useState('');
  const [everyone, setEveryone] = useState(false);
  const [busy, setBusy] = useState(false);

  const input = [styles.input, { backgroundColor: theme.background, color: theme.text }];
  const field = (label: string, value: string, set: (v: string) => void, extra: Partial<React.ComponentProps<typeof TextInput>> = {}) => (
    <>
      <ThemedText type="smallBold">{label}</ThemedText>
      <TextInput style={input} value={value} onChangeText={set} autoCapitalize="none" autoCorrect={false} placeholderTextColor={theme.textSecondary} {...extra} />
    </>
  );

  const submit = async () => {
    const trimmed = name.trim();
    if (!validConnectionName(trimmed)) {
      onMessage('Name: lowercase letters, digits or single underscores, starting with a letter, at most 24 characters.');
      return;
    }
    setBusy(true);
    try {
      await createConnection(config, {
        name: trimmed,
        url: url.trim(),
        token,
        client_id: clientId.trim(),
        client_secret: clientSecret,
        scopes: scopeList(scopes),
        all_clients: everyone,
        clients: clientNames(clients),
      });
      setName('');
      setUrl('');
      setToken('');
      setClientId('');
      setClientSecret('');
      setScopes('');
      setClients('');
      setEveryone(false);
      setAdvanced(false);
      onMessage('Connection added. Tap Connect for OAuth, or Refresh tools for a bearer-token or unauthenticated server.');
      await onAdded();
    } catch (e) {
      onMessage(errorText(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <View style={[styles.card, { backgroundColor: theme.backgroundElement }]}>
      <ThemedText type="subtitle">Add an MCP server</ThemedText>
      {field('Name / tool prefix', name, setName, { placeholder: 'skills, tasks, or hc', maxLength: 24 })}
      {field('MCP endpoint', url, setUrl, { placeholder: 'https://your-server.example/mcp', keyboardType: 'url' })}
      <ThemedText type="small" themeColor="textSecondary">
        Remote HTTPS endpoints only, reachable from PDW.
      </ThemedText>
      <Pressable onPress={() => setAdvanced((v) => !v)}>
        <ThemedText type="small" style={{ color: Palette.link }}>
          {advanced ? 'Hide' : 'Bearer token or pre-registered OAuth client (optional)'}
        </ThemedText>
      </Pressable>
      {advanced ? (
        <>
          {field('Upstream bearer token', token, setToken, { secureTextEntry: true })}
          {field('OAuth client ID', clientId, setClientId)}
          {field('OAuth client secret', clientSecret, setClientSecret, { secureTextEntry: true })}
          {field('OAuth scopes (space-separated)', scopes, setScopes)}
        </>
      ) : null}
      {field('Allowed PDW client names (comma-separated)', clients, setClients, { placeholder: 'codex, claude, web' })}
      <View style={styles.switchRow}>
        <ThemedText type="small" style={styles.switchLabel}>
          Share with every authenticated PDW client, including future ones
        </ThemedText>
        <Switch value={everyone} onValueChange={setEveryone} />
      </View>
      <ThemedText type="small" themeColor="textSecondary">
        {SHARING_WARNING} Leave access empty to keep tools private until you choose clients.
      </ThemedText>
      <Pressable onPress={submit} disabled={busy || !name.trim() || !url.trim()} style={[styles.primary, styles.wide, { backgroundColor: theme.text }, (busy || !name.trim() || !url.trim()) && styles.disabled]}>
        <ThemedText type="smallBold" style={{ color: theme.background }}>
          Add connection
        </ThemedText>
      </Pressable>
    </View>
  );
}

export default function ConnectionsScreen() {
  const config = useConfig();
  const params = useLocalSearchParams<{ auth?: string }>();
  const router = useRouter();
  const [connections, setConnections] = useState<McpConnection[] | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  const load = useCallback(async () => {
    setRefreshing(true);
    try {
      setConnections(await fetchConnections(config));
    } catch (e) {
      setMessage(errorText(e));
    } finally {
      setRefreshing(false);
    }
  }, [config]);

  useFocusEffect(
    useCallback(() => {
      void load();
    }, [load]),
  );

  // Android also delivers the OAuth return link to the app as a deep link;
  // Connect reloads the list itself once the browser closes.
  const shown = message ?? (params.auth ? authMessage(params.auth) : null);
  const dismiss = () => {
    setMessage(null);
    router.setParams({ auth: undefined });
  };

  return (
    <ThemedView style={styles.container}>
      <KeyboardAvoidingView behavior="padding" style={styles.container}>
        <ScrollView contentContainerStyle={styles.content} keyboardShouldPersistTaps="handled" refreshControl={<RefreshControl refreshing={refreshing} onRefresh={load} />}>
          <ThemedText type="small" themeColor="textSecondary">
            Remote MCP servers PDW connects to. Sign in here; their tools work through PDW everywhere, prefixed with the connection name.
          </ThemedText>
          {shown ? (
            <Pressable onPress={dismiss}>
              <ThemedText type="small" selectable>
                {shown}
              </ThemedText>
            </Pressable>
          ) : null}
          {connections && connections.length === 0 ? (
            <ThemedText type="small" themeColor="textSecondary">
              No connections yet. Add a server below, then connect.
            </ThemedText>
          ) : null}
          {(connections ?? []).map((c) => (
            // Keyed by version so a saved change resets the card's edit fields.
            <ConnectionCard key={`${c.name}:${c.version}`} connection={c} onChanged={load} onMessage={setMessage} />
          ))}
          <AddConnection onAdded={load} onMessage={setMessage} />
        </ScrollView>
      </KeyboardAvoidingView>
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  content: { padding: Spacing.three, gap: Spacing.three, paddingBottom: Spacing.six },
  card: { borderRadius: 16, padding: Spacing.three, gap: Spacing.two },
  titleRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two },
  titleText: { flex: 1, gap: 2 },
  metaRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, flexWrap: 'wrap' },
  pill: { borderWidth: 1, borderRadius: 999, paddingHorizontal: 8, paddingVertical: 2 },
  pillText: { fontSize: 12, fontWeight: '600' },
  rowButtons: { flexDirection: 'row', flexWrap: 'wrap', gap: Spacing.two, marginTop: Spacing.one },
  primary: { borderRadius: 999, paddingHorizontal: 16, paddingVertical: 8, alignItems: 'center' },
  wide: { paddingVertical: 12, marginTop: Spacing.one },
  secondary: { borderRadius: 999, paddingHorizontal: 16, paddingVertical: 8 },
  disabled: { opacity: 0.5 },
  access: { gap: Spacing.two, marginTop: Spacing.one },
  input: { borderRadius: 10, paddingHorizontal: Spacing.three, paddingVertical: 10, fontSize: 16 },
  switchRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two },
  switchLabel: { flex: 1 },
});
