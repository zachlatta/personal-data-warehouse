import { Stack, useLocalSearchParams } from 'expo-router';
import { useCallback, useEffect, useState } from 'react';
import { ActivityIndicator, Alert, Pressable, ScrollView, StyleSheet, TextInput, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { StatusPill } from '@/app/(tabs)/mutations';
import { approveMutationRequest, getMutationRequest, rejectMutationRequest, removeMutation, type Mutation, type MutationRequest } from '@/lib/api';
import { formatWhen, pretty } from '@/lib/format';
import { useConfig } from '@/lib/session';

// The fields that make a mutation reviewable at a glance, per operation. Any
// other payload key still renders below, so nothing is hidden — only ordered.
// Nested payload objects (a calendar event, a patch, an email message) read
// better as fields than as a JSON blob: lift their entries one level.
const NESTED_KEYS = ['message', 'event', 'patch'];

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function flattenPayload(payload: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(payload)) {
    if (NESTED_KEYS.includes(key) && isPlainObject(value)) {
      for (const [sub, subValue] of Object.entries(value)) {
        // Google's {dateTime, timeZone} start/end objects collapse to the instant.
        out[sub] = isPlainObject(subValue) && typeof subValue.dateTime === 'string' ? subValue.dateTime : subValue;
      }
    } else {
      out[key] = value;
    }
  }
  return out;
}

const HEADLINE_KEYS = ['to', 'cc', 'bcc', 'subject', 'body_text', 'thread_ids', 'summary', 'start', 'end', 'location', 'description', 'attendees', 'name', 'body', 'append_body', 'folder', 'note_id'];

function MutationCard({ mutation, pending, onRemove }: { mutation: Mutation; pending: boolean; onRemove: () => void }) {
  const theme = useTheme();
  const merged = flattenPayload(mutation.payload ?? {});
  const headline = HEADLINE_KEYS.filter((key) => merged[key] !== undefined && merged[key] !== '' && merged[key] !== null);
  const rest = Object.keys(merged).filter((key) => !HEADLINE_KEYS.includes(key) && merged[key] !== undefined && merged[key] !== '' && merged[key] !== null);
  const removed = mutation.status === 'removed' || mutation.status === 'skipped';
  return (
    <View style={[styles.card, { backgroundColor: theme.backgroundElement }, removed && styles.cardRemoved]}>
      <View style={styles.cardHeader}>
        <ThemedText type="smallBold">{mutation.operation}</ThemedText>
        <StatusPill status={mutation.status} />
      </View>
      <ThemedText type="small" themeColor="textSecondary">
        {mutation.account}
      </ThemedText>
      {mutation.title ? <ThemedText>{mutation.title}</ThemedText> : null}
      {headline.map((key) => (
        <View key={key} style={styles.field}>
          <ThemedText type="small" themeColor="textSecondary">
            {key}
          </ThemedText>
          <ThemedText selectable>{pretty(merged[key])}</ThemedText>
        </View>
      ))}
      {rest.map((key) => (
        <View key={key} style={styles.field}>
          <ThemedText type="small" themeColor="textSecondary">
            {key}
          </ThemedText>
          <ThemedText type="small" selectable>
            {pretty(merged[key])}
          </ThemedText>
        </View>
      ))}
      {mutation.error ? <ThemedText style={styles.error}>{mutation.error}</ThemedText> : null}
      {pending && !removed && mutation.operation === 'gmail.send_email' ? (
        <Pressable onPress={onRemove} style={styles.linkButton}>
          <ThemedText style={styles.linkDanger}>Don’t send this one</ThemedText>
        </Pressable>
      ) : null}
    </View>
  );
}

export default function MutationRequestScreen() {
  const { id } = useLocalSearchParams<{ id: string }>();
  const config = useConfig();
  const theme = useTheme();
  const [request, setRequest] = useState<MutationRequest | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [reason, setReason] = useState('');

  const load = useCallback(async () => {
    if (!id) return;
    try {
      setRequest(await getMutationRequest(config, id));
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [config, id]);

  useEffect(() => {
    if (!id) return;
    let cancelled = false;
    getMutationRequest(config, id)
      .then((loaded) => {
        if (!cancelled) setRequest(loaded);
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [config, id]);

  const act = async (fn: () => Promise<MutationRequest>) => {
    setBusy(true);
    try {
      setRequest(await fn());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const approve = () => {
    if (!request) return;
    Alert.alert('Approve request?', `${request.mutation_count} mutation${request.mutation_count === 1 ? '' : 's'} will run upstream.`, [
      { text: 'Cancel', style: 'cancel' },
      { text: 'Approve', style: 'default', onPress: () => act(() => approveMutationRequest(config, request.id)) },
    ]);
  };
  const deny = () => {
    if (!request) return;
    act(() => rejectMutationRequest(config, request.id, reason));
  };
  const remove = (mutation: Mutation) => {
    if (!request) return;
    Alert.alert('Skip this email?', 'It will not be sent when the request is approved.', [
      { text: 'Cancel', style: 'cancel' },
      {
        text: 'Skip',
        style: 'destructive',
        onPress: async () => {
          setBusy(true);
          try {
            await removeMutation(config, request.id, mutation.id);
            await load();
          } catch (e) {
            setError(e instanceof Error ? e.message : String(e));
          } finally {
            setBusy(false);
          }
        },
      },
    ]);
  };

  if (!request) {
    return (
      <ThemedView style={styles.center}>{error ? <ThemedText style={styles.error}>{error}</ThemedText> : <ActivityIndicator />}</ThemedView>
    );
  }
  const pending = request.status === 'pending_review';
  return (
    <ThemedView style={styles.container}>
      <Stack.Screen options={{ title: pending ? 'Review' : request.status.replace(/_/g, ' ') }} />
      <ScrollView contentContainerStyle={styles.content} keyboardShouldPersistTaps="handled">
        <View style={styles.headerRow}>
          <StatusPill status={request.status} />
          <ThemedText type="small" themeColor="textSecondary">
            {formatWhen(request.created_at)} · by {request.requested_by || 'unknown'}
          </ThemedText>
        </View>
        <ThemedText type="subtitle">{request.title}</ThemedText>
        <ThemedText themeColor="textSecondary">{request.reason}</ThemedText>
        {request.approved_by ? (
          <ThemedText type="small" themeColor="textSecondary">
            {request.status === 'rejected' ? 'Denied' : 'Approved'} by {request.approved_by}
            {request.approved_at ? ` · ${formatWhen(request.approved_at)}` : ''}
          </ThemedText>
        ) : null}
        {request.error ? <ThemedText style={styles.error}>{request.error}</ThemedText> : null}
        {error ? <ThemedText style={styles.error}>{error}</ThemedText> : null}

        {(request.mutations ?? []).map((mutation) => (
          <MutationCard key={mutation.id} mutation={mutation} pending={pending} onRemove={() => remove(mutation)} />
        ))}

        {Object.keys(request.context ?? {}).length > 0 ? (
          <View style={[styles.card, { backgroundColor: theme.backgroundElement }]}>
            <ThemedText type="smallBold" themeColor="textSecondary">
              context
            </ThemedText>
            <ThemedText type="small" selectable>
              {pretty(request.context)}
            </ThemedText>
          </View>
        ) : null}

        {pending ? (
          <View style={styles.actions}>
            <Pressable onPress={approve} disabled={busy} style={[styles.button, styles.approve, busy && styles.disabled]}>
              <ThemedText style={styles.buttonText}>Approve</ThemedText>
            </Pressable>
            <TextInput
              placeholder="Reason for denying (optional)"
              placeholderTextColor={theme.textSecondary}
              value={reason}
              onChangeText={setReason}
              style={[styles.input, { backgroundColor: theme.backgroundElement, color: theme.text }]}
            />
            <Pressable onPress={deny} disabled={busy} style={[styles.button, styles.deny, busy && styles.disabled]}>
              <ThemedText style={styles.buttonText}>Deny</ThemedText>
            </Pressable>
          </View>
        ) : null}
      </ScrollView>
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center', padding: Spacing.four },
  content: { padding: Spacing.three, gap: Spacing.three, paddingBottom: Spacing.five * 2 },
  headerRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two },
  card: { borderRadius: 12, padding: Spacing.three, gap: Spacing.two },
  cardRemoved: { opacity: 0.5 },
  cardHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', gap: Spacing.two },
  field: { gap: 2 },
  actions: { gap: Spacing.two, marginTop: Spacing.two },
  button: { borderRadius: 12, paddingVertical: 14, alignItems: 'center' },
  approve: { backgroundColor: '#16A34A' },
  deny: { backgroundColor: '#DC2626' },
  disabled: { opacity: 0.6 },
  buttonText: { color: '#fff', fontWeight: '600', fontSize: 16 },
  input: { borderRadius: 10, paddingHorizontal: Spacing.three, paddingVertical: 12, fontSize: 16 },
  linkButton: { paddingTop: Spacing.one },
  linkDanger: { color: '#DC2626', fontWeight: '600' },
  error: { color: '#D0342C' },
});
