import { Stack, useLocalSearchParams } from 'expo-router';
import { useCallback, useEffect, useState } from 'react';
import { ActivityIndicator, Alert, KeyboardAvoidingView, Platform, Pressable, ScrollView, StyleSheet, TextInput, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { StatusPill } from '@/app/(tabs)/mutations';
import { approveMutationRequest, getMutationRequest, rejectMutationRequest, removeMutation, type Mutation, type MutationRequest } from '@/lib/api';
import { formatWhen, pretty } from '@/lib/format';
import { isSlackMarkReadMutation, slackMarkReadReview } from '@/lib/mutation-review';
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

function SlackMarkReadCard({ mutation }: { mutation: Mutation }) {
  const theme = useTheme();
  const review = slackMarkReadReview(mutation);
  return (
    <View style={[styles.card, styles.slackCard, { backgroundColor: theme.backgroundElement }]}>
      <View style={styles.cardHeader}>
        <View style={styles.cardHeaderCopy}>
          <ThemedText type="small" themeColor="textSecondary">SLACK · REVIEWED ACTION</ThemedText>
          <ThemedText type="smallBold">Mark {review.conversationLabel || 'conversation'} read</ThemedText>
        </View>
        <StatusPill status={mutation.status} />
      </View>
      <ThemedText type="small" themeColor="textSecondary">{mutation.operation} · {review.account}</ThemedText>

      <View style={[styles.slackAction, { backgroundColor: theme.backgroundSelected }]}>
        <ThemedText type="smallBold" style={styles.slackAccent}>WHAT WILL HAPPEN</ThemedText>
        <ThemedText>{review.effect}</ThemedText>
        <ThemedText type="small" themeColor="textSecondary">{review.boundaryNote}</ThemedText>
        <View style={styles.slackFacts}>
          <View style={styles.slackFact}>
            <ThemedText type="small" themeColor="textSecondary">Conversation</ThemedText>
            <ThemedText type="smallBold">{review.conversationLabel || 'Unknown'}</ThemedText>
          </View>
          <View style={styles.slackFact}>
            <ThemedText type="small" themeColor="textSecondary">Unread now</ThemedText>
            <ThemedText type="smallBold">{review.currentUnreadCount || '—'}</ThemedText>
          </View>
        </View>
      </View>

      <View style={styles.slackContextHeader}>
        <ThemedText type="smallBold" themeColor="textSecondary">{review.contextLabel.toUpperCase()}</ThemedText>
        <ThemedText type="small" themeColor="textSecondary">
          {review.messages.length} message{review.messages.length === 1 ? '' : 's'}
        </ThemedText>
      </View>
      {review.messages.length === 0 ? (
        <View style={styles.slackMissing}>
          <ThemedText type="small" style={styles.error}>Conversation context was unavailable when this proposal was created. Verify the exact target below before approving.</ThemedText>
        </View>
      ) : (
        <View style={styles.slackTranscript}>
          {review.messages.map((message, index) => (
            <View
              key={`${message.messageTs}-${index}`}
              style={[
                styles.slackMessage,
                { backgroundColor: theme.background },
                message.isTarget && styles.slackMessageTarget,
                message.isAfterBoundary && styles.slackMessageAfter,
              ]}>
              <View style={[styles.slackAvatar, message.isTarget && styles.slackAvatarTarget]}>
                <ThemedText type="smallBold" style={message.isTarget && styles.slackAvatarTextTarget}>
                  {(message.actorName.trim()[0] || '?').toUpperCase()}
                </ThemedText>
              </View>
              <View style={styles.slackMessageCopy}>
                <View style={styles.slackMessageHeader}>
                  <ThemedText type="smallBold">{message.actorName}</ThemedText>
                  <ThemedText type="small" themeColor="textSecondary">{formatWhen(message.sentAt)}</ThemedText>
                </View>
                <ThemedText style={message.isAfterBoundary && styles.slackMessageTextAfter}>{message.text}</ThemedText>
                {message.isTarget ? <ThemedText type="smallBold" style={styles.slackBoundaryTag}>READ THROUGH HERE</ThemedText> : null}
                {message.isAfterBoundary ? <ThemedText type="smallBold" themeColor="textSecondary" style={styles.slackAfterTag}>STAYS UNREAD</ThemedText> : null}
              </View>
            </View>
          ))}
        </View>
      )}

      <View style={styles.slackTarget}>
        <ThemedText type="smallBold" themeColor="textSecondary">EXACT SLACK TARGET</ThemedText>
        <ThemedText type="code" selectable>Conversation {review.conversationId}</ThemedText>
        <ThemedText type="code" selectable>Message {review.messageTs}</ThemedText>
        {review.threadTs ? <ThemedText type="code" selectable>Thread {review.threadTs}</ThemedText> : null}
        {review.currentLastRead ? <ThemedText type="code" selectable>Current cursor {review.currentLastRead}</ThemedText> : null}
      </View>
      {mutation.reason ? <ThemedText type="small" themeColor="textSecondary">{mutation.reason}</ThemedText> : null}
      {mutation.error ? <ThemedText style={styles.error}>{mutation.error}</ThemedText> : null}
    </View>
  );
}

function MutationCard({ mutation, pending, onRemove }: { mutation: Mutation; pending: boolean; onRemove: () => void }) {
  const theme = useTheme();
  if (isSlackMarkReadMutation(mutation)) return <SlackMarkReadCard mutation={mutation} />;
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
      <KeyboardAvoidingView style={styles.reviewBody} behavior={Platform.OS === 'ios' ? 'padding' : undefined}>
        <ScrollView style={styles.scroll} contentContainerStyle={styles.content} keyboardShouldPersistTaps="handled">
          <View style={styles.headerRow}>
            <StatusPill status={request.status} />
            <ThemedText type="small" themeColor="textSecondary">
              {formatWhen(request.created_at)} · by {request.requested_by || 'unknown'}
            </ThemedText>
          </View>
          <ThemedText type="subtitle" style={styles.requestTitle}>{request.title}</ThemedText>
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
              <ThemedText type="smallBold" themeColor="textSecondary">context</ThemedText>
              <ThemedText type="small" selectable>{pretty(request.context)}</ThemedText>
            </View>
          ) : null}
        </ScrollView>
        {pending ? (
          <View style={[styles.actions, { backgroundColor: theme.background, borderTopColor: theme.backgroundSelected }]}>
            <TextInput
              placeholder="Reason for denying (optional)"
              placeholderTextColor={theme.textSecondary}
              value={reason}
              onChangeText={setReason}
              style={[styles.input, { backgroundColor: theme.backgroundElement, color: theme.text }]}
            />
            <View style={styles.actionButtons}>
              <Pressable accessibilityRole="button" onPress={deny} disabled={busy} style={[styles.button, styles.deny, busy && styles.disabled]}>
                <ThemedText style={styles.buttonText}>Deny</ThemedText>
              </Pressable>
              <Pressable accessibilityRole="button" onPress={approve} disabled={busy} style={[styles.button, styles.approve, busy && styles.disabled]}>
                <ThemedText style={styles.buttonText}>Approve {request.mutation_count}</ThemedText>
              </Pressable>
            </View>
          </View>
        ) : null}
      </KeyboardAvoidingView>
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  reviewBody: { flex: 1 },
  scroll: { flex: 1 },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center', padding: Spacing.four },
  content: { padding: Spacing.three, gap: Spacing.three, paddingBottom: Spacing.five * 2 },
  headerRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two },
  requestTitle: { fontSize: 28, lineHeight: 34 },
  card: { borderRadius: 12, padding: Spacing.three, gap: Spacing.two },
  cardRemoved: { opacity: 0.5 },
  cardHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', gap: Spacing.two },
  cardHeaderCopy: { flex: 1, minWidth: 0, gap: 2 },
  field: { gap: 2 },
  actions: { gap: Spacing.two, paddingHorizontal: Spacing.three, paddingTop: Spacing.two, paddingBottom: Spacing.three, borderTopWidth: StyleSheet.hairlineWidth },
  actionButtons: { flexDirection: 'row', gap: Spacing.two },
  button: { flex: 1, minHeight: 50, borderRadius: 12, justifyContent: 'center', alignItems: 'center' },
  approve: { backgroundColor: '#16A34A' },
  deny: { backgroundColor: '#DC2626' },
  disabled: { opacity: 0.6 },
  buttonText: { color: '#fff', fontWeight: '600', fontSize: 16 },
  input: { borderRadius: 10, paddingHorizontal: Spacing.three, paddingVertical: 12, fontSize: 16 },
  linkButton: { paddingTop: Spacing.one },
  linkDanger: { color: '#DC2626', fontWeight: '600' },
  error: { color: '#D0342C' },
  slackCard: { borderWidth: StyleSheet.hairlineWidth, borderColor: '#D9770666' },
  slackAction: { borderRadius: 10, borderLeftWidth: 4, borderLeftColor: '#D97706', padding: Spacing.three, gap: Spacing.one },
  slackAccent: { color: '#D97706', letterSpacing: 0.8 },
  slackFacts: { flexDirection: 'row', flexWrap: 'wrap', gap: Spacing.three, marginTop: Spacing.one },
  slackFact: { minWidth: 100, gap: 1 },
  slackContextHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginTop: Spacing.one },
  slackTranscript: { gap: Spacing.one },
  slackMessage: { flexDirection: 'row', gap: 10, borderRadius: 10, padding: 12, borderWidth: StyleSheet.hairlineWidth, borderColor: '#6B728044' },
  slackMessageTarget: { borderColor: '#D97706', borderLeftWidth: 4, paddingLeft: 9 },
  slackMessageAfter: { opacity: 0.58, borderStyle: 'dashed' },
  slackAvatar: { width: 32, height: 32, borderRadius: 16, alignItems: 'center', justifyContent: 'center', backgroundColor: '#6B728044' },
  slackAvatarTarget: { backgroundColor: '#D97706' },
  slackAvatarTextTarget: { color: '#FFFFFF' },
  slackMessageCopy: { flex: 1, minWidth: 0, gap: 3 },
  slackMessageHeader: { flexDirection: 'row', flexWrap: 'wrap', justifyContent: 'space-between', gap: Spacing.one },
  slackMessageTextAfter: { color: '#8B8F98' },
  slackBoundaryTag: { alignSelf: 'flex-start', marginTop: 3, color: '#D97706', fontSize: 11, letterSpacing: 0.8 },
  slackAfterTag: { alignSelf: 'flex-start', marginTop: 3, fontSize: 11, letterSpacing: 0.8 },
  slackMissing: { borderWidth: 1, borderColor: '#D0342C66', borderRadius: 10, padding: 12 },
  slackTarget: { marginTop: Spacing.one, gap: 2 },
});
