import { Stack, useLocalSearchParams } from 'expo-router';
import { useCallback, useEffect, useState } from 'react';
import { ActivityIndicator, Alert, KeyboardAvoidingView, Platform, Pressable, ScrollView, SectionList, StyleSheet, TextInput, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { StatusPill } from '@/app/(tabs)/mutations';
import { approveMutationRequest, getMutationRequest, rejectMutationRequest, removeMutation, type Mutation, type MutationRequest } from '@/lib/api';
import { formatWhen, pretty } from '@/lib/format';
import { isSlackMarkReadMutation, mutationReviewContext, slackMarkReadGroups, slackMarkReadReview, type SlackMarkReadReview } from '@/lib/mutation-review';
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

function SlackMarkReadCard({ mutation, review: suppliedReview, requestReason }: { mutation: Mutation; review?: SlackMarkReadReview; requestReason?: string }) {
  const theme = useTheme();
  const [expanded, setExpanded] = useState(false);
  const review = suppliedReview ?? slackMarkReadReview(mutation);
  const target = review.targetMessage;
  const icon = review.conversationType === 'public_channel' ? '#'
    : review.conversationType === 'private_channel' ? '◈'
      : review.conversationType === 'mpim' ? '◎' : '@';
  return (
    <View style={[styles.slackCompactCard, { backgroundColor: theme.backgroundElement }]}>
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ expanded }}
        accessibilityLabel={`Review ${review.conversationLabel || 'Slack conversation'}`}
        onPress={() => setExpanded((value) => !value)}
        style={({ pressed }) => [styles.slackCompactHeader, pressed && { backgroundColor: theme.backgroundSelected }]}>
        <View style={[styles.slackKindIcon, expanded && styles.slackKindIconOpen]}>
          <ThemedText type="smallBold" style={expanded && styles.slackAccent}>{icon}</ThemedText>
        </View>
        <View style={styles.slackCompactCopy}>
          <View style={styles.slackCompactTitleRow}>
            <ThemedText type="smallBold" numberOfLines={1} style={styles.slackCompactTitle}>{review.conversationLabel || review.conversationId || 'Unknown conversation'}</ThemedText>
            <View style={styles.contextChip}><ThemedText style={styles.contextChipText}>{review.contextLabel === 'Thread context' ? 'THREAD' : 'CHAT'}</ThemedText></View>
          </View>
          <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>
            {target ? `${target.actorName}: ${target.text}` : 'Review exact read boundary'}
          </ThemedText>
        </View>
        <View style={styles.slackCompactTrailing}>
          {review.currentUnreadCount ? <View style={styles.unreadBadge}><ThemedText style={styles.unreadBadgeText}>{review.currentUnreadCount}</ThemedText></View> : null}
          <ThemedText themeColor="textSecondary" style={[styles.chevron, expanded && styles.chevronOpen]}>›</ThemedText>
        </View>
      </Pressable>

      {expanded ? (
        <View style={[styles.slackExpanded, { borderTopColor: theme.backgroundSelected }]}>
          <View style={[styles.slackAction, { backgroundColor: theme.backgroundSelected }]}>
            <ThemedText type="smallBold" style={styles.slackAccent}>READ BOUNDARY</ThemedText>
            <ThemedText type="small">Everything through the highlighted message will be marked read.</ThemedText>
            <ThemedText type="small" themeColor="textSecondary">{review.boundaryNote}</ThemedText>
          </View>

          <View style={styles.slackContextHeader}>
            <ThemedText type="smallBold" themeColor="textSecondary">{review.contextLabel.toUpperCase()}</ThemedText>
            <ThemedText type="small" themeColor="textSecondary">
              {review.messages.length} message{review.messages.length === 1 ? '' : 's'}
            </ThemedText>
          </View>
          {review.messages.length === 0 ? (
            <View style={styles.slackMissing}>
              <ThemedText type="small" style={styles.error}>Context was unavailable. Verify the exact target below before approving.</ThemedText>
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
                    <ThemedText type="small" style={message.isAfterBoundary && styles.slackMessageTextAfter}>{message.text}</ThemedText>
                    {message.isTarget ? <ThemedText type="smallBold" style={styles.slackBoundaryTag}>READ THROUGH HERE</ThemedText> : null}
                    {message.isAfterBoundary ? <ThemedText type="smallBold" themeColor="textSecondary" style={styles.slackAfterTag}>STAYS UNREAD</ThemedText> : null}
                  </View>
                </View>
              ))}
            </View>
          )}

          <View style={styles.slackTarget}>
            <ThemedText type="smallBold" themeColor="textSecondary">EXACT TARGET</ThemedText>
            <ThemedText type="code" selectable>Conversation {review.conversationId}</ThemedText>
            <ThemedText type="code" selectable>Message {review.messageTs}</ThemedText>
            {review.threadTs ? <ThemedText type="code" selectable>Thread {review.threadTs}</ThemedText> : null}
          </View>
          {mutation.reason && mutation.reason !== requestReason ? <ThemedText type="small" themeColor="textSecondary">{mutation.reason}</ThemedText> : null}
          {mutation.error ? <ThemedText style={styles.error}>{mutation.error}</ThemedText> : null}
        </View>
      ) : null}
    </View>
  );
}

function MutationCard({ mutation, pending, onRemove, requestReason }: { mutation: Mutation; pending: boolean; onRemove: () => void; requestReason?: string }) {
  const theme = useTheme();
  if (isSlackMarkReadMutation(mutation)) return <SlackMarkReadCard mutation={mutation} requestReason={requestReason} />;
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

function RequestOverview({
  request,
  error,
  filter,
  onFilter,
}: {
  request: MutationRequest;
  error: string | null;
  filter?: string;
  onFilter?: (value: string) => void;
}) {
  const theme = useTheme();
  const context = mutationReviewContext(request.context);
  return (
    <View style={styles.overview}>
      <View style={[styles.hero, { backgroundColor: theme.backgroundElement }]}>
        <View style={styles.heroIcon}><ThemedText style={styles.heroIconText}>✓</ThemedText></View>
        <View style={styles.heroCopy}>
          <View style={styles.heroEyebrow}>
            <ThemedText type="small" themeColor="textSecondary">MUTATION REQUEST</ThemedText>
            <StatusPill status={request.status} />
          </View>
          <ThemedText type="subtitle" style={styles.requestTitle}>{request.title}</ThemedText>
          {request.reason ? <ThemedText type="small" themeColor="textSecondary" style={styles.requestReason}>{request.reason}</ThemedText> : null}
          <ThemedText type="small" themeColor="textSecondary">
            {formatWhen(request.created_at)} · by {request.requested_by || 'unknown'}
          </ThemedText>
        </View>
      </View>

      {context.counts.length ? (
        <View style={styles.metricGrid}>
          {context.counts.map((item) => (
            <View key={item.key} style={[styles.metricCard, { backgroundColor: theme.backgroundElement }]}>
              <View style={styles.metricIcon}><ThemedText type="smallBold" style={styles.slackAccent}>{item.icon}</ThemedText></View>
              <View style={styles.metricCopy}>
                <ThemedText type="subtitle" style={styles.metricCount}>{item.count}</ThemedText>
                <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>{item.label}</ThemedText>
              </View>
            </View>
          ))}
        </View>
      ) : null}

      {context.selection.length || context.preserved.length ? (
        <View style={styles.guardrailGrid}>
          {context.selection.length ? (
            <View style={[styles.guardrailCard, styles.includedCard, { backgroundColor: theme.backgroundElement }]}>
              <ThemedText type="smallBold" style={styles.slackAccent}>→ INCLUDED</ThemedText>
              {context.selection.map((line) => <ThemedText key={line} type="small" themeColor="textSecondary">• {line}</ThemedText>)}
            </View>
          ) : null}
          {context.preserved.length ? (
            <View style={[styles.guardrailCard, styles.preservedCard, { backgroundColor: theme.backgroundElement }]}>
              <ThemedText type="smallBold" style={styles.preservedTitle}>✓ PRESERVED</ThemedText>
              {context.preserved.map((line) => <ThemedText key={line} type="small" themeColor="textSecondary">• {line}</ThemedText>)}
            </View>
          ) : null}
        </View>
      ) : null}

      {context.snapshotAt || context.source ? (
        <ThemedText type="small" themeColor="textSecondary" numberOfLines={2}>
          {context.snapshotAt ? `Snapshot ${formatWhen(context.snapshotAt)}` : ''}{context.snapshotAt && context.source ? ' · ' : ''}{context.source}
        </ThemedText>
      ) : null}
      {request.approved_by ? (
        <ThemedText type="small" themeColor="textSecondary">
          {request.status === 'rejected' ? 'Denied' : 'Approved'} by {request.approved_by}
          {request.approved_at ? ` · ${formatWhen(request.approved_at)}` : ''}
        </ThemedText>
      ) : null}
      {request.error ? <ThemedText style={styles.error}>{request.error}</ThemedText> : null}
      {error ? <ThemedText style={styles.error}>{error}</ThemedText> : null}
      {onFilter ? (
        <View style={styles.filterBlock}>
          <View style={styles.filterTitleRow}>
            <View>
              <ThemedText type="smallBold">Reviewed boundaries</ThemedText>
              <ThemedText type="small" themeColor="textSecondary">{request.mutation_count} Slack conversations</ThemedText>
            </View>
          </View>
          <TextInput
            accessibilityLabel="Filter conversations"
            placeholder="Filter conversations"
            placeholderTextColor={theme.textSecondary}
            value={filter}
            onChangeText={onFilter}
            clearButtonMode="while-editing"
            style={[styles.filterInput, { backgroundColor: theme.backgroundElement, color: theme.text }]}
          />
        </View>
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
  const [filter, setFilter] = useState('');

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
  const requestMutations = request.mutations ?? [];
  const slackBatch = requestMutations.length > 1 && requestMutations.every((mutation) => isSlackMarkReadMutation(mutation));
  const query = filter.trim().toLowerCase();
  const slackSections = slackBatch
    ? slackMarkReadGroups(requestMutations).map((group) => ({
        ...group,
        data: group.items.filter(({ review }) => {
          if (!query) return true;
          return [review.conversationLabel, review.conversationId, review.conversationType, review.targetMessage?.actorName, review.targetMessage?.text]
            .filter(Boolean).join(' ').toLowerCase().includes(query);
        }),
      })).filter((group) => group.data.length > 0)
    : [];
  const overview = <RequestOverview request={request} error={error} filter={slackBatch ? filter : undefined} onFilter={slackBatch ? setFilter : undefined} />;
  return (
    <ThemedView style={styles.container}>
      <Stack.Screen options={{ title: pending ? 'Review' : request.status.replace(/_/g, ' ') }} />
      <KeyboardAvoidingView style={styles.reviewBody} behavior={Platform.OS === 'ios' ? 'padding' : undefined}>
        {slackBatch ? (
          <SectionList
            style={styles.scroll}
            contentContainerStyle={styles.batchContent}
            sections={slackSections}
            keyExtractor={(item) => item.mutation.id || item.review.messageTs}
            keyboardShouldPersistTaps="handled"
            stickySectionHeadersEnabled
            ListHeaderComponent={overview}
            ListEmptyComponent={query ? <ThemedText type="small" themeColor="textSecondary" style={styles.filterEmpty}>No conversations match that filter.</ThemedText> : null}
            renderSectionHeader={({ section }) => (
              <View style={[styles.batchSectionHeader, { backgroundColor: theme.background }]}>
                <View style={[styles.batchSectionIcon, { backgroundColor: theme.backgroundElement }]}><ThemedText type="smallBold" style={styles.slackAccent}>{section.icon}</ThemedText></View>
                <View style={styles.batchSectionCopy}>
                  <ThemedText type="smallBold">{section.label}</ThemedText>
                  <ThemedText type="small" themeColor="textSecondary">{section.description}</ThemedText>
                </View>
                <View style={[styles.batchCount, { backgroundColor: theme.backgroundElement }]}><ThemedText type="smallBold" themeColor="textSecondary">{section.data.length}</ThemedText></View>
              </View>
            )}
            renderItem={({ item }) => (
              <SlackMarkReadCard mutation={item.mutation as Mutation} review={item.review} requestReason={request.reason} />
            )}
          />
        ) : (
          <ScrollView style={styles.scroll} contentContainerStyle={styles.content} keyboardShouldPersistTaps="handled">
            {overview}
            {requestMutations.map((mutation) => (
              <MutationCard key={mutation.id} mutation={mutation} pending={pending} onRemove={() => remove(mutation)} requestReason={request.reason} />
            ))}
            {Object.keys(request.context ?? {}).length > 0 && mutationReviewContext(request.context).counts.length === 0 ? (
              <View style={[styles.card, { backgroundColor: theme.backgroundElement }]}>
                <ThemedText type="smallBold" themeColor="textSecondary">context</ThemedText>
                <ThemedText type="small" selectable>{pretty(request.context)}</ThemedText>
              </View>
            ) : null}
          </ScrollView>
        )}
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
  batchContent: { paddingBottom: Spacing.five * 2 },
  overview: { padding: Spacing.three, gap: Spacing.two },
  hero: { flexDirection: 'row', gap: 12, borderRadius: 14, padding: 14, borderWidth: StyleSheet.hairlineWidth, borderColor: '#D9770644' },
  heroIcon: { width: 42, height: 42, borderRadius: 12, alignItems: 'center', justifyContent: 'center', backgroundColor: '#D977061A', borderWidth: 1, borderColor: '#D9770666' },
  heroIconText: { color: '#D97706', fontSize: 18, fontWeight: '800' },
  heroCopy: { flex: 1, minWidth: 0, gap: 4 },
  heroEyebrow: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 7 },
  requestTitle: { fontSize: 21, lineHeight: 26 },
  requestReason: { lineHeight: 18 },
  metricGrid: { flexDirection: 'row', flexWrap: 'wrap', gap: Spacing.two },
  metricCard: { width: '48%', minHeight: 62, flexGrow: 1, flexDirection: 'row', alignItems: 'center', gap: 9, borderRadius: 12, padding: 10 },
  metricIcon: { width: 30, height: 30, borderRadius: 9, alignItems: 'center', justifyContent: 'center', backgroundColor: '#D977061A' },
  metricCopy: { flex: 1, minWidth: 0 },
  metricCount: { fontSize: 19, lineHeight: 22 },
  guardrailGrid: { gap: Spacing.two },
  guardrailCard: { gap: 4, borderRadius: 12, padding: 11, borderLeftWidth: 4 },
  includedCard: { borderLeftColor: '#D97706' },
  preservedCard: { borderLeftColor: '#16A34A' },
  preservedTitle: { color: '#16A34A', letterSpacing: 0.8 },
  filterBlock: { gap: Spacing.two, marginTop: Spacing.one },
  filterTitleRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  filterInput: { minHeight: 44, borderRadius: 11, paddingHorizontal: Spacing.three, fontSize: 15 },
  filterEmpty: { paddingHorizontal: Spacing.three, paddingVertical: Spacing.four, textAlign: 'center' },
  batchSectionHeader: { flexDirection: 'row', alignItems: 'center', gap: 10, minHeight: 54, paddingHorizontal: Spacing.three, paddingVertical: 7, borderBottomWidth: StyleSheet.hairlineWidth, borderBottomColor: '#6B728044' },
  batchSectionIcon: { width: 32, height: 32, borderRadius: 9, alignItems: 'center', justifyContent: 'center' },
  batchSectionCopy: { flex: 1, minWidth: 0 },
  batchCount: { minWidth: 32, minHeight: 24, paddingHorizontal: 8, borderRadius: 12, alignItems: 'center', justifyContent: 'center' },
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
  slackCompactCard: { marginHorizontal: Spacing.three, borderBottomWidth: StyleSheet.hairlineWidth, borderBottomColor: '#6B728044', overflow: 'hidden' },
  slackCompactHeader: { minHeight: 58, flexDirection: 'row', alignItems: 'center', gap: 9, paddingHorizontal: 10, paddingVertical: 7 },
  slackKindIcon: { width: 30, height: 30, borderRadius: 9, alignItems: 'center', justifyContent: 'center', backgroundColor: '#6B728022' },
  slackKindIconOpen: { backgroundColor: '#D977061A' },
  slackCompactCopy: { flex: 1, minWidth: 0, gap: 2 },
  slackCompactTitleRow: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  slackCompactTitle: { flexShrink: 1 },
  contextChip: { borderWidth: StyleSheet.hairlineWidth, borderColor: '#6B728066', borderRadius: 4, paddingHorizontal: 4, paddingVertical: 1 },
  contextChipText: { color: '#8B8F98', fontSize: 8, fontWeight: '700', letterSpacing: 0.5 },
  slackCompactTrailing: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  unreadBadge: { minWidth: 25, height: 20, paddingHorizontal: 5, alignItems: 'center', justifyContent: 'center', borderRadius: 10, backgroundColor: '#D977061A' },
  unreadBadgeText: { color: '#D97706', fontSize: 10, fontWeight: '700' },
  chevron: { fontSize: 22, lineHeight: 22, transform: [{ rotate: '0deg' }] },
  chevronOpen: { transform: [{ rotate: '90deg' }], color: '#D97706' },
  slackExpanded: { gap: Spacing.two, padding: 10, borderTopWidth: StyleSheet.hairlineWidth },
  slackAction: { borderRadius: 10, borderLeftWidth: 4, borderLeftColor: '#D97706', padding: 10, gap: Spacing.one },
  slackAccent: { color: '#D97706', letterSpacing: 0.8 },
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
