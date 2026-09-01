import { Stack, useLocalSearchParams } from 'expo-router';
import { useCallback, useEffect, useState } from 'react';
import { ActivityIndicator, Alert, KeyboardAvoidingView, Platform, Pressable, ScrollView, SectionList, StyleSheet, TextInput, View } from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { GmailOverview, GmailThreadRow, type GmailScope } from '@/components/gmail-thread-review';
import { SlackMarkReadCard } from '@/components/slack-read-review';
import { StatusPill } from '@/components/status-pill';
import { approveMutationRequest, getMutationRequest, rejectMutationRequest, removeMutation, type Mutation, type MutationRequest } from '@/lib/api';
import { formatWhen, pretty } from '@/lib/format';
import {
  gmailBatchSummary,
  gmailThreadDayGroups,
  gmailThreadReviews,
  isGmailThreadMutation,
  isSlackMarkReadMutation,
  mutationReviewContext,
  slackMarkReadGroups,
  type GmailThreadReview,
} from '@/lib/mutation-review';
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

function MutationCard({ mutation, pending, onRemove, requestReason, alone }: { mutation: Mutation; pending: boolean; onRemove: () => void; requestReason?: string; alone?: boolean }) {
  const theme = useTheme();
  if (isSlackMarkReadMutation(mutation)) return <SlackMarkReadCard mutation={mutation} requestReason={requestReason} defaultExpanded={alone} />;
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
  const insets = useSafeAreaInsets();
  const [request, setRequest] = useState<MutationRequest | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [filter, setFilter] = useState('');
  const [scope, setScope] = useState<GmailScope>('all');

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
    const running = request.mutations?.length
      ? request.mutations.filter((mutation) => mutation.status === 'pending_review').length
      : request.mutation_count;
    Alert.alert('Approve request?', `${running} mutation${running === 1 ? '' : 's'} will run upstream.`, [
      { text: 'Cancel', style: 'cancel' },
      { text: 'Approve', style: 'default', onPress: () => act(() => approveMutationRequest(config, request.id)) },
    ]);
  };
  // The reason is asked for at the moment of denial rather than parked in a
  // permanent text field: on a phone that field cost a row of the list on
  // every screen, and it was empty almost every time.
  const deny = () => {
    if (!request) return;
    const submit = (reason?: string) => act(() => rejectMutationRequest(config, request.id, (reason ?? '').trim()));
    if (Platform.OS === 'ios') {
      Alert.prompt('Deny this request?', 'Nothing runs upstream. A reason is optional.', [
        { text: 'Cancel', style: 'cancel' },
        { text: 'Deny', style: 'destructive', onPress: submit },
      ], 'plain-text');
      return;
    }
    Alert.alert('Deny this request?', 'Nothing runs upstream.', [
      { text: 'Cancel', style: 'cancel' },
      { text: 'Deny', style: 'destructive', onPress: () => submit('') },
    ]);
  };
  const keepInInbox = (review: GmailThreadReview) => {
    if (!request) return;
    const count = review.threadsInMutation;
    Alert.alert(
      count > 1 ? `Keep ${count} threads in the inbox?` : 'Keep this thread in the inbox?',
      count > 1
        ? 'They are dropped from this request; everything else still runs.'
        : `“${review.subject}” is dropped from this request; everything else still runs.`,
      [
        { text: 'Cancel', style: 'cancel' },
        {
          text: 'Keep',
          style: 'destructive',
          onPress: async () => {
            setBusy(true);
            try {
              await removeMutation(config, request.id, review.mutationId);
              await load();
            } catch (e) {
              setError(e instanceof Error ? e.message : String(e));
            } finally {
              setBusy(false);
            }
          },
        },
      ],
    );
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
  const gmailBatch = requestMutations.length > 0 && requestMutations.every((mutation) => isGmailThreadMutation(mutation));
  const query = filter.trim().toLowerCase();
  // A mutation kept out of the batch stays in the response as a rejected row,
  // so the button has to count what is still going to run, not the request's
  // original size.
  const runningCount = requestMutations.length
    ? requestMutations.filter((mutation) => mutation.status === 'pending_review').length
    : request.mutation_count;
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
  const gmailReviews = gmailBatch ? gmailThreadReviews(requestMutations) : [];
  const gmailSummary = gmailBatchSummary(requestMutations, gmailReviews);
  const gmailScopeCounts: Record<GmailScope, number> = {
    all: gmailReviews.filter((review) => !review.removed).length,
    unread: gmailReviews.filter((review) => !review.removed && review.unread).length,
    automated: gmailReviews.filter((review) => !review.removed && review.automated).length,
    kept: gmailReviews.filter((review) => review.removed).length,
  };
  const gmailVisible = gmailReviews.filter((review) => {
    if (scope === 'kept' ? !review.removed : review.removed) return false;
    if (scope === 'unread' && !review.unread) return false;
    if (scope === 'automated' && !review.automated) return false;
    if (!query) return true;
    return [review.senderName, review.senderAddress, review.subject, review.preview, review.account]
      .filter(Boolean).join(' ').toLowerCase().includes(query);
  });
  const gmailSections = gmailThreadDayGroups(gmailVisible);
  const overview = <RequestOverview request={request} error={error} filter={slackBatch ? filter : undefined} onFilter={slackBatch ? setFilter : undefined} />;
  return (
    <ThemedView style={styles.container}>
      <Stack.Screen options={{ title: pending ? 'Review' : request.status.replace(/_/g, ' ') }} />
      <KeyboardAvoidingView style={styles.reviewBody} behavior={Platform.OS === 'ios' ? 'padding' : undefined}>
        {gmailBatch ? (
          <SectionList
            style={styles.scroll}
            contentContainerStyle={styles.batchContent}
            sections={gmailSections}
            keyExtractor={(item) => item.key}
            keyboardShouldPersistTaps="handled"
            keyboardDismissMode="on-drag"
            stickySectionHeadersEnabled
            initialNumToRender={12}
            ListHeaderComponent={
              <GmailOverview
                request={request}
                summary={gmailSummary}
                error={error}
                filter={filter}
                onFilter={setFilter}
                scope={scope}
                onScope={setScope}
                scopeCounts={gmailScopeCounts}
                visible={gmailVisible.length}
              />
            }
            ListEmptyComponent={
              <ThemedText type="small" themeColor="textSecondary" style={styles.filterEmpty}>
                No threads match that filter.
              </ThemedText>
            }
            renderSectionHeader={({ section }) => (
              <View style={[styles.dayHeader, { backgroundColor: theme.background, borderBottomColor: theme.backgroundSelected }]}>
                <ThemedText type="smallBold" themeColor="textSecondary">{section.label.toUpperCase()}</ThemedText>
                <ThemedText type="small" themeColor="textSecondary">{section.data.length}</ThemedText>
              </View>
            )}
            renderItem={({ item }) => (
              <GmailThreadRow review={item} pending={pending} onKeep={keepInInbox} defaultOpen={gmailReviews.length === 1} />
            )}
          />
        ) : slackBatch ? (
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
              <MutationCard
                key={mutation.id}
                mutation={mutation}
                pending={pending}
                onRemove={() => remove(mutation)}
                requestReason={request.reason}
                alone={requestMutations.length === 1}
              />
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
          <View
            style={[
              styles.actions,
              // Clear of the home indicator; the buttons are the two most
              // consequential controls on the screen.
              { backgroundColor: theme.background, borderTopColor: theme.backgroundSelected, paddingBottom: Spacing.three + insets.bottom },
            ]}>
            <View style={styles.actionButtons}>
              <Pressable accessibilityRole="button" onPress={deny} disabled={busy} style={[styles.button, styles.deny, busy && styles.disabled]}>
                <ThemedText style={styles.buttonText}>Deny all</ThemedText>
              </Pressable>
              <Pressable accessibilityRole="button" onPress={approve} disabled={busy} style={[styles.button, styles.approve, busy && styles.disabled]}>
                <ThemedText style={styles.buttonText}>
                  {gmailBatch ? `${gmailSummary.verb} ${runningCount}` : `Approve ${runningCount}`}
                </ThemedText>
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
  scopeRow: { flexDirection: 'row', flexWrap: 'wrap', gap: Spacing.two, marginTop: Spacing.one },
  scopeChip: { minHeight: 34, justifyContent: 'center', paddingHorizontal: 12, borderRadius: 17 },
  scopeChipActive: { backgroundColor: '#D97706' },
  scopeChipActiveText: { color: '#FFFFFF' },
  dayHeader: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', paddingHorizontal: Spacing.three, paddingVertical: 6, borderBottomWidth: StyleSheet.hairlineWidth },
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
  slackAccent: { color: '#D97706', letterSpacing: 0.8 },
});
