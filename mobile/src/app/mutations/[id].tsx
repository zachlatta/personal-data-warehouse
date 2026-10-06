import { Stack, router, useLocalSearchParams } from 'expo-router';
import { useCallback, useEffect, useRef, useState } from 'react';
import { ActivityIndicator, Alert, KeyboardAvoidingView, Platform, Pressable, ScrollView, SectionList, StyleSheet, TextInput, View, useColorScheme } from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { useScrollLock } from '@/hooks/use-scroll-lock';
import { useTheme } from '@/hooks/use-theme';
import { GmailOverview, GmailThreadRow, type GmailScope } from '@/components/gmail-thread-review';
import { CalendarMutationCard } from '@/components/calendar-mutation-review';
import { AppleContactMutationCard } from '@/components/apple-contact-mutation-review';
import { ContactMutationCard } from '@/components/contact-mutation-review';
import { GmailEmailComposeCard } from '@/components/gmail-email-compose-review';
import { SlackMarkReadCard } from '@/components/slack-read-review';
import { SlackSendMessageCard } from '@/components/slack-send-review';
import { UndoBar, useDecidedRequestIds } from '@/components/undo-bar';
import { StatusPill } from '@/components/status-pill';
import { approveMutationRequest, getMutationRequest, listMutationRequests, rejectMutationRequest, removeMutation, updateEmailMutation, updateSlackMessageMutation, type Mutation, type MutationRequest, type UpdateEmailMutationInput, type UpdateSlackMessageMutationInput } from '@/lib/api';
import { formatWhen, pretty } from '@/lib/format';
import {
  MUTATION_PAGE_SIZE,
  gmailRequestSummary,
  hasGmailThreadMutations,
  gmailThreadDayGroups,
  gmailThreadReviews,
  isAppleContactsMutation,
  isCalendarCreateMutation,
  isContactMutation,
  isGmailSendEmailMutation,
  isGmailThreadMutation,
  isSlackMarkReadMutation,
  isSlackSendMessageMutation,
  mergeMutationPage,
  mutationReviewContext,
  pendingApproveLabel,
  requestDecision,
  requestKindLabel,
  requestLifecycle,
  requestLifecycleNote,
  requestStatusTitle,
  slackMarkReadGroups,
  withMutationStatus,
  type GmailThreadReview,
} from '@/lib/mutation-review';
import { peekMutationRequest, peekMutationRequests, rememberMutationRequest, rememberMutationRequests } from '@/lib/mutation-cache';
import { nextPendingRequestId, pendingReviewCount } from '@/lib/review-queue';
import { decisions } from '@/lib/undo-decision';
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

// An edit on screen that approval has to save first: approval runs the stored
// version, never the screen's.
type PendingEdit = { kind: 'email'; input: UpdateEmailMutationInput } | { kind: 'slack'; input: UpdateSlackMessageMutationInput };

function MutationCard({ mutation, pending, busy, onRemove, onSaveSlackMessage, onPendingChange, requestReason, alone, locked }: { mutation: Mutation; pending: boolean; busy: boolean; onRemove: () => void; onSaveSlackMessage: (input: UpdateSlackMessageMutationInput) => Promise<void>; onPendingChange?: (edit: PendingEdit | null) => void; requestReason?: string; alone?: boolean; locked?: boolean }) {
  const theme = useTheme();
  if (isGmailSendEmailMutation(mutation)) return <GmailEmailComposeCard mutation={mutation} pending={pending} busy={busy} onRemove={onRemove} onPendingChange={(input) => onPendingChange?.(input ? { kind: 'email', input } : null)} requestReason={requestReason} alone={alone} locked={locked} />;
  if (isSlackMarkReadMutation(mutation)) return <SlackMarkReadCard mutation={mutation} requestReason={requestReason} defaultExpanded={alone} />;
  if (isSlackSendMessageMutation(mutation)) return <SlackSendMessageCard mutation={mutation} pending={pending} busy={busy} onSave={onSaveSlackMessage} onPendingChange={(input) => onPendingChange?.(input ? { kind: 'slack', input } : null)} requestReason={requestReason} locked={locked} />;
  if (isCalendarCreateMutation(mutation)) return <CalendarMutationCard mutation={mutation} requestReason={requestReason} />;
  if (isContactMutation(mutation)) return <ContactMutationCard mutation={mutation} pending={pending} onRemove={onRemove} requestReason={requestReason} />;
  if (isAppleContactsMutation(mutation)) return <AppleContactMutationCard mutation={mutation} pending={pending} onRemove={onRemove} requestReason={requestReason} />;
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
    </View>
  );
}

// The request's raw context, for the rare case it says something the review
// does not: folded by default, where it used to be an always-open JSON card
// under every request.
function RequestDetails({ context }: { context: Record<string, unknown> }) {
  const theme = useTheme();
  const [open, setOpen] = useState(false);
  return (
    <View style={[styles.card, { backgroundColor: theme.backgroundElement }]}>
      <Pressable accessibilityRole="button" accessibilityState={{ expanded: open }} onPress={() => setOpen((value) => !value)}>
        <ThemedText type="small" style={styles.link}>{open ? 'Hide request details' : 'Request details'}</ThemedText>
      </Pressable>
      {open ? <ThemedText type="small" selectable>{pretty(context)}</ThemedText> : null}
    </View>
  );
}

function RequestOverview({
  request,
  error,
  filter,
  onFilter,
  flush,
}: {
  request: MutationRequest;
  error: string | null;
  filter?: string;
  onFilter?: (value: string) => void;
  // Inside the padded ScrollView the overview must not pad itself again, or
  // the header card sits indented from every card under it.
  flush?: boolean;
}) {
  const theme = useTheme();
  const context = mutationReviewContext(request.context);
  const lifecycle = requestLifecycle(request);
  const mutations = request.mutations ?? [];
  // An email's source line ("Gmail … thread 1a0e…, 12:35pm ET") says what
  // the reply-to block below already shows, in a form nobody reads.
  const emailOnly = mutations.length > 0 && mutations.every(isGmailSendEmailMutation);
  // The agent's note is two lines until tapped: in full it pushed the email
  // itself below the first screen.
  const [reasonOpen, setReasonOpen] = useState(false);
  return (
    <View style={[styles.overview, flush && styles.overviewFlush]}>
      <View style={[styles.hero, { backgroundColor: theme.backgroundElement }]}>
        <View style={styles.heroCopy}>
          <View style={styles.heroEyebrow}>
            <ThemedText type="small" themeColor="textSecondary">{requestKindLabel(mutations).toUpperCase()}</ThemedText>
            <StatusPill status={request.status} />
          </View>
          <ThemedText type="subtitle" style={styles.requestTitle}>{request.title}</ThemedText>
          {request.reason ? (
            <Pressable accessibilityRole="button" accessibilityState={{ expanded: reasonOpen }} onPress={() => setReasonOpen((value) => !value)}>
              <ThemedText type="small" themeColor="textSecondary" style={styles.requestReason} numberOfLines={reasonOpen ? undefined : 2}>{request.reason}</ThemedText>
            </Pressable>
          ) : null}
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

      {!emailOnly && (context.snapshotAt || context.source) ? (
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
      {lifecycle.withdrawn ? (
        <ThemedText type="small" themeColor="textSecondary">{requestLifecycleNote(request)}</ThemedText>
      ) : request.error ? <ThemedText style={styles.error}>{request.error}</ThemedText> : null}
      {lifecycle.supersededBy ? (
        <Pressable accessibilityRole="link" onPress={() => router.push({ pathname: '/mutations/[id]', params: { id: lifecycle.supersededBy } })}>
          <ThemedText type="small" style={styles.link}>{lifecycle.withdrawn ? 'Replaced by' : 'Superseded by'} {lifecycle.supersededBy} →</ThemedText>
        </Pressable>
      ) : null}
      {lifecycle.replaces ? (
        <Pressable accessibilityRole="link" onPress={() => router.push({ pathname: '/mutations/[id]', params: { id: lifecycle.replaces } })}>
          <ThemedText type="small" style={styles.link}>Replaces an earlier version, withdrawn for this one →</ThemedText>
        </Pressable>
      ) : null}
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
  // The alert that opened this screen carried the request, or the list read
  // it moments ago: paint that at once and let the read below confirm it.
  const [request, setRequest] = useState<MutationRequest | null>(() => (id ? peekMutationRequest(id) : null));
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [filter, setFilter] = useState('');
  const [scope, setScope] = useState<GmailScope>('all');
  // Edits on screen that are not saved yet, by mutation id. The values live in
  // a ref (they change on every keystroke); the summary the button reads is
  // state, and changes only when the count or the draft mode does.
  const pendingEdits = useRef<Record<string, PendingEdit>>({});
  const [pendingSummary, setPendingSummary] = useState({ count: 0, draft: false });
  const markPending = useCallback((mutationId: string, edit: PendingEdit | null) => {
    if (edit) pendingEdits.current[mutationId] = edit;
    else delete pendingEdits.current[mutationId];
    const edits = Object.values(pendingEdits.current);
    const next = { count: edits.length, draft: edits.length > 0 && edits.every((item) => item.kind === 'email' && item.input.delivery_mode === 'draft') };
    setPendingSummary((prev) => (prev.count === next.count && prev.draft === next.draft ? prev : next));
  }, []);
  const scroll = useScrollLock();
  // KeyboardAvoidingView measures itself relative to its parent; under a
  // native header it needs the header's height as an offset, or the keyboard
  // covers the action bar (it did, on every edit in the 2026-09-29 recording).
  const containerRef = useRef<View>(null);
  const [keyboardOffset, setKeyboardOffset] = useState(0);
  const colorScheme = useColorScheme();
  const decided = useDecidedRequestIds();

  // A refresh re-reads the first page — the header, the counts, the rows a
  // reader starts at — and folds it into the pages already scrolled through,
  // so dropping a thread on page five does not throw the reader back to one.
  const load = useCallback(async () => {
    if (!id) return;
    try {
      const fresh = await getMutationRequest(config, id);
      setRequest((current) => rememberMutationRequest(mergeMutationPage(current, fresh)));
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [config, id]);

  // The next page is read as the list nears its end. One read at a time: a
  // fast fling fires onEndReached repeatedly.
  const [loadingMore, setLoadingMore] = useState(false);
  const loadingMoreRef = useRef(false);
  const nextOffset = request?.id === id ? request?.mutations_page?.next_offset ?? null : null;
  const loadMore = useCallback(async () => {
    if (!id || nextOffset === null || loadingMoreRef.current) return;
    loadingMoreRef.current = true;
    setLoadingMore(true);
    try {
      const page = await getMutationRequest(config, id, { offset: nextOffset, limit: MUTATION_PAGE_SIZE });
      setRequest((current) => rememberMutationRequest(mergeMutationPage(current, page)));
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      loadingMoreRef.current = false;
      setLoadingMore(false);
    }
  }, [config, id, nextOffset]);

  useEffect(() => {
    if (!id) return;
    let cancelled = false;
    getMutationRequest(config, id)
      .then((loaded) => {
        rememberMutationRequest(loaded);
        if (!cancelled) setRequest(loaded);
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [config, id]);

  // Read the next request in the queue while this one is being reviewed, so
  // the screen a decision lands on paints at once instead of on a spinner.
  const status = request?.status;
  useEffect(() => {
    if (!id || status !== 'pending_review') return;
    const next = nextPendingRequestId(peekMutationRequests(), id);
    if (!next || peekMutationRequest(next)?.mutations) return;
    getMutationRequest(config, next).then(rememberMutationRequest).catch(() => undefined);
  }, [config, id, status]);

  // No confirm: a decision is held for ten seconds (undo-decision.ts) while
  // the screen moves straight on to the next request waiting, or back to the
  // list when the queue is empty. The Undo bar names what the decision does
  // and an Undo inside the window cancels it before anything is sent, then
  // reopens the request.
  const decide = async (target: MutationRequest, note: string, send: () => Promise<MutationRequest>) => {
    decisions.hold({
      requestId: target.id,
      note: `${note} · ${target.title}`,
      commit: async () => {
        rememberMutationRequest(await send());
      },
    });
    let list = peekMutationRequests();
    if (!list) {
      try {
        list = rememberMutationRequests(await listMutationRequests(config));
      } catch {
        list = null;
      }
    }
    const next = nextPendingRequestId(list, target.id, decisions.decidedRequestIds());
    if (next) router.replace({ pathname: '/mutations/[id]', params: { id: next } });
    else if (router.canGoBack()) router.back();
    else router.replace('/mutations');
  };

  // Edits on screen are saved first, because approval runs the stored
  // version, never the screen's; the note is built from the saved request so
  // it names the recipients that will actually receive it.
  const approve = async () => {
    if (!request) return;
    let current = request;
    const edits = Object.entries(pendingEdits.current);
    if (edits.length) {
      setBusy(true);
      try {
        for (const [mutationId, edit] of edits) {
          if (edit.kind === 'email') await updateEmailMutation(config, request.id, mutationId, edit.input);
          else await updateSlackMessageMutation(config, request.id, mutationId, edit.input);
        }
        current = rememberMutationRequest(await getMutationRequest(config, request.id));
        setRequest(current);
        setError(null);
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
        return;
      } finally {
        setBusy(false);
      }
    }
    await decide(current, requestDecision(current).doneLabel, () => approveMutationRequest(config, current.id));
  };
  const deny = async () => {
    if (!request) return;
    const current = request;
    await decide(current, requestDecision(current).deniedLabel, () => rejectMutationRequest(config, current.id, ''));
  };
  const keepInInbox = (review: GmailThreadReview) => {
    if (!request) return;
    const count = review.threadsInMutation;
    Alert.alert(
      review.action === 'Archive' ? (count > 1 ? `Keep ${count} threads in the inbox?` : 'Keep this thread in the inbox?') : `Skip this action on ${count} thread${count === 1 ? '' : 's'}?`,
      count > 1
        ? 'They are dropped from this request; everything else still runs.'
        : `“${review.subject}” is dropped from this request; everything else still runs.`,
      [
        { text: 'Cancel', style: 'cancel' },
        {
          text: review.action === 'Archive' ? 'Keep' : 'Skip',
          style: 'destructive',
          onPress: async () => {
            setBusy(true);
            try {
              const removed = await removeMutation(config, request.id, review.mutationId);
              setRequest((current) => (current ? withMutationStatus(current, removed) : current));
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
  const saveSlackMessage = async (mutation: Mutation, input: UpdateSlackMessageMutationInput) => {
    if (!request) return;
    setBusy(true);
    try {
      await updateSlackMessageMutation(config, request.id, mutation.id, input);
      await load();
    } finally {
      setBusy(false);
    }
  };
  const remove = (mutation: Mutation) => {
    if (!request) return;
    const contact = isContactMutation(mutation) || isAppleContactsMutation(mutation);
    Alert.alert(contact ? 'Skip this contact?' : 'Skip this email?', contact ? 'It is dropped from this request; every other contact still runs.' : 'It will not be sent when the request is approved.', [
      { text: 'Cancel', style: 'cancel' },
      {
        text: 'Skip',
        style: 'destructive',
        onPress: async () => {
          setBusy(true);
          try {
            const removed = await removeMutation(config, request.id, mutation.id);
            setRequest((current) => (current ? withMutationStatus(current, removed) : current));
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
  const gmailBatch = hasGmailThreadMutations(requestMutations);
  const otherMutations = requestMutations.filter((mutation) => !isGmailThreadMutation(mutation));
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
  const gmailReviews = gmailBatch ? gmailThreadReviews(requestMutations) : [];
  const gmailSummary = gmailRequestSummary(request, gmailReviews);
  const gmailScopeCounts: Record<GmailScope, number> = {
    // "All" is the whole request; the narrower chips count what is loaded.
    all: gmailSummary.threadCount,
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
  const decision = requestDecision(request);
  const queueList = peekMutationRequests();
  const remaining = pending ? pendingReviewCount(queueList, request.id, decided) : null;
  const skipTo = pending ? nextPendingRequestId(queueList, request.id, decided) : null;
  const overview = <RequestOverview request={request} error={error} filter={slackBatch ? filter : undefined} onFilter={slackBatch ? setFilter : undefined} />;
  const moreFooter = nextOffset !== null ? (
    <View style={styles.moreRow}>
      {loadingMore ? <ActivityIndicator /> : (
        <Pressable accessibilityRole="button" onPress={loadMore} style={[styles.moreButton, { backgroundColor: theme.backgroundElement }]}>
          <ThemedText type="smallBold">Load more · {requestMutations.length} of {request.mutation_count}</ThemedText>
        </Pressable>
      )}
    </View>
  ) : null;
  return (
    <ThemedView style={styles.container}>
      <Stack.Screen
        options={{
          title: remaining ? `${remaining} to review` : requestStatusTitle(request.status),
          headerRight: skipTo
            ? () => (
                <Pressable accessibilityRole="button" accessibilityLabel="Skip to the next request" hitSlop={8} onPress={() => router.replace({ pathname: '/mutations/[id]', params: { id: skipTo } })}>
                  <ThemedText type="smallBold" style={{ color: colorScheme === 'dark' ? '#93C5FD' : '#1D4ED8' }}>Skip</ThemedText>
                </Pressable>
              )
            : undefined,
        }}
      />
      <UndoBar
        bottom={pending ? 50 + Spacing.two + Spacing.three * 2 + insets.bottom : Spacing.three + insets.bottom}
        onOpen={(undone) => {
          if (undone.requestId !== id) router.replace({ pathname: '/mutations/[id]', params: { id: undone.requestId } });
        }}
      />
      <View
        ref={containerRef}
        style={styles.reviewBody}
        onLayout={() => containerRef.current?.measureInWindow((_x, y) => setKeyboardOffset((prev) => (prev === y ? prev : y)))}>
      <KeyboardAvoidingView style={styles.reviewBody} behavior={Platform.OS === 'ios' ? 'padding' : undefined} keyboardVerticalOffset={keyboardOffset}>
        {gmailBatch ? (
          <SectionList
            {...scroll.handlers}
            style={styles.scroll}
            contentContainerStyle={styles.batchContent}
            sections={gmailSections}
            keyExtractor={(item) => item.key}
            keyboardShouldPersistTaps="handled"
            keyboardDismissMode="on-drag"
            stickySectionHeadersEnabled
            initialNumToRender={12}
            onEndReached={loadMore}
            onEndReachedThreshold={0.6}
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
                otherActionCount={otherMutations.length}
              />
            }
            ListFooterComponent={<>
              {moreFooter}
              {otherMutations.length ? (
                <View style={styles.content}>
                  <ThemedText type="subtitle">Other actions · {otherMutations.length}</ThemedText>
                  <ThemedText type="small" themeColor="textSecondary">Approval includes these actions too.</ThemedText>
                  {otherMutations.map((mutation) => (
                    <MutationCard key={mutation.id} mutation={mutation} pending={pending} busy={busy} onRemove={() => remove(mutation)} onSaveSlackMessage={(input) => saveSlackMessage(mutation, input)} onPendingChange={(edit) => markPending(mutation.id, edit)} requestReason={request.reason} locked={scroll.locked} />
                  ))}
                </View>
              ) : null}
            </>}
            ListEmptyComponent={
              <ThemedText type="small" themeColor="textSecondary" style={styles.filterEmpty}>
                No threads match that filter.
              </ThemedText>
            }
            renderSectionHeader={({ section }) => gmailReviews.length <= 1 ? null : (
              <View style={[styles.dayHeader, { backgroundColor: theme.background, borderBottomColor: theme.backgroundSelected }]}>
                <ThemedText type="smallBold" themeColor="textSecondary">{section.label.toUpperCase()}</ThemedText>
                <ThemedText type="small" themeColor="textSecondary">{section.data.length}</ThemedText>
              </View>
            )}
            renderItem={({ item }) => (
              <GmailThreadRow review={item} pending={pending && !busy} onKeep={keepInInbox} defaultOpen={gmailReviews.length === 1} />
            )}
          />
        ) : slackBatch ? (
          <SectionList
            {...scroll.handlers}
            style={styles.scroll}
            contentContainerStyle={styles.batchContent}
            sections={slackSections}
            keyExtractor={(item) => item.mutation.id || item.review.messageTs}
            keyboardShouldPersistTaps="handled"
            stickySectionHeadersEnabled
            onEndReached={loadMore}
            onEndReachedThreshold={0.6}
            ListHeaderComponent={overview}
            ListFooterComponent={moreFooter}
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
          <ScrollView {...scroll.handlers} style={styles.scroll} contentContainerStyle={styles.content} keyboardShouldPersistTaps="handled" keyboardDismissMode="interactive">
            <RequestOverview request={request} error={error} flush />
            {request.partial && requestMutations.length === 0 ? (
              <View style={styles.partialRow}>
                <ActivityIndicator />
                <ThemedText type="small" themeColor="textSecondary">Loading {request.mutation_count} mutation{request.mutation_count === 1 ? '' : 's'}…</ThemedText>
              </View>
            ) : null}
            {requestMutations.map((mutation) => (
              <MutationCard
                key={mutation.id}
                mutation={mutation}
                pending={pending}
                busy={busy}
                onRemove={() => remove(mutation)}
                onSaveSlackMessage={(input) => saveSlackMessage(mutation, input)}
                onPendingChange={(edit) => markPending(mutation.id, edit)}
                locked={scroll.locked}
                requestReason={request.reason}
                alone={requestMutations.length === 1}
              />
            ))}
            {moreFooter}
            {request.context && Object.keys(request.context).length > 0 && mutationReviewContext(request.context).counts.length === 0 ? (
              <RequestDetails context={request.context} />
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
              <Pressable accessibilityRole="button" onPress={deny} disabled={busy} style={[styles.button, { backgroundColor: theme.backgroundElement }, busy && styles.disabled]}>
                <ThemedText style={[styles.buttonText, styles.denyText]}>{decision.denyLabel}</ThemedText>
              </Pressable>
              <Pressable accessibilityRole="button" onPress={approve} disabled={busy || decision.running === 0} style={[styles.button, styles.approve, (busy || decision.running === 0) && styles.disabled]}>
                {busy ? <ActivityIndicator color="#fff" /> : <ThemedText style={styles.buttonText}>{pendingApproveLabel(decision, pendingSummary)}</ThemedText>}
              </Pressable>
            </View>
          </View>
        ) : null}
      </KeyboardAvoidingView>
      </View>
    </ThemedView>
  );
}

const styles = StyleSheet.create({
  partialRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, paddingVertical: Spacing.two },
  moreRow: { alignItems: 'center', paddingVertical: Spacing.three, paddingHorizontal: Spacing.three },
  moreButton: { minHeight: 44, alignSelf: 'stretch', borderRadius: 12, alignItems: 'center', justifyContent: 'center', paddingHorizontal: Spacing.three },
  container: { flex: 1 },
  reviewBody: { flex: 1 },
  scroll: { flex: 1 },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center', padding: Spacing.four },
  content: { padding: Spacing.three, gap: Spacing.three, paddingBottom: Spacing.five * 2 },
  batchContent: { paddingBottom: Spacing.five * 2 },
  overview: { padding: Spacing.three, gap: Spacing.two },
  overviewFlush: { padding: 0 },
  hero: { flexDirection: 'row', gap: 12, borderRadius: 14, padding: 14, borderWidth: StyleSheet.hairlineWidth, borderColor: '#D9770644' },
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
  denyText: { color: '#DC2626' },
  disabled: { opacity: 0.6 },
  buttonText: { color: '#fff', fontWeight: '600', fontSize: 16 },
  input: { borderRadius: 10, paddingHorizontal: Spacing.three, paddingVertical: 12, fontSize: 16 },
  error: { color: '#D0342C' },
  link: { color: '#2563EB' },
  slackAccent: { color: '#D97706', letterSpacing: 0.8 },
});
