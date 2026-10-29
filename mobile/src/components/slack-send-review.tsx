import { useEffect, useMemo, useRef, useState } from 'react';
import { Alert, Pressable, StyleSheet, TextInput, View } from 'react-native';

import { Avatar } from '@/components/avatar';
import { ComposeMeta } from '@/components/compose-meta';
import { OpenInSourceButton } from '@/components/open-in-source-button';
import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { useReviewDraftState } from '@/hooks/use-review-draft-state';
import type { Mutation, UpdateSlackMessageMutationInput } from '@/lib/api';
import { openDeepLink } from '@/lib/deep-link';
import { formatWhen } from '@/lib/format';
import { slackSendMessageReview, type SlackSendContextMessage } from '@/lib/mutation-review';

// One message of the thread or conversation the proposed message lands in.
// The parent being replied to is highlighted, and every row opens in Slack:
// the honest answer to "send this?" is often "let me read the thread first".
function ContextRow({ message, surface }: { message: SlackSendContextMessage; surface: string }) {
  const body = (
    <>
      <Avatar name={message.actorName} url={message.avatarUrl} size={32} highlight={message.isThreadParent} />
      <View style={styles.messageCopy}>
        <View style={styles.messageHeader}>
          <ThemedText type="smallBold">{message.actorName}</ThemedText>
          <ThemedText type="small" themeColor="textSecondary">{formatWhen(message.sentAt)}</ThemedText>
        </View>
        <ThemedText type="small">{message.text}</ThemedText>
        {message.isThreadParent ? <ThemedText type="smallBold" style={styles.parentTag}>REPLYING TO THIS</ThemedText> : null}
      </View>
      {message.open ? <OpenInSourceButton link={message.open} compact /> : null}
    </>
  );
  const style = [styles.message, { backgroundColor: surface }, message.isThreadParent && styles.messageParent];
  const link = message.open;
  if (!link) return <View style={style}>{body}</View>;
  return (
    <Pressable
      accessibilityRole="link"
      accessibilityLabel={`Open ${message.actorName}'s message in Slack`}
      onPress={() => {
        void openDeepLink(link).catch((error: unknown) =>
          Alert.alert('Could not open Slack', error instanceof Error ? error.message : String(error)),
        );
      }}
      style={({ pressed }) => [...style, pressed && styles.messagePressed]}>
      {body}
    </Pressable>
  );
}

const SLACK_TEXT_MAX = 4000;

// A Slack send, reviewed as the message it is: who receives it, the words,
// and the thread or conversation they land in. The words are editable while
// the request is pending and save through update-slack-message, so the edit
// is on the server before approval — approval sends what is stored, never
// what is on screen. The recipient and the thread are not editable here or
// on the web: a wrong recipient is a deny.
export function SlackSendMessageCard({
  mutation,
  pending: requestPending = mutation.status === 'pending_review',
  busy = false,
  onSave,
  onPendingChange,
  requestReason,
  locked,
  alone,
}: {
  mutation: Mutation;
  pending?: boolean;
  busy?: boolean;
  onSave?: (input: UpdateSlackMessageMutationInput) => Promise<void>;
  // Unsaved text is reported up: Approve sends the stored message, so the
  // screen saves this on the way to sending.
  onPendingChange?: (input: UpdateSlackMessageMutationInput | null) => void;
  requestReason?: string;
  // The screen's scroll lock: not focusable while the page moves.
  locked?: boolean;
  // The request's only message: drawn on the page itself, not as a card on
  // it, so the draft gets the screen's full width.
  alone?: boolean;
}) {
  const theme = useTheme();
  // What sits on the card, or on the page when there is no card.
  const surface = alone ? theme.backgroundElement : theme.background;
  const review = useMemo(() => slackSendMessageReview(mutation), [mutation]);
  const pending = requestPending && mutation.status === 'pending_review';
  const editable = pending && !!onSave;
  const [text, setText] = useReviewDraftState(`${mutation.request_id}:${mutation.id}:text`, review.text, review.text, pending);
  const [error, setError] = useState('');
  const [saved, setSaved] = useState(false);
  const dirty = text.trim() !== review.text.trim();
  const [focused, setFocused] = useState(false);
  const [showIds, setShowIds] = useState(false);
  // The callback is held in a ref: the parent passes a fresh closure every
  // render, and keying the effects on it made each report re-render the
  // parent, which re-reported — an update loop on the first keystroke.
  const report = useRef(onPendingChange);
  useEffect(() => {
    report.current = onPendingChange;
  }, [onPendingChange]);
  const unsavedText = editable && dirty && text.trim() ? text.trim() : '';
  useEffect(() => {
    report.current?.(unsavedText ? { text: unsavedText } : null);
  }, [unsavedText]);
  useEffect(() => {
    const reporter = report;
    return () => reporter.current?.(null);
  }, []);
  const save = async () => {
    if (!onSave) return;
    const trimmed = text.trim();
    if (!trimmed) { setError('The message cannot be empty.'); return; }
    if (trimmed.length > SLACK_TEXT_MAX) { setError(`Slack messages are at most ${SLACK_TEXT_MAX} characters.`); return; }
    setError('');
    try {
      await onSave({ text: trimmed });
      setSaved(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };
  const icon = review.conversationType === 'public_channel' ? '#'
    : review.conversationType === 'private_channel' ? '◈'
      : review.conversationType === 'mpim' ? '◎' : '@';
  return (
    <View style={[styles.card, alone ? styles.cardAlone : { backgroundColor: theme.backgroundElement }]}>
      <View style={styles.header}>
        {review.avatarUrl && (review.delivery === 'dm' || review.conversationType === 'im') ? (
          <Avatar name={review.recipientLabel || icon} url={review.avatarUrl} size={30} />
        ) : (
          <View style={styles.kindIcon}><ThemedText type="smallBold" style={styles.accent}>{icon}</ThemedText></View>
        )}
        <View style={styles.headerCopy}>
          <View style={styles.titleRow}>
            <ThemedText type="smallBold" numberOfLines={1} style={styles.title}>{review.recipientLabel || 'Unknown recipient'}</ThemedText>
            <View style={styles.chip}><ThemedText style={styles.chipText}>{review.deliveryLabel.toUpperCase()}</ThemedText></View>
          </View>
          <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>
            {review.heading} as {review.account}{review.replyBroadcast ? ' · also to the conversation' : ''}
          </ThemedText>
        </View>
        {review.open ? <OpenInSourceButton link={review.open} compact /> : null}
      </View>

      {alone ? (
        <ThemedText type="small" themeColor="textSecondary">
          {review.effect}{review.verified ? ' Recipient found in the warehouse.' : ''}
        </ThemedText>
      ) : (
        <View style={[styles.action, { backgroundColor: theme.backgroundSelected }]}>
          <ThemedText type="smallBold" style={styles.accent}>WHAT WILL HAPPEN</ThemedText>
          <ThemedText type="small">{review.effect}</ThemedText>
          {review.verified ? <ThemedText type="small" themeColor="textSecondary">Recipient found in the warehouse.</ThemedText> : null}
        </View>
      )}
      {review.warnings.map((warning) => (
        <View key={warning} style={styles.warning}><ThemedText type="small" style={styles.error}>{warning}</ThemedText></View>
      ))}

      {review.messages.length ? (
        <View style={styles.context}>
          <View style={styles.contextHeader}>
            <ThemedText type="smallBold" themeColor="textSecondary">{review.contextLabel.toUpperCase()}</ThemedText>
            <ThemedText type="small" themeColor="textSecondary">{review.messages.length} message{review.messages.length === 1 ? '' : 's'}</ThemedText>
          </View>
          <View style={styles.transcript}>
            {review.messages.map((message, index) => <ContextRow key={`${message.messageTs}-${index}`} message={message} surface={surface} />)}
          </View>
        </View>
      ) : null}

      <View style={styles.compose}>
        <ThemedText type="smallBold" themeColor="textSecondary">
          {review.sent ? 'MESSAGE SENT' : 'YOUR MESSAGE'}{review.edited ? ' · EDITED IN REVIEW' : ''}
        </ThemedText>
        {editable ? (
          <>
            <TextInput
              accessibilityLabel="Message text"
              value={text}
              onChangeText={(value) => { setText(value); setSaved(false); }}
              editable={!busy && (focused || !locked)}
              onFocus={() => setFocused(true)}
              onBlur={() => setFocused(false)}
              multiline
              autoCapitalize="sentences"
              autoCorrect
              maxLength={SLACK_TEXT_MAX}
              style={[styles.input, { color: theme.text, backgroundColor: surface, borderColor: dirty ? '#E0A05C' : '#8A878444' }]}
            />
            <ComposeMeta
              hint={dirty ? 'Edited. Send saves it first.' : saved ? 'Saved. Send sends this text.' : 'Slack formatting: *bold*, _italic_, <@U…> mentions.'}
              length={text.length}
              max={SLACK_TEXT_MAX}
              dirty={dirty}
              busy={busy}
              onSave={() => { void save(); }}
            />
            {error ? <ThemedText type="small" style={styles.error}>{error}</ThemedText> : null}
          </>
        ) : (
          <View style={[styles.bubble, { backgroundColor: surface }]}>
            <ThemedText selectable>{review.text || '(no text)'}</ThemedText>
          </View>
        )}
        {review.sent ? (
          <ThemedText type="small" themeColor="textSecondary">
            {review.sent.alreadySent ? 'Already in Slack from an earlier attempt: ' : 'Posted as message '}{review.sent.messageTs}
          </ThemedText>
        ) : null}
      </View>

      {review.conversationId || review.userId || review.threadTs ? (
        <View style={styles.target}>
          <Pressable
            accessibilityRole="button"
            accessibilityState={{ expanded: showIds }}
            hitSlop={8}
            onPress={() => setShowIds((open) => !open)}
            style={({ pressed }) => pressed && styles.messagePressed}>
            <ThemedText type="small" themeColor="textSecondary">{showIds ? 'Hide Slack IDs' : 'Show Slack IDs'}</ThemedText>
          </Pressable>
          {showIds && review.conversationId ? <ThemedText type="code" selectable>Conversation {review.conversationId}</ThemedText> : null}
          {showIds && review.userId ? <ThemedText type="code" selectable>User {review.userId}</ThemedText> : null}
          {showIds && review.threadTs ? <ThemedText type="code" selectable>Thread {review.threadTs}</ThemedText> : null}
        </View>
      ) : null}
      {mutation.reason && mutation.reason !== requestReason ? <ThemedText type="small" themeColor="textSecondary">{mutation.reason}</ThemedText> : null}
      {mutation.error ? <ThemedText style={styles.error}>{mutation.error}</ThemedText> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  card: { borderRadius: 12, padding: 10, gap: Spacing.two },
  cardAlone: { padding: 0, gap: Spacing.three },
  header: { flexDirection: 'row', alignItems: 'center', gap: 9 },
  kindIcon: { width: 30, height: 30, borderRadius: 9, alignItems: 'center', justifyContent: 'center', backgroundColor: '#E0A05C1A' },
  headerCopy: { flex: 1, minWidth: 0, gap: 2 },
  titleRow: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  title: { flexShrink: 1 },
  chip: { borderWidth: StyleSheet.hairlineWidth, borderColor: '#8A878466', borderRadius: 4, paddingHorizontal: 4, paddingVertical: 1 },
  chipText: { color: '#8A8784', fontSize: 8, fontWeight: '700', letterSpacing: 0.5 },
  accent: { color: '#E0A05C', letterSpacing: 0.8 },
  action: { borderRadius: 10, borderLeftWidth: 4, borderLeftColor: '#E0A05C', padding: 10, gap: Spacing.one },
  warning: { borderWidth: 1, borderColor: '#E5636D66', borderRadius: 10, padding: 10 },
  context: { gap: Spacing.one },
  contextHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  transcript: { gap: Spacing.one },
  message: { flexDirection: 'row', gap: 10, borderRadius: 10, padding: 12, borderWidth: StyleSheet.hairlineWidth, borderColor: '#8A878444', alignItems: 'flex-start' },
  messageParent: { borderColor: '#E0A05C', borderLeftWidth: 4, paddingLeft: 9 },
  messagePressed: { opacity: 0.7 },
  messageCopy: { flex: 1, minWidth: 0, gap: 3 },
  messageHeader: { flexDirection: 'row', flexWrap: 'wrap', justifyContent: 'space-between', gap: Spacing.one },
  parentTag: { alignSelf: 'flex-start', marginTop: 3, color: '#E0A05C', fontSize: 11, letterSpacing: 0.8 },
  compose: { gap: Spacing.one },
  bubble: { borderRadius: 10, padding: 12, borderWidth: StyleSheet.hairlineWidth, borderColor: '#8A878444' },
  input: { minHeight: 110, borderRadius: 10, padding: 12, borderWidth: 1, fontSize: 16, lineHeight: 22, textAlignVertical: 'top' },
  target: { gap: 2 },
  error: { color: '#E5636D' },
});
