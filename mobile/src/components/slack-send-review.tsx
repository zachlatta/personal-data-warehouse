import { useEffect, useMemo, useState } from 'react';
import { Alert, Pressable, StyleSheet, TextInput, View } from 'react-native';

import { Avatar } from '@/components/avatar';
import { OpenInSourceButton } from '@/components/open-in-source-button';
import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { Mutation, UpdateSlackMessageMutationInput } from '@/lib/api';
import { openDeepLink } from '@/lib/deep-link';
import { formatWhen } from '@/lib/format';
import { slackSendMessageReview, type SlackSendContextMessage } from '@/lib/mutation-review';

// One message of the thread or conversation the proposed message lands in.
// The parent being replied to is highlighted, and every row opens in Slack:
// the honest answer to "send this?" is often "let me read the thread first".
function ContextRow({ message }: { message: SlackSendContextMessage }) {
  const theme = useTheme();
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
  const style = [styles.message, { backgroundColor: theme.background }, message.isThreadParent && styles.messageParent];
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
  requestReason,
}: {
  mutation: Mutation;
  pending?: boolean;
  busy?: boolean;
  onSave?: (input: UpdateSlackMessageMutationInput) => Promise<void>;
  requestReason?: string;
}) {
  const theme = useTheme();
  const review = useMemo(() => slackSendMessageReview(mutation), [mutation]);
  const pending = requestPending && mutation.status === 'pending_review';
  const editable = pending && !!onSave;
  const [text, setText] = useState(review.text);
  const [error, setError] = useState('');
  const [saved, setSaved] = useState(false);
  // A reload after a save (or someone else's edit) is the new baseline.
  useEffect(() => { setText(review.text); setSaved(false); }, [review.text]);
  const dirty = text.trim() !== review.text.trim();
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
    <View style={[styles.card, { backgroundColor: theme.backgroundElement }]}>
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

      <View style={[styles.action, { backgroundColor: theme.backgroundSelected }]}>
        <ThemedText type="smallBold" style={styles.accent}>WHAT WILL HAPPEN</ThemedText>
        <ThemedText type="small">{review.effect}</ThemedText>
        {review.verified ? <ThemedText type="small" themeColor="textSecondary">Recipient found in the warehouse.</ThemedText> : null}
      </View>
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
            {review.messages.map((message, index) => <ContextRow key={`${message.messageTs}-${index}`} message={message} />)}
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
              editable={!busy}
              multiline
              autoCapitalize="sentences"
              autoCorrect
              maxLength={SLACK_TEXT_MAX}
              style={[styles.input, { color: theme.text, backgroundColor: theme.background, borderColor: dirty ? '#D97706' : '#6B728044' }]}
            />
            <View style={styles.composeMeta}>
              <ThemedText type="small" themeColor="textSecondary">Slack formatting: *bold*, _italic_, {'<@U…>'} mentions.</ThemedText>
              <ThemedText type="small" themeColor="textSecondary">{text.length} / {SLACK_TEXT_MAX}</ThemedText>
            </View>
            {error ? <ThemedText type="small" style={styles.error}>{error}</ThemedText> : null}
            <View style={styles.composeActions}>
              {saved && !dirty ? <ThemedText type="small" themeColor="textSecondary">Saved — approval sends this text.</ThemedText> : <View />}
              <Pressable
                accessibilityRole="button"
                accessibilityState={{ disabled: busy || !dirty }}
                disabled={busy || !dirty}
                onPress={() => { void save(); }}
                style={({ pressed }) => [styles.saveButton, (busy || !dirty) && styles.saveButtonDisabled, pressed && styles.messagePressed]}>
                <ThemedText type="smallBold" style={styles.saveButtonText}>{busy ? 'Saving…' : 'Save message changes'}</ThemedText>
              </Pressable>
            </View>
          </>
        ) : (
          <View style={[styles.bubble, { backgroundColor: theme.background }]}>
            <ThemedText selectable>{review.text || '(no text)'}</ThemedText>
          </View>
        )}
        {review.sent ? (
          <ThemedText type="small" themeColor="textSecondary">
            {review.sent.alreadySent ? 'Already in Slack from an earlier attempt: ' : 'Posted as message '}{review.sent.messageTs}
          </ThemedText>
        ) : null}
      </View>

      <View style={styles.target}>
        <ThemedText type="smallBold" themeColor="textSecondary">EXACT TARGET</ThemedText>
        {review.conversationId ? <ThemedText type="code" selectable>Conversation {review.conversationId}</ThemedText> : null}
        {review.userId ? <ThemedText type="code" selectable>User {review.userId}</ThemedText> : null}
        {review.threadTs ? <ThemedText type="code" selectable>Thread {review.threadTs}</ThemedText> : null}
      </View>
      {mutation.reason && mutation.reason !== requestReason ? <ThemedText type="small" themeColor="textSecondary">{mutation.reason}</ThemedText> : null}
      {mutation.error ? <ThemedText style={styles.error}>{mutation.error}</ThemedText> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  card: { marginHorizontal: Spacing.three, borderRadius: 12, padding: 10, gap: Spacing.two },
  header: { flexDirection: 'row', alignItems: 'center', gap: 9 },
  kindIcon: { width: 30, height: 30, borderRadius: 9, alignItems: 'center', justifyContent: 'center', backgroundColor: '#D977061A' },
  headerCopy: { flex: 1, minWidth: 0, gap: 2 },
  titleRow: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  title: { flexShrink: 1 },
  chip: { borderWidth: StyleSheet.hairlineWidth, borderColor: '#6B728066', borderRadius: 4, paddingHorizontal: 4, paddingVertical: 1 },
  chipText: { color: '#8B8F98', fontSize: 8, fontWeight: '700', letterSpacing: 0.5 },
  accent: { color: '#D97706', letterSpacing: 0.8 },
  action: { borderRadius: 10, borderLeftWidth: 4, borderLeftColor: '#D97706', padding: 10, gap: Spacing.one },
  warning: { borderWidth: 1, borderColor: '#D0342C66', borderRadius: 10, padding: 10 },
  context: { gap: Spacing.one },
  contextHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  transcript: { gap: Spacing.one },
  message: { flexDirection: 'row', gap: 10, borderRadius: 10, padding: 12, borderWidth: StyleSheet.hairlineWidth, borderColor: '#6B728044', alignItems: 'flex-start' },
  messageParent: { borderColor: '#D97706', borderLeftWidth: 4, paddingLeft: 9 },
  messagePressed: { opacity: 0.7 },
  messageCopy: { flex: 1, minWidth: 0, gap: 3 },
  messageHeader: { flexDirection: 'row', flexWrap: 'wrap', justifyContent: 'space-between', gap: Spacing.one },
  parentTag: { alignSelf: 'flex-start', marginTop: 3, color: '#D97706', fontSize: 11, letterSpacing: 0.8 },
  compose: { gap: Spacing.one },
  bubble: { borderRadius: 10, padding: 12, borderWidth: StyleSheet.hairlineWidth, borderColor: '#6B728044' },
  input: { minHeight: 110, borderRadius: 10, padding: 12, borderWidth: 1, fontSize: 16, lineHeight: 22, textAlignVertical: 'top' },
  composeMeta: { flexDirection: 'row', justifyContent: 'space-between', gap: Spacing.two },
  composeActions: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', gap: Spacing.two },
  saveButton: { paddingHorizontal: 14, paddingVertical: 9, borderRadius: 10, backgroundColor: '#D97706' },
  saveButtonDisabled: { opacity: 0.45 },
  saveButtonText: { color: '#fff' },
  target: { gap: 2 },
  error: { color: '#D0342C' },
});
