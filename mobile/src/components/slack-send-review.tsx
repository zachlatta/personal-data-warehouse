import { Alert, Pressable, StyleSheet, View } from 'react-native';

import { Avatar } from '@/components/avatar';
import { OpenInSourceButton } from '@/components/open-in-source-button';
import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { Mutation } from '@/lib/api';
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

// A Slack send, reviewed as the message it is: who receives it, the words,
// and the thread or conversation they land in. The phone reads and decides;
// the words are edited on the web review page, like an email's.
export function SlackSendMessageCard({ mutation, requestReason }: { mutation: Mutation; requestReason?: string }) {
  const theme = useTheme();
  const review = slackSendMessageReview(mutation);
  const pending = mutation.status === 'pending_review';
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
        <View style={[styles.bubble, { backgroundColor: theme.background }]}>
          <ThemedText selectable>{review.text || '(no text)'}</ThemedText>
        </View>
        {pending ? (
          <ThemedText type="small" themeColor="textSecondary">To change the words, edit this request on the web review page before approving.</ThemedText>
        ) : null}
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
  target: { gap: 2 },
  error: { color: '#D0342C' },
});
