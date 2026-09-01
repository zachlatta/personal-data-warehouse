import { useState } from 'react';
import { Alert, Pressable, StyleSheet, View } from 'react-native';

import { Avatar } from '@/components/avatar';
import { OpenInSourceButton } from '@/components/open-in-source-button';
import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { Mutation } from '@/lib/api';
import { openDeepLink } from '@/lib/deep-link';
import { formatWhen } from '@/lib/format';
import { slackMarkReadReview, type SlackMarkReadReview, type SlackReviewMessage } from '@/lib/mutation-review';

// One message in the reviewed conversation. It opens in Slack on a tap,
// because the honest answer to "mark this read?" is often "let me reply
// first", and a review you cannot act on is a review that gets approved
// unread.
function SlackMessageRow({ message }: { message: SlackReviewMessage }) {
  const theme = useTheme();
  const body = (
    <>
      <Avatar name={message.actorName} url={message.avatarUrl} size={32} highlight={message.isTarget} />
      <View style={styles.slackMessageCopy}>
        <View style={styles.slackMessageHeader}>
          <ThemedText type="smallBold">{message.actorName}</ThemedText>
          <ThemedText type="small" themeColor="textSecondary">{formatWhen(message.sentAt)}</ThemedText>
        </View>
        <ThemedText type="small" style={message.isAfterBoundary && styles.slackMessageTextAfter}>{message.text}</ThemedText>
        {message.isTarget ? <ThemedText type="smallBold" style={styles.slackBoundaryTag}>READ THROUGH HERE</ThemedText> : null}
        {message.isAfterBoundary ? <ThemedText type="smallBold" themeColor="textSecondary" style={styles.slackAfterTag}>STAYS UNREAD</ThemedText> : null}
      </View>
      {message.open ? <OpenInSourceButton link={message.open} compact /> : null}
    </>
  );
  const style = [
    styles.slackMessage,
    { backgroundColor: theme.background },
    message.isTarget && styles.slackMessageTarget,
    message.isAfterBoundary && styles.slackMessageAfter,
  ];
  const link = message.open;
  if (!link) return <View style={style}>{body}</View>;
  return (
    <Pressable
      accessibilityRole="link"
      accessibilityLabel={`Open ${message.actorName}'s message in Slack`}
      onPress={() => {
        // Silence here would read as a dead tap: the native scheme rejects on
        // a phone without Slack, and the https fallback can fail too.
        void openDeepLink(link).catch((error: unknown) =>
          Alert.alert('Could not open Slack', error instanceof Error ? error.message : String(error)),
        );
      }}
      style={({ pressed }) => [...style, pressed && styles.slackMessagePressed]}>
      {body}
    </Pressable>
  );
}

export function SlackMarkReadCard({
  mutation,
  review: suppliedReview,
  requestReason,
  defaultExpanded = false,
}: {
  mutation: Mutation;
  review?: SlackMarkReadReview;
  requestReason?: string;
  // A one-conversation request has nothing to scan, so it opens on arrival.
  defaultExpanded?: boolean;
}) {
  const theme = useTheme();
  const [expanded, setExpanded] = useState(defaultExpanded);
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
        {/* In a DM the conversation IS the person, so their face identifies the
            row; a channel is identified by being a channel, and the glyph is
            what tells public from private at a glance. */}
        {review.avatarUrl && (review.conversationType === 'im' || review.conversationType === 'mpim') ? (
          <Avatar name={review.conversationLabel || icon} url={review.avatarUrl} size={30} />
        ) : (
          <View style={[styles.slackKindIcon, expanded && styles.slackKindIconOpen]}>
            <ThemedText type="smallBold" style={expanded && styles.slackAccent}>{icon}</ThemedText>
          </View>
        )}
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
          {review.open ? <OpenInSourceButton link={review.open} compact /> : null}
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
                <SlackMessageRow key={`${message.messageTs}-${index}`} message={message} />
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

const styles = StyleSheet.create({
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
  slackMessage: { flexDirection: 'row', gap: 10, borderRadius: 10, padding: 12, borderWidth: StyleSheet.hairlineWidth, borderColor: '#6B728044', alignItems: 'flex-start' },
  slackMessageTarget: { borderColor: '#D97706', borderLeftWidth: 4, paddingLeft: 9 },
  slackMessageAfter: { opacity: 0.58, borderStyle: 'dashed' },
  slackMessagePressed: { opacity: 0.7 },
  slackMessageCopy: { flex: 1, minWidth: 0, gap: 3 },
  slackMessageHeader: { flexDirection: 'row', flexWrap: 'wrap', justifyContent: 'space-between', gap: Spacing.one },
  slackMessageTextAfter: { color: '#8B8F98' },
  slackBoundaryTag: { alignSelf: 'flex-start', marginTop: 3, color: '#D97706', fontSize: 11, letterSpacing: 0.8 },
  slackAfterTag: { alignSelf: 'flex-start', marginTop: 3, fontSize: 11, letterSpacing: 0.8 },
  slackMissing: { borderWidth: 1, borderColor: '#D0342C66', borderRadius: 10, padding: 12 },
  slackTarget: { marginTop: Spacing.one, gap: 2 },
  error: { color: '#D0342C' },
});
