import { useState } from 'react';
import { Pressable, StyleSheet, TextInput, View } from 'react-native';

import { Avatar } from '@/components/avatar';
import { OpenInSourceButton } from '@/components/open-in-source-button';
import { StatusPill } from '@/components/status-pill';
import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { MutationRequest } from '@/lib/api';
import { cleanSnippet, formatWhen, truncate } from '@/lib/format';
import type { GmailBatchSummary, GmailThreadReview } from '@/lib/mutation-review';

const ACCENT = '#D97706';
const DANGER = '#DC2626';

function timeOfDay(iso: string): string {
  const date = new Date(iso);
  if (!iso || Number.isNaN(date.getTime())) return '';
  return date.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' });
}

function Chip({ label, tone = 'muted' }: { label: string; tone?: 'muted' | 'accent' | 'danger' }) {
  const color = tone === 'accent' ? ACCENT : tone === 'danger' ? DANGER : '#8B8F98';
  return (
    <View style={[styles.chip, { borderColor: `${color}66` }]}>
      <ThemedText style={[styles.chipText, { color }]}>{label}</ThemedText>
    </View>
  );
}

function ThreadMessage({ message, account }: { message: GmailThreadReview['messages'][number]; account: string }) {
  const theme = useTheme();
  const [full, setFull] = useState(false);
  const body = cleanSnippet(message.text ?? '').trim();
  const long = body.length > 420;
  const recipients = [
    message.to.length ? `to ${message.to.join(', ')}` : '',
    message.cc.length ? `cc ${message.cc.join(', ')}` : '',
  ].filter(Boolean).join(' · ');
  return (
    <View style={[styles.message, { borderColor: theme.backgroundSelected }]}>
      <View style={styles.messageHead}>
        <ThemedText type="smallBold" numberOfLines={1} style={styles.messageSender}>{message.senderName}</ThemedText>
        <ThemedText type="small" themeColor="textSecondary">{timeOfDay(message.sentAt)}</ThemedText>
      </View>
      {message.senderAddress ? (
        <ThemedText type="small" themeColor="textSecondary" numberOfLines={1} selectable>{message.senderAddress}</ThemedText>
      ) : null}
      <ThemedText type="small" themeColor="textSecondary" numberOfLines={2}>{recipients || `to ${account}`}</ThemedText>
      {body ? (
        <ThemedText type="small" style={styles.messageBody}>{full || !long ? body : `${body.slice(0, 420)}…`}</ThemedText>
      ) : (
        <ThemedText type="small" themeColor="textSecondary">This message&rsquo;s text is not in the warehouse. Open it in Gmail before approving.</ThemedText>
      )}
      {long ? (
        <Pressable accessibilityRole="button" onPress={() => setFull((value) => !value)} hitSlop={8}>
          <ThemedText type="smallBold" style={styles.link}>{full ? 'Show less' : 'Show more'}</ThemedText>
        </Pressable>
      ) : null}
    </View>
  );
}

// One thread, rendered the way mail is read: sender, subject, snippet, time.
// Tapping opens the messages themselves, which is the only way to answer the
// question the request actually asks — is any of this mail still wanted here?
export function GmailThreadRow({
  review,
  pending,
  onKeep,
  defaultOpen = false,
}: {
  review: GmailThreadReview;
  pending: boolean;
  onKeep: (review: GmailThreadReview) => void;
  // A one-thread request has nothing to scan, so it opens on arrival.
  defaultOpen?: boolean;
}) {
  const theme = useTheme();
  const [open, setOpen] = useState(defaultOpen);
  const preview = truncate(review.preview ?? '', 140);
  return (
    <View style={[styles.row, { borderBottomColor: theme.backgroundSelected }, review.removed && styles.rowKept]}>
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ expanded: open }}
        accessibilityLabel={`${review.senderName}. ${review.subject}`}
        onPress={() => setOpen((value) => !value)}
        style={({ pressed }) => [styles.rowHead, pressed && { backgroundColor: theme.backgroundElement }]}>
        {/* Email has no profile picture, but the same circle keeps the row
            aligned with the Slack review's and marks an unread thread. */}
        <Avatar name={review.senderName} size={34} highlight={review.unread} style={styles.avatar} />
        <View style={styles.rowCopy}>
          <View style={styles.rowTop}>
            <ThemedText type={review.unread ? 'smallBold' : 'small'} numberOfLines={1} style={styles.rowSender}>
              {review.senderName}
              {review.messageCount > 1 ? <ThemedText type="small" themeColor="textSecondary">{`  ${review.messageCount}`}</ThemedText> : null}
            </ThemedText>
            <ThemedText type="small" themeColor="textSecondary">{timeOfDay(review.latestAt)}</ThemedText>
          </View>
          <ThemedText
            type={review.unread ? 'smallBold' : 'small'}
            numberOfLines={2}
            style={[styles.rowSubject, review.removed && styles.struck]}>
            {review.subject}
          </ThemedText>
          {preview ? <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>{preview}</ThemedText> : null}
          {review.removed || review.unread || review.labels.length ? (
            <View style={styles.rowChips}>
              {review.removed ? <Chip label="KEPT IN INBOX" tone="danger" /> : null}
              {review.unread ? <Chip label="UNREAD" tone="accent" /> : null}
              {review.labels.map((label) => <Chip key={label} label={label} />)}
            </View>
          ) : null}
        </View>
        {review.open ? <OpenInSourceButton link={review.open} compact /> : null}
        <ThemedText themeColor="textSecondary" style={[styles.chevron, open && styles.chevronOpen]}>›</ThemedText>
      </Pressable>

      {open ? (
        <View style={styles.expanded}>
          <ThemedText type="small" themeColor="textSecondary" selectable>
            In {review.account} · thread {review.threadId || 'unknown'}
          </ThemedText>
          {review.messages.map((message, index) => (
            <ThreadMessage key={message.messageId || index} message={message} account={review.account} />
          ))}
          {review.messages.length === 0 ? (
            <ThemedText type="small" themeColor="textSecondary">
              No messages for thread {review.threadId} are in the warehouse. Open it in Gmail before approving.
            </ThemedText>
          ) : null}
          <View style={styles.rowActions}>
            {review.open ? <OpenInSourceButton link={review.open} /> : null}
            {pending && !review.removed ? (
              <Pressable accessibilityRole="button" onPress={() => onKeep(review)} style={styles.rowAction} hitSlop={6}>
                <ThemedText type="smallBold" style={styles.danger}>
                  {review.threadsInMutation > 1 ? `Keep these ${review.threadsInMutation} in the inbox` : 'Keep this in the inbox'}
                </ThemedText>
              </Pressable>
            ) : null}
          </View>
        </View>
      ) : null}
    </View>
  );
}


const GMAIL_SCOPES = [
  { key: 'all', label: 'All' },
  { key: 'unread', label: 'Unread' },
  { key: 'automated', label: 'Automated' },
  { key: 'kept', label: 'Kept' },
] as const;

export type GmailScope = (typeof GMAIL_SCOPES)[number]['key'];

// What an archive batch is really asking: which mail is in it. The headline is
// the verb and the count, the effect sentence says what approval does and what
// it does not do, and the counts underneath are the composition of the batch —
// so the answer to "is anything important in here?" is on screen before the
// first row is read.
export function GmailOverview({
  request,
  summary,
  error,
  filter,
  onFilter,
  scope,
  onScope,
  scopeCounts,
  visible,
}: {
  request: MutationRequest;
  summary: GmailBatchSummary;
  error: string | null;
  filter: string;
  onFilter: (value: string) => void;
  scope: GmailScope;
  onScope: (value: GmailScope) => void;
  scopeCounts: Record<GmailScope, number>;
  visible: number;
}) {
  const theme = useTheme();
  const [showReason, setShowReason] = useState(false);
  return (
    <View style={styles.overview}>
      <View style={[styles.hero, { backgroundColor: theme.backgroundElement }]}>
        <View style={styles.heroCopy}>
          <View style={styles.heroEyebrow}>
            <ThemedText type="small" themeColor="textSecondary">{summary.verb.toUpperCase()}</ThemedText>
            <StatusPill status={request.status} />
          </View>
          <ThemedText type="subtitle" style={styles.requestTitle}>
            {summary.verb} {summary.threadCount} thread{summary.threadCount === 1 ? '' : 's'}
          </ThemedText>
          <ThemedText type="small">{summary.effect}</ThemedText>
          <ThemedText type="small" themeColor="textSecondary">
            {summary.accounts.map((entry) => `${entry.account} (${entry.count})`).join(' · ') || request.title}
          </ThemedText>
          {request.reason ? (
            <Pressable accessibilityRole="button" onPress={() => setShowReason((value) => !value)} hitSlop={6}>
              <ThemedText type="small" themeColor="textSecondary" numberOfLines={showReason ? undefined : 2} style={styles.requestReason}>
                {request.reason}
              </ThemedText>
            </Pressable>
          ) : null}
          <ThemedText type="small" themeColor="textSecondary">
            {formatWhen(request.created_at)} · by {request.requested_by || 'unknown'}
          </ThemedText>
        </View>
      </View>

      {request.approved_by ? (
        <ThemedText type="small" themeColor="textSecondary">
          {request.status === 'rejected' ? 'Denied' : 'Approved'} by {request.approved_by}
          {request.approved_at ? ` · ${formatWhen(request.approved_at)}` : ''}
        </ThemedText>
      ) : null}
      {request.error ? <ThemedText style={styles.error}>{request.error}</ThemedText> : null}
      {error ? <ThemedText style={styles.error}>{error}</ThemedText> : null}

      <View style={styles.scopeRow}>
        {GMAIL_SCOPES.filter((option) => option.key === 'all' || scopeCounts[option.key] > 0).map((option) => {
          const active = scope === option.key;
          return (
            <Pressable
              key={option.key}
              accessibilityRole="button"
              accessibilityState={{ selected: active }}
              onPress={() => onScope(option.key)}
              style={[styles.scopeChip, { backgroundColor: theme.backgroundElement }, active && styles.scopeChipActive]}>
              <ThemedText type="smallBold" style={active ? styles.scopeChipActiveText : undefined}>
                {option.label} {scopeCounts[option.key]}
              </ThemedText>
            </Pressable>
          );
        })}
      </View>
      {/* A short batch is entirely on screen; a filter box would only cost it a row. */}
      {summary.threadCount + summary.keptCount > 8 ? (
        <TextInput
          accessibilityLabel="Filter threads"
          placeholder="Filter by sender or subject"
          placeholderTextColor={theme.textSecondary}
          value={filter}
          onChangeText={onFilter}
          autoCapitalize="none"
          autoCorrect={false}
          clearButtonMode="while-editing"
          style={[styles.filterInput, { backgroundColor: theme.backgroundElement, color: theme.text }]}
        />
      ) : null}
      {visible !== summary.threadCount ? (
        <ThemedText type="small" themeColor="textSecondary">
          Showing {visible} of {summary.threadCount}
          {summary.keptCount ? ` · ${summary.keptCount} kept in the inbox` : ''}
        </ThemedText>
      ) : null}
    </View>
  );
}


const styles = StyleSheet.create({
  overview: { padding: Spacing.three, gap: Spacing.two },
  hero: { borderRadius: 14, padding: 14, borderWidth: StyleSheet.hairlineWidth, borderColor: '#D9770644' },
  heroCopy: { flex: 1, minWidth: 0, gap: 4 },
  heroEyebrow: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 7 },
  requestTitle: { fontSize: 24, lineHeight: 30 },
  requestReason: { lineHeight: 18 },
  scopeRow: { flexDirection: 'row', flexWrap: 'wrap', gap: Spacing.two, marginTop: Spacing.one },
  scopeChip: { minHeight: 34, justifyContent: 'center', paddingHorizontal: 12, borderRadius: 17 },
  scopeChipActive: { backgroundColor: ACCENT },
  scopeChipActiveText: { color: '#FFFFFF' },
  filterInput: { minHeight: 44, borderRadius: 11, paddingHorizontal: Spacing.three, fontSize: 15 },
  error: { color: '#D0342C' },
  row: { borderBottomWidth: StyleSheet.hairlineWidth },
  rowKept: { opacity: 0.45 },
  rowHead: { flexDirection: 'row', alignItems: 'flex-start', gap: 10, minHeight: 64, paddingHorizontal: Spacing.three, paddingVertical: 10 },
  avatar: { marginTop: 2 },
  rowCopy: { flex: 1, minWidth: 0, gap: 1 },
  rowTop: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: Spacing.two },
  rowSender: { flexShrink: 1 },
  rowSubject: { fontWeight: '600' },
  struck: { textDecorationLine: 'line-through' },
  rowChips: { flexDirection: 'row', flexWrap: 'wrap', gap: 5, marginTop: 3 },
  chip: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 4, paddingHorizontal: 5, paddingVertical: 1 },
  chipText: { fontSize: 9, fontWeight: '700', letterSpacing: 0.6 },
  chevron: { fontSize: 20, lineHeight: 22, marginTop: 8 },
  chevronOpen: { transform: [{ rotate: '90deg' }], color: ACCENT },
  expanded: { gap: Spacing.two, paddingHorizontal: Spacing.three, paddingBottom: Spacing.three },
  message: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 10, padding: 10, gap: 3 },
  messageHead: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: Spacing.two },
  messageSender: { flexShrink: 1 },
  messageBody: { marginTop: 2 },
  rowActions: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: Spacing.three, paddingTop: Spacing.one },
  rowAction: { minHeight: 32, justifyContent: 'center' },
  link: { color: '#3c87f7' },
  danger: { color: DANGER },
});
