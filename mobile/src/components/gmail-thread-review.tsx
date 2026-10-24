import { useState } from 'react';
import { Pressable, StyleSheet, TextInput, View } from 'react-native';

import { Avatar } from '@/components/avatar';
import { EmailHtmlBody } from '@/components/email-html-body';
import { OpenInSourceButton } from '@/components/open-in-source-button';
import { StatusPill } from '@/components/status-pill';
import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { MutationRequest } from '@/lib/api';
import { emailBodyForDisplay } from '@/lib/email-html';
import { cleanSnippet, formatWhen, truncate } from '@/lib/format';
import { gmailThreadToggle, type GmailBatchSummary, type GmailThreadReview } from '@/lib/mutation-review';

const ACCENT = '#36A0DA';
const DANGER = '#E5636D';

function timeOfDay(iso: string): string {
  const date = new Date(iso);
  if (!iso || Number.isNaN(date.getTime())) return '';
  return date.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' });
}

function Chip({ label, tone = 'muted' }: { label: string; tone?: 'muted' | 'accent' | 'danger' }) {
  const color = tone === 'accent' ? ACCENT : tone === 'danger' ? DANGER : '#8A8784';
  return (
    <View style={[styles.chip, { borderColor: `${color}66` }]}>
      <ThemedText style={[styles.chipText, { color }]}>{label}</ThemedText>
    </View>
  );
}

function ThreadMessage({ message, account, defaultOpen }: { message: GmailThreadReview['messages'][number]; account: string; defaultOpen: boolean }) {
  const theme = useTheme();
  const [open, setOpen] = useState(defaultOpen);
  const [details, setDetails] = useState(false);
  const body = emailBodyForDisplay(message);
  const text = body.kind === 'text' ? (message.hasFullBody ? body.text : cleanSnippet(body.text).trim()) : '';
  const line = cleanSnippet(message.preview || (body.kind === 'text' ? body.text : '')).trim();
  return (
    <View style={[styles.message, { borderColor: theme.backgroundSelected }]}>
      <Pressable accessibilityRole="button" accessibilityState={{ expanded: open }}
        accessibilityLabel={`${open ? 'Collapse' : 'Expand'} message from ${message.senderName}`}
        onPress={() => setOpen((value) => !value)} style={styles.messageHead}>
        <Avatar name={message.senderName} size={32} />
        <View style={styles.rowCopy}>
          <ThemedText type="smallBold">{message.senderName}</ThemedText>
          <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>
            {open ? `to ${message.to.join(', ') || account}` : truncate(line, 100)}
          </ThemedText>
        </View>
        <ThemedText type="small" themeColor="textSecondary">{timeOfDay(message.sentAt)}</ThemedText>
        <ThemedText themeColor="textSecondary">{open ? '⌃' : '⌄'}</ThemedText>
      </Pressable>
      {open ? <>
        <Pressable accessibilityRole="button" accessibilityState={{ expanded: details }} onPress={() => setDetails((value) => !value)} style={styles.detailsButton}>
          <ThemedText type="small" style={styles.link}>{details ? 'Hide details' : 'Show sender & recipients'}</ThemedText>
        </Pressable>
        {details ? <View style={[styles.messageDetails, { backgroundColor: theme.backgroundElement }]}>
          <ThemedText type="small" selectable>From: {message.senderAddress || message.senderName}</ThemedText>
          <ThemedText type="small" selectable>To: {message.to.join(', ') || account}</ThemedText>
          {message.cc.length ? <ThemedText type="small" selectable>Cc: {message.cc.join(', ')}</ThemedText> : null}
          <ThemedText type="small" selectable>{formatWhen(message.sentAt)}</ThemedText>
        </View> : null}
        {body.kind === 'html' ? <EmailHtmlBody html={body.html} /> : null}
        {text ? <ThemedText selectable style={styles.messageBody}>{text}</ThemedText> : null}
        {!message.hasFullBody ? <ThemedText type="small" themeColor="textSecondary">
          {body.kind !== 'none' ? 'Only a preview is available.' : 'This message’s body is not in the warehouse.'} Open in Superhuman to read the full message before approving.
        </ThemedText> : null}
      </> : null}
    </View>
  );
}

// The thread's place in the batch, as a control: "Archive" while it is in,
// "Keep in inbox" once it is out, one tap either way.
function ThreadToggle({ review, pending, onToggle }: { review: GmailThreadReview; pending: boolean; onToggle: (review: GmailThreadReview) => void }) {
  const toggle = gmailThreadToggle(review, pending);
  if (toggle.state === 'static') {
    return <ThemedText type="small" style={review.removed ? styles.danger : styles.link}>{toggle.label}</ThemedText>;
  }
  const included = toggle.state === 'included';
  return (
    <Pressable
      accessibilityRole="switch"
      accessibilityState={{ checked: included }}
      accessibilityLabel={toggle.accessibilityLabel}
      onPress={() => onToggle(review)}
      hitSlop={{ top: 10, bottom: 10, left: 8, right: 12 }}
      style={({ pressed }) => [styles.toggle, included ? styles.toggleOn : styles.toggleOff, pressed && styles.pressed]}>
      <View style={[styles.toggleBox, included ? styles.toggleBoxOn : styles.toggleBoxOff]}>
        {included ? <ThemedText style={styles.toggleTick}>✓</ThemedText> : null}
      </View>
      <ThemedText type="smallBold" style={included ? styles.toggleOnText : styles.toggleOffText}>{toggle.label}</ThemedText>
    </Pressable>
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
    <View style={[styles.row, { borderBottomColor: theme.backgroundSelected }]}>
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ expanded: open }}
        accessibilityLabel={`${review.senderName}. ${review.subject}`}
        onPress={() => setOpen((value) => !value)}
        style={({ pressed }) => [styles.rowHead, pressed && { backgroundColor: theme.backgroundElement }]}>
        {/* Email has no profile picture, but the same circle keeps the row
            aligned with the Slack review's and marks an unread thread. */}
        <Avatar name={review.senderName} size={40} highlight={review.unread} style={[styles.avatar, review.removed && styles.dim]} />
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
            numberOfLines={open ? undefined : 1}
            style={[styles.rowSubject, review.removed && styles.dim]}>
            {review.subject}
          </ThemedText>
          {preview ? <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>{preview}</ThemedText> : null}
        </View>
        <ThemedText themeColor="textSecondary" style={[styles.chevron, open && styles.chevronOpen]}>›</ThemedText>
      </Pressable>
      {/* Outside the row's button: nested in it, VoiceOver folds the toggle
          into the row and it cannot be reached on its own. */}
      <View style={styles.rowFoot}>
        <ThreadToggle review={review} pending={pending} onToggle={onKeep} />
        {review.unread ? <Chip label="UNREAD" tone="accent" /> : null}
        {review.labels.map((label) => <Chip key={label} label={label} />)}
      </View>

      {open ? (
        <View style={styles.expanded}>
          <ThemedText type="small" themeColor="textSecondary" selectable>
            {review.account}
          </ThemedText>
          {review.messages.map((message, index) => (
            <ThreadMessage key={message.messageId || index} message={message} account={review.account} defaultOpen={index === review.messages.length - 1} />
          ))}
          {review.messageCount > review.messages.length && review.messages.length > 0 ? (
            <ThemedText type="small" themeColor="textSecondary">Showing {review.messages.length} of {review.messageCount} messages. Open in Superhuman for the rest.</ThemedText>
          ) : null}
          {review.messages.length === 0 ? (
            <ThemedText type="small" themeColor="textSecondary">
              No messages for thread {review.threadId} are in the warehouse. Open it in Superhuman before approving.
            </ThemedText>
          ) : null}
          {review.error ? <ThemedText style={styles.error}>{review.error}</ThemedText> : null}
          <View style={styles.rowActions}>
            {review.open ? <OpenInSourceButton link={review.open} /> : null}
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
  { key: 'kept', label: 'Skipped' },
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
  otherActionCount = 0,
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
  otherActionCount?: number;
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
          {otherActionCount ? <ThemedText type="smallBold">Also includes {otherActionCount} other actions below. Approve applies to the whole request.</ThemedText> : null}
          <ThemedText type="small" themeColor="textSecondary">
            {summary.accounts.map((entry) => `${entry.account} (${entry.count})`).join(' · ') || request.title}
          </ThemedText>
          {request.reason ? (
            <Pressable accessibilityRole="button" onPress={() => setShowReason((value) => !value)} hitSlop={6}>
              <ThemedText type="small" style={styles.link}>{showReason ? 'Hide request details' : 'Why these threads?'}</ThemedText>
              {showReason ? <ThemedText type="small" themeColor="textSecondary" style={styles.requestReason}>{request.title}{'\n'}{request.reason}</ThemedText> : null}
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

      {/* One thread has nothing to filter: an "All 1" chip is only noise. */}
      {summary.threadCount + summary.keptCount > 1 ? <View style={styles.scopeRow}>
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
      </View> : null}
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
      {visible !== summary.threadCount + summary.keptCount ? (
        <ThemedText type="small" themeColor="textSecondary">
          Showing {visible} of {summary.threadCount + summary.keptCount}
          {summary.keptCount ? ` · ${summary.keptCount} kept out of this request` : ''}
        </ThemedText>
      ) : null}
    </View>
  );
}


const styles = StyleSheet.create({
  overview: { padding: Spacing.three, gap: Spacing.two },
  hero: { borderRadius: 14, padding: 14, borderWidth: StyleSheet.hairlineWidth, borderColor: '#36A0DA33' },
  heroCopy: { flex: 1, minWidth: 0, gap: 4 },
  heroEyebrow: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 7 },
  requestTitle: { fontSize: 24, lineHeight: 30 },
  requestReason: { lineHeight: 18 },
  scopeRow: { flexDirection: 'row', flexWrap: 'wrap', gap: Spacing.two, marginTop: Spacing.one },
  scopeChip: { minHeight: 34, justifyContent: 'center', paddingHorizontal: 12, borderRadius: 17 },
  scopeChipActive: { backgroundColor: ACCENT },
  scopeChipActiveText: { color: '#FFFFFF' },
  filterInput: { minHeight: 44, borderRadius: 11, paddingHorizontal: Spacing.three, fontSize: 15 },
  error: { color: '#E5636D' },
  row: { borderBottomWidth: StyleSheet.hairlineWidth },
  dim: { opacity: 0.5 },
  rowHead: { flexDirection: 'row', alignItems: 'flex-start', gap: 10, minHeight: 64, paddingHorizontal: Spacing.three, paddingVertical: 10 },
  avatar: { marginTop: 2 },
  rowCopy: { flex: 1, minWidth: 0, gap: 1 },
  rowTop: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: Spacing.two },
  rowSender: { flexShrink: 1 },
  rowSubject: { fontSize: 15, lineHeight: 21 },
  // Aligned under the row's text: padding (16) + avatar (40) + gap (10).
  rowFoot: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 6, marginTop: -4, paddingLeft: 66, paddingRight: Spacing.three, paddingBottom: 10 },
  toggle: { flexDirection: 'row', alignItems: 'center', gap: 6, minHeight: 30, paddingLeft: 6, paddingRight: 10, borderRadius: 15, borderWidth: 1 },
  toggleOn: { borderColor: '#36A0DA', backgroundColor: '#36A0DA1A' },
  toggleOff: { borderColor: '#E0A05C', backgroundColor: '#E0A05C1F' },
  toggleBox: { width: 18, height: 18, borderRadius: 9, alignItems: 'center', justifyContent: 'center' },
  toggleBoxOn: { backgroundColor: '#36A0DA' },
  toggleBoxOff: { borderWidth: 1.5, borderColor: '#E0A05C' },
  toggleTick: { color: '#FFFFFF', fontSize: 12, lineHeight: 14, fontWeight: '800' },
  toggleOnText: { color: '#36A0DA' },
  toggleOffText: { color: '#E0A05C' },
  pressed: { opacity: 0.6 },
  chip: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 4, paddingHorizontal: 5, paddingVertical: 1 },
  chipText: { fontSize: 9, fontWeight: '700', letterSpacing: 0.6 },
  chevron: { fontSize: 20, lineHeight: 22, marginTop: 8 },
  chevronOpen: { transform: [{ rotate: '90deg' }], color: ACCENT },
  expanded: { gap: Spacing.two, paddingHorizontal: Spacing.three, paddingBottom: Spacing.three },
  message: { borderTopWidth: StyleSheet.hairlineWidth, paddingVertical: 12, gap: 6 },
  messageHead: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: Spacing.two },
  messageBody: { marginTop: 8, fontSize: 16, lineHeight: 24 },
  detailsButton: { minHeight: 44, justifyContent: 'center', alignSelf: 'flex-start' },
  messageDetails: { padding: 12, borderRadius: 8, gap: 6 },
  rowActions: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: Spacing.three, paddingTop: Spacing.one },
  rowAction: { minHeight: 44, justifyContent: 'center' },
  link: { color: '#36A0DA' },
  danger: { color: DANGER },
});
