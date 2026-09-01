import type { TimelineDeepLink } from './api';

type MutationLike = {
  id?: string;
  provider?: string;
  operation?: string;
  account?: string;
  status?: string;
  payload?: Record<string, unknown>;
  preview?: Record<string, unknown>;
};

export type SlackReviewMessage = {
  messageTs: string;
  sentAt: string;
  actorName: string;
  text: string;
  position: 'before' | 'target' | 'after';
  isTarget: boolean;
  isAfterBoundary: boolean;
  isFromMe: boolean;
  avatarUrl: string;
  // Where this exact message lives in Slack. Same shape as a timeline row's
  // `open`, so the app opens it with the helper it already has.
  open: TimelineDeepLink | null;
};

export type SlackMarkReadReview = {
  conversationLabel: string;
  conversationId: string;
  messageTs: string;
  account: string;
  effect: string;
  boundaryNote: string;
  currentUnreadCount: number;
  currentLastRead: string;
  contextLabel: string;
  conversationType: string;
  threadTs: string;
  avatarUrl: string;
  open: TimelineDeepLink | null;
  messages: SlackReviewMessage[];
  targetMessage: SlackReviewMessage | null;
};

export type SlackReviewGroup = {
  key: string;
  label: string;
  description: string;
  icon: string;
  types: string[];
  items: { mutation: MutationLike; review: SlackMarkReadReview }[];
};

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function text(value: unknown): string {
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  return '';
}

function count(value: unknown): number {
  const parsed = typeof value === 'number' ? value : Number.parseInt(text(value), 10);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : 0;
}

// A link is only usable when it carries an https url; app_url alone would
// leave a phone without Slack installed with nothing to fall back to.
function deepLink(value: unknown): TimelineDeepLink | null {
  const link = asRecord(value);
  const url = text(link.url);
  if (!url) return null;
  const appURL = text(link.app_url);
  return { url, label: text(link.label) || 'Slack', ...(appURL ? { app_url: appURL } : {}) };
}

export function isSlackMarkReadMutation(mutation: MutationLike): boolean {
  return mutation.provider === 'slack' && mutation.operation === 'slack.mark_conversation_read';
}

export function slackMarkReadReview(mutation: MutationLike): SlackMarkReadReview {
  const payload = asRecord(mutation.payload);
  const preview = asRecord(asRecord(mutation.preview).slack_read);
  const conversationId = text(preview.conversation_id) || text(payload.conversation_id);
  const messageTs = text(preview.message_ts) || text(payload.message_ts);
  const contextKind = text(preview.context_kind) || 'conversation';
  const conversationType = text(preview.conversation_type);
  let conversationLabel = text(preview.conversation_name) || conversationId;
  if ((conversationType === 'public_channel' || conversationType === 'private_channel') && conversationLabel && !conversationLabel.startsWith('#')) {
    conversationLabel = `#${conversationLabel}`;
  }
  const messages = Array.isArray(preview.messages)
    ? preview.messages.map((raw): SlackReviewMessage => {
        const message = asRecord(raw);
        const rawPosition = text(message.position);
        const isTarget = message.is_target === true || rawPosition === 'target' || text(message.message_ts) === messageTs;
        const isAfterBoundary = !isTarget && rawPosition === 'after';
        return {
          messageTs: text(message.message_ts),
          sentAt: text(message.sent_at),
          actorName: message.is_from_me === true ? 'You' : text(message.actor_name) || 'Unknown',
          text: text(message.text) || '(no text)',
          position: isTarget ? 'target' : isAfterBoundary ? 'after' : 'before',
          isTarget,
          isAfterBoundary,
          isFromMe: message.is_from_me === true,
          avatarUrl: text(message.avatar_url),
          open: deepLink(message.open),
        };
      })
    : [];
  const targetMessage = messages.find((message) => message.isTarget) ?? null;
  if (conversationType === 'im' && /^U[A-Z0-9]+$/i.test(conversationLabel) && targetMessage && !targetMessage.isFromMe && targetMessage.actorName !== 'Unknown') {
    conversationLabel = targetMessage.actorName;
  } else if (conversationType === 'mpim' && conversationLabel.startsWith('mpdm-')) {
    const rawMessages = Array.isArray(preview.messages) ? preview.messages.map(asRecord) : [];
    const selfNames = new Set(rawMessages.filter((message) => message.is_from_me === true).flatMap((message) => {
      const name = text(message.actor_name).trim().replace(/\s+/g, ' ').toLowerCase();
      return name ? [name, name.replaceAll(' ', '.'), name.replaceAll(' ', '')] : [];
    }));
    const participants = conversationLabel.slice(5).replace(/-\d+$/, '').split('--')
      .filter((name) => name && !selfNames.has(name.toLowerCase()))
      .map((name) => name.replaceAll('.', ' '));
    if (participants.length) conversationLabel = participants.join(', ');
  }

  return {
    conversationLabel,
    conversationId,
    messageTs,
    account: text(mutation.account),
    effect: text(preview.effect) || 'Everything in this conversation through the highlighted message will be marked read.',
    boundaryNote: 'Messages after the boundary stay unread.',
    currentUnreadCount: count(preview.current_unread_count),
    currentLastRead: text(preview.current_last_read),
    contextLabel: contextKind === 'thread' ? 'Thread context' : 'Conversation context',
    conversationType,
    threadTs: text(preview.thread_ts),
    avatarUrl: text(preview.avatar_url) || targetMessage?.avatarUrl || '',
    open: deepLink(preview.open) ?? targetMessage?.open ?? null,
    messages,
    targetMessage,
  };
}

const SLACK_REVIEW_GROUPS = [
  { key: 'direct', label: 'Direct messages', description: 'One-to-one conversations', icon: '@', types: ['im'] },
  { key: 'group', label: 'Group DMs', description: 'Small-group conversations', icon: '◎', types: ['mpim'] },
  { key: 'private', label: 'Private channels', description: 'Private workspace channels', icon: '◈', types: ['private_channel'] },
  { key: 'public', label: 'Public channels', description: 'Public workspace channels', icon: '#', types: ['public_channel'] },
  { key: 'other', label: 'Other conversations', description: 'Slack conversations', icon: '•', types: [] as string[] },
];

export function slackMarkReadGroups(mutations: MutationLike[]): SlackReviewGroup[] {
  const groups: SlackReviewGroup[] = SLACK_REVIEW_GROUPS.map((definition) => ({ ...definition, items: [] }));
  for (const mutation of mutations ?? []) {
    if (!isSlackMarkReadMutation(mutation)) continue;
    const review = slackMarkReadReview(mutation);
    const group = groups.find((candidate) => candidate.types.includes(review.conversationType)) ?? groups[groups.length - 1];
    group.items.push({ mutation, review });
  }
  return groups.filter((group) => group.items.length > 0);
}

const REVIEW_COUNT_DEFINITIONS = [
  ['generic_channel', 'Generic channels', '#'],
  ['automated_dm', 'Automated DMs', '⚙'],
  ['terminal_direct', 'Direct acknowledgements', '@'],
  ['terminal_group', 'Group acknowledgements', '◎'],
] as const;

export type MutationReviewContext = {
  total: number;
  source: string;
  snapshotAt: string;
  counts: { key: string; label: string; icon: string; count: number }[];
  preserved: string[];
  selection: string[];
};

function stringList(value: unknown): string[] {
  return Array.isArray(value) ? value.map(text).filter(Boolean) : [];
}

export function mutationReviewContext(value: unknown): MutationReviewContext {
  const context = asRecord(value);
  const rawCounts = asRecord(context.candidate_counts);
  const claimed = new Set<string>();
  const counts: MutationReviewContext['counts'] = [];
  for (const [key, label, icon] of REVIEW_COUNT_DEFINITIONS) {
    if (!(key in rawCounts)) continue;
    claimed.add(key);
    counts.push({ key, label, icon, count: count(rawCounts[key]) });
  }
  for (const key of Object.keys(rawCounts).sort()) {
    if (claimed.has(key)) continue;
    const label = key.replaceAll('_', ' ').replace(/^./, (character) => character.toUpperCase());
    counts.push({ key, label, icon: '•', count: count(rawCounts[key]) });
  }
  return {
    total: counts.reduce((sum, item) => sum + item.count, 0),
    source: text(context.source),
    snapshotAt: text(context.snapshot_utc),
    counts,
    preserved: stringList(context.preserved),
    selection: stringList(context.selection),
  };
}

// --- gmail thread review ----------------------------------------------------
//
// A Gmail archive/label request is a batch of threads, and the phone used to
// render each one as its raw payload — a thread id and a label id, in pretty
// JSON. Nothing about "archive 43 threads" is reviewable that way: the whole
// question is which mail is in the batch. The app's own API already carries
// the answer in `preview.threads` (subject, sender, snippet, per-message
// bodies), so this turns that into rows an inbox reader recognizes.

export const GMAIL_THREAD_OPERATIONS = ['gmail.archive_threads', 'gmail.unarchive_threads', 'gmail.modify_thread_labels'];

export function isGmailThreadMutation(mutation: MutationLike): boolean {
  return GMAIL_THREAD_OPERATIONS.includes(text(mutation.operation));
}

export type GmailReviewMessage = {
  messageId: string;
  senderName: string;
  senderAddress: string;
  to: string[];
  cc: string[];
  sentAt: string;
  text: string;
  unread: boolean;
};

export type GmailThreadReview = {
  key: string;
  mutationId: string;
  mutationStatus: string;
  threadsInMutation: number;
  account: string;
  threadId: string;
  subject: string;
  senderName: string;
  senderAddress: string;
  preview: string;
  latestAt: string;
  messageCount: number;
  labels: string[];
  unread: boolean;
  automated: boolean;
  removed: boolean;
  open: TimelineDeepLink | null;
  messages: GmailReviewMessage[];
};

function list(value: unknown): string[] {
  return Array.isArray(value) ? value.map(text).filter(Boolean) : [];
}

function records(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.map(asRecord) : [];
}

// Every thread in an archive batch carries Inbox, the row draws its own unread
// marker, and the rest are Gmail's private bookkeeping: a chip repeating any of
// them on all 43 rows is noise. The comparison is case-insensitive because the
// app hands back labels the warehouse has already formatted ("Unread") beside
// ones it has not ("UNREAD").
const HIDDEN_GMAIL_LABELS = new Set(['INBOX', 'TRASH', 'SPAM', 'CATEGORY_PERSONAL', 'UNREAD']);
const NAMED_GMAIL_LABELS: Record<string, string> = {
  IMPORTANT: 'Important',
  STARRED: 'Starred',
  SENT: 'Sent',
  CATEGORY_UPDATES: 'Updates',
  CATEGORY_PROMOTIONS: 'Promotions',
  CATEGORY_SOCIAL: 'Social',
  CATEGORY_FORUMS: 'Forums',
};

export function formatGmailLabel(value: unknown): string {
  const label = text(value);
  // Label_29 is a user label's opaque id, which says nothing to a reader.
  if (!label || label.startsWith('Label_') || HIDDEN_GMAIL_LABELS.has(label.toUpperCase())) return '';
  return NAMED_GMAIL_LABELS[label.toUpperCase()] ?? label.replace(/^CATEGORY_/, '').replace(/_/g, ' ');
}

function hasUnreadLabel(labels: string[]): boolean {
  return labels.some((label) => label.trim().replace(/ /g, '_').toUpperCase() === 'UNREAD');
}

// A local part that no person answers from. The list is deliberately short and
// literal: it feeds a count and an opt-in filter chip, never a hidden regroup,
// so a miss costs a wrong number rather than a buried email.
const AUTOMATED_LOCALS = new Set([
  'no-reply', 'noreply', 'no_reply', 'donotreply', 'do-not-reply', 'notification', 'notifications', 'notify',
  'alert', 'alerts', 'news', 'newsletter', 'mail', 'mailer', 'mailer-daemon', 'postmaster', 'bounce', 'bounces',
  'info', 'support', 'help', 'billing', 'invoice', 'invoices', 'receipt', 'receipts', 'updates', 'update',
  'hello', 'team', 'marketing', 'automated', 'auto', 'robot', 'bot', 'system', 'service', 'admin', 'root',
]);

function addressLocalPart(address: string): string {
  const at = address.indexOf('@');
  return (at >= 0 ? address.slice(0, at) : address).toLowerCase();
}

export function looksAutomatedSender(address: string, senderName: string, labels: string[]): boolean {
  if (labels.includes('Promotions')) return true;
  // "someone via Slack", "… via Google Docs": a relay, not the person.
  if (/\bvia\s+\S+$/i.test(senderName)) return true;
  const local = addressLocalPart(address);
  if (AUTOMATED_LOCALS.has(local)) return true;
  return /^(no-?reply|do-?not-?reply|notifications?|mailer|bounce|auto)[._-]/.test(local);
}

function titleCase(value: string): string {
  return value.split(/[._-]+/).filter(Boolean).map((part) => part[0].toUpperCase() + part.slice(1).toLowerCase()).join(' ');
}

// The name to show for a sender: Gmail's own display name when the warehouse
// carried it, then a "Name <address>" header, then the address, made readable.
export function gmailSenderName(fromName: unknown, fromAddress: unknown, subject = ''): string {
  const name = text(fromName);
  if (name) return name;
  const from = text(fromAddress);
  const angle = from.indexOf('<');
  if (angle >= 0) {
    const inline = from.slice(0, angle).trim().replace(/^"+|"+$/g, '');
    if (inline) return inline;
  }
  const address = from.replace(/^[<>]+|[<>]+$/g, '');
  const at = address.indexOf('@');
  if (at < 0) return address || subject || 'Unknown sender';
  const local = address.slice(0, at);
  const domain = address.slice(at + 1);
  if (!AUTOMATED_LOCALS.has(local.toLowerCase())) return titleCase(local);
  // A bulk sender's local part says nothing, so fall back to the registrable
  // part of the domain — "updates.brand.co.uk" reads as Brand, not Updates.
  const parts = domain.split('.').filter(Boolean);
  const registrable = parts.length > 2 && parts[parts.length - 2].length <= 3 ? parts[parts.length - 3] : parts[parts.length - 2];
  return titleCase(registrable || domain || local);
}

function gmailReviewMessage(raw: unknown): GmailReviewMessage {
  const message = asRecord(raw);
  const labels = list(message.label_ids);
  const address = text(message.from_address);
  return {
    messageId: text(message.message_id),
    senderName: gmailSenderName(message.from_name, address),
    senderAddress: address,
    to: list(message.to_addresses),
    cc: list(message.cc_addresses),
    sentAt: text(message.internal_date),
    text: text(message.preview_text) || text(message.snippet),
    unread: hasUnreadLabel(labels),
  };
}

export function gmailThreadReviews(mutations: MutationLike[]): GmailThreadReview[] {
  const reviews: GmailThreadReview[] = [];
  for (const mutation of mutations ?? []) {
    if (!isGmailThreadMutation(mutation)) continue;
    const preview = asRecord(mutation.preview);
    const payloadThreadIDs = list(asRecord(mutation.payload).thread_ids);
    let threads = records(preview.threads);
    // A request that predates thread previews, or a thread whose messages have
    // left the warehouse, still has to render as a row rather than vanish.
    if (threads.length === 0) threads = payloadThreadIDs.map((thread_id) => ({ thread_id }));
    for (const thread of threads) {
      const rawLabels = list(thread.labels);
      const labels = rawLabels.map(formatGmailLabel).filter(Boolean);
      const messages = records(thread.messages);
      const messageLabels = messages.flatMap((message) => list(message.label_ids));
      const address = text(thread.latest_from_address);
      const senderName = gmailSenderName(thread.latest_from_name, address, text(thread.subject));
      const threadId = text(thread.thread_id);
      const url = gmailThreadUrl(text(mutation.account), threadId);
      reviews.push({
        key: `${text(mutation.id)}:${threadId}`,
        mutationId: text(mutation.id),
        mutationStatus: text(mutation.status) || 'pending_review',
        threadsInMutation: Math.max(threads.length, 1),
        account: text(mutation.account),
        threadId,
        subject: text(thread.subject) || '(no subject)',
        senderName,
        senderAddress: address,
        preview: text(thread.latest_preview),
        latestAt: text(thread.latest_at),
        messageCount: count(thread.message_count) || messages.length || 1,
        labels,
        unread: hasUnreadLabel(rawLabels) || hasUnreadLabel(messageLabels),
        automated: looksAutomatedSender(address, senderName, labels),
        removed: text(mutation.status) === 'rejected' || text(mutation.status) === 'removed' || text(mutation.status) === 'skipped',
        open: url ? { url, label: 'Gmail' } : null,
        messages: messages.map(gmailReviewMessage),
      });
    }
  }
  return reviews.sort((a, b) => (a.latestAt < b.latestAt ? 1 : a.latestAt > b.latestAt ? -1 : 0));
}

export type GmailBatchSummary = {
  verb: string;
  effect: string;
  threadCount: number;
  keptCount: number;
  automatedCount: number;
  unreadCount: number;
  accounts: { account: string; count: number }[];
};

export function gmailBatchSummary(mutations: MutationLike[], reviews: GmailThreadReview[]): GmailBatchSummary {
  const operation = text((mutations ?? []).find(isGmailThreadMutation)?.operation);
  const live = reviews.filter((review) => !review.removed);
  const accounts: { account: string; count: number }[] = [];
  for (const review of live) {
    const found = accounts.find((entry) => entry.account === review.account);
    if (found) found.count += 1;
    else accounts.push({ account: review.account, count: 1 });
  }
  const noun = live.length === 1 ? 'this thread' : 'these threads';
  const verb = operation === 'gmail.unarchive_threads' ? 'Unarchive' : operation === 'gmail.modify_thread_labels' ? 'Relabel' : 'Archive';
  const effect = operation === 'gmail.unarchive_threads'
    ? `Puts ${noun} back in the Inbox.`
    : operation === 'gmail.modify_thread_labels'
      ? `Changes the labels on ${noun}. Nothing leaves the Inbox.`
      : `Takes ${noun} out of the Inbox. Nothing is deleted, and search still finds them.`;
  return {
    verb,
    effect,
    threadCount: live.length,
    keptCount: reviews.length - live.length,
    automatedCount: live.filter((review) => review.automated).length,
    unreadCount: live.filter((review) => review.unread).length,
    accounts,
  };
}

// Threads group by the day they last moved, newest first — the order mail is
// read in, and the one grouping that invents nothing.
export function gmailThreadDayGroups(reviews: GmailThreadReview[], now = new Date()): { key: string; label: string; data: GmailThreadReview[] }[] {
  const groups: { key: string; label: string; data: GmailThreadReview[] }[] = [];
  for (const review of reviews) {
    // Keyed by the LOCAL day, not the UTC prefix of the timestamp: an 02:12Z
    // message is the previous evening in New York, and keying on the string
    // splits one day into two sections that then carry the same label.
    const key = localDayKey(review.latestAt);
    let group = groups.find((candidate) => candidate.key === key);
    if (!group) {
      group = { key, label: reviewDayLabel(review.latestAt, now), data: [] };
      groups.push(group);
    }
    group.data.push(review);
  }
  return groups;
}

export function reviewDayLabel(iso: string, now = new Date()): string {
  const date = new Date(iso);
  if (!iso || Number.isNaN(date.getTime())) return 'Unknown date';
  const days = Math.round((startOfDay(now).getTime() - startOfDay(date).getTime()) / 86400000);
  if (days === 0) return 'Today';
  if (days === 1) return 'Yesterday';
  if (days > 1 && days < 7) return date.toLocaleDateString(undefined, { weekday: 'long' });
  return date.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' });
}

function startOfDay(date: Date): Date {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function localDayKey(iso: string): string {
  const date = new Date(iso);
  if (!iso || Number.isNaN(date.getTime())) return 'unknown';
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

// The Gmail thread the row is about, in the account it belongs to. Gmail's own
// /u/?authuser= form addresses the mailbox by address rather than by the
// profile index, which is what makes a link correct on a phone signed in to
// several accounts.
export function gmailThreadUrl(account: string, threadId: string): string {
  const id = text(threadId);
  if (!id) return '';
  const mailbox = text(account);
  const prefix = mailbox ? `https://mail.google.com/mail/u/?authuser=${encodeURIComponent(mailbox)}` : 'https://mail.google.com/mail/u/0';
  return `${prefix}#all/${encodeURIComponent(id)}`;
}
