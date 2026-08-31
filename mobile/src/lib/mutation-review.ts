type MutationLike = {
  id?: string;
  provider?: string;
  operation?: string;
  account?: string;
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
