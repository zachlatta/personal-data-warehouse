type MutationLike = {
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
  threadTs: string;
  messages: SlackReviewMessage[];
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

  return {
    conversationLabel: text(preview.conversation_name) || conversationId,
    conversationId,
    messageTs,
    account: text(mutation.account),
    effect: text(preview.effect) || 'Everything in this conversation through the highlighted message will be marked read.',
    boundaryNote: 'Messages after the boundary stay unread.',
    currentUnreadCount: count(preview.current_unread_count),
    currentLastRead: text(preview.current_last_read),
    contextLabel: contextKind === 'thread' ? 'Thread context' : 'Conversation context',
    threadTs: text(preview.thread_ts),
    messages,
  };
}
