import test from 'node:test';
import assert from 'node:assert/strict';

import { isSlackMarkReadMutation, slackMarkReadReview } from './mutation-review.ts';

test('Slack mark-read review explains the whole-conversation boundary', () => {
  const review = slackMarkReadReview({
    provider: 'slack',
    operation: 'slack.mark_conversation_read',
    account: 'hackclub',
    payload: { conversation_id: 'D1', message_ts: '1593473566.000200' },
    preview: {},
  });

  assert.equal(review.conversationLabel, 'D1');
  assert.equal(review.effect, 'Everything in this conversation through the highlighted message will be marked read.');
  assert.equal(review.boundaryNote, 'Messages after the boundary stay unread.');
  assert.equal(review.conversationId, 'D1');
  assert.equal(review.messageTs, '1593473566.000200');
});

test('Slack mark-read review exposes Marcus and surrounding messages in order', () => {
  const mutation = {
    provider: 'slack',
    operation: 'slack.mark_conversation_read',
    account: 'hackclub',
    payload: { conversation_id: 'D1', message_ts: '1593473566.000200' },
    preview: {
      slack_read: {
        conversation_name: 'Marcus',
        conversation_type: 'im',
        current_unread_count: 3,
        current_last_read: '1593473500.000100',
        context_kind: 'conversation',
        messages: [
          { message_ts: '1593473500.000100', sent_at: '2026-08-29T14:00:00Z', actor_name: 'You', text: 'Did you see this?', is_from_me: true, position: 'before' },
          { message_ts: '1593473566.000200', sent_at: '2026-08-29T14:01:00Z', actor_name: 'Marcus', text: 'Yep — all handled.', is_target: true, position: 'target' },
          { message_ts: '1593473600.000300', sent_at: '2026-08-29T14:02:00Z', actor_name: 'Marcus', text: 'One more thing.', position: 'after' },
        ],
      },
    },
  };

  assert.equal(isSlackMarkReadMutation(mutation), true);
  const review = slackMarkReadReview(mutation);
  assert.equal(review.conversationLabel, 'Marcus');
  assert.equal(review.currentUnreadCount, 3);
  assert.equal(review.contextLabel, 'Conversation context');
  assert.deepEqual(
    review.messages.map((message) => [message.actorName, message.text, message.position]),
    [
      ['You', 'Did you see this?', 'before'],
      ['Marcus', 'Yep — all handled.', 'target'],
      ['Marcus', 'One more thing.', 'after'],
    ],
  );
  assert.equal(review.messages[1].isTarget, true);
  assert.equal(review.messages[2].isAfterBoundary, true);
  assert.equal(isSlackMarkReadMutation({ provider: 'slack', operation: 'slack.send_message' }), false);
});

