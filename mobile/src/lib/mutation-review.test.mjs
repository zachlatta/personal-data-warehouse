import test from 'node:test';
import assert from 'node:assert/strict';

import { isSlackMarkReadMutation, mutationReviewContext, slackMarkReadGroups, slackMarkReadReview } from './mutation-review.ts';

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

test('mobile Slack review replaces raw DM and group-DM slugs with readable names', () => {
  assert.equal(slackMarkReadReview({ provider: 'slack', operation: 'slack.mark_conversation_read', preview: { slack_read: {
    conversation_type: 'im', conversation_name: 'U012345', messages: [{ actor_name: 'Grace Hopper', text: 'done', is_target: true }],
  } } }).conversationLabel, 'Grace Hopper');
  assert.equal(slackMarkReadReview({ provider: 'slack', operation: 'slack.mark_conversation_read', preview: { slack_read: {
    conversation_type: 'mpim', conversation_name: 'mpdm-review.owner--ada.lovelace--gracehopper-1',
    messages: [{ actor_name: 'Review Owner', text: 'thanks', is_from_me: true }],
  } } }).conversationLabel, 'ada lovelace, gracehopper');
});

test('mobile review groups Slack batches and keeps the target preview on each compact row', () => {
  const mutation = (id, conversation_type, conversation_name, text) => ({
    id,
    provider: 'slack',
    operation: 'slack.mark_conversation_read',
    preview: { slack_read: {
      conversation_type,
      conversation_name,
      message_ts: id,
      messages: [{ message_ts: id, actor_name: 'Ada', text, is_target: true, position: 'target' }],
    } },
  });
  const groups = slackMarkReadGroups([
    mutation('1', 'private_channel', 'hq', 'noted'),
    mutation('2', 'im', 'Grace', 'done'),
    mutation('3', 'public_channel', 'announcements', 'shipped'),
  ]);
  assert.deepEqual(groups.map((group) => [group.key, group.label, group.items.length]), [
    ['direct', 'Direct messages', 1],
    ['private', 'Private channels', 1],
    ['public', 'Public channels', 1],
  ]);
  assert.equal(groups[0].items[0].review.targetMessage?.text, 'done');
});

test('mobile review context calculates visual batch totals and preserved rules', () => {
  const context = mutationReviewContext({
    snapshot_utc: '2026-08-29T20:48:19Z',
    candidate_counts: { generic_channel: 173, automated_dm: 2, terminal_direct: 15, terminal_group: 8 },
    preserved: ['Newer messages', 'Mentions'],
    selection: ['Only reviewed snapshots'],
  });
  assert.equal(context.total, 198);
  assert.equal(context.counts[0].label, 'Generic channels');
  assert.deepEqual(context.preserved, ['Newer messages', 'Mentions']);
});
