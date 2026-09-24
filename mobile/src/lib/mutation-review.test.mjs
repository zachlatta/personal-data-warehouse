import test from 'node:test';
import assert from 'node:assert/strict';

import {
  appleContactPointExists,
  appleContactsBatchSummary,
  appleContactsReview,
  assembleEmailBody,
  calendarDayLayout,
  calendarMutationReview,
  contactBatchSummary,
  contactMutationReview,
  formatGmailLabel,
  gmailBatchSummary,
  gmailEmailReview,
  gmailEmailUpdateInput,
  hasGmailThreadMutations,
  gmailSenderName,
  gmailThreadDayGroups,
  gmailThreadReviews,
  gmailThreadUrl,
  isAppleContactsMutation,
  isCalendarCreateMutation,
  isContactMutation,
  isGmailSendEmailMutation,
  isGmailThreadMutation,
  isSlackMarkReadMutation,
  isSlackSendMessageMutation,
  looksAutomatedSender,
  mutationReviewContext,
  requestLifecycle,
  requestLifecycleNote,
  slackMarkReadGroups,
  slackMarkReadReview,
  slackSendMessageReview,
} from './mutation-review.ts';

test('a calendar create mutation becomes a complete day view with real conflicts and guests', () => {
  const mutation = {
    id: 'mut-calendar',
    provider: 'google_calendar',
    operation: 'calendar.create_event',
    account: 'zach@example.test',
    status: 'pending_review',
    title: 'Create event: Pickleball',
    result: {
      calendar_id: 'primary',
      event_id: 'created-pickleball',
      response: {
        id: 'created-pickleball',
        htmlLink: 'https://calendar.google.com/calendar/event?eid=created-pickleball',
        organizer: { email: 'zach@example.test' },
      },
    },
    payload: {
      calendar_id: 'primary',
      send_updates: 'all',
      event: {
        summary: 'Pickleball at Davis Park',
        description: 'PlayTime Scheduler session, 4.0–4.5.',
        location: 'Davis Park',
        start: { dateTime: '2026-09-05T09:00:00', timeZone: 'America/New_York' },
        end: { dateTime: '2026-09-05T11:00:00', timeZone: 'America/New_York' },
        attendees: [
          { email: 'zach@example.test', displayName: 'Zach', self: true, responseStatus: 'accepted' },
          { email: 'ada@example.test', displayName: 'Ada Lovelace', organizer: true, responseStatus: 'accepted' },
          { email: 'grace@example.test', displayName: 'Grace Hopper', optional: true, responseStatus: 'needsAction' },
        ],
      },
    },
    preview: {
      event: {
        operation: 'create', calendar_id: 'primary', send_updates: 'all', summary: 'Pickleball at Davis Park',
        description: 'PlayTime Scheduler session, 4.0–4.5.', location: 'Davis Park',
        start: { dateTime: '2026-09-05T09:00:00', timeZone: 'America/New_York' },
        end: { dateTime: '2026-09-05T11:00:00', timeZone: 'America/New_York' },
        attendees: [
          { email: 'zach@example.test', displayName: 'Zach', self: true, responseStatus: 'accepted' },
          { email: 'ada@example.test', displayName: 'Ada Lovelace', organizer: true, responseStatus: 'accepted' },
          { email: 'grace@example.test', displayName: 'Grace Hopper', optional: true, responseStatus: 'needsAction' },
        ],
      },
      calendar_day: {
        time_zone: 'America/New_York',
        day_start: '2026-09-05T00:00:00-04:00',
        day_end: '2026-09-06T00:00:00-04:00',
        proposed_start_at: '2026-09-05T09:00:00-04:00',
        proposed_end_at: '2026-09-05T11:00:00-04:00',
        source_synced_at: '2026-09-02T20:46:00Z',
        events: [
          { event_id: 'early', calendar_id: 'primary', summary: 'Morning run', start_at: '2026-09-05T07:30:00-04:00', end_at: '2026-09-05T08:15:00-04:00' },
          {
            event_id: 'conflict', calendar_id: 'work', summary: 'Breakfast with Ada', location: 'Davis Square',
            start_at: '2026-09-05T09:30:00-04:00', end_at: '2026-09-05T10:30:00-04:00', transparency: 'opaque',
            attendees: [{ email: 'zach@example.test', self: true, responseStatus: 'accepted' }, { email: 'ada@example.test', displayName: 'Ada Lovelace' }],
          },
          {
            event_id: 'declined', calendar_id: 'primary', summary: 'Declined hold',
            start_at: '2026-09-05T09:15:00-04:00', end_at: '2026-09-05T09:45:00-04:00',
            attendees: [{ email: 'zach@example.test', self: true, responseStatus: 'declined' }],
          },
          {
            event_id: 'transparent', calendar_id: 'primary', summary: 'Travel time',
            start_at: '2026-09-05T10:00:00-04:00', end_at: '2026-09-05T10:30:00-04:00', transparency: 'transparent',
          },
          { event_id: 'later', calendar_id: 'primary', summary: 'Lunch', start_at: '2026-09-05T12:30:00-04:00', end_at: '2026-09-05T13:30:00-04:00' },
          // An executed request can already be visible in the next calendar
          // sync. It remains the blue proposal, not a fake conflict with itself.
          { event_id: 'created-pickleball', calendar_id: 'zach@example.test', summary: 'Pickleball at Davis Park', start_at: '2026-09-05T09:00:00-04:00', end_at: '2026-09-05T11:00:00-04:00' },
          { event_id: 'all-day', calendar_id: 'primary', summary: 'Hack Club retreat', start_date: '2026-09-05', end_date: '2026-09-06', is_all_day: true },
        ],
      },
    },
  };

  assert.equal(isCalendarCreateMutation(mutation), true);
  assert.equal(isCalendarCreateMutation({ provider: 'google_calendar', operation: 'calendar.update_event' }), false);
  const review = calendarMutationReview(mutation);
  assert.equal(review.operation, 'create');
  assert.equal(review.title, 'Pickleball at Davis Park');
  assert.equal(review.dateLabel, 'Saturday, September 5');
  assert.equal(review.timeLabel, '9:00–11:00 AM EDT');
  assert.equal(review.durationLabel, '2 hr');
  assert.equal(review.otherAttendees.length, 2);
  assert.deepEqual(review.otherAttendees.map((attendee) => attendee.displayName), ['Ada Lovelace', 'Grace Hopper']);
  assert.equal(review.otherAttendees[0].organizer, true);
  assert.equal(review.otherAttendees[1].responseLabel, 'Awaiting reply');
  assert.equal(review.conflicts.length, 2);
  assert.deepEqual(review.conflicts.map((event) => event.title), ['Breakfast with Ada', 'Hack Club retreat']);
  assert.equal(review.availability, 'conflict');
  assert.equal(review.proposed.id, 'created-pickleball');
  assert.equal(review.proposed.htmlLink, 'https://calendar.google.com/calendar/event?eid=created-pickleball');
  assert.equal(review.proposed.organizerEmail, 'zach@example.test');
  assert.equal(review.existingEvents.some((event) => event.id === 'created-pickleball'), false);
  assert.equal(review.allDayEvents[0].title, 'Hack Club retreat');
  assert.deepEqual(review.timedEvents.map((event) => event.title), [
    'Morning run', 'Pickleball at Davis Park', 'Declined hold', 'Breakfast with Ada', 'Travel time', 'Lunch',
  ]);
  assert.equal(review.sourceSyncedAt, '2026-09-02T20:46:00Z');

  const layout = calendarDayLayout(review);
  assert.equal(layout.startHour, 6);
  assert.equal(layout.endHour, 15);
  const proposed = layout.blocks.find((block) => block.event.proposed);
  const conflict = layout.blocks.find((block) => block.event.id === 'conflict');
  assert.ok(proposed);
  assert.ok(conflict);
  assert.ok(proposed.columnCount > 1, 'overlapping events should occupy calendar lanes');
  assert.equal(conflict.conflict, true);
});

test('a calendar review says when availability could not be loaded instead of claiming the time is clear', () => {
  const mutation = {
    provider: 'google_calendar', operation: 'calendar.create_event', account: 'zach@example.test',
    payload: { event: { summary: 'Focus', start: { dateTime: '2026-09-05T09:00:00Z' }, end: { dateTime: '2026-09-05T10:00:00Z' } } },
    preview: { event: { summary: 'Focus', start: { dateTime: '2026-09-05T09:00:00Z' }, end: { dateTime: '2026-09-05T10:00:00Z' } } },
  };
  const review = calendarMutationReview(mutation);
  assert.equal(review.availability, 'unavailable');
  assert.equal(review.conflicts.length, 0);
  assert.equal(review.otherAttendees.length, 0);

  const loaded = calendarMutationReview({
    ...mutation,
    preview: {
      ...mutation.preview,
      calendar_day: {
        time_zone: 'UTC', day_start: '2026-09-05T00:00:00Z', day_end: '2026-09-06T00:00:00Z',
        proposed_start_at: '2026-09-05T09:00:00Z', proposed_end_at: '2026-09-05T10:00:00Z', events: [],
      },
    },
  });
  assert.equal(loaded.availability, 'clear');
});

test('a multi-day add detects an all-day conflict on any covered date', () => {
  const review = calendarMutationReview({
    provider: 'google_calendar', operation: 'calendar.create_event', account: 'zach@example.test',
    payload: {
      event: {
        summary: 'Offsite',
        start: { date: '2026-09-05' },
        end: { date: '2026-09-07' },
      },
    },
    preview: {
      calendar_day: {
        time_zone: 'America/New_York',
        day_start: '2026-09-05T00:00:00-04:00',
        day_end: '2026-09-07T00:00:00-04:00',
        proposed_start_at: '2026-09-05T00:00:00-04:00',
        proposed_end_at: '2026-09-07T00:00:00-04:00',
        proposed_start_date: '2026-09-05',
        proposed_end_date: '2026-09-07',
        proposed_is_all_day: true,
        events: [
          { event_id: 'sunday', summary: 'Retreat', start_date: '2026-09-06', end_date: '2026-09-07', is_all_day: true },
        ],
      },
    },
  });
  assert.equal(review.durationLabel, '2 days');
  assert.deepEqual(review.conflicts.map((event) => event.title), ['Retreat']);
});

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

test('a Slack review row carries the face and the permalink of its target', () => {
  const review = slackMarkReadReview({
    provider: 'slack',
    operation: 'slack.mark_conversation_read',
    account: 'example',
    payload: { conversation_id: 'D1', message_ts: '1593473566.000200' },
    preview: {
      slack_read: {
        conversation_name: 'Marcus',
        conversation_type: 'im',
        messages: [
          {
            message_ts: '1593473566.000200', sent_at: '2026-08-29T14:01:00Z', actor_name: 'Marcus',
            text: 'Yep — all handled.', is_target: true, position: 'target',
            avatar_url: 'https://avatars.example.test/marcus.png',
            open: { url: 'https://example.slack.com/archives/D1/p1593473566000200', label: 'Slack', app_url: 'slack://channel?team=T1&id=D1&message=1593473566.000200' },
          },
          { message_ts: '1593473600.000300', sent_at: '2026-08-29T14:02:00Z', actor_name: 'Marcus', text: 'One more.', position: 'after' },
        ],
      },
    },
  });

  assert.equal(review.messages[0].avatarUrl, 'https://avatars.example.test/marcus.png');
  assert.equal(review.messages[0].open.app_url, 'slack://channel?team=T1&id=D1&message=1593473566.000200');
  // The row inherits the target's face and link when the preview gives it none.
  assert.equal(review.avatarUrl, 'https://avatars.example.test/marcus.png');
  assert.equal(review.open.url, 'https://example.slack.com/archives/D1/p1593473566000200');
  // A message the warehouse could not link is left unlinked rather than
  // pointed at the conversation, which would open the wrong place.
  assert.equal(review.messages[1].open, null);
  assert.equal(review.messages[1].avatarUrl, '');
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

// --- gmail thread review ----------------------------------------------------

// One archive mutation per thread, which is the shape the warehouse proposes.
function archiveMutation(id, thread, status = 'pending_review') {
  return {
    id,
    status,
    provider: 'gmail',
    operation: 'gmail.archive_threads',
    account: 'zach@example.test',
    payload: { thread_ids: [thread.thread_id], remove_label_ids: ['INBOX'] },
    preview: { thread_count: 1, threads: [thread] },
  };
}

test('a gmail archive batch reads as an inbox, newest first', () => {
  const mutations = [
    archiveMutation('mut-1', {
      thread_id: 'thread-1',
      subject: 'Re: Half day monday',
      latest_from_address: 'marcus@example.test',
      latest_from_name: 'Marcus Bell',
      latest_at: '2026-08-30T15:36:00Z',
      latest_preview: 'Approved! No problem at all',
      message_count: 2,
      labels: ['Inbox', 'Forums'],
      messages: [
        { message_id: 'm1', from_address: 'zach@example.test', from_name: 'Zach Lata', to_addresses: ['marcus@example.test'], internal_date: '2026-08-30T15:00:00Z', snippet: 'Can I take Monday off?' },
        { message_id: 'm2', from_address: 'marcus@example.test', from_name: 'Marcus Bell', to_addresses: ['zach@example.test'], internal_date: '2026-08-30T15:36:00Z', preview_text: 'Approved! No problem at all' },
      ],
    }),
    archiveMutation('mut-2', {
      thread_id: 'thread-2',
      subject: 'Your order is confirmed',
      latest_from_address: 'no-reply@t.printworks.test',
      latest_from_name: 'Printworks',
      latest_at: '2026-08-31T18:15:00Z',
      latest_preview: 'Order confirmed.',
      message_count: 1,
      labels: ['Inbox', 'Updates', 'Unread'],
      messages: [{ message_id: 'm3', from_address: 'no-reply@t.printworks.test', internal_date: '2026-08-31T18:15:00Z', label_ids: ['INBOX', 'UNREAD'], snippet: 'Order confirmed.' }],
    }),
  ];

  assert.equal(isGmailThreadMutation(mutations[0]), true);
  assert.equal(isGmailThreadMutation({ provider: 'gmail', operation: 'gmail.send_email' }), false);
  assert.equal(isGmailThreadMutation({ provider: 'slack', operation: 'slack.mark_conversation_read' }), false);

  const reviews = gmailThreadReviews(mutations);
  assert.deepEqual(reviews.map((review) => review.subject), ['Your order is confirmed', 'Re: Half day monday']);
  const [order, halfDay] = reviews;
  assert.equal(order.senderName, 'Printworks');
  assert.equal(order.unread, true);
  assert.equal(order.automated, true);
  // Inbox is on every row and unread has its own marker, so neither is a chip.
  assert.deepEqual(order.labels, ['Updates']);
  assert.equal(halfDay.senderName, 'Marcus Bell');
  assert.equal(halfDay.unread, false);
  assert.equal(halfDay.automated, false);
  assert.equal(halfDay.messageCount, 2);
  assert.equal(halfDay.messages[1].text, 'Approved! No problem at all');
  assert.equal(halfDay.messages[0].senderName, 'Zach Lata');
  assert.equal(halfDay.mutationId, 'mut-1');
  assert.equal(halfDay.threadsInMutation, 1);
});

test('a thread with no preview still renders a row, and a removed one is marked kept', () => {
  const bare = { id: 'mut-3', status: 'pending_review', provider: 'gmail', operation: 'gmail.archive_threads', account: 'zach@example.test', payload: { thread_ids: ['thread-9'] }, preview: {} };
  const [review] = gmailThreadReviews([bare]);
  assert.equal(review.threadId, 'thread-9');
  assert.equal(review.subject, '(no subject)');
  assert.equal(review.messages.length, 0);
  assert.equal(review.removed, false);

  const removed = gmailThreadReviews([{ ...bare, status: 'rejected' }]);
  assert.equal(removed[0].removed, true);
});

test('the sender name prefers the real header, then the address, and never a bulk local part', () => {
  assert.equal(gmailSenderName('Parcelco', 'pkginfo@parcelco.test'), 'Parcelco');
  assert.equal(gmailSenderName('', 'Marcus Bell <marcus@example.test>'), 'Marcus Bell');
  assert.equal(gmailSenderName('', 'marcus.bell@example.test'), 'Marcus Bell');
  // "no-reply" says nothing, so the domain answers instead — and the answer is
  // the registrable label, not the bulk-mail subdomain in front of it.
  assert.equal(gmailSenderName('', 'no-reply@t.printworks.test'), 'Printworks');
  assert.equal(gmailSenderName('', 'news@updates.examplebrand.co.uk'), 'Examplebrand');
  assert.equal(gmailSenderName('', '', 'Fallback subject'), 'Fallback subject');
});

test('automated senders are the no-reply shapes, a relay, and anything Gmail calls promotional', () => {
  assert.equal(looksAutomatedSender('no-reply@printworks.test', 'Printworks', []), true);
  assert.equal(looksAutomatedSender('notification@relay.test', 'Marcus Bell via Chat', []), true);
  assert.equal(looksAutomatedSender('marcus@example.test', 'Marcus Bell via Chat', []), true);
  assert.equal(looksAutomatedSender('conf@example.test', 'Example Conf', ['Promotions']), true);
  assert.equal(looksAutomatedSender('noreply.billing@example.test', 'Example', []), true);
  assert.equal(looksAutomatedSender('marcus@example.test', 'Marcus Bell', ['Updates']), false);
});

test('the batch summary counts what will still run and says what approval does', () => {
  const thread = (id, at, extra = {}) => ({ thread_id: id, subject: id, latest_from_address: 'marcus@example.test', latest_at: at, message_count: 1, messages: [], ...extra });
  const mutations = [
    archiveMutation('mut-1', thread('a', '2026-08-31T18:15:00Z')),
    archiveMutation('mut-2', thread('b', '2026-08-31T19:15:00Z', { latest_from_address: 'no-reply@example.test' })),
    archiveMutation('mut-3', thread('c', '2026-08-31T20:15:00Z'), 'rejected'),
  ];
  mutations[1].account = 'zach@other.test';

  const summary = gmailBatchSummary(mutations, gmailThreadReviews(mutations));
  assert.equal(summary.verb, 'Archive');
  assert.equal(summary.effect, 'Takes these threads out of the Inbox. Nothing is deleted, and search still finds them.');
  assert.equal(summary.threadCount, 2);
  assert.equal(summary.keptCount, 1);
  assert.equal(summary.automatedCount, 1);
  assert.deepEqual(summary.accounts, [{ account: 'zach@other.test', count: 1 }, { account: 'zach@example.test', count: 1 }]);

  const unarchive = mutations.slice(0, 1).map((mutation) => ({ ...mutation, operation: 'gmail.unarchive_threads' }));
  assert.equal(gmailBatchSummary(unarchive, gmailThreadReviews(unarchive)).verb, 'Unarchive');
  assert.equal(gmailBatchSummary(unarchive, gmailThreadReviews(unarchive)).effect, 'Puts this thread back in the Inbox.');
});

test('threads group by the day they landed in, in the reader’s own timezone', () => {
  // 00:12 local is still "yesterday evening" reading, and the UTC prefix of
  // that instant is a different date — keying on the string split one day in
  // two and labelled both of them the same.
  const now = new Date(2026, 8, 1, 12, 0, 0);
  const at = (daysAgo, hour) => new Date(2026, 8, 1 - daysAgo, hour, 12).toISOString();
  const reviews = [
    { key: 'a', latestAt: at(0, 9) },
    { key: 'b', latestAt: at(1, 23) },
    { key: 'c', latestAt: at(1, 0) },
    { key: 'd', latestAt: at(9, 8) },
  ];
  const groups = gmailThreadDayGroups(reviews, now);
  assert.deepEqual(groups.map((group) => group.label), ['Today', 'Yesterday', new Date(2026, 7, 23, 8, 12).toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' })]);
  assert.deepEqual(groups.map((group) => group.data.length), [1, 2, 1]);
});

test('gmail labels hide what every row carries and name what Gmail names', () => {
  assert.equal(formatGmailLabel('CATEGORY_PROMOTIONS'), 'Promotions');
  assert.equal(formatGmailLabel('SENT'), 'Sent');
  assert.equal(formatGmailLabel('Label_29'), '');
  assert.equal(formatGmailLabel('INBOX'), '');
  // The warehouse formats labels before the app sees them, so the already
  // formatted spellings have to be hidden too.
  assert.equal(formatGmailLabel('Inbox'), '');
  assert.equal(formatGmailLabel('Unread'), '');
});

test('a thread row links to that thread in the mailbox it belongs to', () => {
  assert.equal(gmailThreadUrl('zach@example.test', 'thread-1'), 'https://mail.google.com/mail/u/?authuser=zach%40example.test#all/thread-1');
  assert.equal(gmailThreadUrl('', 'thread-1'), 'https://mail.google.com/mail/u/0#all/thread-1');
  assert.equal(gmailThreadUrl('zach@example.test', ''), '');
});

test('mixed Gmail and Slack requests keep the inbox review surface', () => {
  const mail = archiveMutation('mail', { thread_id: 't', subject: 'Receipt' });
  const mixed = [mail, { id: 'slack', operation: 'slack.mark_conversation_read' }];
  assert.equal(hasGmailThreadMutations(mixed), true);
  assert.equal(hasGmailThreadMutations([mixed[1]]), false);
  assert.equal(hasGmailThreadMutations([]), false);
  assert.equal(gmailThreadReviews(mixed).length, 1);
});

test('expanded Gmail messages use the entire body, not the inbox snippet', () => {
  const body = 'Hello,\n\n' + 'Full message. '.repeat(200) + '\n\nOn Wed, someone wrote:\nOriginal message';
  const [review] = gmailThreadReviews([archiveMutation('mail', {
    thread_id: 't', messages: [{ message_id: 'm', body_text: body, preview_text: 'Short snippet' }],
  })]);
  assert.equal(review.messages[0].text, body);
  assert.equal(review.messages[0].hasFullBody, true);
  const [legacy] = gmailThreadReviews([archiveMutation('old', {
    thread_id: 't', messages: [{ snippet: 'Only a preview' }],
  })]);
  assert.equal(legacy.messages[0].hasFullBody, false);
});

test('each thread states its own action, including mixed Gmail operations', () => {
  const archive = archiveMutation('a', { thread_id: 'a' });
  const restore = { ...archiveMutation('b', { thread_id: 'b' }), operation: 'gmail.unarchive_threads' };
  const relabel = { ...archiveMutation('c', { thread_id: 'c' }), operation: 'gmail.modify_thread_labels', payload: { thread_ids: ['c'], remove_label_ids: ['INBOX'], add_labels: ['Receipts'] } };
  const reviews = gmailThreadReviews([archive, restore, relabel]);
  assert.equal(reviews[0].action, 'Archive');
  assert.equal(reviews[1].action, 'Move to inbox');
  assert.match(reviews[2].action, /Receipts/);
  assert.match(reviews[2].action, /INBOX/);
  assert.equal(gmailBatchSummary([archive, restore], reviews.slice(0, 2)).verb, 'Review');
});

test('partial thread previews never hide another thread affected by the same action', () => {
  const mutation = archiveMutation('batch', { thread_id: 'known', subject: 'Known mail' });
  mutation.payload.thread_ids = ['known', 'missing'];
  const reviews = gmailThreadReviews([mutation]);
  assert.deepEqual(reviews.map((review) => review.threadId), ['known', 'missing']);
  assert.equal(reviews[1].messages.length, 0);
  assert.equal(reviews[0].threadsInMutation, 2);
});

// --- contacts --------------------------------------------------------------

function contactMutation(operation, preview = {}) {
  return {
    id: 'mut-contact',
    provider: 'google_people',
    operation: 'contacts.batch_mutation',
    account: 'zach@example.test',
    status: 'pending_review',
    title: 'Create contact',
    payload: { operations: [operation] },
    preview: { operations: [{ ...operation, op_index: 0, ...preview }] },
  };
}

test('a create_contact operation reads as a contact card, not a JSON dump', () => {
  const mutation = contactMutation({
    op: 'create_contact',
    client_op_id: 'op-0',
    person: {
      names: [{ givenName: 'Nova', familyName: 'Example' }],
      nicknames: [{ value: 'Nov' }],
      emailAddresses: [{ type: 'work', value: 'nova@example.test' }, { value: 'nova@personal.test' }],
      phoneNumbers: [{ type: 'mobile', value: '+18025550100' }],
      organizations: [{ name: 'Hack Club', title: 'Deputy to the Founder' }],
      urls: [{ type: 'work', value: 'https://example.test/nova' }],
      addresses: [{ formattedValue: '1 Main St, Springfield' }],
      biographies: [{ value: 'Met at the summit. Runs the events team.', contentType: 'TEXT_PLAIN' }],
    },
  });
  assert.equal(isContactMutation(mutation), true);
  const [review] = contactMutationReview(mutation);
  assert.equal(review.op, 'create_contact');
  assert.equal(review.verb, 'Create');
  assert.equal(review.name, 'Nova Example');
  assert.equal(review.nickname, 'Nov');
  assert.equal(review.role, 'Deputy to the Founder · Hack Club');
  assert.deepEqual(review.points.map((p) => [p.kind, p.label, p.value]), [
    ['email', 'work', 'nova@example.test'],
    ['email', '', 'nova@personal.test'],
    ['phone', 'mobile', '+18025550100'],
    ['url', 'work', 'https://example.test/nova'],
    ['address', '', '1 Main St, Springfield'],
  ]);
  assert.equal(review.note, 'Met at the summit. Runs the events team.');
  assert.equal(review.effect, 'Creates a new Google Contact.');
  assert.equal(review.warning, '');
  assert.deepEqual(review.changes, []);
});

test('an update_contact operation shows the before → after per masked field and names a cleared field as a wipe', () => {
  const mutation = contactMutation(
    {
      op: 'update_contact',
      client_op_id: 'op-1',
      resource_name: 'people/c123',
      expected_etag: 'etag-old',
      update_person_fields: ['emailAddresses', 'organizations', 'phoneNumbers'],
      clear_person_fields: ['phoneNumbers'],
      person: {
        resourceName: 'people/c123',
        etag: 'etag-old',
        emailAddresses: [{ value: 'new@example.test' }],
        organizations: [{ name: 'Hack Club', title: 'Engineer' }],
      },
    },
    {
      after: { emailAddresses: [{ value: 'new@example.test' }], organizations: [{ name: 'Hack Club', title: 'Engineer' }] },
      before: {
        etag: 'etag-old',
        names: [{ displayName: 'Sam Example' }],
        emailAddresses: [{ value: 'old@example.test' }],
        organizations: [{ name: 'Hack Club', title: 'Engineer' }],
        phoneNumbers: [{ value: '+18025550199' }],
      },
      contact_found: true,
      current_etag: 'etag-old',
      etag_is_current: true,
    },
  );
  const [review] = contactMutationReview(mutation);
  assert.equal(review.verb, 'Update');
  // The name comes from the synced card because the patch does not carry one.
  assert.equal(review.name, 'Sam Example');
  assert.equal(review.resourceName, 'people/c123');
  assert.deepEqual(review.changes, [
    { field: 'emailAddresses', label: 'Email', before: 'old@example.test', after: 'new@example.test', kind: 'changed' },
    { field: 'organizations', label: 'Organization', before: 'Engineer, Hack Club', after: 'Engineer, Hack Club', kind: 'unchanged' },
    { field: 'phoneNumbers', label: 'Phone', before: '+18025550199', after: '', kind: 'cleared' },
  ]);
  assert.match(review.effect, /Replaces Email, Organization/);
  assert.match(review.effect, /Clears Phone/);
  assert.equal(review.warning, '');
});

test('a stale etag or a missing synced card is a warning at review time, not a failed run later', () => {
  const stale = contactMutation(
    { op: 'update_contact', resource_name: 'people/c1', expected_etag: 'a', update_person_fields: ['names'], person: { names: [{ displayName: 'X' }] } },
    { before: { etag: 'b', names: [{ displayName: 'Old' }] }, contact_found: true, current_etag: 'b', etag_is_current: false },
  );
  assert.match(contactMutationReview(stale)[0].warning, /changed since this was proposed/);
  const missing = contactMutation(
    { op: 'delete_contact', resource_name: 'people/c2', expected_etag: 'a' },
    { contact_found: false },
  );
  const [review] = contactMutationReview(missing);
  assert.equal(review.verb, 'Delete');
  assert.equal(review.name, 'people/c2');
  assert.match(review.warning, /not in the synced Google Contacts copy/);
  assert.equal(review.effect, 'Deletes this contact from Google Contacts.');
});

test('a contact batch summary counts what will still run and picks the approve verb', () => {
  const create = (id, status = 'pending_review') => ({ ...contactMutation({ op: 'create_contact', person: { names: [{ givenName: 'A' }] } }), id, status });
  const update = { ...contactMutation({ op: 'update_contact', resource_name: 'people/x', expected_etag: 'e', update_person_fields: ['names'], person: { names: [{ givenName: 'B' }] } }), id: 'u' };
  assert.deepEqual(contactBatchSummary([create('a'), create('b'), create('c', 'rejected')]), { create: 2, update: 0, delete: 0, running: 2, verb: 'Create' });
  assert.deepEqual(contactBatchSummary([create('a'), update]), { create: 1, update: 1, delete: 0, running: 2, verb: 'Approve' });
  assert.deepEqual(contactBatchSummary([{ ...update, payload: { operations: [{ op: 'delete_contact', resource_name: 'people/x', expected_etag: 'e' }] } }]), { create: 0, update: 0, delete: 1, running: 1, verb: 'Delete' });
  assert.equal(isContactMutation({ operation: 'gmail.send_email', provider: 'gmail' }), false);
});

// --- apple contacts --------------------------------------------------------


function appleMutation(operation, payload, preview) {
  return { id: 'mut-apple', provider: 'apple_contacts', operation, account: 'zach@example.test', status: 'pending_review', payload, preview: { contact: preview } };
}

test('an apple merge reads as the kept card first, names the deletions, and warns about a vanished card', () => {
  const mutation = appleMutation('apple_contacts.merge_contacts',
    { keep_card_id: 'K:ABPerson', merge_card_ids: ['M:ABPerson', 'G:ABPerson'], contact: { family_name: 'Lovelace', emails: [{ label: 'work', value: 'ada@example.test' }] } },
    {
      action: 'merge', name: 'Lovelace', keep_card_id: 'K:ABPerson', merge_card_ids: ['M:ABPerson', 'G:ABPerson'],
      contact: { family_name: 'Lovelace', emails: [{ label: 'work', value: 'ada@example.test' }] },
      changes: ['family_name', 'emails (added)', '2 card(s) deleted after merge'],
      cards: [
        { card_id: 'M:ABPerson', display_name: 'Ada L', emails: [{ label: 'home', value: 'ADA@example.test' }], phones: [{ label: 'mobile', value: '(802) 555-0100', canonicalForm: '+18025550100' }] },
        { card_id: 'K:ABPerson', display_name: 'Ada Lovelace', organization: 'Hack Club', job_title: 'Cofounder', emails: [], phones: [], urls: [], note: 'met at summit' },
        { card_id: 'G:ABPerson', missing: true },
      ],
    });
  assert.equal(isAppleContactsMutation(mutation), true);
  const review = appleContactsReview(mutation);
  assert.equal(review.op, 'merge');
  assert.equal(review.verb, 'Merge');
  assert.equal(review.name, 'Lovelace');
  assert.equal(review.role, 'Cofounder · Hack Club');
  assert.deepEqual(review.cards.map((c) => [c.cardId, c.kept, c.missing]), [['K:ABPerson', true, false], ['M:ABPerson', false, false], ['G:ABPerson', false, true]]);
  assert.deepEqual(review.cards[1].points.map((p) => [p.kind, p.label, p.value]), [['email', 'home', 'ADA@example.test'], ['phone', 'mobile', '(802) 555-0100']]);
  assert.equal(review.deletedCount, 2);
  assert.deepEqual(review.destructive, ['2 cards deleted after the merge']);
  assert.match(review.warning, /no longer in the synced address book/);
  assert.deepEqual(review.changes, [{ field: 'family_name', label: 'Last name', before: '', after: 'Lovelace', kind: 'changed' }]);
  // the proposed work email already sits on the merged card as a home email (case-insensitively), so the row says so
  assert.equal(appleContactPointExists(review.points[0], review.cards), true);
  assert.equal(appleContactPointExists({ kind: 'email', label: '', value: 'someone-else@example.test' }, review.cards), false);
  assert.equal(appleContactPointExists({ kind: 'phone', label: '', value: '+1 802-555-0100' }, review.cards), true);
});

test('an apple update shows before → after against the card today and says what is removed', () => {
  const mutation = appleMutation('apple_contacts.update_contact',
    { card_id: 'K:ABPerson', contact: { organization: 'Hack Club', job_title: 'Controller', append_note: 'seen 2026-09', phones: [{ label: 'mobile', value: '+18025550100' }] }, remove: { emails: ['old@example.test'] } },
    {
      action: 'update', card_id: 'K:ABPerson', name: '',
      contact: { organization: 'Hack Club', job_title: 'Controller', append_note: 'seen 2026-09', phones: [{ label: 'mobile', value: '+18025550100' }] },
      remove: { emails: ['old@example.test'] }, changes: ['note (appended)', 'job_title', 'organization', 'phones (added)', 'emails (removed)'],
      cards: [{ card_id: 'K:ABPerson', display_name: 'Sierra Example', organization: 'Hack Club', job_title: '', emails: [{ label: 'work', value: 'old@example.test' }], phones: [], urls: [] }],
    });
  const review = appleContactsReview(mutation);
  assert.equal(review.op, 'update');
  assert.equal(review.name, 'Sierra Example');
  assert.equal(review.role, 'Controller · Hack Club');
  assert.deepEqual(review.changes, [
    { field: 'organization', label: 'Organization', before: 'Hack Club', after: 'Hack Club', kind: 'unchanged' },
    { field: 'job_title', label: 'Title', before: '', after: 'Controller', kind: 'changed' },
  ]);
  assert.equal(review.appendNote, 'seen 2026-09');
  assert.deepEqual(review.removed, [{ kind: 'email', label: '', value: 'old@example.test' }]);
  assert.deepEqual(review.destructive, ['1 value removed from the card']);
  assert.equal(review.warning, '');
  assert.equal(review.deletedCount, 0);
});

test('an apple create reads from the proposed contact alone and the batch verb follows the operations', () => {
  const create = appleMutation('apple_contacts.create_contact',
    { contact: { given_name: 'Ada', family_name: 'Lovelace', organization: 'Hack Club', emails: [{ label: 'work', value: 'ada@example.test' }], note: 'why she is here' } },
    { action: 'create', name: 'Ada Lovelace', contact: { given_name: 'Ada', family_name: 'Lovelace', organization: 'Hack Club', emails: [{ label: 'work', value: 'ada@example.test' }], note: 'why she is here' }, changes: ['emails (added)', 'family_name', 'given_name', 'note (replaced)', 'organization'] });
  const review = appleContactsReview(create);
  assert.equal(review.op, 'create');
  assert.equal(review.name, 'Ada Lovelace');
  assert.equal(review.note, 'why she is here');
  assert.deepEqual(review.destructive, []);
  assert.deepEqual(review.changes, []);
  assert.deepEqual(review.points, [{ kind: 'email', label: 'work', value: 'ada@example.test' }]);
  assert.equal(appleContactsBatchSummary([create, { ...create, id: 'b' }]).verb, 'Create');
  assert.equal(appleContactsBatchSummary([create, appleMutation('apple_contacts.merge_contacts', { keep_card_id: 'K', merge_card_ids: ['M'] }, { action: 'merge' })]).verb, 'Approve');
  assert.equal(appleContactsBatchSummary([{ ...create, status: 'removed' }]).running, 0);
});

// --- gmail.send_email editing ------------------------------------------------

function sendEmailMutation(overrides = {}) {
  return {
    id: 'mut-email',
    provider: 'gmail',
    operation: 'gmail.send_email',
    account: 'zach@example.test',
    status: 'pending_review',
    payload: { delivery_mode: 'send', message: { to: ['vendor@example.test'], subject: 'Re: quote', body_text: 'Sounds good.' } },
    preview: {},
    email: {
      delivery_mode: 'send',
      has_variants: true,
      message: { to: ['vendor@example.test'], cc: [], bcc: [], subject: 'Re: quote', editor_text: 'Sounds good.', editor_html: '<div>Sounds good.</div>', signature_html: '<div class="gmail_signature"><b>Zach</b></div>', signature_text: 'Zach', quoted_html: '<div class="gmail_quote">On Mon, they wrote:<br>hi</div>', quoted_text: 'On Mon, they wrote:\nhi', reply_to_thread_id: 't-9', in_reply_to: '<m1@example.test>', references: ['<m0@example.test>', '<m1@example.test>'] },
      variants: [
        { id: 'variant-1', title: 'Direct', selected: false, to: ['vendor@example.test'], cc: [], bcc: [], subject: 'Re: quote', editor_text: 'Sounds good.', editor_html: '<div>Sounds good.</div>', signature_html: '', signature_text: '', quoted_html: '', quoted_text: '', reply_to_thread_id: 't-9', in_reply_to: '', references: [] },
        { id: 'variant-2', title: 'Softer', selected: true, to: ['vendor@example.test'], cc: ['boss@example.test'], bcc: [], subject: 'Re: quote', editor_text: 'Maybe.', editor_html: '<div>Maybe.</div>', signature_html: '<div class="gmail_signature"><b>Zach</b></div>', signature_text: 'Zach', quoted_html: '<div class="gmail_quote">older</div>', quoted_text: 'older', reply_to_thread_id: 't-9', in_reply_to: '<m1@example.test>', references: ['<m1@example.test>'] },
      ],
      reply_threads: [
        { thread_id: 't-9', subject: 'quote', messages: [{ message_id: 'm1', from_address: 'vendor@example.test', from_name: 'Vendor', to_addresses: ['zach@example.test'], internal_date: '2026-08-30T15:00:00Z', body_text: 'hi there' }] },
      ],
    },
    ...overrides,
  };
}

test('a send_email mutation becomes an editable composer: variants, the selected one marked, parts as text', () => {
  const mutation = sendEmailMutation();
  assert.equal(isGmailSendEmailMutation(mutation), true);
  assert.equal(isGmailSendEmailMutation({ provider: 'gmail', operation: 'gmail.archive_threads' }), false);
  const review = gmailEmailReview(mutation);
  assert.equal(review.deliveryMode, 'send');
  assert.equal(review.hasVariants, true);
  assert.deepEqual(review.variants.map((variant) => variant.id), ['variant-1', 'variant-2']);
  assert.equal(review.selectedVariantId, 'variant-2');
  const softer = review.variants[1];
  assert.equal(softer.title, 'Softer');
  assert.equal(softer.editorText, 'Maybe.');
  assert.equal(softer.signatureText, 'Zach');
  assert.equal(softer.quotedText, 'older');
  assert.deepEqual(softer.cc, ['boss@example.test']);
  assert.equal(softer.replyToThreadId, 't-9');
  assert.deepEqual(softer.references, ['<m1@example.test>']);
  assert.equal(review.replyThreads.length, 1);
  assert.equal(review.replyThreads[0].subject, 'quote');
  assert.equal(review.replyThreads[0].messages[0].senderName, 'Vendor');
  assert.equal(review.replyThreads[0].messages[0].text, 'hi there');
});

test('a send_email mutation without variants offers its one message as the only variant', () => {
  const mutation = sendEmailMutation();
  mutation.email = { ...mutation.email, has_variants: false, variants: [] };
  const review = gmailEmailReview(mutation);
  assert.equal(review.hasVariants, false);
  assert.equal(review.variants.length, 1);
  assert.equal(review.variants[0].id, '');
  assert.equal(review.variants[0].editorText, 'Sounds good.');
  assert.equal(review.selectedVariantId, '');
});

test('a send_email mutation with no server view falls back to the payload message', () => {
  const mutation = sendEmailMutation({ email: undefined });
  const review = gmailEmailReview(mutation);
  assert.equal(review.variants.length, 1);
  assert.equal(review.variants[0].editorText, 'Sounds good.');
  assert.deepEqual(review.variants[0].to, ['vendor@example.test']);
});

test('the edited plain text is assembled into the body the server splits again: editor, signature, quote', () => {
  const body = assembleEmailBody({
    editorText: 'Hi there,\n\nSee <you> Monday.\nThanks  \n\n',
    signatureHTML: '<div class="gmail_signature"><b>Zach</b></div>',
    signatureText: 'Zach',
    quotedHTML: '<div class="gmail_quote">older</div>',
    quotedText: 'older',
  });
  assert.equal(body.body_html, '<div>Hi there,</div><div><br></div><div>See &lt;you&gt; Monday.<br>Thanks</div><div><br></div><div class="gmail_signature"><b>Zach</b></div><div><br></div><div class="gmail_quote">older</div>');
  assert.equal(body.body_text, 'Hi there,\n\nSee <you> Monday.\nThanks\n\nZach\n\nolder\n');
  const bare = assembleEmailBody({ editorText: 'Just this.', signatureHTML: '', signatureText: '', quotedHTML: '', quotedText: '' });
  assert.equal(bare.body_html, '<div>Just this.</div>');
  assert.equal(bare.body_text, 'Just this.\n');
});

test('the update input carries the recipients split, the assembled body and the reply headers', () => {
  const review = gmailEmailReview(sendEmailMutation());
  const input = gmailEmailUpdateInput(review.variants[1], { to: 'a@example.test, b@example.test\nc@example.test', cc: '', bcc: ' d@example.test ', subject: '  Re: quote  ', editorText: 'Maybe not.' }, 'draft');
  assert.equal(input.delivery_mode, 'draft');
  assert.equal(input.selected_variant_id, 'variant-2');
  assert.deepEqual(input.message.to, ['a@example.test', 'b@example.test', 'c@example.test']);
  assert.deepEqual(input.message.cc, []);
  assert.deepEqual(input.message.bcc, ['d@example.test']);
  assert.equal(input.message.subject, 'Re: quote');
  assert.equal(input.message.body_html, '<div>Maybe not.</div><div><br></div><div class="gmail_signature"><b>Zach</b></div><div><br></div><div class="gmail_quote">older</div>');
  assert.equal(input.message.body_text, 'Maybe not.\n\nZach\n\nolder\n');
  assert.equal(input.message.reply_to_thread_id, 't-9');
  assert.equal(input.message.in_reply_to, '<m1@example.test>');
  assert.deepEqual(input.message.references, ['<m1@example.test>']);
});

test('requestLifecycle reports an agent withdrawal with its reason and replacement', () => {
  const life = requestLifecycle({ status: 'withdrawn', withdrawn_by: 'codex', error: 'sent by hand', superseded_by: 'req_2', replaces: '', withdrawn_at: '2026-09-24T10:00:00Z' });
  assert.deepEqual(life, { withdrawn: { by: 'codex', reason: 'sent by hand', at: '2026-09-24T10:00:00Z' }, supersededBy: 'req_2', replaces: '' });
  assert.equal(requestLifecycle({ status: 'pending_review', error: 'x' }).withdrawn, null);
  assert.equal(requestLifecycle({ status: 'withdrawn' }).withdrawn.by, 'an agent');
  assert.equal(requestLifecycleNote({ status: 'withdrawn', withdrawn_by: 'codex', error: 'sent by hand' }), 'Withdrawn by codex: sent by hand');
  assert.equal(requestLifecycleNote({ status: 'withdrawn' }), 'Withdrawn by an agent.');
  assert.equal(requestLifecycleNote({ status: 'rejected', error: 'no' }), '');
});

test('a Slack send is reviewed as a message to a named recipient, with its thread and warnings', () => {
  const mutation = {
    id: 'mut-send', provider: 'slack', operation: 'slack.send_message', account: 'zrl', status: 'pending_review',
    payload: { conversation_id: 'C1', user_id: '', text: 'Deploy is fixed.', thread_ts: '1593473600.000300', reply_broadcast: true },
    preview: { slack_message: {
      conversation_id: 'C1', thread_ts: '1593473600.000300', delivery: 'thread_reply', reply_broadcast: true, text: 'Draft.',
      team_id: 'T1', conversation_type: 'public_channel', conversation_name: 'ops', recipient_label: '#ops',
      conversation_found: true, thread_found: true, is_member: false,
      warnings: ['You are not a member of this channel; Slack will reject the post (not_in_channel).'],
      messages: [
        { message_ts: '1593473600.000300', user_id: 'UMARCUS', actor_name: 'Marcus', text: 'Can someone look?', is_thread_parent: true, open: { url: 'https://example.slack.com/archives/C1/p1593473600000300', app_url: 'slack://channel?team=T1&id=C1' } },
        { message_ts: '1593473660.000400', user_id: 'UME', actor_name: 'Zach', is_from_me: true, text: 'Looking.' },
      ],
      open: { url: 'https://example.slack.com/archives/C1/p1593473600000300' },
    } },
  };
  assert.equal(isSlackSendMessageMutation(mutation), true);
  assert.equal(isSlackMarkReadMutation(mutation), false);
  const review = slackSendMessageReview(mutation);
  assert.equal(review.heading, 'Reply in Slack thread');
  assert.equal(review.deliveryLabel, 'Thread reply');
  assert.equal(review.recipientLabel, '#ops');
  assert.equal(review.text, 'Deploy is fixed.');
  assert.equal(review.replyBroadcast, true);
  assert.equal(review.verified, true);
  assert.equal(review.warnings.length, 1);
  assert.equal(review.contextLabel, 'The thread');
  assert.equal(review.messages[0].isThreadParent, true);
  assert.equal(review.messages[0].open.app_url, 'slack://channel?team=T1&id=C1');
  assert.equal(review.messages[1].actorName, 'You');
  assert.equal(review.sent, null);
});

test('a Slack DM names the person, and an unresolved send warns instead of looking verified', () => {
  const dm = slackSendMessageReview({
    provider: 'slack', operation: 'slack.send_message', account: 'zrl',
    payload: { conversation_id: '', user_id: 'UMARCUS', text: 'Hi.' },
    preview: { slack_message: { user_id: 'UMARCUS', delivery: 'dm', team_id: 'T1', conversation_type: 'im', recipient_label: 'Marcus', recipient_found: true, resolved_conversation_id: 'D1', avatar_url: 'https://avatars.example.test/m.png', warnings: [] } },
    result: { message_ts: '1700000000.000100', already_sent: true },
  });
  assert.equal(dm.heading, 'Send Slack DM');
  assert.equal(dm.recipientLabel, 'Marcus');
  assert.equal(dm.conversationId, 'D1');
  assert.equal(dm.avatarUrl, 'https://avatars.example.test/m.png');
  assert.deepEqual(dm.sent, { messageTs: '1700000000.000100', alreadySent: true });
  const unresolved = slackSendMessageReview({ provider: 'slack', operation: 'slack.send_message', account: 'zrl', payload: { conversation_id: 'C9', text: 'hi' }, preview: {} });
  assert.equal(unresolved.resolved, false);
  assert.equal(unresolved.verified, false);
  assert.equal(unresolved.recipientLabel, 'C9');
  assert.match(unresolved.warnings[0], /not checked against the warehouse/);
});
