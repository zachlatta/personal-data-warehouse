import test from 'node:test';
import assert from 'node:assert/strict';

import {
  calendarDayLayout,
  calendarMutationReview,
  formatGmailLabel,
  gmailBatchSummary,
  gmailSenderName,
  gmailThreadDayGroups,
  gmailThreadReviews,
  gmailThreadUrl,
  isCalendarCreateMutation,
  isGmailThreadMutation,
  isSlackMarkReadMutation,
  looksAutomatedSender,
  mutationReviewContext,
  slackMarkReadGroups,
  slackMarkReadReview,
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
