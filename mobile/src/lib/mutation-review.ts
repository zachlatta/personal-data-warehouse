import type { TimelineDeepLink } from './api';

type MutationLike = {
  id?: string;
  provider?: string;
  operation?: string;
  account?: string;
  status?: string;
  payload?: Record<string, unknown>;
  preview?: Record<string, unknown>;
  result?: Record<string, unknown>;
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

// --- calendar review -------------------------------------------------------
//
// A calendar mutation is a scheduling decision, not a JSON-inspection task.
// The server resolves the proposal's timezone and places the owner's synced
// events for that day in preview.calendar_day. These helpers turn that stable
// snapshot into the calendar, conflict summary, and guest list the phone
// renders. Keeping the calculations pure makes the difficult parts — timezone
// display, declined events, overlap lanes — testable without a simulator.

export function isCalendarCreateMutation(mutation: MutationLike): boolean {
  return mutation.provider === 'google_calendar' && mutation.operation === 'calendar.create_event';
}

export type CalendarReviewAttendee = {
  email: string;
  displayName: string;
  responseStatus: string;
  responseLabel: string;
  organizer: boolean;
  optional: boolean;
  self: boolean;
  resource: boolean;
  comment: string;
  additionalGuests: number;
};

export type CalendarReviewEvent = {
  id: string;
  calendarId: string;
  title: string;
  description: string;
  location: string;
  startAt: string;
  endAt: string;
  startDate: string;
  endDate: string;
  allDay: boolean;
  status: string;
  transparency: string;
  visibility: string;
  eventType: string;
  colorId: string;
  creatorEmail: string;
  organizerEmail: string;
  htmlLink: string;
  conferenceLink: string;
  attendees: CalendarReviewAttendee[];
  recurrence: string[];
  reminders: Record<string, unknown>;
  proposed: boolean;
  raw: Record<string, unknown>;
};

export type CalendarMutationReview = {
  operation: 'create';
  account: string;
  calendarId: string;
  sendUpdates: string;
  title: string;
  timeZone: string;
  dateLabel: string;
  timeLabel: string;
  durationLabel: string;
  dayStart: string;
  dayEnd: string;
  sourceSyncedAt: string;
  availability: 'clear' | 'conflict' | 'unavailable';
  proposed: CalendarReviewEvent;
  attendees: CalendarReviewAttendee[];
  otherAttendees: CalendarReviewAttendee[];
  conflicts: CalendarReviewEvent[];
  existingEvents: CalendarReviewEvent[];
  timedEvents: CalendarReviewEvent[];
  allDayEvents: CalendarReviewEvent[];
};

export type CalendarLayoutBlock = {
  event: CalendarReviewEvent;
  top: number;
  height: number;
  column: number;
  columnCount: number;
  conflict: boolean;
};

export type CalendarDayLayout = {
  startHour: number;
  endHour: number;
  height: number;
  hourHeight: number;
  hours: { hour: number; label: string; top: number }[];
  blocks: CalendarLayoutBlock[];
};

function calendarAttendees(value: unknown, account: string): CalendarReviewAttendee[] {
  return records(value).map((item) => {
    const email = text(item.email);
    const displayName = text(item.displayName) || text(item.display_name);
    const responseStatus = text(item.responseStatus) || text(item.response_status);
    return {
      email,
      displayName,
      responseStatus,
      responseLabel: calendarResponseLabel(responseStatus),
      organizer: item.organizer === true,
      optional: item.optional === true,
      self: item.self === true || (!!email && email.toLowerCase() === account.toLowerCase()),
      resource: item.resource === true,
      comment: text(item.comment),
      additionalGuests: count(item.additionalGuests ?? item.additional_guests),
    };
  }).filter((attendee) => attendee.email || attendee.displayName);
}

function calendarResponseLabel(status: string): string {
  switch (status) {
    case 'accepted': return 'Accepted';
    case 'declined': return 'Declined';
    case 'tentative': return 'Maybe';
    case 'needsAction': return 'Awaiting reply';
    default: return status;
  }
}

function calendarConferenceLink(event: Record<string, unknown>): string {
  const direct = text(event.conference_link) || text(event.hangoutLink);
  if (direct) return direct;
  const entries = records(asRecord(event.conferenceData).entryPoints);
  return text(entries.find((entry) => text(entry.uri))?.uri);
}

function calendarEventFromRecord(rawValue: unknown, account: string, proposed = false): CalendarReviewEvent {
  const raw = asRecord(rawValue);
  const start = asRecord(raw.start);
  const end = asRecord(raw.end);
  const allDay = raw.is_all_day === true || raw.is_all_day === 1 || !!text(raw.start_date) || !!text(start.date);
  return {
    id: text(raw.event_id) || (proposed ? 'proposed' : ''),
    calendarId: text(raw.calendar_id) || 'primary',
    title: text(raw.summary) || '(untitled event)',
    description: text(raw.description),
    location: text(raw.location),
    startAt: text(raw.start_at) || text(start.dateTime),
    endAt: text(raw.end_at) || text(end.dateTime),
    startDate: text(raw.start_date) || text(start.date),
    endDate: text(raw.end_date) || text(end.date),
    allDay,
    status: text(raw.status) || 'confirmed',
    transparency: text(raw.transparency) || 'opaque',
    visibility: text(raw.visibility) || 'default',
    eventType: text(raw.event_type) || text(raw.eventType) || 'default',
    colorId: text(raw.color_id) || text(raw.colorId),
    creatorEmail: text(raw.creator_email) || text(asRecord(raw.creator).email),
    organizerEmail: text(raw.organizer_email) || text(asRecord(raw.organizer).email),
    htmlLink: text(raw.html_link) || text(raw.htmlLink),
    conferenceLink: calendarConferenceLink(raw),
    attendees: calendarAttendees(raw.attendees, account),
    recurrence: stringList(raw.recurrence),
    reminders: asRecord(raw.reminders),
    proposed,
    raw,
  };
}

function proposalEventRecord(mutation: MutationLike): Record<string, unknown> {
  const payload = asRecord(mutation.payload);
  const preview = asRecord(mutation.preview);
  const previewEvent = asRecord(preview.event);
  const payloadEvent = mutation.operation === 'calendar.update_event' ? asRecord(payload.patch) : asRecord(payload.event);
  const merged = { ...payloadEvent, ...previewEvent };
  // The stored preview historically retained fewer attendee flags than the
  // executable payload. Prefer the payload's full invite when it exists.
  if (Array.isArray(payloadEvent.attendees)) merged.attendees = payloadEvent.attendees;
  return merged;
}

function validDate(value: string): Date | null {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function formatCalendarDate(instant: Date | null, dateValue: string, timeZone: string): string {
  if (instant) return new Intl.DateTimeFormat('en-US', { timeZone: timeZone || undefined, weekday: 'long', month: 'long', day: 'numeric' }).format(instant);
  if (dateValue) {
    const date = new Date(`${dateValue}T12:00:00Z`);
    if (!Number.isNaN(date.getTime())) return new Intl.DateTimeFormat('en-US', { timeZone: 'UTC', weekday: 'long', month: 'long', day: 'numeric' }).format(date);
  }
  return 'Date unavailable';
}

function timeParts(instant: Date, timeZone: string): { clock: string; period: string; zone: string } {
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: timeZone || undefined,
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
    timeZoneName: 'short',
  });
  const parts: Record<string, string> = {};
  for (const part of formatter.formatToParts(instant)) parts[part.type] = part.value;
  return {
    clock: `${parts.hour ?? ''}:${parts.minute ?? '00'}`,
    period: parts.dayPeriod ?? '',
    zone: parts.timeZoneName ?? '',
  };
}

function calendarTimeRange(start: Date | null, end: Date | null, allDay: boolean, timeZone: string): string {
  if (allDay) return 'All day';
  if (!start) return 'Time unavailable';
  const first = timeParts(start, timeZone);
  if (!end) return `${first.clock} ${first.period} ${first.zone}`.trim();
  const last = timeParts(end, timeZone);
  const startPeriod = first.period === last.period ? '' : ` ${first.period}`;
  const zone = first.zone === last.zone ? first.zone : `${first.zone}/${last.zone}`;
  return `${first.clock}${startPeriod}–${last.clock} ${last.period}${zone ? ` ${zone}` : ''}`.trim();
}

function calendarDuration(start: Date | null, end: Date | null, allDay: boolean, startDate: string, endDate: string): string {
  let minutes = start && end ? Math.round((end.getTime() - start.getTime()) / 60000) : 0;
  if (allDay && startDate && endDate) minutes = Math.round((Date.parse(`${endDate}T00:00:00Z`) - Date.parse(`${startDate}T00:00:00Z`)) / 60000);
  if (!Number.isFinite(minutes) || minutes <= 0) return '';
  if (allDay) {
    const days = Math.max(1, Math.round(minutes / 1440));
    return days === 1 ? '1 day' : `${days} days`;
  }
  const hours = Math.floor(minutes / 60);
  const remainder = minutes % 60;
  if (!hours) return `${remainder} min`;
  if (!remainder) return `${hours} hr`;
  return `${hours} hr ${remainder} min`;
}

function eventDeclinedByOwner(event: CalendarReviewEvent): boolean {
  return event.attendees.some((attendee) => attendee.self && attendee.responseStatus === 'declined');
}

function calendarEventsOverlap(proposed: CalendarReviewEvent, existing: CalendarReviewEvent, dayStart: string, dayEnd: string): boolean {
  if (existing.status === 'cancelled' || existing.transparency === 'transparent' || eventDeclinedByOwner(existing)) return false;
  if (proposed.id && proposed.id !== 'proposed' && proposed.id === existing.id) return false;
  if (existing.allDay) {
    const proposalStartDate = (proposed.startDate || dayStart || proposed.startAt).slice(0, 10);
    const proposalEndDate = (proposed.endDate || dayEnd).slice(0, 10);
    return !!proposalStartDate
      && (!existing.startDate || !proposalEndDate || existing.startDate < proposalEndDate)
      && (!existing.endDate || existing.endDate > proposalStartDate);
  }
  const proposedStart = validDate(proposed.startAt);
  const proposedEnd = validDate(proposed.endAt);
  const existingStart = validDate(existing.startAt);
  const existingEnd = validDate(existing.endAt);
  if (!proposedStart || !proposedEnd || !existingStart || !existingEnd) return false;
  return existingStart < proposedEnd && existingEnd > proposedStart;
}

export function calendarMutationReview(mutation: MutationLike): CalendarMutationReview {
  const payload = asRecord(mutation.payload);
  const result = asRecord(mutation.result);
  const preview = asRecord(mutation.preview);
  const day = asRecord(preview.calendar_day);
  const proposedInput = proposalEventRecord(mutation);
  // Once execution has returned, Google's response is the most complete form
  // of the event (real id/link, organizer, conference data and RSVP state).
  // Pending reviews still use the exact executable payload.
  const executedEvent = asRecord(result.response);
  const proposedRaw = { ...proposedInput, ...executedEvent };
  const account = text(mutation.account);
  const proposed = calendarEventFromRecord({
    ...proposedRaw,
    start_at: text(day.proposed_start_at) || text(proposedRaw.start_at),
    end_at: text(day.proposed_end_at) || text(proposedRaw.end_at),
    start_date: text(day.proposed_start_date) || text(proposedRaw.start_date),
    end_date: text(day.proposed_end_date) || text(proposedRaw.end_date),
    is_all_day: day.proposed_is_all_day === true || proposedRaw.is_all_day,
  }, account, true);
  proposed.id = text(proposedRaw.event_id) || text(payload.event_id) || text(result.event_id) || text(executedEvent.id) || proposed.id;
  proposed.calendarId = text(proposedRaw.calendar_id) || text(payload.calendar_id) || text(result.calendar_id) || proposed.calendarId;

  const existingEvents = records(day.events)
    .map((event) => calendarEventFromRecord(event, account))
    // The mutation calls the target calendar "primary" while the sync may
    // store that same calendar by its email address. Google's event id is the
    // stable identity across those aliases, so an executed add must not draw
    // or conflict with itself a second time.
    .filter((event) => proposed.id === 'proposed' || event.id !== proposed.id);
  const conflicts = existingEvents.filter((event) => calendarEventsOverlap(proposed, event, text(day.day_start), text(day.day_end)));
  const timedEvents = [...existingEvents.filter((event) => !event.allDay), ...(proposed.allDay ? [] : [proposed])]
    .sort((a, b) => {
      const start = (validDate(a.startAt)?.getTime() ?? Number.MAX_SAFE_INTEGER) - (validDate(b.startAt)?.getTime() ?? Number.MAX_SAFE_INTEGER);
      if (start) return start;
      if (a.proposed !== b.proposed) return a.proposed ? -1 : 1;
      return a.title.localeCompare(b.title);
    });
  const allDayEvents = [...existingEvents.filter((event) => event.allDay), ...(proposed.allDay ? [proposed] : [])];
  const start = validDate(proposed.startAt);
  const end = validDate(proposed.endAt);
  const timeZone = text(day.time_zone) || text(asRecord(proposedRaw.start).timeZone) || 'America/New_York';
  const attendees = proposed.attendees;
  const otherAttendees = attendees.filter((attendee) => !attendee.self && !attendee.resource);
  return {
    operation: 'create',
    account,
    calendarId: proposed.calendarId,
    sendUpdates: text(proposedRaw.send_updates) || text(payload.send_updates) || 'all',
    title: proposed.title,
    timeZone,
    dateLabel: formatCalendarDate(start, proposed.startDate, timeZone),
    timeLabel: calendarTimeRange(start, end, proposed.allDay, timeZone),
    durationLabel: calendarDuration(start, end, proposed.allDay, proposed.startDate, proposed.endDate),
    dayStart: text(day.day_start),
    dayEnd: text(day.day_end),
    sourceSyncedAt: text(day.source_synced_at),
    availability: Object.keys(day).length === 0 ? 'unavailable' : conflicts.length ? 'conflict' : 'clear',
    proposed,
    attendees,
    otherAttendees,
    conflicts,
    existingEvents,
    timedEvents,
    allDayEvents,
  };
}

function layoutMinute(iso: string, dayStartMs: number): number | null {
  const value = validDate(iso);
  if (!value) return null;
  return Math.max(0, Math.min(1440, (value.getTime() - dayStartMs) / 60000));
}

function hourLabel(hour: number): string {
  if (hour === 0 || hour === 24) return '12 AM';
  if (hour === 12) return '12 PM';
  return hour < 12 ? `${hour} AM` : `${hour - 12} PM`;
}

export function calendarDayLayout(review: CalendarMutationReview, hourHeight = 72): CalendarDayLayout {
  let dayStartMs = validDate(review.dayStart)?.getTime();
  if (!Number.isFinite(dayStartMs)) {
    const proposedStart = validDate(review.proposed.startAt) ?? new Date();
    const localStart = new Date(proposedStart.getFullYear(), proposedStart.getMonth(), proposedStart.getDate());
    dayStartMs = localStart.getTime();
  }
  const conflictIDs = new Set(review.conflicts.map((event) => `${event.calendarId}\0${event.id}`));
  const rawBlocks = review.timedEvents.flatMap((event) => {
    const start = layoutMinute(event.startAt, dayStartMs as number);
    const end = layoutMinute(event.endAt, dayStartMs as number);
    if (start === null || end === null || end <= start) return [];
    return [{ event, start, end, column: 0, columnCount: 1 }];
  }).sort((a, b) => a.start - b.start || a.end - b.end || (a.event.proposed ? -1 : 1));

  let cluster: typeof rawBlocks = [];
  let clusterEnd = -1;
  const flushCluster = () => {
    if (!cluster.length) return;
    const laneEnds: number[] = [];
    for (const block of cluster) {
      let lane = laneEnds.findIndex((end) => end <= block.start);
      if (lane < 0) lane = laneEnds.length;
      block.column = lane;
      laneEnds[lane] = block.end;
    }
    const columns = Math.max(1, laneEnds.length);
    for (const block of cluster) block.columnCount = columns;
    cluster = [];
  };
  for (const block of rawBlocks) {
    if (cluster.length && block.start >= clusterEnd) flushCluster();
    cluster.push(block);
    clusterEnd = Math.max(clusterEnd, block.end);
  }
  flushCluster();

  const minimum = rawBlocks.length ? Math.min(...rawBlocks.map((block) => block.start)) : 8 * 60;
  const maximum = rawBlocks.length ? Math.max(...rawBlocks.map((block) => block.end)) : 12 * 60;
  let startHour = Math.max(0, Math.floor(minimum / 60) - 1);
  let endHour = Math.min(24, Math.ceil(maximum / 60) + 1);
  if (endHour - startHour < 4) {
    const missing = 4 - (endHour - startHour);
    startHour = Math.max(0, startHour - Math.floor(missing / 2));
    endHour = Math.min(24, startHour + 4);
    startHour = Math.max(0, endHour - 4);
  }
  const height = (endHour - startHour) * hourHeight;
  const hours = Array.from({ length: endHour - startHour + 1 }, (_, index) => {
    const hour = startHour + index;
    return { hour, label: hourLabel(hour), top: index * hourHeight };
  });
  const blocks: CalendarLayoutBlock[] = rawBlocks.map((block) => ({
    event: block.event,
    top: Math.max(0, ((block.start - startHour * 60) / 60) * hourHeight),
    height: Math.max(42, ((block.end - block.start) / 60) * hourHeight),
    column: block.column,
    columnCount: block.columnCount,
    conflict: conflictIDs.has(`${block.event.calendarId}\0${block.event.id}`),
  }));
  return { startHour, endHour, height, hourHeight, hours, blocks };
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
