import { useMemo, useState } from 'react';
import { Linking, Pressable, StyleSheet, View, type DimensionValue } from 'react-native';

import { StatusPill } from '@/components/status-pill';
import { ThemedText } from '@/components/themed-text';
import { useTheme } from '@/hooks/use-theme';
import type { Mutation } from '@/lib/api';
import { formatWhen, pretty } from '@/lib/format';
import {
  calendarDayLayout,
  calendarMutationReview,
  type CalendarReviewAttendee,
  type CalendarReviewEvent,
} from '@/lib/mutation-review';

const CALENDAR_BLUE = '#2563EB';
const CALENDAR_BLUE_LIGHT = '#60A5FA';
const CLEAR_GREEN = '#16A34A';
const CONFLICT_RED = '#DC2626';
const WARNING_AMBER = '#D97706';
const TIME_GUTTER = 54;

function calendarTile(review: ReturnType<typeof calendarMutationReview>): { month: string; day: string } {
  if (review.proposed.startDate) {
    const [year, month, day] = review.proposed.startDate.split('-').map(Number);
    if (year && month && day) {
      const date = new Date(Date.UTC(year, month - 1, day, 12));
      return {
        month: date.toLocaleDateString('en-US', { timeZone: 'UTC', month: 'short' }).toUpperCase(),
        day: String(day),
      };
    }
  }
  const date = new Date(review.proposed.startAt);
  if (!Number.isNaN(date.getTime())) {
    return {
      month: date.toLocaleDateString('en-US', { timeZone: review.timeZone, month: 'short' }).toUpperCase(),
      day: date.toLocaleDateString('en-US', { timeZone: review.timeZone, day: 'numeric' }),
    };
  }
  return { month: 'DATE', day: '—' };
}

function notificationsLabel(sendUpdates: string): string {
  if (sendUpdates === 'none') return 'Do not email guests';
  if (sendUpdates === 'externalOnly') return 'Email external guests only';
  return 'Email all guests';
}

function compactEventTime(event: CalendarReviewEvent, timeZone: string): string {
  if (event.allDay) return 'All day';
  const start = new Date(event.startAt);
  const end = new Date(event.endAt);
  if (Number.isNaN(start.getTime())) return 'Time unavailable';
  const one = (date: Date) => date.toLocaleTimeString('en-US', { timeZone, hour: 'numeric', minute: '2-digit' }).replace(' ', '\u00a0');
  if (Number.isNaN(end.getTime())) return one(start);
  return `${one(start)}–${one(end)}`;
}

function attendeeInitials(attendee: CalendarReviewAttendee): string {
  const source = attendee.displayName || attendee.email || '?';
  const beforeAt = source.split('@')[0].replace(/[._-]/g, ' ');
  const words = beforeAt.split(/\s+/).filter(Boolean);
  if (!words.length) return '?';
  if (words.length === 1) return words[0][0].toUpperCase();
  return `${words[0][0]}${words[words.length - 1][0]}`.toUpperCase();
}

function attendeeColor(attendee: CalendarReviewAttendee): string {
  const palette = ['#2563EB', '#7C3AED', '#DB2777', '#C2410C', '#0F766E', '#4D7C0F'];
  const seed = attendee.email || attendee.displayName;
  let hash = 0;
  for (const character of seed) hash = (hash + (character.codePointAt(0) ?? 0)) % palette.length;
  return palette[hash];
}

function responseColor(status: string): string {
  if (status === 'accepted') return CLEAR_GREEN;
  if (status === 'declined') return CONFLICT_RED;
  if (status === 'tentative') return WARNING_AMBER;
  return '#6B7280';
}

function openURL(url: string) {
  if (url) void Linking.openURL(url);
}

function Availability({ review }: { review: ReturnType<typeof calendarMutationReview> }) {
  const theme = useTheme();
  const conflict = review.availability === 'conflict';
  const unavailable = review.availability === 'unavailable';
  const color = conflict ? CONFLICT_RED : unavailable ? WARNING_AMBER : CLEAR_GREEN;
  const headline = conflict
    ? `${review.conflicts.length} calendar conflict${review.conflicts.length === 1 ? '' : 's'}`
    : unavailable ? 'Availability not loaded' : 'No calendar conflicts';
  const freshness = review.sourceSyncedAt ? ` Schedule data: ${formatWhen(review.sourceSyncedAt)}.` : '';
  const detail = conflict
    ? review.conflicts.map((event) => `${event.title} · ${compactEventTime(event, review.timeZone)}`).join('\n')
    : unavailable
      ? 'The invite is complete below, but nearby events could not be read. This is not a claim that the time is clear.'
      : `Nothing else marked busy overlaps this event.${freshness}`;
  return (
    <View style={[styles.availability, { borderColor: `${color}66`, backgroundColor: `${color}14` }]}>
      <View style={[styles.availabilityIcon, { backgroundColor: `${color}22` }]}>
        <ThemedText style={[styles.availabilityIconText, { color }]}>{conflict ? '!' : unavailable ? '?' : '✓'}</ThemedText>
      </View>
      <View style={styles.availabilityCopy}>
        <ThemedText type="smallBold" style={{ color }}>{headline}</ThemedText>
        <ThemedText type="small" style={[styles.availabilityDetail, { color: theme.textSecondary }]}>{detail}</ThemedText>
      </View>
    </View>
  );
}

function AllDayRows({ events, review }: { events: CalendarReviewEvent[]; review: ReturnType<typeof calendarMutationReview> }) {
  if (!events.length) return null;
  const conflictIDs = new Set(review.conflicts.map((event) => `${event.calendarId}\0${event.id}`));
  return (
    <View style={styles.allDaySection}>
      <ThemedText type="small" themeColor="textSecondary" style={styles.allDayLabel}>ALL-DAY</ThemedText>
      <View style={styles.allDayRows}>
        {events.map((event, index) => {
          const conflict = conflictIDs.has(`${event.calendarId}\0${event.id}`);
          return (
            <Pressable
              accessibilityRole={event.htmlLink ? 'link' : undefined}
              key={`${event.calendarId}:${event.id}:${index}`}
              disabled={!event.htmlLink}
              onPress={() => openURL(event.htmlLink)}
              style={[
                styles.allDayEvent,
                event.proposed && styles.allDayProposed,
                conflict && styles.allDayConflict,
                event.transparency === 'transparent' && styles.transparentEvent,
                event.attendees.some((attendee) => attendee.self && attendee.responseStatus === 'declined') && styles.declinedEvent,
              ]}>
              <ThemedText type="smallBold" style={event.proposed ? styles.whiteText : undefined} numberOfLines={2}>{event.title}</ThemedText>
              <View style={styles.inlineTags}>
                {event.proposed ? <ThemedText style={styles.proposedTag}>TO ADD</ThemedText> : null}
                {conflict ? <ThemedText style={styles.conflictTag}>BUSY</ThemedText> : null}
              </View>
            </Pressable>
          );
        })}
      </View>
    </View>
  );
}

function DayCalendar({ review }: { review: ReturnType<typeof calendarMutationReview> }) {
  const theme = useTheme();
  const layout = useMemo(() => calendarDayLayout(review), [review]);
  return (
    <View style={[styles.calendar, { backgroundColor: theme.background }]}>
      <View style={styles.calendarHeader}>
        <View>
          <ThemedText type="smallBold">{review.dateLabel}</ThemedText>
          <ThemedText type="small" themeColor="textSecondary">
            {review.existingEvents.length} other event{review.existingEvents.length === 1 ? '' : 's'} · {review.timeZone}
          </ThemedText>
        </View>
        <View style={styles.calendarLegend}>
          <View style={[styles.legendDot, { backgroundColor: CALENDAR_BLUE }]} />
          <ThemedText type="small" themeColor="textSecondary">to add</ThemedText>
        </View>
      </View>

      <AllDayRows events={review.allDayEvents} review={review} />

      {layout.blocks.length ? (
        <View style={[styles.timeGrid, { height: layout.height }]}>
          {layout.hours.map((hour) => (
            <View key={hour.hour} style={[styles.hourLine, { top: hour.top, borderTopColor: theme.backgroundSelected }]}>
              {hour.hour < layout.endHour ? <ThemedText type="small" themeColor="textSecondary" style={styles.hourLabel}>{hour.label}</ThemedText> : null}
            </View>
          ))}
          <View pointerEvents="box-none" style={[styles.eventCanvas, { left: TIME_GUTTER }]}>
            {layout.blocks.map((block, index) => {
              const event = block.event;
              const left = `${(block.column / block.columnCount) * 100}%` as DimensionValue;
              const width = `${100 / block.columnCount}%` as DimensionValue;
              const blockBackground = event.proposed ? CALENDAR_BLUE : theme.backgroundElement;
              const textColor = event.proposed ? '#FFFFFF' : theme.text;
              const secondaryColor = event.proposed ? '#DBEAFE' : theme.textSecondary;
              return (
                <Pressable
                  accessibilityLabel={`${event.proposed ? 'Event to add' : 'Existing event'}: ${event.title}, ${compactEventTime(event, review.timeZone)}`}
                  accessibilityRole={event.htmlLink ? 'link' : undefined}
                  disabled={!event.htmlLink}
                  key={`${event.calendarId}:${event.id}:${index}`}
                  onPress={() => openURL(event.htmlLink)}
                  style={[
                    styles.eventBlock,
                    {
                      top: block.top,
                      height: block.height,
                      left,
                      width,
                      backgroundColor: blockBackground,
                      borderColor: block.conflict ? CONFLICT_RED : event.proposed ? CALENDAR_BLUE_LIGHT : theme.backgroundSelected,
                    },
                    event.transparency === 'transparent' && styles.transparentEvent,
                    event.attendees.some((attendee) => attendee.self && attendee.responseStatus === 'declined') && styles.declinedEvent,
                  ]}>
                  <ThemedText style={[styles.eventTime, { color: secondaryColor }]} numberOfLines={1}>{compactEventTime(event, review.timeZone)}</ThemedText>
                  <ThemedText style={[styles.eventTitle, { color: textColor }]} numberOfLines={block.height > 64 ? 2 : 1}>{event.title}</ThemedText>
                  {block.height > 74 && event.location ? <ThemedText style={[styles.eventMeta, { color: secondaryColor }]} numberOfLines={1}>⌖ {event.location}</ThemedText> : null}
                  {event.proposed ? <ThemedText style={styles.blockProposedTag}>TO ADD</ThemedText> : null}
                  {block.conflict ? <View style={styles.conflictRail} /> : null}
                </Pressable>
              );
            })}
          </View>
        </View>
      ) : null}

      {review.existingEvents.length ? <Agenda review={review} /> : (
        <View style={[styles.emptyDay, { borderTopColor: theme.backgroundSelected }]}>
          <ThemedText type="small" themeColor="textSecondary">No other synced events on this day.</ThemedText>
        </View>
      )}
    </View>
  );
}

function Agenda({ review }: { review: ReturnType<typeof calendarMutationReview> }) {
  const theme = useTheme();
  const conflictIDs = new Set(review.conflicts.map((event) => `${event.calendarId}\0${event.id}`));
  const events = [...review.allDayEvents, ...review.timedEvents].filter((event) => !event.proposed);
  return (
    <View style={[styles.agenda, { borderTopColor: theme.backgroundSelected }]}>
      <ThemedText type="smallBold" themeColor="textSecondary" style={styles.sectionEyebrow}>DAY AGENDA</ThemedText>
      {events.map((event, index) => {
        const conflict = conflictIDs.has(`${event.calendarId}\0${event.id}`);
        const guestCount = event.attendees.filter((attendee) => !attendee.self && !attendee.resource).length;
        return (
          <Pressable
            accessibilityRole={event.htmlLink ? 'link' : undefined}
            disabled={!event.htmlLink}
            key={`${event.calendarId}:${event.id}:agenda:${index}`}
            onPress={() => openURL(event.htmlLink)}
            style={[
              styles.agendaRow,
              { borderBottomColor: theme.backgroundSelected },
              event.transparency === 'transparent' && styles.transparentAgenda,
              event.attendees.some((attendee) => attendee.self && attendee.responseStatus === 'declined') && styles.declinedEvent,
            ]}>
            <ThemedText type="small" themeColor="textSecondary" style={styles.agendaTime}>{compactEventTime(event, review.timeZone)}</ThemedText>
            <View style={styles.agendaCopy}>
              <ThemedText type="smallBold" numberOfLines={2}>{event.title}</ThemedText>
              {event.location || guestCount ? (
                <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>
                  {[event.location, guestCount ? `${guestCount} guest${guestCount === 1 ? '' : 's'}` : ''].filter(Boolean).join(' · ')}
                </ThemedText>
              ) : null}
            </View>
            {conflict ? <ThemedText style={styles.agendaConflict}>CONFLICT</ThemedText> : null}
            {event.htmlLink ? <ThemedText themeColor="textSecondary">↗</ThemedText> : null}
          </Pressable>
        );
      })}
    </View>
  );
}

function Invite({ review }: { review: ReturnType<typeof calendarMutationReview> }) {
  const theme = useTheme();
  const people = review.attendees;
  return (
    <View style={[styles.section, { backgroundColor: theme.background }]}>
      <View style={styles.sectionHeader}>
        <View>
          <ThemedText type="smallBold" themeColor="textSecondary" style={styles.sectionEyebrow}>INVITE</ThemedText>
          <ThemedText type="subtitle" style={styles.sectionTitle}>
            {review.otherAttendees.length ? `${review.otherAttendees.length} other${review.otherAttendees.length === 1 ? '' : 's'}` : 'Just you'}
          </ThemedText>
        </View>
        <ThemedText type="small" themeColor="textSecondary">{notificationsLabel(review.sendUpdates)}</ThemedText>
      </View>
      {!people.length ? (
        <View style={[styles.noGuests, { backgroundColor: theme.backgroundElement }]}>
          <ThemedText type="smallBold">No guests on this event</ThemedText>
          <ThemedText type="small" themeColor="textSecondary">Approving adds it only to {review.account || 'your calendar'}.</ThemedText>
        </View>
      ) : people.map((attendee, index) => (
        <View key={`${attendee.email}:${index}`} style={[styles.attendee, { borderTopColor: theme.backgroundSelected }]}>
          <View style={[styles.avatar, { backgroundColor: attendeeColor(attendee) }]}><ThemedText style={styles.avatarText}>{attendeeInitials(attendee)}</ThemedText></View>
          <View style={styles.attendeeCopy}>
            <ThemedText type="smallBold">{attendee.self ? 'You' : attendee.displayName || attendee.email}</ThemedText>
            {attendee.email && (attendee.self || attendee.displayName) ? <ThemedText type="small" themeColor="textSecondary" selectable>{attendee.email}</ThemedText> : null}
            {attendee.comment ? <ThemedText type="small" themeColor="textSecondary">“{attendee.comment}”</ThemedText> : null}
          </View>
          <View style={styles.attendeeTags}>
            {attendee.organizer ? <ThemedText style={styles.neutralTag}>ORGANIZER</ThemedText> : null}
            {attendee.optional ? <ThemedText style={styles.neutralTag}>OPTIONAL</ThemedText> : null}
            {attendee.resource ? <ThemedText style={styles.neutralTag}>ROOM</ThemedText> : null}
            {attendee.additionalGuests ? <ThemedText style={styles.neutralTag}>+{attendee.additionalGuests}</ThemedText> : null}
            {attendee.responseLabel ? <ThemedText style={[styles.responseTag, { color: responseColor(attendee.responseStatus), borderColor: `${responseColor(attendee.responseStatus)}66` }]}>{attendee.responseLabel}</ThemedText> : null}
          </View>
        </View>
      ))}
    </View>
  );
}

function Details({ mutation, review }: { mutation: Mutation; review: ReturnType<typeof calendarMutationReview> }) {
  const theme = useTheme();
  const [showTechnical, setShowTechnical] = useState(false);
  const event = review.proposed;
  const raw = event.raw;
  const guestRules = [
    raw.guestsCanModify === true ? 'Guests can modify' : '',
    raw.guestsCanInviteOthers === false ? 'Guests cannot invite others' : '',
    raw.guestsCanSeeOtherGuests === false ? 'Guest list hidden from guests' : '',
    raw.anyoneCanAddSelf === true ? 'Anyone can add themselves' : '',
  ].filter(Boolean);
  const reminderText = Object.keys(event.reminders).length ? pretty(event.reminders) : 'Calendar default';
  return (
    <View style={[styles.section, { backgroundColor: theme.background }]}>
      <ThemedText type="smallBold" themeColor="textSecondary" style={styles.sectionEyebrow}>EVENT DETAILS</ThemedText>
      {event.location ? <DetailRow icon="⌖" label="Location" value={event.location} /> : null}
      {event.conferenceLink ? <DetailRow icon="◉" label="Video call" value={event.conferenceLink} link /> : null}
      {event.description ? (
        <View style={styles.description}>
          <ThemedText type="small" themeColor="textSecondary">Description</ThemedText>
          <ThemedText selectable>{event.description}</ThemedText>
        </View>
      ) : null}
      <View style={[styles.detailGrid, { borderTopColor: theme.backgroundSelected }]}>
        <DetailFact label="Account" value={review.account} />
        <DetailFact label="Calendar" value={review.calendarId} />
        <DetailFact label="Notifications" value={notificationsLabel(review.sendUpdates)} />
        <DetailFact label="Availability" value={event.transparency === 'transparent' ? 'Free' : 'Busy'} />
        <DetailFact label="Visibility" value={event.visibility === 'default' ? 'Calendar default' : event.visibility} />
        <DetailFact label="Organizer" value={event.organizerEmail || review.account} />
        {event.creatorEmail && event.creatorEmail !== event.organizerEmail ? <DetailFact label="Created by" value={event.creatorEmail} /> : null}
        {event.recurrence.length ? <DetailFact label="Repeats" value={event.recurrence.join('; ')} /> : null}
        <DetailFact label="Reminders" value={reminderText} />
        {guestRules.length ? <DetailFact label="Guest permissions" value={guestRules.join(' · ')} /> : null}
        {event.eventType !== 'default' ? <DetailFact label="Event type" value={event.eventType} /> : null}
        {review.sourceSyncedAt ? <DetailFact label="Schedule data" value={formatWhen(review.sourceSyncedAt)} /> : null}
      </View>
      <Pressable accessibilityRole="button" onPress={() => setShowTechnical((value) => !value)} style={[styles.technicalToggle, { borderTopColor: theme.backgroundSelected }]}>
        <ThemedText type="smallBold">{showTechnical ? 'Hide' : 'Show'} technical details</ThemedText>
        <ThemedText themeColor="textSecondary">{showTechnical ? '⌃' : '⌄'}</ThemedText>
      </Pressable>
      {showTechnical ? (
        <View style={[styles.technical, { backgroundColor: theme.backgroundElement }]}>
          <ThemedText type="code" selectable>{pretty(mutation)}</ThemedText>
        </View>
      ) : null}
    </View>
  );
}

function DetailRow({ icon, label, value, link = false }: { icon: string; label: string; value: string; link?: boolean }) {
  return (
    <Pressable accessibilityRole={link ? 'link' : undefined} disabled={!link} onPress={() => openURL(value)} style={styles.detailRow}>
      <View style={styles.detailIcon}><ThemedText>{icon}</ThemedText></View>
      <View style={styles.detailRowCopy}>
        <ThemedText type="small" themeColor="textSecondary">{label}</ThemedText>
        <ThemedText style={link ? styles.linkText : undefined} selectable={!link}>{value}</ThemedText>
      </View>
      {link ? <ThemedText themeColor="textSecondary">↗</ThemedText> : null}
    </Pressable>
  );
}

function DetailFact({ label, value }: { label: string; value: string }) {
  if (!value) return null;
  return (
    <View style={styles.detailFact}>
      <ThemedText type="small" themeColor="textSecondary">{label}</ThemedText>
      <ThemedText type="small" selectable>{value}</ThemedText>
    </View>
  );
}

export function CalendarMutationCard({ mutation, requestReason }: { mutation: Mutation; requestReason?: string }) {
  const theme = useTheme();
  const review = useMemo(() => calendarMutationReview(mutation), [mutation]);
  const tile = calendarTile(review);
  const removed = mutation.status === 'removed' || mutation.status === 'skipped';
  return (
    <View style={[styles.card, { backgroundColor: theme.backgroundElement }, removed && styles.removed]}>
      <View style={styles.hero}>
        <View style={styles.dateTile}>
          <View style={styles.dateTileMonth}><ThemedText style={styles.dateTileMonthText}>{tile.month}</ThemedText></View>
          <ThemedText style={styles.dateTileDay}>{tile.day}</ThemedText>
        </View>
        <View style={styles.heroCopy}>
          <View style={styles.heroEyebrow}>
            <ThemedText type="smallBold" style={styles.calendarBlue}>ADD TO CALENDAR</ThemedText>
            <StatusPill status={mutation.status} />
          </View>
          <ThemedText type="subtitle" style={styles.title}>{review.title}</ThemedText>
          <ThemedText style={styles.when}>{review.timeLabel}{review.durationLabel ? ` · ${review.durationLabel}` : ''}</ThemedText>
          {review.proposed.location ? <ThemedText type="small" themeColor="textSecondary">⌖ {review.proposed.location}</ThemedText> : null}
        </View>
      </View>
      {mutation.reason && mutation.reason !== requestReason ? <ThemedText type="small" themeColor="textSecondary" style={styles.reason}>{mutation.reason}</ThemedText> : null}
      <Availability review={review} />
      <DayCalendar review={review} />
      <Invite review={review} />
      <Details mutation={mutation} review={review} />
      {mutation.error ? <ThemedText style={styles.error}>{mutation.error}</ThemedText> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  card: { borderRadius: 18, padding: 12, gap: 12, overflow: 'hidden' },
  removed: { opacity: 0.55 },
  hero: { flexDirection: 'row', gap: 12, alignItems: 'flex-start', padding: 2 },
  dateTile: { width: 58, minHeight: 64, overflow: 'hidden', borderRadius: 12, alignItems: 'center', backgroundColor: '#FFFFFF', borderWidth: StyleSheet.hairlineWidth, borderColor: '#D1D5DB' },
  dateTileMonth: { alignSelf: 'stretch', alignItems: 'center', paddingVertical: 3, backgroundColor: CALENDAR_BLUE },
  dateTileMonthText: { color: '#FFFFFF', fontSize: 10, lineHeight: 13, fontWeight: '800', letterSpacing: 1.1 },
  dateTileDay: { color: '#111827', fontSize: 28, lineHeight: 39, fontWeight: '600' },
  heroCopy: { flex: 1, minWidth: 0, gap: 3 },
  heroEyebrow: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 7 },
  calendarBlue: { color: CALENDAR_BLUE, fontSize: 11, letterSpacing: 0.7 },
  title: { fontSize: 22, lineHeight: 27 },
  when: { fontSize: 16, lineHeight: 22, fontWeight: '600' },
  reason: { paddingHorizontal: 2 },
  availability: { flexDirection: 'row', alignItems: 'flex-start', gap: 10, borderWidth: 1, borderRadius: 13, padding: 11 },
  availabilityIcon: { width: 30, height: 30, alignItems: 'center', justifyContent: 'center', borderRadius: 15 },
  availabilityIconText: { fontSize: 17, fontWeight: '800' },
  availabilityCopy: { flex: 1, minWidth: 0 },
  availabilityDetail: { lineHeight: 18, marginTop: 1 },
  calendar: { borderRadius: 14, overflow: 'hidden' },
  calendarHeader: { minHeight: 59, flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', gap: 8, paddingHorizontal: 12, paddingVertical: 9 },
  calendarLegend: { flexDirection: 'row', alignItems: 'center', gap: 5 },
  legendDot: { width: 7, height: 7, borderRadius: 4 },
  allDaySection: { flexDirection: 'row', gap: 5, paddingHorizontal: 6, paddingBottom: 8 },
  allDayLabel: { width: TIME_GUTTER - 8, textAlign: 'right', fontSize: 9, lineHeight: 15 },
  allDayRows: { flex: 1, gap: 4 },
  allDayEvent: { minHeight: 34, borderRadius: 7, borderLeftWidth: 3, borderLeftColor: '#6B7280', paddingHorizontal: 8, paddingVertical: 5, flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', gap: 6 },
  allDayProposed: { backgroundColor: CALENDAR_BLUE, borderLeftColor: CALENDAR_BLUE_LIGHT },
  allDayConflict: { borderLeftColor: CONFLICT_RED },
  inlineTags: { flexDirection: 'row', gap: 4 },
  proposedTag: { overflow: 'hidden', borderRadius: 4, paddingHorizontal: 4, paddingVertical: 1, backgroundColor: '#FFFFFF33', color: '#FFFFFF', fontSize: 8, lineHeight: 11, fontWeight: '800' },
  conflictTag: { overflow: 'hidden', borderRadius: 4, paddingHorizontal: 4, paddingVertical: 1, backgroundColor: '#DC262622', color: '#FCA5A5', fontSize: 8, lineHeight: 11, fontWeight: '800' },
  whiteText: { color: '#FFFFFF' },
  timeGrid: { position: 'relative', marginRight: 7 },
  hourLine: { position: 'absolute', left: 0, right: 0, borderTopWidth: StyleSheet.hairlineWidth },
  hourLabel: { position: 'absolute', width: TIME_GUTTER - 7, top: -9, paddingRight: 7, fontSize: 9, textAlign: 'right' },
  eventCanvas: { position: 'absolute', top: 0, right: 0, bottom: 0 },
  eventBlock: { position: 'absolute', overflow: 'hidden', borderWidth: 1, borderRadius: 7, paddingHorizontal: 6, paddingVertical: 4, marginHorizontal: 2 },
  transparentEvent: { opacity: 0.58, borderStyle: 'dashed' },
  declinedEvent: { opacity: 0.45 },
  eventTime: { fontSize: 9, lineHeight: 12, fontWeight: '600' },
  eventTitle: { fontSize: 11, lineHeight: 14, fontWeight: '700' },
  eventMeta: { fontSize: 9, lineHeight: 12 },
  blockProposedTag: { position: 'absolute', right: 4, bottom: 3, color: '#DBEAFE', fontSize: 7, lineHeight: 9, fontWeight: '900', letterSpacing: 0.6 },
  conflictRail: { position: 'absolute', top: 0, left: 0, bottom: 0, width: 3, backgroundColor: CONFLICT_RED },
  emptyDay: { minHeight: 48, alignItems: 'center', justifyContent: 'center', borderTopWidth: StyleSheet.hairlineWidth, padding: 10 },
  agenda: { borderTopWidth: StyleSheet.hairlineWidth, padding: 10 },
  agendaRow: { minHeight: 48, flexDirection: 'row', alignItems: 'center', gap: 8, borderBottomWidth: StyleSheet.hairlineWidth, paddingVertical: 6 },
  agendaTime: { width: 73, fontSize: 10, lineHeight: 14 },
  agendaCopy: { flex: 1, minWidth: 0 },
  agendaConflict: { color: CONFLICT_RED, fontSize: 8, lineHeight: 11, fontWeight: '800', letterSpacing: 0.4 },
  transparentAgenda: { opacity: 0.65 },
  section: { borderRadius: 14, padding: 12, gap: 9 },
  sectionHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'flex-start', gap: 8 },
  sectionEyebrow: { fontSize: 10, lineHeight: 15, letterSpacing: 0.8 },
  sectionTitle: { fontSize: 20, lineHeight: 25 },
  noGuests: { borderRadius: 10, padding: 10, gap: 2 },
  attendee: { minHeight: 57, flexDirection: 'row', alignItems: 'center', gap: 9, borderTopWidth: StyleSheet.hairlineWidth, paddingTop: 8 },
  avatar: { width: 34, height: 34, borderRadius: 17, alignItems: 'center', justifyContent: 'center' },
  avatarText: { color: '#FFFFFF', fontSize: 12, lineHeight: 15, fontWeight: '800' },
  attendeeCopy: { flex: 1, minWidth: 0 },
  attendeeTags: { maxWidth: 115, flexDirection: 'row', flexWrap: 'wrap', justifyContent: 'flex-end', gap: 3 },
  neutralTag: { overflow: 'hidden', borderRadius: 4, paddingHorizontal: 4, paddingVertical: 1, backgroundColor: '#6B728022', color: '#9CA3AF', fontSize: 7, lineHeight: 10, fontWeight: '800' },
  responseTag: { overflow: 'hidden', borderRadius: 4, borderWidth: StyleSheet.hairlineWidth, paddingHorizontal: 4, paddingVertical: 1, fontSize: 8, lineHeight: 11, fontWeight: '700' },
  description: { gap: 2, paddingVertical: 3 },
  detailRow: { minHeight: 48, flexDirection: 'row', alignItems: 'center', gap: 9 },
  detailIcon: { width: 28, alignItems: 'center' },
  detailRowCopy: { flex: 1, minWidth: 0 },
  linkText: { color: CALENDAR_BLUE },
  detailGrid: { flexDirection: 'row', flexWrap: 'wrap', columnGap: 10, rowGap: 9, borderTopWidth: StyleSheet.hairlineWidth, paddingTop: 10 },
  detailFact: { width: '47%', flexGrow: 1, minWidth: 130 },
  technicalToggle: { minHeight: 44, flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', borderTopWidth: StyleSheet.hairlineWidth, marginTop: 2 },
  technical: { borderRadius: 9, padding: 10 },
  error: { color: CONFLICT_RED },
});
