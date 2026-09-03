package mutations

import (
	"testing"
	"time"
)

func TestCalendarDayPreviewTargetsResolveTheProposalInItsCalendarTimezone(t *testing.T) {
	mutations := []Mutation{{
		Provider:  CalendarProvider,
		Operation: CalendarCreateEventOperation,
		Account:   "zach@example.test",
		Preview: map[string]any{
			"event": map[string]any{
				"calendar_id": "primary",
				"start": map[string]any{
					"dateTime": "2026-09-05T09:00:00",
					"timeZone": "America/New_York",
				},
				"end": map[string]any{
					"dateTime": "2026-09-05T11:00:00",
					"timeZone": "America/New_York",
				},
			},
		},
	}}

	targets := calendarDayPreviewTargets(mutations)
	if len(targets) != 1 {
		t.Fatalf("targets = %#v", targets)
	}
	target := targets[0]
	if got := target.Start.Format(time.RFC3339); got != "2026-09-05T09:00:00-04:00" {
		t.Fatalf("Start = %q", got)
	}
	if got := target.End.Format(time.RFC3339); got != "2026-09-05T11:00:00-04:00" {
		t.Fatalf("End = %q", got)
	}
	if got := target.DayStart.Format(time.RFC3339); got != "2026-09-05T00:00:00-04:00" {
		t.Fatalf("DayStart = %q", got)
	}
	if got := target.DayEnd.Format(time.RFC3339); got != "2026-09-06T00:00:00-04:00" {
		t.Fatalf("DayEnd = %q", got)
	}
	if target.TimeZone != "America/New_York" || target.Account != "zach@example.test" {
		t.Fatalf("target metadata = %#v", target)
	}
}

func TestCalendarDayPreviewTargetsSupportAllDayEvents(t *testing.T) {
	targets := calendarDayPreviewTargets([]Mutation{{
		Provider:  CalendarProvider,
		Operation: CalendarCreateEventOperation,
		Account:   "zach@example.test",
		Preview: map[string]any{"event": map[string]any{
			"start": map[string]any{"date": "2026-09-05"},
			"end":   map[string]any{"date": "2026-09-07"},
		}},
	}})
	if len(targets) != 1 {
		t.Fatalf("targets = %#v", targets)
	}
	if !targets[0].AllDay || targets[0].StartDate != "2026-09-05" || targets[0].EndDate != "2026-09-07" {
		t.Fatalf("all-day target = %#v", targets[0])
	}
	if got := targets[0].DayEnd.Format("2006-01-02"); got != "2026-09-07" {
		t.Fatalf("multi-day preview should include every covered day, DayEnd = %q", got)
	}
}

func TestApplyCalendarDayPreviewRowsCarriesTheWholeDayAndInviteDetails(t *testing.T) {
	mutations := []Mutation{{
		Provider:  CalendarProvider,
		Operation: CalendarCreateEventOperation,
		Account:   "zach@example.test",
		Preview: map[string]any{"event": map[string]any{
			"summary":     "Pickleball",
			"calendar_id": "primary",
			"start":       map[string]any{"dateTime": "2026-09-05T09:00:00-04:00"},
			"end":         map[string]any{"dateTime": "2026-09-05T11:00:00-04:00"},
		}},
	}}
	targets := calendarDayPreviewTargets(mutations)
	synced := time.Date(2026, 9, 2, 20, 45, 0, 0, time.UTC)
	rows := []calendarDayPreviewRow{
		{
			TargetIndex:    0,
			CalendarID:     "work@example.test",
			EventID:        "evt-overlap",
			Status:         "confirmed",
			Summary:        "Breakfast with Ada",
			Description:    "Planning over coffee",
			Location:       "Davis Square",
			CreatorEmail:   "ada@example.test",
			OrganizerEmail: "ada@example.test",
			Start:          time.Date(2026, 9, 5, 13, 30, 0, 0, time.UTC),
			End:            time.Date(2026, 9, 5, 14, 30, 0, 0, time.UTC),
			HTMLLink:       "https://calendar.google.com/calendar/event?eid=abc",
			AttendeesJSON:  `[{"email":"zach@example.test","self":true,"responseStatus":"accepted"},{"email":"ada@example.test","displayName":"Ada Lovelace","organizer":true,"responseStatus":"accepted"}]`,
			RemindersJSON:  `{"useDefault":true}`,
			RecurrenceJSON: `[]`,
			RawJSON:        `{"transparency":"opaque","visibility":"private","colorId":"7","hangoutLink":"https://meet.google.com/abc-defg-hij"}`,
			SyncedAt:       synced,
		},
		{
			TargetIndex:    0,
			CalendarID:     "primary",
			EventID:        "evt-all-day",
			Status:         "confirmed",
			Summary:        "Hack Club retreat",
			Start:          time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC),
			End:            time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC),
			StartDate:      "2026-09-05",
			EndDate:        "2026-09-06",
			AllDay:         true,
			AttendeesJSON:  `[]`,
			RemindersJSON:  `{}`,
			RecurrenceJSON: `[]`,
			RawJSON:        `{}`,
			SyncedAt:       synced.Add(time.Minute),
		},
	}

	got := applyCalendarDayPreviewRows(mutations, targets, rows)
	day := mapFromAny(got[0].Preview["calendar_day"])
	if day["time_zone"] != "America/New_York" {
		t.Fatalf("time_zone = %#v", day["time_zone"])
	}
	if day["proposed_start_at"] != "2026-09-05T09:00:00-04:00" || day["proposed_end_at"] != "2026-09-05T11:00:00-04:00" {
		t.Fatalf("resolved proposal times = %#v", day)
	}
	if day["source_synced_at"] != "2026-09-02T20:46:00Z" {
		t.Fatalf("source_synced_at = %#v", day["source_synced_at"])
	}
	events := mapSliceFromAny(day["events"])
	if len(events) != 2 {
		t.Fatalf("events = %#v", events)
	}
	if events[0]["event_id"] != "evt-all-day" || events[1]["event_id"] != "evt-overlap" {
		t.Fatalf("events are not calendar ordered: %#v", events)
	}
	timed := events[1]
	if timed["description"] != "Planning over coffee" || timed["location"] != "Davis Square" {
		t.Fatalf("human details = %#v", timed)
	}
	if timed["organizer_email"] != "ada@example.test" || timed["conference_link"] != "https://meet.google.com/abc-defg-hij" {
		t.Fatalf("organizer/conference = %#v", timed)
	}
	if timed["transparency"] != "opaque" || timed["visibility"] != "private" || timed["color_id"] != "7" {
		t.Fatalf("calendar metadata = %#v", timed)
	}
	attendees := mapSliceFromAny(timed["attendees"])
	if len(attendees) != 2 || attendees[1]["displayName"] != "Ada Lovelace" || attendees[0]["self"] != true {
		t.Fatalf("attendees = %#v", attendees)
	}
}
