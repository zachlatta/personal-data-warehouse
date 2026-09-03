package mutations

import (
	"encoding/json"
	"sort"
	"strings"
	"time"
	_ "time/tzdata" // The production app is a static distroless binary.
)

const defaultCalendarPreviewTimeZone = "America/New_York"

// calendarDayPreviewTarget is one calendar mutation whose proposed interval
// can be placed on a day view. MutationIndex is deliberately carried through
// the SQL result: a request may add several events on the same day, and each
// one needs its own overlap calculation on the phone.
type calendarDayPreviewTarget struct {
	MutationIndex int
	Account       string
	TimeZone      string
	Start         time.Time
	End           time.Time
	DayStart      time.Time
	DayEnd        time.Time
	AllDay        bool
	StartDate     string
	EndDate       string
}

// calendarDayPreviewRow is the human-relevant part of a synced Calendar row.
// The raw JSON is read only for Google fields that the faithful base table
// does not promote to columns (availability, visibility, colour and meeting
// link). It is never sent to the client wholesale.
type calendarDayPreviewRow struct {
	TargetIndex    int
	CalendarID     string
	EventID        string
	Status         string
	Summary        string
	Description    string
	Location       string
	CreatorEmail   string
	OrganizerEmail string
	Start          time.Time
	End            time.Time
	StartDate      string
	EndDate        string
	AllDay         bool
	HTMLLink       string
	AttendeesJSON  string
	RemindersJSON  string
	RecurrenceJSON string
	EventType      string
	RawJSON        string
	UpdatedAt      time.Time
	SyncedAt       time.Time
}

func calendarDayPreviewTargets(mutations []Mutation) []calendarDayPreviewTarget {
	targets := make([]calendarDayPreviewTarget, 0)
	for index, mutation := range mutations {
		if mutation.Provider != CalendarProvider || mutation.Operation != CalendarCreateEventOperation {
			continue
		}
		event := mapFromAny(mutation.Preview["event"])
		if len(event) == 0 {
			event = mapFromAny(mapFromAny(mutation.Payload)["event"])
		}
		timeZone := calendarEventTimeZone(event)
		location := calendarPreviewLocation(timeZone)
		start, startDate, startAllDay, startOK := parseCalendarPreviewBoundary(event["start"], location)
		end, endDate, endAllDay, endOK := parseCalendarPreviewBoundary(event["end"], location)
		if !startOK || !endOK || !end.After(start) {
			continue
		}

		dayStart := time.Date(start.In(location).Year(), start.In(location).Month(), start.In(location).Day(), 0, 0, 0, 0, location)
		dayEnd := dayStart.AddDate(0, 0, 1)
		allDay := startAllDay && endAllDay
		if allDay {
			// Google all-day end dates are exclusive. Using the parsed end as the
			// query boundary includes every day the proposal covers and no more.
			dayEnd = end
		} else {
			lastCovered := end.Add(-time.Nanosecond).In(location)
			lastDayStart := time.Date(lastCovered.Year(), lastCovered.Month(), lastCovered.Day(), 0, 0, 0, 0, location)
			dayEnd = lastDayStart.AddDate(0, 0, 1)
		}

		targets = append(targets, calendarDayPreviewTarget{
			MutationIndex: index,
			Account:       normalizeAccount(mutation.Account),
			TimeZone:      timeZone,
			Start:         start,
			End:           end,
			DayStart:      dayStart,
			DayEnd:        dayEnd,
			AllDay:        allDay,
			StartDate:     startDate,
			EndDate:       endDate,
		})
	}
	return targets
}

func calendarEventTimeZone(event map[string]any) string {
	for _, key := range []string{"start", "end"} {
		if zone := strings.TrimSpace(stringFromAny(mapFromAny(event[key])["timeZone"])); zone != "" {
			if _, err := time.LoadLocation(zone); err == nil {
				return zone
			}
		}
	}
	// PDW is a personal warehouse and its calendar owner lives in New York.
	// More importantly, a hand-authored Google Calendar proposal may omit the
	// zone beside a naive dateTime; interpreting that value in the server's UTC
	// locale would move the event by four or five hours in the conflict view.
	return defaultCalendarPreviewTimeZone
}

func calendarPreviewLocation(timeZone string) *time.Location {
	if location, err := time.LoadLocation(strings.TrimSpace(timeZone)); err == nil {
		return location
	}
	location, err := time.LoadLocation(defaultCalendarPreviewTimeZone)
	if err == nil {
		return location
	}
	return time.UTC
}

func parseCalendarPreviewBoundary(value any, location *time.Location) (time.Time, string, bool, bool) {
	boundary := mapFromAny(value)
	if len(boundary) == 0 {
		// Older callers occasionally stored the dateTime as a string. Google
		// will validate execution independently, but the review can still show
		// the interval honestly.
		boundary = map[string]any{"dateTime": stringFromAny(value)}
	}
	if dateValue := strings.TrimSpace(stringFromAny(boundary["date"])); dateValue != "" {
		parsed, err := time.ParseInLocation("2006-01-02", dateValue, location)
		return parsed, dateValue, true, err == nil
	}
	dateTime := strings.TrimSpace(stringFromAny(boundary["dateTime"]))
	if dateTime == "" {
		return time.Time{}, "", false, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, dateTime); err == nil {
			return parsed, "", false, true
		}
	}
	for _, layout := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05", "2006-01-02T15:04", "2006-01-02 15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, dateTime, location); err == nil {
			return parsed, "", false, true
		}
	}
	return time.Time{}, "", false, false
}

func applyCalendarDayPreviewRows(mutations []Mutation, targets []calendarDayPreviewTarget, rows []calendarDayPreviewRow) []Mutation {
	out := make([]Mutation, len(mutations))
	copy(out, mutations)
	rowsByTarget := map[int][]calendarDayPreviewRow{}
	for _, row := range rows {
		rowsByTarget[row.TargetIndex] = append(rowsByTarget[row.TargetIndex], row)
	}
	for _, target := range targets {
		if target.MutationIndex < 0 || target.MutationIndex >= len(out) {
			continue
		}
		targetRows := append([]calendarDayPreviewRow{}, rowsByTarget[target.MutationIndex]...)
		sort.SliceStable(targetRows, func(i, j int) bool {
			if targetRows[i].AllDay != targetRows[j].AllDay {
				return targetRows[i].AllDay
			}
			if !targetRows[i].Start.Equal(targetRows[j].Start) {
				return targetRows[i].Start.Before(targetRows[j].Start)
			}
			if !targetRows[i].End.Equal(targetRows[j].End) {
				return targetRows[i].End.Before(targetRows[j].End)
			}
			return targetRows[i].EventID < targetRows[j].EventID
		})

		events := make([]map[string]any, 0, len(targetRows))
		latestSync := time.Time{}
		for _, row := range targetRows {
			events = append(events, calendarDayPreviewEvent(row))
			if row.SyncedAt.After(latestSync) {
				latestSync = row.SyncedAt
			}
		}
		day := map[string]any{
			"time_zone":         target.TimeZone,
			"day_start":         target.DayStart.Format(time.RFC3339),
			"day_end":           target.DayEnd.Format(time.RFC3339),
			"proposed_start_at": target.Start.Format(time.RFC3339),
			"proposed_end_at":   target.End.Format(time.RFC3339),
			"events":            events,
		}
		if target.AllDay {
			day["proposed_start_date"] = target.StartDate
			day["proposed_end_date"] = target.EndDate
			day["proposed_is_all_day"] = true
		}
		if !latestSync.IsZero() {
			day["source_synced_at"] = latestSync.UTC().Format(time.RFC3339)
		}
		preview := cloneMap(out[target.MutationIndex].Preview)
		preview["calendar_day"] = day
		out[target.MutationIndex].Preview = preview
	}
	return out
}

func calendarDayPreviewEvent(row calendarDayPreviewRow) map[string]any {
	raw := decodeJSONMap([]byte(row.RawJSON))
	event := map[string]any{
		"calendar_id":     strings.TrimSpace(row.CalendarID),
		"event_id":        strings.TrimSpace(row.EventID),
		"status":          strings.TrimSpace(row.Status),
		"summary":         strings.TrimSpace(row.Summary),
		"description":     strings.TrimSpace(row.Description),
		"location":        strings.TrimSpace(row.Location),
		"creator_email":   strings.TrimSpace(row.CreatorEmail),
		"organizer_email": strings.TrimSpace(row.OrganizerEmail),
		"start_at":        row.Start.Format(time.RFC3339),
		"end_at":          row.End.Format(time.RFC3339),
		"is_all_day":      row.AllDay,
		"html_link":       strings.TrimSpace(row.HTMLLink),
		"attendees":       decodeCalendarAttendees(row.AttendeesJSON),
		"recurrence":      decodeJSONStringArray(row.RecurrenceJSON),
		"reminders":       decodeJSONMap([]byte(row.RemindersJSON)),
		"event_type":      strings.TrimSpace(row.EventType),
	}
	if row.StartDate != "" {
		event["start_date"] = row.StartDate
	}
	if row.EndDate != "" {
		event["end_date"] = row.EndDate
	}
	if !row.UpdatedAt.IsZero() {
		event["updated_at"] = row.UpdatedAt.UTC().Format(time.RFC3339)
	}
	for _, key := range []string{"transparency", "visibility", "colorId", "hangoutLink"} {
		if value := strings.TrimSpace(stringFromAny(raw[key])); value != "" {
			switch key {
			case "colorId":
				event["color_id"] = value
			case "hangoutLink":
				event["conference_link"] = value
			default:
				event[key] = value
			}
		}
	}
	if event["conference_link"] == nil {
		conference := mapFromAny(raw["conferenceData"])
		for _, entry := range mapSliceFromAny(conference["entryPoints"]) {
			if uri := strings.TrimSpace(stringFromAny(entry["uri"])); uri != "" {
				event["conference_link"] = uri
				break
			}
		}
	}
	return event
}

func decodeCalendarAttendees(value string) []map[string]any {
	var raw []map[string]any
	if err := json.Unmarshal([]byte(value), &raw); err != nil {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(raw))
	for _, attendee := range raw {
		if entry := calendarAttendeeEntry(attendee); entry != nil {
			out = append(out, entry)
		}
	}
	return out
}
