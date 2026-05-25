package mutations

import (
	"strings"
)

func calendarEventPreview(event map[string]any, operation string, calendarID string, sendUpdates string, eventID string, expectedEtag string) map[string]any {
	out := map[string]any{
		"operation":    operation,
		"calendar_id":  calendarID,
		"send_updates": sendUpdates,
	}
	if eventID != "" {
		out["event_id"] = eventID
	}
	if expectedEtag != "" {
		out["expected_etag"] = expectedEtag
	}
	for _, key := range []string{
		"summary", "description", "location", "start", "end",
		"attendees", "recurrence", "reminders", "transparency",
		"visibility", "status", "color_id", "colorId",
	} {
		if value, ok := event[key]; ok {
			out[key] = value
		}
	}
	if attendees := calendarAttendeesPreview(event["attendees"]); attendees != nil {
		out["attendees"] = attendees
	}
	return out
}

func calendarAttendeesPreview(value any) []map[string]any {
	rawList, ok := value.([]any)
	if !ok {
		// Already typed?
		if typed, isTyped := value.([]map[string]any); isTyped {
			out := make([]map[string]any, 0, len(typed))
			for _, item := range typed {
				if entry := calendarAttendeeEntry(item); entry != nil {
					out = append(out, entry)
				}
			}
			return out
		}
		return nil
	}
	out := make([]map[string]any, 0, len(rawList))
	for _, raw := range rawList {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if entry := calendarAttendeeEntry(item); entry != nil {
			out = append(out, entry)
		}
	}
	return out
}

func calendarAttendeeEntry(item map[string]any) map[string]any {
	email := strings.TrimSpace(stringFromAny(item["email"]))
	if email == "" {
		return nil
	}
	entry := map[string]any{"email": email}
	if name := strings.TrimSpace(stringFromAny(item["displayName"])); name != "" {
		entry["displayName"] = name
	}
	if status := strings.TrimSpace(stringFromAny(item["responseStatus"])); status != "" {
		entry["responseStatus"] = status
	}
	if optional, ok := item["optional"].(bool); ok && optional {
		entry["optional"] = true
	}
	if organizer, ok := item["organizer"].(bool); ok && organizer {
		entry["organizer"] = true
	}
	return entry
}
