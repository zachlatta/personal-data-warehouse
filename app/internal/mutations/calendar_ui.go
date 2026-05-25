package mutations

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
)

var calendarURLPattern = regexp.MustCompile(`https?://[^\s<>"']+`)

var calendarReviewLocation = func() *time.Location {
	if time.Local == nil {
		return time.UTC
	}
	return time.Local
}

func renderCalendarMutation(w http.ResponseWriter, mutation Mutation) {
	preview, _ := mutation.Preview["event"].(map[string]any)
	if preview == nil {
		preview = map[string]any{}
	}
	operation := strings.ToLower(stringFromAny(preview["operation"]))
	if operation == "" {
		switch mutation.Operation {
		case CalendarCreateEventOperation:
			operation = "create"
		case CalendarUpdateEventOperation:
			operation = "update"
		case CalendarDeleteEventOperation:
			operation = "delete"
		}
	}
	verb := calendarVerbForOperation(operation)
	calendarID := stringFromAny(preview["calendar_id"])
	if calendarID == "" {
		calendarID = "primary"
	}
	sendUpdates := stringFromAny(preview["send_updates"])
	if sendUpdates == "" {
		sendUpdates = "all"
	}
	eventID := stringFromAny(preview["event_id"])
	expectedEtag := stringFromAny(preview["expected_etag"])

	title := strings.TrimSpace(stringFromAny(preview["summary"]))
	if operation == "update" && title == "" {
		patch, _ := mutation.Payload["patch"].(map[string]any)
		title = strings.TrimSpace(stringFromAny(patch["summary"]))
	}
	if operation == "delete" && title == "" {
		title = "Cancel this event"
	}
	if title == "" {
		title = mutation.Title
	}
	if title == "" {
		title = verb + " calendar event"
	}

	fmt.Fprintf(w, `<article class="mutation calendar-mutation %s">`, html.EscapeString(operation))

	// Banner: operation + status + account
	fmt.Fprintf(w, `<div class="calendar-banner">`)
	fmt.Fprintf(w, `<div class="calendar-banner-left"><span class="calendar-op-badge">%s</span><span class="calendar-banner-account">%s</span></div>`,
		html.EscapeString(verb),
		html.EscapeString(mutation.Account),
	)
	fmt.Fprintf(w, `<span class="calendar-status-pill %s">%s</span>`,
		html.EscapeString(calendarStatusClass(mutation.Status)),
		html.EscapeString(humanStatus(mutation.Status)),
	)
	fmt.Fprintf(w, `</div>`)

	// Body
	fmt.Fprintf(w, `<div class="calendar-body">`)
	fmt.Fprintf(w, `<h3 class="calendar-title">%s</h3>`, html.EscapeString(title))

	if reason := strings.TrimSpace(mutation.Reason); reason != "" {
		fmt.Fprintf(w, `<p class="calendar-reason">%s</p>`, html.EscapeString(reason))
	}

	// When block (hero)
	renderCalendarWhen(w, preview)

	// Meta strip
	renderCalendarMetaStrip(w, calendarID, sendUpdates, preview)

	// Location
	if location := strings.TrimSpace(stringFromAny(preview["location"])); location != "" {
		fmt.Fprintf(w,
			`<div class="calendar-section calendar-location"><span class="calendar-icon" aria-hidden="true">📍</span><div>%s</div></div>`,
			html.EscapeString(location),
		)
	}

	// Description
	if description := strings.TrimSpace(stringFromAny(preview["description"])); description != "" {
		fmt.Fprintf(w,
			`<div class="calendar-section calendar-description"><h4>Description</h4><p>%s</p></div>`,
			renderCalendarMultilineText(description),
		)
	}

	// Attendees
	if attendees := calendarAttendeesPreview(preview["attendees"]); len(attendees) > 0 {
		renderCalendarAttendees(w, attendees)
	}

	// Update diff
	if operation == "update" {
		renderCalendarUpdateDiff(w, mutation)
	}

	// Delete confirmation
	if operation == "delete" {
		fmt.Fprintf(w,
			`<div class="calendar-section calendar-delete-warning"><strong>This event will be cancelled.</strong> %s</div>`,
			html.EscapeString(deleteNotificationSentence(sendUpdates)),
		)
	}

	// Technical footer: identifiers
	if eventID != "" || expectedEtag != "" {
		fmt.Fprintf(w, `<details class="calendar-tech"><summary>Technical details</summary><dl>`)
		if eventID != "" {
			fmt.Fprintf(w, `<div><dt>Event ID</dt><dd><code>%s</code></dd></div>`, html.EscapeString(eventID))
		}
		if expectedEtag != "" {
			fmt.Fprintf(w, `<div><dt>Expected etag</dt><dd><code>%s</code></dd></div>`, html.EscapeString(expectedEtag))
		}
		fmt.Fprintf(w, `</dl></details>`)
	}

	fmt.Fprintf(w, `</div>`) // body
	fmt.Fprintf(w, `</article>`)
}

func renderCalendarWhen(w http.ResponseWriter, preview map[string]any) {
	start := preview["start"]
	end := preview["end"]
	startMap, _ := start.(map[string]any)
	endMap, _ := end.(map[string]any)
	if startMap == nil && endMap == nil {
		return
	}
	startDate, startTime, _, startAllDay := parseCalendarTime(startMap)
	endDate, endTime, _, _ := parseCalendarTime(endMap)
	if startDate == nil && endDate == nil {
		return
	}

	fmt.Fprintf(w, `<div class="calendar-when">`)
	fmt.Fprintf(w, `<span class="calendar-when-icon" aria-hidden="true">🗓</span>`)
	fmt.Fprintf(w, `<div class="calendar-when-body">`)
	if startAllDay {
		fmt.Fprintf(w, `<div class="calendar-when-date">%s</div>`,
			html.EscapeString(formatFullDate(*startDate)),
		)
		if endDate != nil {
			// Google calendar end date is exclusive — show inclusive range
			inclusive := endDate.AddDate(0, 0, -1)
			if !inclusive.Equal(*startDate) {
				fmt.Fprintf(w, `<div class="calendar-when-time">All day · through %s</div>`,
					html.EscapeString(formatFullDate(inclusive)),
				)
			} else {
				fmt.Fprintf(w, `<div class="calendar-when-time">All day</div>`)
			}
		} else {
			fmt.Fprintf(w, `<div class="calendar-when-time">All day</div>`)
		}
	} else if startTime != nil {
		localStart, localEnd, localStartTz, localEndTz := calendarLocalTimeRange(*startTime, endTime)
		fmt.Fprintf(w, `<div class="calendar-when-date">%s</div>`,
			html.EscapeString(formatFullDate(localStart)),
		)
		timeStr := html.EscapeString(formatTimeRange(localStart, localEnd, localStartTz, localEndTz))
		fmt.Fprintf(w, `<div class="calendar-when-time">%s</div>`, timeStr)
	}
	fmt.Fprintf(w, `</div></div>`)
}

func renderCalendarMetaStrip(w http.ResponseWriter, calendarID string, sendUpdates string, preview map[string]any) {
	fmt.Fprintf(w, `<ul class="calendar-meta">`)
	fmt.Fprintf(w,
		`<li class="calendar-meta-item"><span class="calendar-icon" aria-hidden="true">🗓</span><span><span class="calendar-meta-label">Calendar</span><code>%s</code></span></li>`,
		html.EscapeString(calendarID),
	)
	fmt.Fprintf(w,
		`<li class="calendar-meta-item calendar-meta-notify %s"><span class="calendar-icon" aria-hidden="true">%s</span><span><span class="calendar-meta-label">Notifications</span>%s</span></li>`,
		html.EscapeString(notifyClass(sendUpdates)),
		html.EscapeString(notifyIcon(sendUpdates)),
		html.EscapeString(sendUpdatesLabel(sendUpdates)),
	)
	if rrules := stringSliceForList(preview["recurrence"]); len(rrules) > 0 {
		fmt.Fprintf(w,
			`<li class="calendar-meta-item calendar-meta-recurrence"><span class="calendar-icon" aria-hidden="true">🔁</span><span><span class="calendar-meta-label">Repeats</span>%s</span></li>`,
			html.EscapeString(humanRecurrence(rrules)),
		)
	}
	fmt.Fprintf(w, `</ul>`)
}

func renderCalendarAttendees(w http.ResponseWriter, attendees []map[string]any) {
	fmt.Fprintf(w, `<div class="calendar-section calendar-attendees"><h4>Guests (%d)</h4><ul>`, len(attendees))
	for _, attendee := range attendees {
		email := stringFromAny(attendee["email"])
		displayName := stringFromAny(attendee["displayName"])
		responseStatus := stringFromAny(attendee["responseStatus"])
		organizer := false
		if v, ok := attendee["organizer"].(bool); ok {
			organizer = v
		}
		optional := false
		if v, ok := attendee["optional"].(bool); ok {
			optional = v
		}
		initials := calendarInitials(displayName, email)
		colorClass := calendarAvatarColor(email + displayName)
		fmt.Fprintf(w, `<li class="calendar-attendee">`)
		fmt.Fprintf(w, `<span class="calendar-avatar %s" aria-hidden="true">%s</span>`,
			html.EscapeString(colorClass),
			html.EscapeString(initials),
		)
		fmt.Fprintf(w, `<div class="calendar-attendee-body">`)
		if displayName != "" {
			fmt.Fprintf(w, `<div class="calendar-attendee-name">%s</div>`, html.EscapeString(displayName))
			fmt.Fprintf(w, `<div class="calendar-attendee-email">%s</div>`, html.EscapeString(email))
		} else {
			fmt.Fprintf(w, `<div class="calendar-attendee-name">%s</div>`, html.EscapeString(email))
		}
		tags := []string{}
		if organizer {
			tags = append(tags, `<span class="calendar-attendee-tag organizer">Organizer</span>`)
		}
		if optional {
			tags = append(tags, `<span class="calendar-attendee-tag optional">Optional</span>`)
		}
		if responseStatus != "" {
			tags = append(tags, fmt.Sprintf(`<span class="calendar-attendee-tag rsvp-%s">%s</span>`,
				html.EscapeString(responseStatus),
				html.EscapeString(humanResponseStatus(responseStatus)),
			))
		}
		if len(tags) > 0 {
			fmt.Fprintf(w, `<div class="calendar-attendee-tags">%s</div>`, strings.Join(tags, ""))
		}
		fmt.Fprintf(w, `</div></li>`)
	}
	fmt.Fprintf(w, `</ul></div>`)
}

func renderCalendarUpdateDiff(w http.ResponseWriter, mutation Mutation) {
	patch, _ := mutation.Payload["patch"].(map[string]any)
	if len(patch) == 0 {
		return
	}
	keys := make([]string, 0, len(patch))
	for key := range patch {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fmt.Fprintf(w, `<div class="calendar-section calendar-patch"><h4>Changes</h4><table>`)
	fmt.Fprintf(w, `<thead><tr><th>Field</th><th>New value</th></tr></thead><tbody>`)
	for _, key := range keys {
		fmt.Fprintf(w, `<tr class="calendar-patch-row"><th scope="row">%s</th><td>%s</td></tr>`,
			html.EscapeString(humanFieldName(key)),
			renderCalendarPatchValue(key, patch[key]),
		)
	}
	fmt.Fprintf(w, `</tbody></table></div>`)
}

func renderCalendarPatchValue(key string, value any) string {
	switch typed := value.(type) {
	case string:
		return html.EscapeString(typed)
	case []any:
		if key == "recurrence" {
			rules := stringSliceForList(typed)
			return html.EscapeString(humanRecurrence(rules))
		}
		if key == "attendees" {
			emails := []string{}
			for _, item := range typed {
				if m, ok := item.(map[string]any); ok {
					if email := strings.TrimSpace(stringFromAny(m["email"])); email != "" {
						emails = append(emails, email)
					}
				}
			}
			return html.EscapeString(strings.Join(emails, ", "))
		}
	case map[string]any:
		if key == "start" || key == "end" {
			if formatted := formatCalendarPatchTime(typed); formatted != "" {
				return html.EscapeString(formatted)
			}
		}
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return html.EscapeString(fmt.Sprint(value))
	}
	return `<pre>` + html.EscapeString(string(data)) + `</pre>`
}

func renderCalendarMultilineText(text string) string {
	lines := strings.Split(text, "\n")
	parts := make([]string, 0, len(lines))
	for _, line := range lines {
		parts = append(parts, autolinkURLs(line))
	}
	return strings.Join(parts, "<br>")
}

func autolinkURLs(text string) string {
	out := strings.Builder{}
	cursor := 0
	for _, match := range calendarURLPattern.FindAllStringIndex(text, -1) {
		start, end := match[0], match[1]
		// Trim trailing punctuation commonly attached to URLs in prose
		trimmed := end
		for trimmed > start && strings.ContainsRune(".,;:!?)\"'", rune(text[trimmed-1])) {
			trimmed--
		}
		out.WriteString(html.EscapeString(text[cursor:start]))
		raw := text[start:trimmed]
		escaped := html.EscapeString(raw)
		out.WriteString(`<a href="`)
		out.WriteString(escaped)
		out.WriteString(`" rel="nofollow noopener" target="_blank">`)
		out.WriteString(escaped)
		out.WriteString(`</a>`)
		out.WriteString(html.EscapeString(text[trimmed:end]))
		cursor = end
	}
	out.WriteString(html.EscapeString(text[cursor:]))
	return out.String()
}

// --- helpers ---

func calendarVerbForOperation(op string) string {
	switch op {
	case "create":
		return "Create event"
	case "update":
		return "Update event"
	case "delete":
		return "Delete event"
	}
	return "Change event"
}

func calendarStatusClass(status string) string {
	switch status {
	case "pending_review":
		return "pending"
	case "approved":
		return "approved"
	case "succeeded", "observed":
		return "succeeded"
	case "rejected", "failed_terminal", "blocked_missing_credentials":
		return "failed"
	case "failed_retryable":
		return "retry"
	}
	return ""
}

func humanStatus(status string) string {
	switch status {
	case "pending_review":
		return "Pending review"
	case "blocked_missing_credentials":
		return "Blocked"
	case "failed_retryable":
		return "Retrying"
	case "failed_terminal":
		return "Failed"
	}
	if status == "" {
		return ""
	}
	return strings.ToUpper(status[:1]) + status[1:]
}

func notifyClass(value string) string {
	switch value {
	case "none":
		return "off"
	case "externalOnly":
		return "external"
	}
	return "all"
}

func notifyIcon(value string) string {
	if value == "none" {
		return "🔕"
	}
	return "🔔"
}

func sendUpdatesLabel(value string) string {
	switch value {
	case "none":
		return "Don't notify"
	case "externalOnly":
		return "External guests only"
	}
	return "All guests"
}

func deleteNotificationSentence(value string) string {
	switch value {
	case "none":
		return "Guests will not be notified."
	case "externalOnly":
		return "External guests will be notified."
	}
	return "Guests will be notified."
}

func humanFieldName(key string) string {
	switch key {
	case "summary":
		return "Summary"
	case "description":
		return "Description"
	case "location":
		return "Location"
	case "start":
		return "Start"
	case "end":
		return "End"
	case "attendees":
		return "Guests"
	case "recurrence":
		return "Recurrence"
	}
	if key == "" {
		return key
	}
	runes := []rune(key)
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

func humanResponseStatus(status string) string {
	switch status {
	case "accepted":
		return "Accepted"
	case "declined":
		return "Declined"
	case "tentative":
		return "Maybe"
	case "needsAction":
		return "Awaiting reply"
	}
	return status
}

func calendarInitials(name, email string) string {
	source := strings.TrimSpace(name)
	if source == "" {
		source = strings.TrimSpace(email)
	}
	if source == "" {
		return "?"
	}
	// For emails, derive from the part before @
	if at := strings.Index(source, "@"); at >= 0 {
		source = source[:at]
	}
	source = strings.ReplaceAll(source, ".", " ")
	source = strings.ReplaceAll(source, "_", " ")
	source = strings.ReplaceAll(source, "-", " ")
	fields := strings.Fields(source)
	if len(fields) == 0 {
		return strings.ToUpper(string([]rune(source)[0:1]))
	}
	if len(fields) == 1 {
		return strings.ToUpper(string([]rune(fields[0])[0:1]))
	}
	first := []rune(fields[0])[0]
	last := []rune(fields[len(fields)-1])[0]
	return strings.ToUpper(string(first) + string(last))
}

var calendarAvatarPalette = []string{"a", "b", "c", "d", "e", "f", "g", "h"}

func calendarAvatarColor(seed string) string {
	if seed == "" {
		return calendarAvatarPalette[0]
	}
	sum := 0
	for _, r := range seed {
		sum += int(r)
	}
	return "color-" + calendarAvatarPalette[sum%len(calendarAvatarPalette)]
}

func parseCalendarTime(value map[string]any) (date *time.Time, timeOfDay *time.Time, timezone string, allDay bool) {
	if value == nil {
		return nil, nil, "", false
	}
	tz := strings.TrimSpace(stringFromAny(value["timeZone"]))
	if d := strings.TrimSpace(stringFromAny(value["date"])); d != "" {
		if parsed, err := time.Parse("2006-01-02", d); err == nil {
			return &parsed, nil, "", true
		}
	}
	if dt := strings.TrimSpace(stringFromAny(value["dateTime"])); dt != "" {
		// Try RFC3339 first (covers ...Z and ...+HH:MM forms)
		if parsed, err := time.Parse(time.RFC3339Nano, dt); err == nil {
			d := time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, parsed.Location())
			return &d, &parsed, tz, false
		}
		if tz != "" {
			if loc, err := time.LoadLocation(tz); err == nil {
				if parsed, err := time.ParseInLocation("2006-01-02T15:04:05", dt, loc); err == nil {
					d := time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, parsed.Location())
					return &d, &parsed, tz, false
				}
			}
		}
		// Fall back to naive form without timezone
		if parsed, err := time.Parse("2006-01-02T15:04:05", dt); err == nil {
			d := time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, parsed.Location())
			return &d, &parsed, tz, false
		}
	}
	return nil, nil, "", false
}

func formatFullDate(t time.Time) string {
	return t.Format("Monday, January 2, 2006")
}

func formatTimeRange(start time.Time, end *time.Time, startTz, endTz string) string {
	startStr := formatTimeOfDay(start)
	if end == nil {
		if startTz != "" {
			return startStr + " " + startTz
		}
		return startStr
	}
	endStr := formatTimeOfDay(*end)
	tz := startTz
	if tz == "" {
		tz = endTz
	}
	if startTz != "" && endTz != "" && startTz != endTz {
		return startStr + " " + startTz + " – " + endStr + " " + endTz
	}
	// Combine AM/PM smartly: if same period, e.g. "9:00 – 9:30 AM"
	if strings.HasSuffix(startStr, " AM") && strings.HasSuffix(endStr, " AM") {
		startStr = strings.TrimSuffix(startStr, " AM")
	} else if strings.HasSuffix(startStr, " PM") && strings.HasSuffix(endStr, " PM") {
		startStr = strings.TrimSuffix(startStr, " PM")
	}
	if tz != "" {
		return startStr + " – " + endStr + " " + tz
	}
	return startStr + " – " + endStr
}

func formatTimeOfDay(t time.Time) string {
	return t.Format("3:04 PM")
}

func calendarLocalTimeRange(start time.Time, end *time.Time) (time.Time, *time.Time, string, string) {
	loc := calendarReviewLocation()
	if loc == nil {
		loc = time.UTC
	}
	localStart := start.In(loc)
	var localEnd *time.Time
	if end != nil {
		converted := end.In(loc)
		localEnd = &converted
	}
	endTz := ""
	if localEnd != nil {
		endTz = calendarTimeZoneLabel(*localEnd)
	}
	return localStart, localEnd, calendarTimeZoneLabel(localStart), endTz
}

func calendarTimeZoneLabel(value time.Time) string {
	name, offset := value.Zone()
	if name != "" {
		return name
	}
	if offset == 0 {
		return "UTC"
	}
	sign := "+"
	if offset < 0 {
		sign = "-"
		offset = -offset
	}
	return fmt.Sprintf("UTC%s%02d:%02d", sign, offset/3600, (offset%3600)/60)
}

func formatCalendarPatchTime(value map[string]any) string {
	date, timeOfDay, _, allDay := parseCalendarTime(value)
	if allDay {
		if date == nil {
			return ""
		}
		return formatFullDate(*date) + " (all day)"
	}
	if timeOfDay == nil {
		return ""
	}
	localTime, _, localTz, _ := calendarLocalTimeRange(*timeOfDay, nil)
	return formatFullDate(localTime) + " " + formatTimeOfDay(localTime) + " " + localTz
}

// humanRecurrence converts a Google Calendar RRULE list into a short English
// summary. Falls back to the raw RRULE when something unusual is encountered.
func humanRecurrence(rules []string) string {
	if len(rules) == 0 {
		return ""
	}
	parts := []string{}
	for _, raw := range rules {
		summary := humanSingleRecurrence(raw)
		if summary == "" {
			summary = raw
		}
		parts = append(parts, summary)
	}
	return strings.Join(parts, "; ")
}

func humanSingleRecurrence(raw string) string {
	if !strings.HasPrefix(raw, "RRULE:") {
		return ""
	}
	body := strings.TrimPrefix(raw, "RRULE:")
	props := map[string]string{}
	for _, segment := range strings.Split(body, ";") {
		if eq := strings.Index(segment, "="); eq > 0 {
			props[strings.ToUpper(segment[:eq])] = segment[eq+1:]
		}
	}
	freq := strings.ToUpper(props["FREQ"])
	interval := props["INTERVAL"]
	byDay := props["BYDAY"]

	base := ""
	switch freq {
	case "DAILY":
		base = "Daily"
		if byDay == "MO,TU,WE,TH,FR" {
			base = "Every weekday"
		} else if interval != "" && interval != "1" {
			base = fmt.Sprintf("Every %s days", interval)
		}
	case "WEEKLY":
		base = "Weekly"
		if interval != "" && interval != "1" {
			base = fmt.Sprintf("Every %s weeks", interval)
		}
		if byDay != "" && byDay != "MO,TU,WE,TH,FR" {
			base += " on " + humanByDay(byDay)
		}
	case "MONTHLY":
		base = "Monthly"
		if interval != "" && interval != "1" {
			base = fmt.Sprintf("Every %s months", interval)
		}
	case "YEARLY":
		base = "Annually"
	default:
		return ""
	}

	if count := strings.TrimSpace(props["COUNT"]); count != "" {
		base += ", for " + count + " occurrences"
	} else if until := strings.TrimSpace(props["UNTIL"]); until != "" {
		if parsed, err := time.Parse("20060102T150405Z", until); err == nil {
			base += ", until " + parsed.Format("January 2, 2006")
		} else if parsed, err := time.Parse("20060102", until); err == nil {
			base += ", until " + parsed.Format("January 2, 2006")
		}
	}
	return base
}

func humanByDay(spec string) string {
	dayNames := map[string]string{
		"MO": "Monday", "TU": "Tuesday", "WE": "Wednesday",
		"TH": "Thursday", "FR": "Friday", "SA": "Saturday", "SU": "Sunday",
	}
	out := []string{}
	for _, code := range strings.Split(spec, ",") {
		if name := dayNames[strings.ToUpper(strings.TrimSpace(code))]; name != "" {
			out = append(out, name)
		}
	}
	if len(out) == 0 {
		return spec
	}
	if len(out) == 1 {
		return out[0]
	}
	return strings.Join(out[:len(out)-1], ", ") + " and " + out[len(out)-1]
}

func stringSliceForList(value any) []string {
	switch typed := value.(type) {
	case []string:
		return typed
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := strings.TrimSpace(stringFromAny(item)); text != "" {
				out = append(out, text)
			}
		}
		return out
	}
	return nil
}
