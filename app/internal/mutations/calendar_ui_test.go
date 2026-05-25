package mutations

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReviewUIRendersCalendarCreateEvent(t *testing.T) {
	withCalendarReviewLocation(t, "UTC")
	store := &reviewStore{requests: []Request{{
		ID:        "req-cal-create",
		Status:    "pending_review",
		Title:     "Schedule sync",
		Reason:    "weekly catch up",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-cal-create",
			Provider:  CalendarProvider,
			Operation: CalendarCreateEventOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Create event: PDW Calendar Sync",
			Reason:    "weekly catch up",
			Payload: map[string]any{
				"calendar_id":  "primary",
				"send_updates": "all",
				"event": map[string]any{
					"summary":     "PDW Calendar Sync",
					"description": "Discuss data warehouse roadmap",
					"location":    "Zoom",
					"start":       map[string]any{"dateTime": "2030-01-02T18:00:00Z", "timeZone": "UTC"},
					"end":         map[string]any{"dateTime": "2030-01-02T18:30:00Z", "timeZone": "UTC"},
					"attendees": []any{
						map[string]any{"email": "one@example.test", "organizer": true},
						map[string]any{"email": "two@example.test"},
					},
					"recurrence": []any{"RRULE:FREQ=WEEKLY;COUNT=4"},
				},
			},
			Preview: map[string]any{
				"event": map[string]any{
					"operation":    "create",
					"calendar_id":  "primary",
					"send_updates": "all",
					"summary":      "PDW Calendar Sync",
					"description":  "Discuss data warehouse roadmap",
					"location":     "Zoom",
					"start":        map[string]any{"dateTime": "2030-01-02T18:00:00Z", "timeZone": "UTC"},
					"end":          map[string]any{"dateTime": "2030-01-02T18:30:00Z", "timeZone": "UTC"},
					"attendees": []map[string]any{
						{"email": "one@example.test", "organizer": true},
						{"email": "two@example.test"},
					},
					"recurrence": []any{"RRULE:FREQ=WEEKLY;COUNT=4"},
				},
			},
		}},
	}}}
	body := fetchRequestDetail(t, store)

	mustContain(t, body, []string{
		`class="mutation calendar-mutation create"`,
		`class="calendar-banner"`,
		`Create event`,
		`PDW Calendar Sync`,
		`class="calendar-when"`,
		`Wednesday, January 2, 2030`,
		`6:00`,
		`6:30`,
		`UTC`,
		`class="calendar-meta"`,
		`primary`,
		`calendar-meta-recurrence`,
		`Weekly`,
		`for 4 occurrences`,
		`calendar-description`,
		`Discuss data warehouse roadmap`,
		`Zoom`,
		`calendar-attendees`,
		`one@example.test`,
		`two@example.test`,
		`Organizer`,
		// avatar initials should appear
		`calendar-avatar`,
		`>O<`,
	})
}

func TestReviewUIRendersCalendarUpdateEventWithDiff(t *testing.T) {
	store := &reviewStore{requests: []Request{{
		ID:        "req-cal-update",
		Status:    "pending_review",
		Title:     "Rename sync",
		Reason:    "renaming",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-cal-update",
			Provider:  CalendarProvider,
			Operation: CalendarUpdateEventOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Update event: Renamed sync",
			Payload: map[string]any{
				"calendar_id":   "primary",
				"send_updates":  "none",
				"event_id":      "evt-abc",
				"expected_etag": `"etag-1"`,
				"patch": map[string]any{
					"summary":     "Renamed sync",
					"location":    "Zoom (HD)",
					"description": "Updated description",
				},
			},
			Preview: map[string]any{
				"event": map[string]any{
					"operation":     "update",
					"calendar_id":   "primary",
					"send_updates":  "none",
					"event_id":      "evt-abc",
					"expected_etag": `"etag-1"`,
					"summary":       "Renamed sync",
					"description":   "Updated description",
					"location":      "Zoom (HD)",
				},
			},
		}},
	}}}
	body := fetchRequestDetail(t, store)

	mustContain(t, body, []string{
		`class="mutation calendar-mutation update"`,
		`Update event`,
		`Renamed sync`,
		`evt-abc`,
		// Field-level diff rows
		`calendar-patch`,
		`calendar-patch-row`,
		`>Summary<`,
		`>Renamed sync<`,
		`>Description<`,
		`>Updated description<`,
		`>Location<`,
		`>Zoom (HD)<`,
		// Notification chip shows "Don't notify"
		`Don&#39;t notify`,
	})
}

func TestReviewUIRendersTimedCalendarEventInLocalTimezone(t *testing.T) {
	withCalendarReviewLocation(t, "America/New_York")
	store := &reviewStore{requests: []Request{{
		ID:        "req-cal-local",
		Status:    "pending_review",
		Title:     "Move roadmap sync",
		Reason:    "moving the time and room",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-cal-local",
			Provider:  CalendarProvider,
			Operation: CalendarUpdateEventOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Update event: PDW roadmap sync (renamed)",
			Reason:    "moving the time and room; renaming to clarify scope",
			Payload: map[string]any{
				"calendar_id":  "primary",
				"send_updates": "none",
				"event_id":     "evt-local",
				"patch": map[string]any{
					"summary": "PDW roadmap sync (renamed)",
					"start":   map[string]any{"dateTime": "2026-05-28T18:00:00Z", "timeZone": "UTC"},
					"end":     map[string]any{"dateTime": "2026-05-28T18:30:00Z", "timeZone": "UTC"},
				},
			},
			Preview: map[string]any{
				"event": map[string]any{
					"operation":    "update",
					"calendar_id":  "primary",
					"send_updates": "none",
					"event_id":     "evt-local",
					"summary":      "PDW roadmap sync (renamed)",
					"start":        map[string]any{"dateTime": "2026-05-28T18:00:00Z", "timeZone": "UTC"},
					"end":          map[string]any{"dateTime": "2026-05-28T18:30:00Z", "timeZone": "UTC"},
				},
			},
		}},
	}}}
	body := fetchRequestDetail(t, store)

	mustContain(t, body, []string{
		`Thursday, May 28, 2026`,
		`2:00`,
		`2:30 PM EDT`,
		`Thursday, May 28, 2026 2:00 PM EDT`,
		`Thursday, May 28, 2026 2:30 PM EDT`,
	})
	mustNotContain(t, body, []string{
		`6:00 – 6:30 PM UTC`,
		`2026-05-28T18:00:00Z (UTC)`,
		`2026-05-28T18:30:00Z (UTC)`,
	})
}

func TestReviewUIRendersCalendarDeleteEventWithDangerStyling(t *testing.T) {
	store := &reviewStore{requests: []Request{{
		ID:        "req-cal-delete",
		Status:    "pending_review",
		Title:     "Delete sync",
		Reason:    "cancelled",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-cal-delete",
			Provider:  CalendarProvider,
			Operation: CalendarDeleteEventOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Delete event evt-abc",
			Payload: map[string]any{
				"calendar_id":   "primary",
				"send_updates":  "all",
				"event_id":      "evt-abc",
				"expected_etag": `"etag-1"`,
			},
			Preview: map[string]any{
				"event": map[string]any{
					"operation":     "delete",
					"calendar_id":   "primary",
					"send_updates":  "all",
					"event_id":      "evt-abc",
					"expected_etag": `"etag-1"`,
				},
			},
		}},
	}}}
	body := fetchRequestDetail(t, store)

	mustContain(t, body, []string{
		`class="mutation calendar-mutation delete"`,
		`Delete event`,
		`evt-abc`,
		`calendar-delete-warning`,
		`This event will be cancelled`,
		`Guests will be notified.`,
	})
}

func TestReviewUIRendersCalendarAllDayEvent(t *testing.T) {
	store := &reviewStore{requests: []Request{{
		ID:        "req-cal-allday",
		Status:    "pending_review",
		Title:     "Day off",
		Reason:    "vacation",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-cal-allday",
			Provider:  CalendarProvider,
			Operation: CalendarCreateEventOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Create event: Day off",
			Payload: map[string]any{
				"calendar_id":  "primary",
				"send_updates": "all",
				"event": map[string]any{
					"summary": "Day off",
					"start":   map[string]any{"date": "2030-07-04"},
					"end":     map[string]any{"date": "2030-07-05"},
				},
			},
			Preview: map[string]any{
				"event": map[string]any{
					"operation":    "create",
					"calendar_id":  "primary",
					"send_updates": "all",
					"summary":      "Day off",
					"start":        map[string]any{"date": "2030-07-04"},
					"end":          map[string]any{"date": "2030-07-05"},
				},
			},
		}},
	}}}
	body := fetchRequestDetail(t, store)

	mustContain(t, body, []string{
		`Thursday, July 4, 2030`,
		`All day`,
	})
}

func TestReviewUIAutoLinksURLsInDescription(t *testing.T) {
	store := &reviewStore{requests: []Request{{
		ID:        "req-cal-linked",
		Status:    "pending_review",
		Title:     "Linked desc",
		Reason:    "test",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-cal-linked",
			Provider:  CalendarProvider,
			Operation: CalendarCreateEventOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Create event: Linked",
			Payload: map[string]any{
				"calendar_id":  "primary",
				"send_updates": "all",
				"event": map[string]any{
					"summary":     "Linked",
					"description": "Join us at https://meet.example/pdw or read https://example.com/agenda",
					"start":       map[string]any{"dateTime": "2030-01-02T15:00:00Z", "timeZone": "UTC"},
					"end":         map[string]any{"dateTime": "2030-01-02T15:30:00Z", "timeZone": "UTC"},
				},
			},
			Preview: map[string]any{
				"event": map[string]any{
					"operation":    "create",
					"calendar_id":  "primary",
					"send_updates": "all",
					"summary":      "Linked",
					"description":  "Join us at https://meet.example/pdw or read https://example.com/agenda",
					"start":        map[string]any{"dateTime": "2030-01-02T15:00:00Z", "timeZone": "UTC"},
					"end":          map[string]any{"dateTime": "2030-01-02T15:30:00Z", "timeZone": "UTC"},
				},
			},
		}},
	}}}
	body := fetchRequestDetail(t, store)

	mustContain(t, body, []string{
		`<a href="https://meet.example/pdw"`,
		`<a href="https://example.com/agenda"`,
	})
}

func TestReviewUIDeleteEventFallsBackToCancelTitleWhenSummaryMissing(t *testing.T) {
	store := &reviewStore{requests: []Request{{
		ID:        "req-cal-delete-nosummary",
		Status:    "pending_review",
		Title:     "Delete event evt-x",
		Reason:    "cancelled",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-cal-delete-nosummary",
			Provider:  CalendarProvider,
			Operation: CalendarDeleteEventOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Delete event evt-x",
			Payload: map[string]any{
				"calendar_id":  "primary",
				"send_updates": "all",
				"event_id":     "evt-x",
			},
			Preview: map[string]any{
				"event": map[string]any{
					"operation":    "delete",
					"calendar_id":  "primary",
					"send_updates": "all",
					"event_id":     "evt-x",
				},
			},
		}},
	}}}
	body := fetchRequestDetail(t, store)

	mustContain(t, body, []string{
		`Cancel this event`,
		`evt-x`,
	})
}

func TestReviewUIRendersDailyRecurrenceInPlainEnglish(t *testing.T) {
	store := &reviewStore{requests: []Request{{
		ID:        "req-cal-daily",
		Status:    "pending_review",
		Title:     "Daily standup",
		Reason:    "stand up",
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Mutations: []Mutation{{
			ID:        "mut-cal-daily",
			Provider:  CalendarProvider,
			Operation: CalendarCreateEventOperation,
			Account:   "zach@example.test",
			Status:    "pending_review",
			Title:     "Create event: Daily standup",
			Payload: map[string]any{
				"calendar_id":  "primary",
				"send_updates": "all",
				"event": map[string]any{
					"summary":    "Daily standup",
					"start":      map[string]any{"dateTime": "2030-01-02T15:00:00Z", "timeZone": "UTC"},
					"end":        map[string]any{"dateTime": "2030-01-02T15:15:00Z", "timeZone": "UTC"},
					"recurrence": []any{"RRULE:FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR;UNTIL=20300601T000000Z"},
				},
			},
			Preview: map[string]any{
				"event": map[string]any{
					"operation":    "create",
					"calendar_id":  "primary",
					"send_updates": "all",
					"summary":      "Daily standup",
					"start":        map[string]any{"dateTime": "2030-01-02T15:00:00Z", "timeZone": "UTC"},
					"end":          map[string]any{"dateTime": "2030-01-02T15:15:00Z", "timeZone": "UTC"},
					"recurrence":   []any{"RRULE:FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR;UNTIL=20300601T000000Z"},
				},
			},
		}},
	}}}
	body := fetchRequestDetail(t, store)

	mustContain(t, body, []string{
		`Every weekday`,
		`until June 1, 2030`,
	})
}

func TestParseCalendarTimeUsesRFC3339OffsetDate(t *testing.T) {
	date, timeOfDay, _, allDay := parseCalendarTime(map[string]any{
		"dateTime": "2030-01-01T23:30:00-05:00",
	})
	if allDay || date == nil || timeOfDay == nil {
		t.Fatalf("unexpected parsed values: date=%v time=%v allDay=%v", date, timeOfDay, allDay)
	}
	if got := formatFullDate(*date); got != "Tuesday, January 1, 2030" {
		t.Fatalf("date = %q", got)
	}
}

func fetchRequestDetail(t *testing.T, store *reviewStore) string {
	t.Helper()
	request := store.requests[0]
	service := NewService(store, Config{
		BaseURL:          "https://mcp.example.test",
		UIPassword:       "correct horse battery staple",
		SessionSecret:    "0123456789abcdef0123456789abcdef",
		SessionTTL:       time.Hour,
		CalendarAccounts: []string{"zach@example.test"},
		Now:              func() time.Time { return time.Unix(1700000000, 0).UTC() },
	})
	handler := service.HTTPHandler()
	cookies := loginCookies(t, handler)

	response := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/mutation-review/requests/"+request.ID, nil)
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body = %q", response.Code, response.Body.String())
	}
	return response.Body.String()
}

func mustContain(t *testing.T, body string, fragments []string) {
	t.Helper()
	for _, want := range fragments {
		if !strings.Contains(body, want) {
			t.Fatalf("page missing %q\n--- body ---\n%s", want, body)
		}
	}
}

func mustNotContain(t *testing.T, body string, fragments []string) {
	t.Helper()
	for _, unwanted := range fragments {
		if strings.Contains(body, unwanted) {
			t.Fatalf("page unexpectedly contained %q\n--- body ---\n%s", unwanted, body)
		}
	}
}

func withCalendarReviewLocation(t *testing.T, name string) {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("LoadLocation(%q): %v", name, err)
	}
	old := calendarReviewLocation
	calendarReviewLocation = func() *time.Location { return loc }
	t.Cleanup(func() { calendarReviewLocation = old })
}
