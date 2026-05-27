package mutations

import (
	"context"
	"strings"
	"testing"
)

func TestProposeMutationCalendarCreateEvent(t *testing.T) {
	store := &recordingStore{request: Request{
		ID:     "req-cal-create",
		Status: "pending_review",
		Mutations: []Mutation{{
			ID: "mut-cal-create",
		}},
	}}
	service := NewService(store, Config{
		BaseURL:          "https://mcp.example.test",
		CalendarAccounts: []string{"zach@example.test"},
	})

	response, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Schedule sync",
		Reason: "schedule sync",
		Mutations: []map[string]any{{
			"type":         CalendarCreateEventOperation,
			"account":      "ZACH@example.test",
			"calendar_id":  " primary ",
			"send_updates": "all",
			"event": map[string]any{
				"summary": "Hello",
				"start":   map[string]any{"dateTime": "2030-01-01T10:00:00", "timeZone": "America/Los_Angeles"},
				"end":     map[string]any{"dateTime": "2030-01-01T10:30:00", "timeZone": "America/Los_Angeles"},
				"attendees": []any{
					map[string]any{"email": "one@example.test"},
					map[string]any{"email": "two@example.test"},
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("ProposeMutation returned error: %v", err)
	}
	if response.RequestID != "req-cal-create" || response.Status != "pending_review" {
		t.Fatalf("unexpected response: %#v", response)
	}
	if response.ApprovalURL != "https://mcp.example.test/mutation-review/requests/req-cal-create" {
		t.Fatalf("ApprovalURL = %q", response.ApprovalURL)
	}
	if len(store.createCalls) != 1 || len(store.createCalls[0].Mutations) != 1 {
		t.Fatalf("unexpected create calls: %#v", store.createCalls)
	}
	mutation := store.createCalls[0].Mutations[0]
	if mutation.Type != CalendarCreateEventOperation {
		t.Fatalf("Type = %q", mutation.Type)
	}
	if mutation.Account != "zach@example.test" {
		t.Fatalf("Account = %q", mutation.Account)
	}
	if mutation.CalendarID != "primary" {
		t.Fatalf("CalendarID = %q", mutation.CalendarID)
	}
	if mutation.SendUpdates != "all" {
		t.Fatalf("SendUpdates = %q", mutation.SendUpdates)
	}
	if got, _ := mutation.Event["summary"].(string); got != "Hello" {
		t.Fatalf("event summary = %#v", mutation.Event["summary"])
	}
}

func TestProposeMutationCalendarCreateEventRequiresEventAndDates(t *testing.T) {
	store := &recordingStore{}
	service := NewService(store, Config{CalendarAccounts: []string{"zach@example.test"}})

	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Create",
		Reason: "test",
		Mutations: []map[string]any{{
			"type":    CalendarCreateEventOperation,
			"account": "zach@example.test",
		}},
	}); err == nil || !strings.Contains(err.Error(), "event") {
		t.Fatalf("expected event error, got %v", err)
	}

	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Create",
		Reason: "test",
		Mutations: []map[string]any{{
			"type":    CalendarCreateEventOperation,
			"account": "zach@example.test",
			"event":   map[string]any{"summary": "no times"},
		}},
	}); err == nil || !strings.Contains(err.Error(), "start") {
		t.Fatalf("expected start error, got %v", err)
	}

	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Create",
		Reason: "test",
		Mutations: []map[string]any{{
			"type":    CalendarCreateEventOperation,
			"account": "zach@example.test",
			"event": map[string]any{
				"summary": "no end",
				"start":   map[string]any{"dateTime": "2030-01-01T10:00:00", "timeZone": "UTC"},
			},
		}},
	}); err == nil || !strings.Contains(err.Error(), "end") {
		t.Fatalf("expected end error, got %v", err)
	}
}

func TestProposeMutationCalendarUpdateEvent(t *testing.T) {
	store := &recordingStore{request: Request{
		ID:        "req-cal-update",
		Status:    "pending_review",
		Mutations: []Mutation{{ID: "mut-cal-update"}},
	}}
	service := NewService(store, Config{
		BaseURL:          "https://mcp.example.test",
		CalendarAccounts: []string{"zach@example.test"},
	})

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Rename event",
		Reason: "rename",
		Mutations: []map[string]any{{
			"type":          CalendarUpdateEventOperation,
			"account":       "zach@example.test",
			"calendar_id":   "primary",
			"event_id":      "evt-abc",
			"expected_etag": `"etag-1"`,
			"patch":         map[string]any{"summary": "Renamed"},
			"send_updates":  "all",
		}},
	})
	if err != nil {
		t.Fatalf("ProposeMutation returned error: %v", err)
	}
	if len(store.createCalls) != 1 {
		t.Fatalf("unexpected create calls: %#v", store.createCalls)
	}
	mutation := store.createCalls[0].Mutations[0]
	if mutation.Type != CalendarUpdateEventOperation {
		t.Fatalf("Type = %q", mutation.Type)
	}
	if mutation.EventID != "evt-abc" {
		t.Fatalf("EventID = %q", mutation.EventID)
	}
	if mutation.ExpectedEtag != `"etag-1"` {
		t.Fatalf("ExpectedEtag = %q", mutation.ExpectedEtag)
	}
	if got, _ := mutation.Patch["summary"].(string); got != "Renamed" {
		t.Fatalf("patch summary = %#v", mutation.Patch)
	}
}

func TestProposeMutationCalendarUpdateEventRequiresEventIDAndPatch(t *testing.T) {
	store := &recordingStore{}
	service := NewService(store, Config{CalendarAccounts: []string{"zach@example.test"}})

	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Update",
		Reason: "test",
		Mutations: []map[string]any{{
			"type":    CalendarUpdateEventOperation,
			"account": "zach@example.test",
			"patch":   map[string]any{"summary": "X"},
		}},
	}); err == nil || !strings.Contains(err.Error(), "event_id") {
		t.Fatalf("expected event_id error, got %v", err)
	}

	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Update",
		Reason: "test",
		Mutations: []map[string]any{{
			"type":     CalendarUpdateEventOperation,
			"account":  "zach@example.test",
			"event_id": "evt-abc",
		}},
	}); err == nil || !strings.Contains(err.Error(), "patch") {
		t.Fatalf("expected patch error, got %v", err)
	}
}

func TestProposeMutationCalendarDeleteEvent(t *testing.T) {
	store := &recordingStore{request: Request{
		ID:        "req-cal-delete",
		Status:    "pending_review",
		Mutations: []Mutation{{ID: "mut-cal-delete"}},
	}}
	service := NewService(store, Config{
		BaseURL:          "https://mcp.example.test",
		CalendarAccounts: []string{"zach@example.test"},
	})

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Delete event evt-abc",
		Reason: "no longer needed",
		Mutations: []map[string]any{{
			"type":          CalendarDeleteEventOperation,
			"account":       "zach@example.test",
			"calendar_id":   "primary",
			"event_id":      "evt-abc",
			"expected_etag": `"etag-1"`,
			"send_updates":  "all",
		}},
	})
	if err != nil {
		t.Fatalf("ProposeMutation returned error: %v", err)
	}
	mutation := store.createCalls[0].Mutations[0]
	if mutation.Type != CalendarDeleteEventOperation {
		t.Fatalf("Type = %q", mutation.Type)
	}
	if mutation.EventID != "evt-abc" {
		t.Fatalf("EventID = %q", mutation.EventID)
	}
}

func TestProposeMutationCalendarRejectsSendUpdates(t *testing.T) {
	store := &recordingStore{}
	service := NewService(store, Config{CalendarAccounts: []string{"zach@example.test"}})

	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Create",
		Reason: "test",
		Mutations: []map[string]any{{
			"type":    CalendarCreateEventOperation,
			"account": "zach@example.test",
			"event": map[string]any{
				"summary": "Hello",
				"start":   map[string]any{"dateTime": "2030-01-01T10:00:00", "timeZone": "UTC"},
				"end":     map[string]any{"dateTime": "2030-01-01T10:30:00", "timeZone": "UTC"},
			},
			"send_updates": "yes-please",
		}},
	}); err == nil || !strings.Contains(err.Error(), "send_updates") {
		t.Fatalf("expected send_updates error, got %v", err)
	}
}

func TestProposeMutationCalendarRejectsUnconfiguredAccount(t *testing.T) {
	store := &recordingStore{}
	service := NewService(store, Config{CalendarAccounts: []string{"zach@example.test"}})

	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Create",
		Reason: "test",
		Mutations: []map[string]any{{
			"type":    CalendarCreateEventOperation,
			"account": "stranger@example.test",
			"event": map[string]any{
				"summary": "Hello",
				"start":   map[string]any{"dateTime": "2030-01-01T10:00:00", "timeZone": "UTC"},
				"end":     map[string]any{"dateTime": "2030-01-01T10:30:00", "timeZone": "UTC"},
			},
		}},
	}); err == nil || !strings.Contains(err.Error(), "CALENDAR_ACCOUNTS") {
		t.Fatalf("expected CALENDAR_ACCOUNTS error, got %v", err)
	}
}

func TestNormalizeForStorageCalendarCreateEvent(t *testing.T) {
	stored, err := normalizeForStorage(CreateRequestInput{
		Title:  "Schedule sync",
		Reason: "weekly catch-up",
		Mutations: []MutationInput{{
			Type:        CalendarCreateEventOperation,
			Account:     "zach@example.test",
			CalendarID:  "primary",
			SendUpdates: "all",
			Event: map[string]any{
				"summary": "Sync",
				"start":   map[string]any{"dateTime": "2030-01-01T10:00:00", "timeZone": "UTC"},
				"end":     map[string]any{"dateTime": "2030-01-01T10:30:00", "timeZone": "UTC"},
				"attendees": []any{
					map[string]any{"email": "one@example.test"},
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("normalizeForStorage error: %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("len(stored) = %d", len(stored))
	}
	if stored[0].Provider != "google_calendar" || stored[0].Operation != CalendarCreateEventOperation {
		t.Fatalf("provider/operation: %#v", stored[0])
	}
	if stored[0].Payload["calendar_id"] != "primary" {
		t.Fatalf("calendar_id = %#v", stored[0].Payload["calendar_id"])
	}
	if stored[0].Payload["send_updates"] != "all" {
		t.Fatalf("send_updates = %#v", stored[0].Payload["send_updates"])
	}
	preview, ok := stored[0].Preview["event"].(map[string]any)
	if !ok {
		t.Fatalf("preview.event missing or wrong type: %#v", stored[0].Preview)
	}
	if preview["summary"] != "Sync" {
		t.Fatalf("preview summary = %#v", preview["summary"])
	}
	if preview["operation"] != "create" {
		t.Fatalf("preview operation = %#v", preview["operation"])
	}
}

func TestNormalizeForStorageCalendarUpdateEvent(t *testing.T) {
	stored, err := normalizeForStorage(CreateRequestInput{
		Title:  "Update",
		Reason: "rename",
		Mutations: []MutationInput{{
			Type:         CalendarUpdateEventOperation,
			Account:      "zach@example.test",
			CalendarID:   "primary",
			EventID:      "evt-abc",
			ExpectedEtag: `"etag-1"`,
			SendUpdates:  "all",
			Patch:        map[string]any{"summary": "Renamed"},
		}},
	})
	if err != nil {
		t.Fatalf("normalizeForStorage error: %v", err)
	}
	if stored[0].Operation != CalendarUpdateEventOperation {
		t.Fatalf("operation = %q", stored[0].Operation)
	}
	if stored[0].Payload["event_id"] != "evt-abc" {
		t.Fatalf("event_id = %#v", stored[0].Payload["event_id"])
	}
	if stored[0].Payload["expected_etag"] != `"etag-1"` {
		t.Fatalf("expected_etag = %#v", stored[0].Payload["expected_etag"])
	}
	patch, ok := stored[0].Payload["patch"].(map[string]any)
	if !ok || patch["summary"] != "Renamed" {
		t.Fatalf("patch = %#v", stored[0].Payload["patch"])
	}
}

func TestProposeMutationAcceptsMultipleCalendarOperations(t *testing.T) {
	store := &recordingStore{request: Request{
		ID:        "req-mixed",
		Status:    "pending_review",
		Mutations: []Mutation{{ID: "mut-a"}, {ID: "mut-b"}},
	}}
	service := NewService(store, Config{
		BaseURL:          "https://mcp.example.test",
		CalendarAccounts: []string{"zach@example.test"},
		GmailAccounts:    []string{"zach@example.test"},
	})

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Mixed batch",
		Reason: "test mixed batch",
		Mutations: []map[string]any{
			{
				"type":         CalendarCreateEventOperation,
				"account":      "zach@example.test",
				"calendar_id":  "primary",
				"send_updates": "all",
				"event": map[string]any{
					"summary": "Sync",
					"start":   map[string]any{"dateTime": "2030-01-01T10:00:00", "timeZone": "UTC"},
					"end":     map[string]any{"dateTime": "2030-01-01T10:30:00", "timeZone": "UTC"},
				},
			},
			{
				"type":          CalendarUpdateEventOperation,
				"account":       "zach@example.test",
				"calendar_id":   "primary",
				"event_id":      "evt-abc",
				"expected_etag": `"etag-1"`,
				"send_updates":  "none",
				"patch":         map[string]any{"summary": "Renamed"},
			},
		},
	})
	if err != nil {
		t.Fatalf("ProposeMutation returned error: %v", err)
	}
	if len(store.createCalls) != 1 || len(store.createCalls[0].Mutations) != 2 {
		t.Fatalf("unexpected create calls: %#v", store.createCalls)
	}
	first := store.createCalls[0].Mutations[0]
	second := store.createCalls[0].Mutations[1]
	if first.Type != CalendarCreateEventOperation || second.Type != CalendarUpdateEventOperation {
		t.Fatalf("mutation types: %q %q", first.Type, second.Type)
	}
	if second.EventID != "evt-abc" || second.ExpectedEtag != `"etag-1"` || second.SendUpdates != "none" {
		t.Fatalf("second mutation = %#v", second)
	}
}

func TestMutationInputFromMapAcceptsCalendarFields(t *testing.T) {
	mutation, err := mutationInputFromMap(map[string]any{
		"type":          CalendarCreateEventOperation,
		"account":       "zach@example.test",
		"calendar_id":   "primary",
		"send_updates":  "all",
		"event":         map[string]any{"summary": "Hi"},
		"event_id":      "evt-abc",
		"expected_etag": `"etag-1"`,
		"patch":         map[string]any{"summary": "Renamed"},
	}, 0)
	if err != nil {
		t.Fatalf("mutationInputFromMap error: %v", err)
	}
	if mutation.CalendarID != "primary" || mutation.EventID != "evt-abc" || mutation.ExpectedEtag != `"etag-1"` || mutation.SendUpdates != "all" {
		t.Fatalf("unexpected mutation: %#v", mutation)
	}
	if mutation.Event["summary"] != "Hi" {
		t.Fatalf("event = %#v", mutation.Event)
	}
	if mutation.Patch["summary"] != "Renamed" {
		t.Fatalf("patch = %#v", mutation.Patch)
	}
}
