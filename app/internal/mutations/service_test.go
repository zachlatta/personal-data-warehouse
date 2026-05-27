package mutations

import (
	"context"
	"strings"
	"testing"
	"time"
)

type recordingStore struct {
	createCalls []CreateRequestInput
	request     Request
	err         error
}

func (s *recordingStore) CreateRequest(_ context.Context, input CreateRequestInput) (Request, error) {
	s.createCalls = append(s.createCalls, input)
	if s.err != nil {
		return Request{}, s.err
	}
	return s.request, nil
}

func (s *recordingStore) ListRequests(context.Context, RequestFilter) ([]Request, error) {
	return nil, nil
}

func (s *recordingStore) GetRequest(context.Context, string) (Request, error) {
	return Request{}, nil
}

func (s *recordingStore) UpdateGmailEmailMutation(context.Context, string, string, UpdateGmailEmailMutationInput, string) (Mutation, error) {
	return Mutation{}, nil
}

func (s *recordingStore) RemoveMutation(context.Context, string, string, string) (Mutation, error) {
	return Mutation{}, nil
}

func (s *recordingStore) ApproveRequest(context.Context, string, string) (Request, error) {
	return Request{}, nil
}

func (s *recordingStore) RejectRequest(context.Context, string, string, string) (Request, error) {
	return Request{}, nil
}

func TestProposeMutationGmailArchiveThreads(t *testing.T) {
	store := &recordingStore{request: Request{
		ID:     "req-123",
		Status: "pending_review",
		Mutations: []Mutation{{
			ID: "mut-123",
		}},
	}}
	service := NewService(store, Config{
		BaseURL: "https://mcp.example.test",
		Now:     func() time.Time { return time.Unix(1700000000, 0).UTC() },
	})

	response, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Archive stale mail",
		Reason: "clear stale mail",
		Mutations: []map[string]any{{
			"type":       GmailArchiveOperation,
			"account":    "ZACH@example.test",
			"thread_ids": []any{" thread-1 ", "", "thread-2"},
		}},
		Context: map[string]any{"source": "test"},
	})
	if err != nil {
		t.Fatalf("ProposeMutation returned error: %v", err)
	}

	if response.RequestID != "req-123" || response.Status != "pending_review" {
		t.Fatalf("unexpected response: %#v", response)
	}
	if got := strings.Join(response.MutationIDs, ","); got != "mut-123" {
		t.Fatalf("MutationIDs = %q", got)
	}
	if response.ApprovalURL != "https://mcp.example.test/mutation-review/requests/req-123" {
		t.Fatalf("ApprovalURL = %q", response.ApprovalURL)
	}
	if len(store.createCalls) != 1 {
		t.Fatalf("CreateRequest calls = %d", len(store.createCalls))
	}
	call := store.createCalls[0]
	if call.Title != "Archive stale mail" || call.Reason != "clear stale mail" || call.RequestedBy != "mcp" {
		t.Fatalf("unexpected request input: %#v", call)
	}
	if got := call.Context["source"]; got != "test" {
		t.Fatalf("context source = %#v", got)
	}
	if len(call.Mutations) != 1 {
		t.Fatalf("mutation count = %d", len(call.Mutations))
	}
	mutation := call.Mutations[0]
	if mutation.Type != GmailArchiveOperation || mutation.Account != "zach@example.test" {
		t.Fatalf("unexpected mutation input: %#v", mutation)
	}
	if got := strings.Join(mutation.ThreadIDs, ","); got != "thread-1,thread-2" {
		t.Fatalf("ThreadIDs = %q", got)
	}
}

func TestProposeMutationRejectsEmptyGmailThreadIDsBeforeStore(t *testing.T) {
	store := &recordingStore{}
	service := NewService(store, Config{BaseURL: "https://mcp.example.test"})

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Archive stale mail",
		Reason: "clear stale mail",
		Mutations: []map[string]any{{
			"type":       GmailArchiveOperation,
			"account":    "zach@example.test",
			"thread_ids": []any{" "},
		}},
	})
	if err == nil {
		t.Fatal("expected empty thread_ids error")
	}
	if !strings.Contains(err.Error(), "thread_ids") {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.createCalls) != 0 {
		t.Fatalf("CreateRequest should not have been called: %#v", store.createCalls)
	}
}

func TestProposeMutationGmailSendEmail(t *testing.T) {
	store := &recordingStore{request: Request{
		ID:     "req-email",
		Status: "pending_review",
		Mutations: []Mutation{{
			ID: "mut-email",
		}},
	}}
	service := NewService(store, Config{BaseURL: "https://mcp.example.test"})

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Reply",
		Reason: "follow up",
		Mutations: []map[string]any{{
			"type":          GmailSendEmailOperation,
			"account":       "ZACH@example.test",
			"delivery_mode": "draft",
			"message": map[string]any{
				"to":                 []any{" one@example.test "},
				"cc":                 []any{"two@example.test"},
				"bcc":                []any{"secret@example.test"},
				"subject":            " Hello ",
				"body_text":          "Body",
				"reply_to_thread_id": " thread-1 ",
			},
		}},
	})
	if err != nil {
		t.Fatalf("ProposeMutation returned error: %v", err)
	}

	if len(store.createCalls) != 1 || len(store.createCalls[0].Mutations) != 1 {
		t.Fatalf("unexpected create calls: %#v", store.createCalls)
	}
	mutation := store.createCalls[0].Mutations[0]
	if mutation.Type != GmailSendEmailOperation || mutation.Account != "zach@example.test" || mutation.DeliveryMode != "draft" {
		t.Fatalf("unexpected email mutation: %#v", mutation)
	}
	if got := strings.TrimSpace(stringFromAny(mutation.Message["subject"])); got != "Hello" {
		t.Fatalf("subject = %#v", mutation.Message["subject"])
	}
	if got := strings.Join(stringSliceFromAny(mutation.Message["to"]), ","); got != "one@example.test" {
		t.Fatalf("to = %q", got)
	}
	if got := strings.TrimSpace(stringFromAny(mutation.Message["reply_to_thread_id"])); got != "thread-1" {
		t.Fatalf("reply_to_thread_id = %#v", mutation.Message["reply_to_thread_id"])
	}
}

func TestProposeMutationGmailSendEmailAcceptsTitledVariants(t *testing.T) {
	store := &recordingStore{request: Request{ID: "req-email", Status: "pending_review"}}
	service := NewService(store, Config{BaseURL: "https://mcp.example.test"})

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Reply",
		Reason: "offer choices",
		Mutations: []map[string]any{{
			"type":          GmailSendEmailOperation,
			"account":       "zach@example.test",
			"delivery_mode": "send",
			"message": map[string]any{
				"to":        []any{"one@example.test"},
				"subject":   "Hello",
				"body_text": "Base body",
			},
			"variants": []map[string]any{
				{"title": "Direct Reply", "body_text": "Direct body"},
				{"title": "Softer Ask", "body_text": "Softer body"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("ProposeMutation returned error: %v", err)
	}

	mutation := store.createCalls[0].Mutations[0]
	if len(mutation.EmailVariants) != 2 {
		t.Fatalf("variants = %#v", mutation.EmailVariants)
	}
	if mutation.EmailVariants[0].Title != "Direct Reply" || mutation.EmailVariants[1].Title != "Softer Ask" {
		t.Fatalf("variant titles = %#v", mutation.EmailVariants)
	}
}

func TestProposeMutationRejectsGmailVariantWithoutTwoWordTitle(t *testing.T) {
	store := &recordingStore{}
	service := NewService(store, Config{BaseURL: "https://mcp.example.test"})

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Reply",
		Reason: "offer choices",
		Mutations: []map[string]any{{
			"type":    GmailSendEmailOperation,
			"account": "zach@example.test",
			"message": map[string]any{
				"to":        []any{"one@example.test"},
				"subject":   "Hello",
				"body_text": "Body",
			},
			"variants": []map[string]any{
				{"title": "Direct", "body_text": "Direct body"},
			},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "variant title") {
		t.Fatalf("expected variant title error, got %v", err)
	}
	if len(store.createCalls) != 0 {
		t.Fatalf("CreateRequest should not have been called: %#v", store.createCalls)
	}
}

func TestProposeMutationContactMutations(t *testing.T) {
	store := &recordingStore{request: Request{
		ID:     "req-contact",
		Status: "pending_review",
		Mutations: []Mutation{{
			ID: "mut-contact",
		}},
	}}
	service := NewService(store, Config{BaseURL: "https://mcp.example.test"})

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Add contact",
		Reason: "add missing contact",
		Mutations: []map[string]any{{
			"type":    GooglePeopleContactsOperation,
			"account": "ZACH@example.test",
			"operations": []map[string]any{
				{"op": "create_contact", "person": map[string]any{"names": []any{map[string]any{"givenName": "New"}}}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("ProposeMutation returned error: %v", err)
	}

	if len(store.createCalls) != 1 || len(store.createCalls[0].Mutations) != 1 {
		t.Fatalf("unexpected create calls: %#v", store.createCalls)
	}
	mutation := store.createCalls[0].Mutations[0]
	if mutation.Type != GooglePeopleContactsOperation || mutation.Account != "zach@example.test" || len(mutation.Operations) != 1 {
		t.Fatalf("unexpected contact mutation: %#v", mutation)
	}
}

func TestProposeMutationAcceptsOperationAliasAndContactBatchOperation(t *testing.T) {
	store := &recordingStore{request: Request{
		ID:     "req-contact",
		Status: "pending_review",
		Mutations: []Mutation{{
			ID: "mut-contact",
		}},
	}}
	service := NewService(store, Config{BaseURL: "https://mcp.example.test"})

	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Add contact",
		Reason: "add missing contact",
		Mutations: []map[string]any{{
			"operation": ContactsBatchMutationOperation,
			"account":   "ZACH@example.test",
			"operations": []map[string]any{
				{"op": "create_contact", "person": map[string]any{"names": []any{map[string]any{"givenName": "New"}}}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("ProposeMutation returned error: %v", err)
	}

	if len(store.createCalls) != 1 || len(store.createCalls[0].Mutations) != 1 {
		t.Fatalf("unexpected create calls: %#v", store.createCalls)
	}
	mutation := store.createCalls[0].Mutations[0]
	if mutation.Type != ContactsBatchMutationOperation || mutation.Account != "zach@example.test" || len(mutation.Operations) != 1 {
		t.Fatalf("unexpected contact mutation: %#v", mutation)
	}
}

func TestMutationHelpDocumentsSupportedMutationTypes(t *testing.T) {
	help := MutationHelp()
	types := map[string]bool{}
	for _, mutationType := range help.Mutations {
		types[mutationType.Type] = true
		if len(mutationType.Fields) == 0 {
			t.Fatalf("%s has no documented fields", mutationType.Type)
		}
		if len(mutationType.Example) == 0 {
			t.Fatalf("%s has no example", mutationType.Type)
		}
	}

	for _, expected := range []string{
		GmailArchiveOperation,
		GmailUnarchiveOperation,
		GmailSendEmailOperation,
		GooglePeopleContactsOperation,
		CalendarCreateEventOperation,
		CalendarUpdateEventOperation,
		CalendarDeleteEventOperation,
	} {
		if !types[expected] {
			t.Fatalf("help missing mutation type %s", expected)
		}
	}
}
