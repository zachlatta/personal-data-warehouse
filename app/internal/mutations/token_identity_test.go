package mutations

import (
	"context"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

// The name in "Bearer <client>:<token>" is whatever the caller sends; a
// policy grant must bind to the token that authenticated, so the proposal
// records that token's id beside the self-reported name.
func TestAProposalRecordsTheAuthenticatedTokenBesideTheChosenName(t *testing.T) {
	store := &recordingStore{request: Request{ID: "req-1", Status: StatusPendingReview}}
	service := NewService(store, Config{BaseURL: "https://pdw.example"})
	ctx := auth.WithPrincipal(context.Background(), auth.Principal{
		Scope: auth.ScopeAgent, TokenID: "tok_heating", ClientName: "wf:heating-winter",
	})
	if _, err := service.ProposeMutation(ctx, ProposeMutationInput{
		Title:     "Archive",
		Reason:    "test",
		Mutations: []map[string]any{{"type": GmailArchiveOperation, "account": "zach@example.test", "thread_ids": []any{"t1"}}},
	}); err != nil {
		t.Fatal(err)
	}
	call := store.createCalls[0]
	if call.RequestedByTokenID != "tok_heating" || call.RequestedBy != "wf:heating-winter" {
		t.Fatalf("requested_by = %q, token = %q", call.RequestedBy, call.RequestedByTokenID)
	}
}

func TestTheStorePersistsTheProposingTokenID(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	request, err := store.CreateRequest(ctx, CreateRequestInput{
		Title:              "token id " + t.Name() + randomSuffix(t),
		Reason:             "integration test",
		RequestedBy:        "test-host",
		RequestedByTokenID: "tok_abc",
		Mutations: []MutationInput{{
			Type:       GooglePeopleContactsOperation,
			Account:    "zach@example.test",
			Operations: []map[string]any{{"op": "delete_contact", "resource_name": "people/" + randomSuffix(t), "etag": "etag"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.RequestedByTokenID != "tok_abc" {
		t.Fatalf("RequestedByTokenID = %q", request.RequestedByTokenID)
	}
	listed, err := store.ListRequests(ctx, RequestFilter{Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range listed {
		if row.ID == request.ID && row.RequestedByTokenID != "tok_abc" {
			t.Fatalf("list lost the token id: %+v", row)
		}
	}
}

func randomSuffix(t *testing.T) string {
	t.Helper()
	id, err := randomID("t")
	if err != nil {
		t.Fatal(err)
	}
	return id
}
