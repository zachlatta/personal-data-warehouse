package mutations

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

// Both ensure paths (this one and Python's) must declare the three
// withdrawal columns, or whichever bootstraps a database first decides its
// shape; the Python side is pinned by
// tests/test_upstream_mutations_postgres.py.
func TestWithdrawDDLAddsTheColumnsToExistingDatabases(t *testing.T) {
	for _, column := range []string{"replaces_request_id", "withdrawn_by", "withdrawn_at"} {
		var found bool
		for _, statement := range upstreamMutationSchemaStatements {
			if strings.Contains(statement, "ADD COLUMN IF NOT EXISTS "+column) && strings.Contains(statement, "@upstream_mutation_requests") {
				found = true
			}
		}
		if !found {
			t.Fatalf("no idempotent ALTER adds %s to existing databases", column)
		}
	}
}

func TestValidateWithdrawInputRequiresAReason(t *testing.T) {
	if err := validateWithdrawInput("req_1", WithdrawInput{}); err == nil || !strings.Contains(err.Error(), "reason") {
		t.Fatalf("expected a reason error, got %v", err)
	}
	if err := validateWithdrawInput("", WithdrawInput{Reason: "x"}); err == nil {
		t.Fatal("expected an id error")
	}
	if err := validateWithdrawInput("req_1", WithdrawInput{Reason: "x", ReplacedBy: "req_1"}); err == nil || !strings.Contains(err.Error(), "itself") {
		t.Fatalf("expected a self-replacement error, got %v", err)
	}
	if err := validateWithdrawInput("req_1", WithdrawInput{Reason: "x", ExpectedRevision: -1}); err == nil {
		t.Fatal("expected a revision error")
	}
	if err := validateWithdrawInput("req_1", WithdrawInput{Reason: "done by hand", ReplacedBy: "req_2", ExpectedRevision: 3}); err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}
}

// The stale-version guard: an untouched request needs no version stated; one
// a reviewer has changed needs the revision the agent read, and it must match.
func TestCheckPendingRevisionIsTheStaleVersionGuard(t *testing.T) {
	untouched := requestLock{ID: "req_1", Status: StatusPendingReview, Revision: 1}
	if err := checkPendingRevision(untouched, 0, "withdrawn"); err != nil {
		t.Fatalf("untouched request should withdraw without a stated revision: %v", err)
	}
	if err := checkPendingRevision(untouched, 1, "withdrawn"); err != nil {
		t.Fatalf("matching revision should pass: %v", err)
	}
	edited := requestLock{ID: "req_1", Status: StatusPendingReview, Revision: 3}
	err := checkPendingRevision(edited, 0, "withdrawn")
	var stateErr *RequestStateError
	if !errors.As(err, &stateErr) || stateErr.Revision != 3 || !strings.Contains(err.Error(), "expected_revision 3") {
		t.Fatalf("edited request without a stated revision must be refused with the current revision: %v", err)
	}
	if err := checkPendingRevision(edited, 2, "replaced"); err == nil || !strings.Contains(err.Error(), "revision 3, not 2") {
		t.Fatalf("stale revision must be refused: %v", err)
	}
	if err := checkPendingRevision(edited, 3, "replaced"); err != nil {
		t.Fatalf("current revision should pass: %v", err)
	}
}

// Every non-pending status refuses, and the refusal says why in terms the
// agent can act on: an approved or finished request must not be re-proposed.
func TestRefusalForStatusNamesTheDuplicateHazard(t *testing.T) {
	for _, status := range []string{"approved", "executing", "succeeded", "observed", "rejected", "withdrawn", "failed_terminal"} {
		err := checkPendingRevision(requestLock{ID: "req_1", Status: status, Revision: 1}, 0, "withdrawn")
		var stateErr *RequestStateError
		if !errors.As(err, &stateErr) || stateErr.Status != status {
			t.Fatalf("%s: expected a RequestStateError carrying the status, got %v", status, err)
		}
		if !strings.Contains(err.Error(), "req_1") {
			t.Fatalf("%s: refusal should name the request: %v", status, err)
		}
	}
	for _, status := range []string{"approved", "executing", "succeeded", "observed"} {
		err := checkPendingRevision(requestLock{ID: "req_1", Status: status}, 0, "replaced")
		if !strings.Contains(err.Error(), "repeat") && !strings.Contains(err.Error(), "duplicat") {
			t.Fatalf("%s: refusal should warn about repeating the effect: %v", status, err)
		}
	}
}

func TestToolsExposeWithdrawMutation(t *testing.T) {
	tools := Tools(NewService(&recordingStore{}, Config{}))
	names := map[string]bool{}
	for _, tl := range tools {
		names[tl.Name()] = true
	}
	for _, want := range []string{"propose_mutation", "propose_mutation_help", "withdraw_mutation"} {
		if !names[want] {
			t.Fatalf("tool %s not registered: %v", want, names)
		}
	}
	if Tools(nil) != nil {
		t.Fatal("nil service must register no tools")
	}
}

func TestAgentFacingErrorMapsStateConflictsToInvalidInput(t *testing.T) {
	var invalid *tool.InvalidInputError
	if err := agentFacingError(&RequestStateError{Message: "already approved"}); !errors.As(err, &invalid) || invalid.Message != "already approved" {
		t.Fatalf("state error should become invalid input: %v", err)
	}
	if err := agentFacingError(invalidProposalInput(errors.New("bad"))); !errors.As(err, &invalid) {
		t.Fatalf("input error should become invalid input: %v", err)
	}
	other := errors.New("database down")
	if err := agentFacingError(other); !errors.Is(err, other) {
		t.Fatalf("storage errors must pass through: %v", err)
	}
	if agentFacingError(nil) != nil {
		t.Fatal("nil stays nil")
	}
}

func TestMutationHelpDocumentsReplacementAndWithdrawal(t *testing.T) {
	doc := MutationHelp()
	if !strings.Contains(doc.Overview, "withdraw_mutation") || !strings.Contains(doc.Overview, "replaces_request_id") {
		t.Fatalf("overview does not teach the correction path: %s", doc.Overview)
	}
	names := map[string]bool{}
	for _, arg := range doc.Common {
		names[arg.Name] = true
	}
	for _, want := range []string{"replaces_request_id", "replaces_reason", "replaces_revision"} {
		if !names[want] {
			t.Fatalf("common fields omit %s", want)
		}
	}
}

type withdrawRecordingStore struct {
	recordingStore
	withdrawn   []string
	inputs      []WithdrawInput
	withdrawErr error
	result      Request
}

func (s *withdrawRecordingStore) WithdrawRequest(_ context.Context, id string, input WithdrawInput) (Request, error) {
	s.withdrawn = append(s.withdrawn, id)
	s.inputs = append(s.inputs, input)
	if s.withdrawErr != nil {
		return Request{}, s.withdrawErr
	}
	return s.result, nil
}

func TestWithdrawMutationRequiresAReasonBeforeTheStore(t *testing.T) {
	store := &withdrawRecordingStore{}
	service := NewService(store, Config{BaseURL: "https://mcp.example.test"})
	_, err := service.WithdrawMutation(context.Background(), WithdrawMutationInput{RequestID: "req_1"})
	var inputErr *proposalInputError
	if !errors.As(err, &inputErr) || !strings.Contains(err.Error(), "reason") {
		t.Fatalf("expected an input error about the reason, got %v", err)
	}
	if len(store.withdrawn) != 0 {
		t.Fatal("store must not be reached without a reason")
	}
}

func TestWithdrawMutationPassesTheActorReasonAndRevisionThrough(t *testing.T) {
	store := &withdrawRecordingStore{result: Request{ID: "req_1", Status: StatusWithdrawn, SupersededBy: "req_2", Revision: 4}}
	var hooked []string
	service := NewService(store, Config{BaseURL: "https://mcp.example.test", RequestWithdrawn: func(_ context.Context, r Request) { hooked = append(hooked, r.ID) }})
	response, err := service.WithdrawMutation(context.Background(), WithdrawMutationInput{
		RequestID: " req_1 ", Reason: " done by hand ", ReplacedByRequestID: "req_2", ExpectedRevision: 3,
	})
	if err != nil {
		t.Fatalf("WithdrawMutation: %v", err)
	}
	if got := store.inputs[0]; got.Reason != "done by hand" || got.ReplacedBy != "req_2" || got.ExpectedRevision != 3 || got.Actor != "mcp" {
		t.Fatalf("store input = %#v", got)
	}
	if store.withdrawn[0] != "req_1" {
		t.Fatalf("withdrawn id = %q", store.withdrawn[0])
	}
	if response.Status != StatusWithdrawn || response.ReplacedByRequestID != "req_2" || response.Revision != 4 || response.ReviewURL != "https://mcp.example.test/mutation-review/requests/req_1" {
		t.Fatalf("response = %#v", response)
	}
	if len(hooked) != 1 || hooked[0] != "req_1" {
		t.Fatalf("withdrawn hook = %v", hooked)
	}
}

func TestWithdrawMutationReportsAMissingRequestAsInput(t *testing.T) {
	store := &withdrawRecordingStore{withdrawErr: ErrNotFound}
	service := NewService(store, Config{})
	_, err := service.WithdrawMutation(context.Background(), WithdrawMutationInput{RequestID: "req_x", Reason: "gone"})
	var inputErr *proposalInputError
	if !errors.As(err, &inputErr) || !strings.Contains(err.Error(), "req_x") {
		t.Fatalf("expected an input error naming the request, got %v", err)
	}
}

func TestProposeMutationCarriesTheReplacementToTheStore(t *testing.T) {
	store := &recordingStore{request: Request{ID: "req_2", Status: StatusPendingReview, Revision: 1}}
	service := NewService(store, Config{BaseURL: "https://mcp.example.test"})
	base := ProposeMutationInput{
		Title:  "Archive stale mail v2",
		Reason: "corrected set",
		Mutations: []map[string]any{{
			"type": GmailArchiveOperation, "account": "zach@example.test", "thread_ids": []any{"t1"},
		}},
	}
	// A reason is required with the id.
	withID := base
	withID.ReplacesRequestID = "req_1"
	if _, err := service.ProposeMutation(context.Background(), withID); err == nil || !strings.Contains(err.Error(), "replaces_reason") {
		t.Fatalf("replaces without a reason must be refused: %v", err)
	}
	// And the reason means nothing without the id.
	reasonOnly := base
	reasonOnly.ReplacesReason = "v1 was wrong"
	if _, err := service.ProposeMutation(context.Background(), reasonOnly); err == nil || !strings.Contains(err.Error(), "replaces_request_id") {
		t.Fatalf("replaces_reason without an id must be refused: %v", err)
	}
	if len(store.createCalls) != 0 {
		t.Fatal("store must not be reached with a malformed replacement")
	}
	full := base
	full.ReplacesRequestID = " req_1 "
	full.ReplacesReason = "spot-check found actionable threads in v1"
	full.ReplacesRevision = 2
	response, err := service.ProposeMutation(context.Background(), full)
	if err != nil {
		t.Fatalf("ProposeMutation: %v", err)
	}
	if response.Revision != 1 {
		t.Fatalf("response should carry the revision: %#v", response)
	}
	call := store.createCalls[0]
	if call.Replaces == nil || call.Replaces.RequestID != "req_1" || call.Replaces.Reason != "spot-check found actionable threads in v1" || call.Replaces.ExpectedRevision != 2 {
		t.Fatalf("replacement not carried to the store: %#v", call.Replaces)
	}
}
