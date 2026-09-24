package mutations

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func requestEvents(t *testing.T, store *PostgresStore, id string) []string {
	t.Helper()
	rows, err := queryContext(context.Background(), store.db, `
		SELECT event_type || '/' || actor_type || '/' || actor_id
		FROM @upstream_mutation_request_events WHERE request_id = $1 ORDER BY event_index
	`, id)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, s)
	}
	return out
}

func mutationStatuses(t *testing.T, store *PostgresStore, id string) []string {
	t.Helper()
	request, err := store.GetRequest(context.Background(), id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	var out []string
	for _, m := range request.Mutations {
		out = append(out, m.Status)
	}
	return out
}

func contactMutation(label string) MutationInput {
	return MutationInput{Type: GooglePeopleContactsOperation, Account: "zach@example.test", Operations: []map[string]any{{
		"op": "delete_contact", "resource_name": "people/" + strings.ReplaceAll(label, " ", "-"), "etag": "etag-" + label,
	}}}
}

func TestWithdrawRequestTakesAPendingRequestBackWithProvenance(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	old := seedRequest(t, store, "pending v1")
	replacement := seedRequest(t, store, "v2")

	updated, err := store.WithdrawRequest(ctx, old.ID, WithdrawInput{Reason: "spot-check found actionable threads", ReplacedBy: replacement.ID, Actor: "claude-code"})
	if err != nil {
		t.Fatalf("WithdrawRequest: %v", err)
	}
	if updated.Status != StatusWithdrawn || updated.Error != "spot-check found actionable threads" || updated.WithdrawnBy != "claude-code" || updated.WithdrawnAt.Unix() <= 0 {
		t.Fatalf("withdrawn row = %#v", updated)
	}
	if updated.SupersededBy != replacement.ID || updated.Revision != old.Revision+1 {
		t.Fatalf("link/revision = %q / %d", updated.SupersededBy, updated.Revision)
	}
	if got := mutationStatuses(t, store, old.ID); len(got) != 1 || got[0] != StatusWithdrawn {
		t.Fatalf("mutation rows = %v", got)
	}
	if got := requestEvents(t, store, old.ID); len(got) != 2 || got[1] != "withdrawn/agent/claude-code" {
		t.Fatalf("events = %v", got)
	}
	linked, err := store.GetRequest(ctx, replacement.ID)
	if err != nil {
		t.Fatalf("get replacement: %v", err)
	}
	if linked.ReplacesRequestID != old.ID {
		t.Fatalf("replacement should point back: %q", linked.ReplacesRequestID)
	}
	if got := requestEvents(t, store, replacement.ID); len(got) != 2 || got[1] != "replaces/agent/claude-code" {
		t.Fatalf("replacement events = %v", got)
	}
	// Terminal: nothing moves it afterwards.
	if _, err := store.ApproveRequest(ctx, old.ID, "web-ui"); err == nil || !strings.Contains(err.Error(), StatusWithdrawn) {
		t.Fatalf("approve after withdraw must be refused: %v", err)
	}
	if _, err := store.RejectRequest(ctx, old.ID, "web-ui", "no"); err == nil {
		t.Fatal("reject after withdraw must be refused")
	}
	if _, err := store.WithdrawRequest(ctx, old.ID, WithdrawInput{Reason: "again"}); err == nil || !strings.Contains(err.Error(), "already withdrawn") {
		t.Fatalf("second withdraw must be refused: %v", err)
	}
}

// The duplicate-send guard, both directions: once a human approved, the agent
// cannot pull the request; once the agent withdrew, the human cannot approve.
func TestWithdrawRequestRefusesAnApprovedRequest(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	request := seedRequest(t, store, "approved")
	if _, err := store.ApproveRequest(ctx, request.ID, "app:iphone"); err != nil {
		t.Fatalf("approve: %v", err)
	}
	_, err := store.WithdrawRequest(ctx, request.ID, WithdrawInput{Reason: "changed my mind"})
	var stateErr *RequestStateError
	if !errors.As(err, &stateErr) || stateErr.Status != "approved" || !strings.Contains(err.Error(), "do not propose the same change again") {
		t.Fatalf("expected an approved refusal, got %v", err)
	}
	after, _ := store.GetRequest(ctx, request.ID)
	if after.Status != "approved" || after.Error != "" {
		t.Fatalf("approved request must be untouched: %#v", after)
	}
	for _, status := range []string{"executing", "succeeded", "observed", "rejected", "failed_terminal"} {
		other := seedRequest(t, store, "status "+status)
		setRequestStatus(t, store, other.ID, status)
		if _, err := store.WithdrawRequest(ctx, other.ID, WithdrawInput{Reason: "x"}); !errors.As(err, &stateErr) || stateErr.Status != status {
			t.Fatalf("%s: expected a state refusal, got %v", status, err)
		}
	}
}

// A human edit bumps the revision; from then on the agent must name the
// revision it read, and a stale one is refused.
func TestWithdrawRequestRefusesAStaleRevisionAfterAReviewerEdit(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	fresh := seedRequest(t, store, "fresh")
	if fresh.Revision != 1 {
		t.Fatalf("fresh revision = %d", fresh.Revision)
	}
	// Two mutations, so a reviewer can drop one (the edit path every
	// mutation type shares).
	edited, err := store.CreateRequest(ctx, CreateRequestInput{
		Title: fresh.Title + " two", Reason: "integration test", RequestedBy: "test",
		Mutations: []MutationInput{contactMutation("a " + fresh.Title), contactMutation("b " + fresh.Title)},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := store.RemoveMutation(ctx, edited.ID, edited.Mutations[0].ID, "app:web"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if after, _ := store.GetRequest(ctx, edited.ID); after.Revision != 2 {
		t.Fatalf("revision after a reviewer edit = %d", after.Revision)
	}
	var stateErr *RequestStateError
	_, err = store.WithdrawRequest(ctx, edited.ID, WithdrawInput{Reason: "x"})
	if !errors.As(err, &stateErr) || stateErr.Revision != 2 || !strings.Contains(err.Error(), "expected_revision 2") {
		t.Fatalf("unstated revision after an edit must be refused with the current one: %v", err)
	}
	_, err = store.WithdrawRequest(ctx, edited.ID, WithdrawInput{Reason: "x", ExpectedRevision: 1})
	if !errors.As(err, &stateErr) || !strings.Contains(err.Error(), "revision 2, not 1") {
		t.Fatalf("stale revision must be refused: %v", err)
	}
	if still, _ := store.GetRequest(ctx, edited.ID); still.Status != StatusPendingReview {
		t.Fatalf("refused withdrawals must not move the request: %s", still.Status)
	}
	done, err := store.WithdrawRequest(ctx, edited.ID, WithdrawInput{Reason: "seen the edit; still moot", ExpectedRevision: 2})
	if err != nil || done.Status != StatusWithdrawn {
		t.Fatalf("current revision should withdraw: %v %s", err, done.Status)
	}
	// The row the reviewer had already dropped keeps its own record.
	if got := mutationStatuses(t, store, edited.ID); len(got) != 2 || got[0] != "rejected" || got[1] != StatusWithdrawn {
		t.Fatalf("mutation rows = %v", got)
	}
}

func TestWithdrawRequestRefusesADeadReplacement(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	request := seedRequest(t, store, "pending")
	if _, err := store.WithdrawRequest(ctx, request.ID, WithdrawInput{Reason: "x", ReplacedBy: "req_does_not_exist"}); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("missing replacement must be refused: %v", err)
	}
	denied := seedRequest(t, store, "denied")
	setRequestStatus(t, store, denied.ID, "rejected")
	if _, err := store.WithdrawRequest(ctx, request.ID, WithdrawInput{Reason: "x", ReplacedBy: denied.ID}); err == nil || !strings.Contains(err.Error(), "cannot stand in") {
		t.Fatalf("denied replacement must be refused: %v", err)
	}
	if after, _ := store.GetRequest(ctx, request.ID); after.Status != StatusPendingReview {
		t.Fatalf("request must be untouched after a refused withdrawal: %s", after.Status)
	}
}

// Proposing with replaces_request_id closes the old request out in the same
// transaction: a pending one is withdrawn and both rows link to each other.
func TestCreateRequestWithReplacesWithdrawsThePendingOriginalAtomically(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	old := seedRequest(t, store, "v1")
	replacement, err := store.CreateRequest(ctx, CreateRequestInput{
		Title: old.Title + " v2", Reason: "corrected", RequestedBy: "codex",
		Mutations: []MutationInput{contactMutation("v2 " + old.Title)},
		Replaces:  &RequestReplacement{RequestID: old.ID, Reason: "v1 missed the CC list"},
	})
	if err != nil {
		t.Fatalf("create replacement: %v", err)
	}
	if replacement.Status != StatusPendingReview || replacement.ReplacesRequestID != old.ID {
		t.Fatalf("replacement = %#v", replacement)
	}
	withdrawn, _ := store.GetRequest(ctx, old.ID)
	if withdrawn.Status != StatusWithdrawn || withdrawn.SupersededBy != replacement.ID || withdrawn.Error != "v1 missed the CC list" || withdrawn.WithdrawnBy != "codex" {
		t.Fatalf("original = %#v", withdrawn)
	}
	if got := requestEvents(t, store, replacement.ID); len(got) != 2 || got[1] != "replaces/agent/codex" {
		t.Fatalf("replacement events = %v", got)
	}
	// Only the replacement can be approved now.
	if _, err := store.ApproveRequest(ctx, old.ID, "web-ui"); err == nil {
		t.Fatal("the withdrawn original must not be approvable")
	}
	if _, err := store.ApproveRequest(ctx, replacement.ID, "web-ui"); err != nil {
		t.Fatalf("the replacement should approve: %v", err)
	}
}

// The other half of the duplicate-send guard: when the original was approved
// before the replacement arrived, the replacement is refused and NOT created.
func TestCreateRequestWithReplacesRefusesAnApprovedOriginalAndCreatesNothing(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	old := seedRequest(t, store, "approved v1")
	if _, err := store.ApproveRequest(ctx, old.ID, "app:iphone"); err != nil {
		t.Fatalf("approve: %v", err)
	}
	before, _ := store.ListRequests(ctx, RequestFilter{Limit: 500})
	input := CreateRequestInput{
		Title: old.Title + " v2", Reason: "corrected", RequestedBy: "codex",
		Mutations: []MutationInput{contactMutation("v2 " + old.Title)},
		Replaces:  &RequestReplacement{RequestID: old.ID, Reason: "invite the missing guest to every event"},
	}
	_, err := store.CreateRequest(ctx, input)
	var stateErr *RequestStateError
	if !errors.As(err, &stateErr) || stateErr.Status != "approved" {
		t.Fatalf("expected an approved refusal, got %v", err)
	}
	after, _ := store.ListRequests(ctx, RequestFilter{Limit: 500})
	if len(after) != len(before) {
		t.Fatalf("a refused replacement must create nothing: %d -> %d requests", len(before), len(after))
	}
	for _, status := range []string{"executing", "observed"} {
		setRequestStatus(t, store, old.ID, status)
		if _, err := store.CreateRequest(ctx, input); !errors.As(err, &stateErr) || stateErr.Status != status {
			t.Fatalf("%s: expected a state refusal, got %v", status, err)
		}
	}
}

// A dead original is linked the way a human's "Mark superseded" links it, and
// its failed status stays as the record.
func TestCreateRequestWithReplacesLinksADeadOriginalWithoutRewritingIt(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	for _, status := range []string{"failed_terminal", "blocked_missing_credentials", "rejected"} {
		old := seedRequest(t, store, "dead "+status)
		setRequestStatus(t, store, old.ID, status)
		replacement, err := store.CreateRequest(ctx, CreateRequestInput{
			Title: old.Title + " again", Reason: "fixed", RequestedBy: "codex",
			Mutations: []MutationInput{contactMutation("again " + old.Title)},
			Replaces:  &RequestReplacement{RequestID: old.ID, Reason: "the bug behind it is fixed"},
		})
		if err != nil {
			t.Fatalf("%s: create: %v", status, err)
		}
		linked, _ := store.GetRequest(ctx, old.ID)
		if linked.Status != status || linked.SupersededBy != replacement.ID {
			t.Fatalf("%s: original = %s / %q", status, linked.Status, linked.SupersededBy)
		}
		if got := requestEvents(t, store, old.ID); got[len(got)-1] != "superseded/agent/codex" {
			t.Fatalf("%s: events = %v", status, got)
		}
		// A second replacement of the same original is refused: replace the tip.
		_, err = store.CreateRequest(ctx, CreateRequestInput{
			Title: old.Title + " third", Reason: "fixed", RequestedBy: "codex",
			Mutations: []MutationInput{contactMutation("third " + old.Title)},
			Replaces:  &RequestReplacement{RequestID: old.ID, Reason: "again"},
		})
		if err == nil || !strings.Contains(err.Error(), "already replaced by "+replacement.ID) {
			t.Fatalf("%s: chaining onto a replaced request must be refused: %v", status, err)
		}
	}
}

// Same content, same idempotency key: the retried proposal returns the stored
// request and the replacement is applied exactly once.
func TestCreateRequestWithReplacesIsIdempotentOnRetry(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	old := seedRequest(t, store, "v1 retry")
	input := CreateRequestInput{
		Title: old.Title + " v2", Reason: "corrected", RequestedBy: "codex",
		Mutations: []MutationInput{contactMutation("v2 " + old.Title)},
		Replaces:  &RequestReplacement{RequestID: old.ID, Reason: "v1 was wrong"},
	}
	first, err := store.CreateRequest(ctx, input)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := store.CreateRequest(ctx, input)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("retry minted a second request: %s vs %s", first.ID, second.ID)
	}
	withdrawn, _ := store.GetRequest(ctx, old.ID)
	if withdrawn.Status != StatusWithdrawn || withdrawn.SupersededBy != first.ID || withdrawn.Revision != old.Revision+1 {
		t.Fatalf("original must be withdrawn exactly once: %#v", withdrawn)
	}
	if got := requestEvents(t, store, first.ID); len(got) != 2 {
		t.Fatalf("the replaces event must be written once: %v", got)
	}
	// Proposing the identical content "as a replacement of itself" is the
	// one shape the idempotent path can produce, and it is refused.
	self := input
	self.Replaces = &RequestReplacement{RequestID: first.ID, Reason: "oops"}
	if _, err := store.CreateRequest(ctx, self); err == nil || !strings.Contains(err.Error(), "same proposal") {
		t.Fatalf("self-replacement must be refused: %v", err)
	}
}

// An approval and a withdrawal racing on one request resolve to exactly one
// winner: the row lock serializes them and the loser reads the other's status.
func TestApproveAndWithdrawRaceHasExactlyOneWinner(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	for round := 0; round < 6; round++ {
		request := seedRequest(t, store, "race")
		var wg sync.WaitGroup
		var approveErr, withdrawErr error
		wg.Add(2)
		go func() { defer wg.Done(); _, approveErr = store.ApproveRequest(ctx, request.ID, "app:iphone") }()
		go func() {
			defer wg.Done()
			_, withdrawErr = store.WithdrawRequest(ctx, request.ID, WithdrawInput{Reason: "moot"})
		}()
		wg.Wait()
		if (approveErr == nil) == (withdrawErr == nil) {
			t.Fatalf("round %d: exactly one must win: approve=%v withdraw=%v", round, approveErr, withdrawErr)
		}
		final, _ := store.GetRequest(ctx, request.ID)
		switch {
		case approveErr == nil && final.Status != "approved":
			t.Fatalf("round %d: approve won but status = %s", round, final.Status)
		case withdrawErr == nil && final.Status != StatusWithdrawn:
			t.Fatalf("round %d: withdraw won but status = %s", round, final.Status)
		}
		rows := mutationStatuses(t, store, request.ID)
		if rows[0] != final.Status {
			t.Fatalf("round %d: mutation row %s disagrees with request %s", round, rows[0], final.Status)
		}
	}
}
