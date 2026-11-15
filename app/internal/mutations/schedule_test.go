package mutations

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

func TestScheduleKeysAreLiftedOffBeforeTheTypeParsesTheEntry(t *testing.T) {
	input, err := mutationInputFromMap(map[string]any{
		"type": ResideoSetThermostatOperation, "device_id": "DEV1", "heat_setpoint": 55, "hold": "permanent", "reason": "trip",
		"not_before": "2026-12-04T07:30:00-05:00", "not_after": "2026-12-04T09:30:00-05:00",
		"precondition": map[string]any{"kind": "not_home", "within_minutes": 120},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := input.Raw["not_before"]; ok {
		t.Fatal("the type parser must not see schedule keys (the thermostat refuses unknown fields)")
	}
	if _, err := thermostatChangeFromInput(input); err != nil {
		t.Fatalf("a scheduled thermostat change must parse: %v", err)
	}
	want := time.Date(2026, 12, 4, 12, 30, 0, 0, time.UTC)
	if !input.Schedule.NotBefore.Equal(want) || input.Schedule.Precondition["kind"] != "not_home" {
		t.Fatalf("schedule = %+v", input.Schedule)
	}
}

func TestScheduleValidation(t *testing.T) {
	now := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	ok := []MutationSchedule{
		{},
		{NotBefore: now.Add(72 * time.Hour), NotAfter: now.Add(74 * time.Hour)},
		{Precondition: map[string]any{"kind": "not_home", "within_minutes": 120.0}},
	}
	for _, schedule := range ok {
		if err := validateSchedule(ResideoSetThermostatOperation, schedule, now); err != nil {
			t.Fatalf("%+v: %v", schedule, err)
		}
	}
	bad := map[string]MutationSchedule{
		"days ahead":  {NotBefore: now.Add(90 * 24 * time.Hour)},
		"past":        {NotAfter: now.Add(-time.Minute)},
		"inverted":    {NotBefore: now.Add(2 * time.Hour), NotAfter: now.Add(time.Hour)},
		"kind":        {Precondition: map[string]any{"kind": "weather"}},
		"no window":   {Precondition: map[string]any{"kind": "not_home"}},
		"stray field": {Precondition: map[string]any{"kind": "not_home", "within_minutes": 60.0, "radius": 5.0}},
	}
	for name, schedule := range bad {
		if err := validateSchedule(ResideoSetThermostatOperation, schedule, now); err == nil {
			t.Fatalf("%s: validated", name)
		}
	}
	if _, _, err := scheduleFromMap(map[string]any{"not_before": "tomorrow"}); err == nil {
		t.Fatal("a time without a zone must be refused")
	}
}

func TestAScheduledStepIsStoredAndCanBeCancelledByItsProposerOnly(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeDryRun)
	ctx := context.Background()
	at := time.Now().Add(48 * time.Hour).UTC().Truncate(time.Second)
	proposer := auth.WithPrincipal(ctx, auth.Principal{Scope: auth.ScopeAgent, TokenID: f.tokenID, ClientName: "wf:heating-winter"})
	proposal, err := f.service.ProposeMutation(proposer, ProposeMutationInput{
		Title: "Trip " + randomSuffix(t), Reason: "trip plan",
		Mutations: []map[string]any{
			{"type": ResideoSetThermostatOperation, "device_id": f.device, "heat_setpoint": 55, "hold": "permanent", "reason": "leaving",
				"not_before": at.Format(time.RFC3339), "precondition": map[string]any{"kind": "not_home", "within_minutes": 120}},
			{"type": ResideoSetThermostatOperation, "device_id": f.device, "heat_setpoint": 68, "hold": "permanent", "reason": "back",
				"not_before": at.Add(72 * time.Hour).Format(time.RFC3339)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := f.store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if !request.NotBefore.Equal(at) || !request.Mutations[0].NotBefore.Equal(at) || request.Mutations[0].Precondition["kind"] != "not_home" {
		t.Fatalf("request not_before %v, mutation %v %v", request.NotBefore, request.Mutations[0].NotBefore, request.Mutations[0].Precondition)
	}
	if _, err := f.store.ApproveRequest(ctx, proposal.RequestID, "app:test"); err != nil {
		t.Fatal(err)
	}
	approved, _ := f.store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if !canCancel(approved) {
		t.Fatal("an approved plan waiting to run must offer Cancel")
	}
	other := auth.WithPrincipal(ctx, auth.Principal{Scope: auth.ScopeAgent, TokenID: "tok_other", ClientName: "wf:heating-winter"})
	if _, err := f.service.CancelMutation(other, CancelMutationInput{RequestID: proposal.RequestID, Reason: "not mine"}); err == nil {
		t.Fatal("another token cancelled the plan")
	}
	response, err := f.service.CancelMutation(proposer, CancelMutationInput{RequestID: proposal.RequestID, Reason: "trip moved"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != StatusCancelled {
		t.Fatalf("status %s", response.Status)
	}
	cancelled, _ := f.store.GetRequest(ctx, proposal.RequestID, AllMutations)
	for _, mutation := range cancelled.Mutations {
		if mutation.Status != StatusCancelled {
			t.Fatalf("mutation %s is %s", mutation.ID, mutation.Status)
		}
	}
	if _, err := f.service.CancelMutation(proposer, CancelMutationInput{RequestID: proposal.RequestID, Reason: "again"}); err == nil || !strings.Contains(err.Error(), "nothing waiting") {
		t.Fatalf("a second cancel: %v", err)
	}
}

func TestARunningStepCannotBeCancelled(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeDryRun)
	ctx := context.Background()
	proposal, err := f.propose(t, f.tokenID, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ApproveRequest(ctx, proposal.RequestID, "app:test"); err != nil {
		t.Fatal(err)
	}
	if _, err := execContext(ctx, f.store.db, `UPDATE @upstream_mutations SET status = 'executing' WHERE request_id = $1`, proposal.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.CancelRequest(ctx, proposal.RequestID, CancelInput{Actor: "app:test", Reason: "stop"}); err == nil || !strings.Contains(err.Error(), "running") {
		t.Fatalf("cancel of a running step: %v", err)
	}
}

// A precondition is checked only by the cloud worker, right before the
// thermostat executor runs. Device workers (Notes, Contacts, texts, printer)
// never check one, so a precondition on any other type is refused at
// proposal rather than silently ignored when the step runs.
func TestAPreconditionIsRefusedOnATypeWhoseWorkerDoesNotCheckIt(t *testing.T) {
	store := &recordingStore{}
	service := NewService(store, Config{AppleNotesAccounts: []string{"zach@example.test"}})
	precondition := map[string]any{"kind": "not_home", "within_minutes": 120}
	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{Title: "note", Reason: "x", Mutations: []map[string]any{{
		"type": AppleNotesCreateNoteOperation, "account": "zach@example.test", "body": "hello", "folder": "PDW Agent", "precondition": precondition,
	}}})
	if err == nil || !strings.Contains(err.Error(), "precondition") || !strings.Contains(err.Error(), ResideoSetThermostatOperation) {
		t.Fatalf("err = %v", err)
	}
	if len(store.createCalls) != 0 {
		t.Fatal("a refused proposal reached the store")
	}
	_, err = service.ProposeMutation(context.Background(), ProposeMutationInput{Title: "heat", Reason: "x", Mutations: []map[string]any{{
		"type": ResideoSetThermostatOperation, "device_id": "DEV1", "heat_setpoint": 55, "hold": "permanent", "reason": "away", "precondition": precondition,
	}}})
	if err != nil {
		t.Fatalf("the thermostat takes a precondition: %v", err)
	}
}

// Cancelling the waiting step of a plan whose other step already failed
// leaves a failed request, not one stuck "approved" with nothing to run, and
// the request's mutation_statuses say what each step ended as.
func TestCancellingRollsTheRequestUpWithTheSharedLadder(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeDryRun)
	ctx := context.Background()
	at := time.Now().Add(48 * time.Hour).UTC().Truncate(time.Second)
	for _, first := range []string{"failed_terminal", "succeeded", "skipped_precondition"} {
		proposal, err := f.service.ProposeMutation(ctx, ProposeMutationInput{
			Title: "Plan " + randomSuffix(t), Reason: "trip plan",
			Mutations: []map[string]any{
				{"type": ResideoSetThermostatOperation, "device_id": f.device, "heat_setpoint": 55, "hold": "permanent", "reason": "leaving"},
				{"type": ResideoSetThermostatOperation, "device_id": f.device, "heat_setpoint": 68, "hold": "permanent", "reason": "back", "not_before": at.Format(time.RFC3339)},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.ApproveRequest(ctx, proposal.RequestID, "app:test"); err != nil {
			t.Fatal(err)
		}
		if _, err := execContext(ctx, f.store.db, `UPDATE @upstream_mutations SET status = $2 WHERE request_id = $1 AND request_index = 0`, proposal.RequestID, first); err != nil {
			t.Fatal(err)
		}
		request, err := f.service.CancelRequest(ctx, proposal.RequestID, CancelInput{Actor: "app:test", Reason: "trip cancelled"})
		if err != nil {
			t.Fatal(err)
		}
		if request.Status != first {
			t.Fatalf("%s + cancelled: request is %s", first, request.Status)
		}
		statuses, _ := request.Result["mutation_statuses"].(map[string]any)
		if len(statuses) != 2 {
			t.Fatalf("%s + cancelled: mutation_statuses = %#v", first, request.Result)
		}
	}
}

// A request starts when its first step can: if any step runs once approved,
// the request has no "Runs at" (it would claim a later start than the truth),
// and it has a deadline only when every step has one.
func TestRequestLevelTimesAreSetOnlyWhenEveryStepHasOne(t *testing.T) {
	at := time.Date(2026, 12, 4, 12, 0, 0, 0, time.UTC)
	step := func(notBefore, notAfter time.Time) storedMutation {
		return storedMutation{Schedule: MutationSchedule{NotBefore: notBefore, NotAfter: notAfter}}
	}
	mixed := []storedMutation{step(time.Time{}, time.Time{}), step(at.Add(72*time.Hour), at.Add(74*time.Hour))}
	if got := requestNotBefore(mixed); !got.IsZero() {
		t.Fatalf("a request with an immediate step got not_before %s", got)
	}
	if got := requestNotAfter(mixed); !got.IsZero() {
		t.Fatalf("a request with an open-ended step got not_after %s", got)
	}
	both := []storedMutation{step(at.Add(72*time.Hour), at.Add(74*time.Hour)), step(at, at.Add(2*time.Hour))}
	if got := requestNotBefore(both); !got.Equal(at) {
		t.Fatalf("not_before = %s, want the earliest step's %s", got, at)
	}
	if got := requestNotAfter(both); !got.Equal(at.Add(74 * time.Hour)) {
		t.Fatalf("not_after = %s, want the latest step's", got)
	}
}

// Cancel and the worker's claim/send-back take a request's rows in one order:
// its mutations, then the request (postgres.py claims mutations FOR UPDATE,
// then updates the request when it sends one back). Overlapping, the cancel
// waits instead of deadlocking.
func TestCancelAndAClaimSendingBackDoNotDeadlock(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeDryRun)
	ctx := context.Background()
	proposal, err := f.propose(t, f.tokenID, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ApproveRequest(ctx, proposal.RequestID, "app:test"); err != nil {
		t.Fatal(err)
	}
	// The claim's transaction: lock the request's mutations, as the Python
	// claim does with FOR UPDATE SKIP LOCKED.
	claim, err := f.store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = claim.Rollback() }()
	if _, err := claim.ExecContext(ctx, expandForTest(`SELECT id FROM @upstream_mutations WHERE request_id = $1 FOR UPDATE`), proposal.RequestID); err != nil {
		t.Fatal(err)
	}
	cancelled := make(chan error, 1)
	go func() {
		_, err := f.service.CancelRequest(ctx, proposal.RequestID, CancelInput{Actor: "app:test", Reason: "trip moved"})
		cancelled <- err
	}()
	time.Sleep(300 * time.Millisecond)
	// The send-back: back to review, the mutations then the request.
	if _, err := claim.ExecContext(ctx, expandForTest(`UPDATE @upstream_mutations SET status = 'pending_review', approved_by = '' WHERE request_id = $1`), proposal.RequestID); err != nil {
		t.Fatalf("send-back mutations: %v", err)
	}
	if _, err := claim.ExecContext(ctx, expandForTest(`UPDATE @upstream_mutation_requests SET status = 'pending_review', approved_by = '' WHERE id = $1`), proposal.RequestID); err != nil {
		t.Fatalf("send-back request (deadlock?): %v", err)
	}
	if err := claim.Commit(); err != nil {
		t.Fatal(err)
	}
	err = <-cancelled
	if err != nil && strings.Contains(err.Error(), "deadlock") {
		t.Fatalf("cancel deadlocked: %v", err)
	}
	// The send-back won: nothing waits to run, so the cancel says so.
	if err == nil || !strings.Contains(err.Error(), "nothing waiting") {
		t.Fatalf("cancel after the send-back: %v", err)
	}
}

func expandForTest(statement string) string { return warehouse.ExpandRelations(statement) }

// Every Go path that decides a request takes the same order as the worker:
// a deny overlapping a transaction that holds the request's mutations and
// then updates the request waits, and neither deadlocks.
func TestARequestDecisionTakesTheWorkersLockOrder(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeDryRun)
	ctx := context.Background()
	proposal, err := f.propose(t, f.tokenID, 61)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := f.store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = worker.Rollback() }()
	if _, err := worker.ExecContext(ctx, expandForTest(`SELECT id FROM @upstream_mutations WHERE request_id = $1 FOR UPDATE`), proposal.RequestID); err != nil {
		t.Fatal(err)
	}
	denied := make(chan error, 1)
	go func() {
		_, err := f.store.RejectRequest(ctx, proposal.RequestID, "app:test", "no")
		denied <- err
	}()
	time.Sleep(300 * time.Millisecond)
	if _, err := worker.ExecContext(ctx, expandForTest(`UPDATE @upstream_mutation_requests SET updated_at = now() WHERE id = $1`), proposal.RequestID); err != nil {
		t.Fatalf("the worker's request update (deadlock?): %v", err)
	}
	if err := worker.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-denied; err != nil {
		t.Fatalf("deny: %v", err)
	}
}
