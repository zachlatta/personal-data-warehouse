package queue

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// fakeStore is the Python tests' _FakeWarehouse: it records every call and
// hands back the canned claimable rows.
type fakeStore struct {
	claimable    []Mutation
	claimErr     error
	ensured      int
	reclaimCalls [][]Operation
	reclaimActor string
	reclaimAfter time.Duration
	claimCalls   []claimCall
	completed    []completion
	failed       []failure
	closed       int
}

type claimCall struct {
	limit     int
	claimedBy string
	providers []string
}
type completion struct {
	id     string
	result map[string]any
	actor  string
}
type failure struct {
	id, status, err string
	result          map[string]any
	actor           string
}

func (f *fakeStore) EnsureTables(context.Context) error { f.ensured++; return nil }
func (f *fakeStore) ReclaimStaleExecuting(_ context.Context, staleAfter time.Duration, idempotent []Operation, actorID string) (int, error) {
	f.reclaimCalls = append(f.reclaimCalls, idempotent)
	f.reclaimActor = actorID
	f.reclaimAfter = staleAfter
	return 0, nil
}
func (f *fakeStore) ClaimApproved(_ context.Context, limit int, claimedBy string, providers []string) ([]Mutation, error) {
	f.claimCalls = append(f.claimCalls, claimCall{limit, claimedBy, providers})
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	claimed := f.claimable
	f.claimable = nil
	return claimed, nil
}
func (f *fakeStore) Complete(_ context.Context, id string, result map[string]any, actor string) error {
	f.completed = append(f.completed, completion{id, result, actor})
	return nil
}
func (f *fakeStore) Fail(_ context.Context, id, status, errText string, result map[string]any, actor string) error {
	f.failed = append(f.failed, failure{id, status, errText, result, actor})
	return nil
}
func (f *fakeStore) Close() error { f.closed++; return nil }

type fakeExecutor struct {
	results []Result
	seen    []Mutation
}

func (f *fakeExecutor) Execute(m Mutation) Result {
	f.seen = append(f.seen, m)
	r := f.results[0]
	f.results = f.results[1:]
	return r
}

var alwaysAcquire = LockFunc(func(context.Context) (func(), bool, error) { return func() {}, true, nil })

func notesOptions(store Store, executor Executor) Options {
	return Options{
		Store:      store,
		Executor:   executor,
		Lock:       alwaysAcquire,
		Provider:   "apple_notes",
		Idempotent: []Operation{{Provider: "apple_notes", Operation: "apple_notes.update_note"}},
		ClaimedBy:  "mac:test:apple_notes_mutation_worker",
	}
}

func TestTheWorkerClaimsOnlyItsProviderWithTheDefaults(t *testing.T) {
	store := &fakeStore{}
	opts := notesOptions(store, &fakeExecutor{})
	opts.EnsureTables = true
	summary, err := Process(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if summary != (Summary{}) {
		t.Fatalf("summary %+v", summary)
	}
	if store.ensured != 1 {
		t.Fatalf("ensured %d", store.ensured)
	}
	want := claimCall{limit: DefaultBatchSize, claimedBy: "mac:test:apple_notes_mutation_worker", providers: []string{"apple_notes"}}
	if len(store.claimCalls) != 1 || !reflect.DeepEqual(store.claimCalls[0], want) {
		t.Fatalf("claim calls %+v", store.claimCalls)
	}
	if store.reclaimAfter != DefaultReclaimAfter || store.reclaimActor != "mac:test:apple_notes_mutation_worker" {
		t.Fatalf("reclaim after %v actor %q", store.reclaimAfter, store.reclaimActor)
	}
}

func TestACreateIsNeverReclaimedBecauseReplayingItDuplicatesTheNote(t *testing.T) {
	store := &fakeStore{}
	if _, err := Process(context.Background(), notesOptions(store, &fakeExecutor{})); err != nil {
		t.Fatal(err)
	}
	if len(store.reclaimCalls) != 1 {
		t.Fatalf("reclaim calls %v", store.reclaimCalls)
	}
	reclaimable := store.reclaimCalls[0]
	if !reflect.DeepEqual(reclaimable, []Operation{{"apple_notes", "apple_notes.update_note"}}) {
		t.Fatalf("reclaimable %v", reclaimable)
	}
	for _, op := range reclaimable {
		if op.Operation == "apple_notes.create_note" {
			t.Fatal("create_note must not be reclaimable")
		}
	}
}

func TestASuccessfulMutationIsCompletedWithItsResult(t *testing.T) {
	store := &fakeStore{claimable: []Mutation{{ID: "mut-1", Provider: "apple_notes"}}}
	executor := &fakeExecutor{results: []Result{{Status: StatusSucceeded, ResultJSON: map[string]any{"note_id": "x-coredata://A/ICNote/p1"}}}}

	summary, err := Process(context.Background(), notesOptions(store, executor))
	if err != nil {
		t.Fatal(err)
	}
	if summary != (Summary{Claimed: 1, Succeeded: 1}) {
		t.Fatalf("summary %+v", summary)
	}
	want := []completion{{"mut-1", map[string]any{"note_id": "x-coredata://A/ICNote/p1"}, "mac:test:apple_notes_mutation_worker"}}
	if !reflect.DeepEqual(store.completed, want) || len(store.failed) != 0 {
		t.Fatalf("completed %+v failed %+v", store.completed, store.failed)
	}
	if len(executor.seen) != 1 || executor.seen[0].ID != "mut-1" {
		t.Fatalf("executor saw %+v", executor.seen)
	}
}

func TestABlockedAutomationGrantIsRecordedAsBlockedNotFailed(t *testing.T) {
	store := &fakeStore{claimable: []Mutation{{ID: "mut-2", Provider: "apple_notes"}}}
	executor := &fakeExecutor{results: []Result{{Status: StatusBlockedMissingCredentials, Error: "Automation permission"}}}

	summary, err := Process(context.Background(), notesOptions(store, executor))
	if err != nil {
		t.Fatal(err)
	}
	if summary.BlockedMissingCredentials != 1 || summary.Claimed != 1 {
		t.Fatalf("summary %+v", summary)
	}
	if len(store.failed) != 1 || store.failed[0].status != StatusBlockedMissingCredentials || store.failed[0].err != "Automation permission" || store.failed[0].actor != "mac:test:apple_notes_mutation_worker" {
		t.Fatalf("failed %+v", store.failed)
	}
}

func TestEveryOutcomeIsCountedAndWrittenInClaimOrder(t *testing.T) {
	// The Contacts worker test: two rows, one succeeds, one is terminal.
	store := &fakeStore{claimable: []Mutation{{ID: "m1"}, {ID: "m2"}, {ID: "m3"}}}
	executor := &fakeExecutor{results: []Result{
		{Status: StatusSucceeded, ResultJSON: map[string]any{"card_id": "x"}},
		{Status: StatusFailedTerminal, Error: "gone"},
		{Status: StatusFailedRetryable, Error: "busy", ResultJSON: map[string]any{"k": "v"}},
	}}
	opts := notesOptions(store, executor)
	opts.Provider = "apple_contacts"
	opts.Idempotent = []Operation{{"apple_contacts", "apple_contacts.update_contact"}}
	opts.ReclaimAfter = time.Minute
	opts.Limit = 7

	summary, err := Process(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if summary != (Summary{Claimed: 3, Succeeded: 1, FailedTerminal: 1, FailedRetryable: 1}) {
		t.Fatalf("summary %+v", summary)
	}
	if !reflect.DeepEqual(store.claimCalls[0].providers, []string{"apple_contacts"}) || store.claimCalls[0].limit != 7 {
		t.Fatalf("claim %+v", store.claimCalls[0])
	}
	if !reflect.DeepEqual(store.reclaimCalls, [][]Operation{{{"apple_contacts", "apple_contacts.update_contact"}}}) || store.reclaimAfter != time.Minute {
		t.Fatalf("reclaim %v after %v", store.reclaimCalls, store.reclaimAfter)
	}
	if !reflect.DeepEqual(store.completed, []completion{{"m1", map[string]any{"card_id": "x"}, opts.ClaimedBy}}) {
		t.Fatalf("completed %+v", store.completed)
	}
	if !reflect.DeepEqual(store.failed, []failure{
		{"m2", StatusFailedTerminal, "gone", nil, opts.ClaimedBy},
		{"m3", StatusFailedRetryable, "busy", map[string]any{"k": "v"}, opts.ClaimedBy},
	}) {
		t.Fatalf("failed %+v", store.failed)
	}
	if summary.Describe("Apple Contacts") != "Apple Contacts mutations: claimed=3 succeeded=1 retryable=1 terminal=1 blocked=0" {
		t.Fatalf("describe %q", summary.Describe("Apple Contacts"))
	}
}

func TestALostLockReportsASkipRatherThanAnEmptySuccess(t *testing.T) {
	store := &fakeStore{}
	opts := notesOptions(store, &fakeExecutor{})
	opts.Lock = LockFunc(func(context.Context) (func(), bool, error) { return nil, false, nil })
	opts.EnsureTables = true

	summary, err := Process(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if summary != (Summary{SkippedDueToLock: true}) {
		t.Fatalf("summary %+v", summary)
	}
	if len(store.claimCalls) != 0 || len(store.reclaimCalls) != 0 || store.ensured != 0 {
		t.Fatalf("a skipped pass must touch nothing: %+v", store)
	}
	if summary.Describe("Apple Notes") != "Apple Notes mutations skipped: another worker holds the lock" {
		t.Fatalf("describe %q", summary.Describe("Apple Notes"))
	}
}

func TestTheLockIsReleasedAndErrorsPropagate(t *testing.T) {
	released := 0
	store := &fakeStore{claimErr: errors.New("boom")}
	opts := notesOptions(store, &fakeExecutor{})
	opts.Lock = LockFunc(func(context.Context) (func(), bool, error) { return func() { released++ }, true, nil })
	if _, err := Process(context.Background(), opts); err == nil || err.Error() != "boom" {
		t.Fatalf("err %v", err)
	}
	if released != 1 {
		t.Fatalf("released %d", released)
	}
	opts.Lock = LockFunc(func(context.Context) (func(), bool, error) { return nil, false, errors.New("lock db down") })
	if _, err := Process(context.Background(), opts); err == nil || err.Error() != "lock db down" {
		t.Fatalf("err %v", err)
	}
	if _, err := Process(context.Background(), Options{}); err == nil {
		t.Fatal("missing deps must be an error")
	}
}
