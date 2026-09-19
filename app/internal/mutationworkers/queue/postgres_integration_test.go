package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
)

// testStores opens the app's own mutation store (to propose and approve a
// request the way the review UI does) and the worker's store against a real
// Postgres when PDW_TEST_POSTGRES_URL points at a throwaway database, and
// skips otherwise. Every other test in this package runs against a fake, so
// without this the claim / reclaim / complete / fail SQL — the half that
// actually touches the warehouse — has no coverage at all.
func testStores(t *testing.T) (*mutations.PostgresStore, *PostgresStore) {
	t.Helper()
	url := strings.TrimSpace(os.Getenv("PDW_TEST_POSTGRES_URL"))
	if url == "" {
		t.Skip("PDW_TEST_POSTGRES_URL is not set")
	}
	ctx := context.Background()
	app, err := mutations.NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatalf("open app store: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	worker, err := OpenPostgres(ctx, url, 30*time.Second)
	if err != nil {
		t.Fatalf("open worker store: %v", err)
	}
	t.Cleanup(func() { _ = worker.Close() })
	if err := worker.EnsureTables(ctx); err != nil {
		t.Fatalf("ensure tables: %v", err)
	}
	return app, worker
}

// approvedRequest proposes one apple_notes / apple_contacts request with the
// given mutations and approves it, returning the approved mutation ids in
// request order. Titles carry the test name and a nanosecond stamp because
// CreateRequest deduplicates by content hash.
func approvedRequest(t *testing.T, app *mutations.PostgresStore, label string, inputs ...mutations.MutationInput) mutations.Request {
	t.Helper()
	ctx := context.Background()
	title := fmt.Sprintf("%s %s %d", t.Name(), label, time.Now().UnixNano())
	for i := range inputs {
		if inputs[i].Account == "" {
			inputs[i].Account = "zach@example.test"
		}
		if inputs[i].NoteID == "" && inputs[i].Type == mutations.AppleNotesUpdateNoteOperation {
			inputs[i].NoteID = "x-coredata://STORE/ICNote/p" + fmt.Sprint(time.Now().UnixNano()%100000)
		}
	}
	request, err := app.CreateRequest(ctx, mutations.CreateRequestInput{
		Title: title, Reason: "integration test", RequestedBy: "test", Mutations: inputs,
	})
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	request, err = app.ApproveRequest(ctx, request.ID, "test-reviewer")
	if err != nil {
		t.Fatalf("approve request: %v", err)
	}
	if request.Status != "approved" {
		t.Fatalf("request status after approval %q", request.Status)
	}
	return request
}

type mutationRow struct {
	Status       string
	ClaimedBy    string
	ClaimedAt    time.Time
	AttemptCount int64
	Error        string
	Result       map[string]any
	ExecutedAt   time.Time
}

func readMutation(t *testing.T, store *PostgresStore, id string) mutationRow {
	t.Helper()
	var row mutationRow
	var result string
	err := store.db.QueryRow(expand(`SELECT status, claimed_by, claimed_at, attempt_count, error, result_json::text, executed_at FROM @upstream_mutations WHERE id = $1`), id).
		Scan(&row.Status, &row.ClaimedBy, &row.ClaimedAt, &row.AttemptCount, &row.Error, &result, &row.ExecutedAt)
	if err != nil {
		t.Fatalf("read mutation %s: %v", id, err)
	}
	row.Result = decodeJSONMap(result)
	return row
}

func readEvents(t *testing.T, store *PostgresStore, id string) []map[string]any {
	t.Helper()
	rows, err := store.db.Query(expand(`SELECT event_index, event_type, actor_type, actor_id, event_json::text FROM @upstream_mutation_events WHERE mutation_id = $1 ORDER BY event_index`), id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var index int64
		var eventType, actorType, actorID, payload string
		if err := rows.Scan(&index, &eventType, &actorType, &actorID, &payload); err != nil {
			t.Fatal(err)
		}
		out = append(out, map[string]any{"index": index, "type": eventType, "actor_type": actorType, "actor_id": actorID, "json": decodeJSONMap(payload)})
	}
	return out
}

func requestStatus(t *testing.T, store *PostgresStore, id string) (string, map[string]any) {
	t.Helper()
	var status, result string
	if err := store.db.QueryRow(expand(`SELECT status, result_json::text FROM @upstream_mutation_requests WHERE id = $1`), id).Scan(&status, &result); err != nil {
		t.Fatal(err)
	}
	return status, decodeJSONMap(result)
}

func TestIntegrationClaimApprovedTakesOnlyTheProviderInApprovalOrder(t *testing.T) {
	app, worker := testStores(t)
	ctx := context.Background()
	notes := approvedRequest(t, app, "notes", mutations.MutationInput{Type: mutations.AppleNotesCreateNoteOperation, Body: "hello", Folder: "PDW Agent"})
	contacts := approvedRequest(t, app, "contacts", mutations.MutationInput{Type: mutations.AppleContactsCreateContactOperation, Contact: map[string]any{"given_name": "Ann"}})
	actor := "mac:test-host:apple_notes_mutation_worker"

	claimed, err := worker.ClaimApproved(ctx, 50, actor, []string{"apple_notes"})
	if err != nil {
		t.Fatal(err)
	}
	var mine *Mutation
	for i := range claimed {
		if claimed[i].Provider != "apple_notes" {
			t.Fatalf("claimed a foreign provider row: %+v", claimed[i])
		}
		if claimed[i].RequestID == contacts.ID {
			t.Fatal("claimed the apple_contacts request")
		}
		if claimed[i].RequestID == notes.ID {
			mine = &claimed[i]
		}
	}
	if mine == nil {
		t.Fatalf("our approved note was not claimed; claimed %d rows", len(claimed))
	}
	if mine.Operation != mutations.AppleNotesCreateNoteOperation || mine.Status != "executing" || mine.AttemptCount != 1 || mine.Account != "zach@example.test" {
		t.Fatalf("claimed row %+v", *mine)
	}
	if mine.Payload["body"] != "hello" || mine.Payload["folder"] != "PDW Agent" {
		t.Fatalf("payload %v", mine.Payload)
	}
	row := readMutation(t, worker, mine.ID)
	if row.Status != "executing" || row.ClaimedBy != actor || row.AttemptCount != 1 || time.Since(row.ClaimedAt) > time.Minute {
		t.Fatalf("row %+v", row)
	}
	events := readEvents(t, worker, mine.ID)
	last := events[len(events)-1]
	if last["type"] != "claimed" || last["actor_type"] != "dagster" || last["actor_id"] != actor || last["json"].(map[string]any)["attempt_count"] != float64(1) {
		t.Fatalf("last event %v", last)
	}
	if status, _ := requestStatus(t, worker, notes.ID); status != "executing" {
		t.Fatalf("request status %q", status)
	}
	// A second claim finds nothing left for the provider on this request.
	again, err := worker.ClaimApproved(ctx, 50, actor, []string{"apple_notes"})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range again {
		if m.ID == mine.ID {
			t.Fatal("an executing row was claimed twice")
		}
	}
	// limit <= 0 claims nothing at all.
	if none, err := worker.ClaimApproved(ctx, 0, actor, []string{"apple_contacts"}); err != nil || len(none) != 0 {
		t.Fatalf("limit 0: %v %v", none, err)
	}
	// Clean up the contacts row so a later test is not confused by it.
	if _, err := worker.ClaimApproved(ctx, 50, "mac:test-host:apple_contacts_mutation_worker", []string{"apple_contacts"}); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationCompleteAndFailWriteTheResultAndRollUpTheRequest(t *testing.T) {
	app, worker := testStores(t)
	ctx := context.Background()
	request := approvedRequest(t, app, "pair",
		mutations.MutationInput{Type: mutations.AppleNotesUpdateNoteOperation, AppendBody: "one"},
		mutations.MutationInput{Type: mutations.AppleNotesUpdateNoteOperation, AppendBody: "two"},
	)
	actor := "mac:test-host:apple_notes_mutation_worker"
	claimed, err := worker.ClaimApproved(ctx, 50, actor, []string{"apple_notes"})
	if err != nil {
		t.Fatal(err)
	}
	var ours []Mutation
	for _, m := range claimed {
		if m.RequestID == request.ID {
			ours = append(ours, m)
		}
	}
	if len(ours) != 2 {
		t.Fatalf("claimed %d of our rows", len(ours))
	}

	result := map[string]any{"note_id": "x-coredata://A/ICNote/p1", "action": "update", "previous_body": "<div>old</div>"}
	if err := worker.Complete(ctx, ours[0].ID, result, actor); err != nil {
		t.Fatal(err)
	}
	row := readMutation(t, worker, ours[0].ID)
	if row.Status != "succeeded" || row.Error != "" || row.Result["previous_body"] != "<div>old</div>" || time.Since(row.ExecutedAt) > time.Minute {
		t.Fatalf("completed row %+v", row)
	}
	events := readEvents(t, worker, ours[0].ID)
	last := events[len(events)-1]
	if last["type"] != "executed" || last["actor_id"] != actor || last["json"].(map[string]any)["note_id"] != "x-coredata://A/ICNote/p1" {
		t.Fatalf("executed event %v", last)
	}
	// One succeeded, one still executing: the request reads executing.
	if status, _ := requestStatus(t, worker, request.ID); status != "executing" {
		t.Fatalf("request status %q", status)
	}

	if err := worker.Fail(ctx, ours[1].ID, "bogus", "x", nil, actor); err == nil || !strings.Contains(err.Error(), "unsupported failure status") {
		t.Fatalf("bogus status err %v", err)
	}
	if err := worker.Fail(ctx, ours[1].ID, StatusBlockedMissingCredentials, "Automation permission for Notes.app is not granted", map[string]any{"k": "v"}, actor); err != nil {
		t.Fatal(err)
	}
	row = readMutation(t, worker, ours[1].ID)
	if row.Status != StatusBlockedMissingCredentials || !strings.Contains(row.Error, "Automation") || row.Result["k"] != "v" {
		t.Fatalf("failed row %+v", row)
	}
	events = readEvents(t, worker, ours[1].ID)
	last = events[len(events)-1]
	eventJSON := last["json"].(map[string]any)
	if last["type"] != "failed" || eventJSON["status"] != StatusBlockedMissingCredentials || eventJSON["result"].(map[string]any)["k"] != "v" || !strings.Contains(eventJSON["error"].(string), "Automation") {
		t.Fatalf("failed event %v", last)
	}
	for i := 1; i < len(events); i++ {
		if events[i]["index"].(int64) != events[i-1]["index"].(int64)+1 {
			t.Fatalf("event indexes are not contiguous: %v", events)
		}
	}
	status, requestResult := requestStatus(t, worker, request.ID)
	if status != StatusBlockedMissingCredentials {
		t.Fatalf("request status %q", status)
	}
	statuses := requestResult["mutation_statuses"].(map[string]any)
	if statuses[ours[0].ID] != "succeeded" || statuses[ours[1].ID] != StatusBlockedMissingCredentials {
		t.Fatalf("mutation_statuses %v", statuses)
	}

	// A retryable failure goes back to the claimable pool and, once it
	// succeeds too, the request reads succeeded.
	if err := worker.Fail(ctx, ours[1].ID, StatusFailedRetryable, "busy", nil, actor); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := worker.ClaimApproved(ctx, 50, actor, []string{"apple_notes"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range reclaimed {
		if m.ID == ours[1].ID {
			found = true
			if m.AttemptCount != 2 {
				t.Fatalf("attempt_count %d", m.AttemptCount)
			}
		}
	}
	if !found {
		t.Fatal("failed_retryable row was not claimable again")
	}
	if err := worker.Complete(ctx, ours[1].ID, map[string]any{}, actor); err != nil {
		t.Fatal(err)
	}
	if status, _ := requestStatus(t, worker, request.ID); status != "succeeded" {
		t.Fatalf("request status %q", status)
	}
	got, err := app.GetRequest(ctx, request.ID)
	if err != nil || got.Status != "succeeded" {
		t.Fatalf("app sees %q err %v", got.Status, err)
	}
}

func TestIntegrationReclaimResetsOnlyStaleIdempotentClaims(t *testing.T) {
	app, worker := testStores(t)
	ctx := context.Background()
	request := approvedRequest(t, app, "reclaim",
		mutations.MutationInput{Type: mutations.AppleNotesUpdateNoteOperation, AppendBody: "stale update"},
		mutations.MutationInput{Type: mutations.AppleNotesCreateNoteOperation, Body: "stale create"},
		mutations.MutationInput{Type: mutations.AppleNotesUpdateNoteOperation, AppendBody: "fresh update"},
	)
	actor := "mac:old-host:apple_notes_mutation_worker"
	claimed, err := worker.ClaimApproved(ctx, 50, actor, []string{"apple_notes"})
	if err != nil {
		t.Fatal(err)
	}
	byOp := map[string][]Mutation{}
	for _, m := range claimed {
		if m.RequestID == request.ID {
			byOp[m.Operation] = append(byOp[m.Operation], m)
		}
	}
	if len(byOp[mutations.AppleNotesUpdateNoteOperation]) != 2 || len(byOp[mutations.AppleNotesCreateNoteOperation]) != 1 {
		t.Fatalf("claimed %v", byOp)
	}
	var staleUpdate, freshUpdate Mutation
	for _, m := range byOp[mutations.AppleNotesUpdateNoteOperation] {
		if m.Payload["append_body"] == "stale update" {
			staleUpdate = m
		} else {
			freshUpdate = m
		}
	}
	staleCreate := byOp[mutations.AppleNotesCreateNoteOperation][0]
	// Age two of the claims past the reclaim window.
	old := time.Now().UTC().Add(-2 * time.Hour)
	for _, id := range []string{staleUpdate.ID, staleCreate.ID} {
		if _, err := worker.db.Exec(expand(`UPDATE @upstream_mutations SET claimed_at = $1 WHERE id = $2`), old, id); err != nil {
			t.Fatal(err)
		}
	}

	idempotent := []Operation{{Provider: "apple_notes", Operation: mutations.AppleNotesUpdateNoteOperation}}
	if n, err := worker.ReclaimStaleExecuting(ctx, time.Hour, nil, "x"); err != nil || n != 0 {
		t.Fatalf("no idempotent operations: %d %v", n, err)
	}
	n, err := worker.ReclaimStaleExecuting(ctx, time.Hour, idempotent, "mac:new-host:apple_notes_mutation_worker")
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("reclaimed %d", n)
	}
	row := readMutation(t, worker, staleUpdate.ID)
	if row.Status != "approved" || row.ClaimedBy != "" || !row.ClaimedAt.Equal(Epoch) || row.AttemptCount != 1 {
		t.Fatalf("stale update after reclaim %+v", row)
	}
	events := readEvents(t, worker, staleUpdate.ID)
	last := events[len(events)-1]
	eventJSON := last["json"].(map[string]any)
	if last["type"] != "reclaimed" || last["actor_id"] != "mac:new-host:apple_notes_mutation_worker" ||
		eventJSON["previous_claimed_by"] != actor || eventJSON["attempt_count"] != float64(1) || eventJSON["stale_after_seconds"] != float64(3600) {
		t.Fatalf("reclaimed event %v", last)
	}
	// A stale create is NOT reclaimed: replaying it makes a second note.
	if row := readMutation(t, worker, staleCreate.ID); row.Status != "executing" || row.ClaimedBy != actor {
		t.Fatalf("stale create must stay executing: %+v", row)
	}
	// A fresh update is not stale yet.
	if row := readMutation(t, worker, freshUpdate.ID); row.Status != "executing" || row.ClaimedBy != actor {
		t.Fatalf("fresh update must stay executing: %+v", row)
	}
	// The reclaimed row is claimable again, at the head of the queue by its
	// original approved_at, and its attempt count keeps growing.
	again, err := worker.ClaimApproved(ctx, 50, "mac:new-host:apple_notes_mutation_worker", []string{"apple_notes"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range again {
		if m.ID == staleUpdate.ID {
			found = true
			if m.AttemptCount != 2 {
				t.Fatalf("attempt_count %d", m.AttemptCount)
			}
		}
	}
	if !found {
		t.Fatal("reclaimed row was not claimable")
	}
}

func TestIntegrationProcessEndToEndThroughTheRealStore(t *testing.T) {
	app, worker := testStores(t)
	ctx := context.Background()
	request := approvedRequest(t, app, "process",
		mutations.MutationInput{Type: mutations.AppleContactsUpdateContactOperation, CardID: "8537DF38-BF0D-4468-9061-D2D41468E05A:ABPerson", Contact: map[string]any{"organization": "Acme"}},
	)
	var seen []Mutation
	executor := ExecutorFunc(func(m Mutation) Result {
		seen = append(seen, m)
		if m.RequestID != request.ID {
			return Result{Status: StatusFailedRetryable, Error: "not mine"}
		}
		return Result{Status: StatusSucceeded, ResultJSON: map[string]any{"card_id": "8537DF38-BF0D-4468-9061-D2D41468E05A:ABPerson", "action": "update", "changed": true}}
	})
	summary, err := Process(ctx, Options{
		Store:      worker,
		Executor:   executor,
		Lock:       alwaysAcquire,
		Provider:   "apple_contacts",
		Idempotent: []Operation{{Provider: "apple_contacts", Operation: mutations.AppleContactsUpdateContactOperation}},
		ClaimedBy:  "mac:test-host:apple_contacts_mutation_worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Claimed < 1 || summary.Succeeded < 1 {
		t.Fatalf("summary %+v", summary)
	}
	var mine *Mutation
	for i := range seen {
		if seen[i].RequestID == request.ID {
			mine = &seen[i]
		}
		if seen[i].Provider != "apple_contacts" {
			t.Fatalf("executor saw a foreign provider %+v", seen[i])
		}
	}
	if mine == nil || mine.Payload["card_id"] != "8537DF38-BF0D-4468-9061-D2D41468E05A:ABPerson" {
		t.Fatalf("executor did not see our row: %+v", seen)
	}
	got, err := app.GetRequest(ctx, request.ID)
	if err != nil || got.Status != "succeeded" || len(got.Mutations) != 1 || got.Mutations[0].Status != "succeeded" {
		t.Fatalf("request %+v err %v", got, err)
	}
	payload, _ := json.Marshal(got.Mutations[0].Result)
	if !strings.Contains(string(payload), `"changed":true`) {
		t.Fatalf("result %s", payload)
	}
}

func TestIntegrationAppleContactsMergedCardTargetReadsTheNewestSucceededMerge(t *testing.T) {
	app, worker := testStores(t)
	ctx := context.Background()
	stamp := fmt.Sprintf("%012X", time.Now().UnixNano()%0xFFFFFFFFFFFF)
	gone := "AF0D7001-E8F8-4772-AE89-" + stamp + ":ABPerson"
	kept := "AACADB21-739D-4B5F-A14E-" + stamp + ":ABPerson"
	request := approvedRequest(t, app, "merge",
		mutations.MutationInput{Type: mutations.AppleContactsMergeContactsOperation, KeepCardID: kept, MergeCardIDs: []string{gone}},
		// A merge that never ran says nothing about where the card went.
		mutations.MutationInput{Type: mutations.AppleContactsMergeContactsOperation, KeepCardID: "0EBE0000-739D-4B5F-A14E-" + stamp + ":ABPerson", MergeCardIDs: []string{gone}},
	)
	actor := "mac:test-host:apple_contacts_mutation_worker"
	claimed, err := worker.ClaimApproved(ctx, 500, actor, []string{"apple_contacts"})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range claimed {
		if m.RequestID != request.ID {
			continue
		}
		if m.Payload["keep_card_id"] == kept {
			err = worker.Complete(ctx, m.ID, map[string]any{"action": "merge"}, actor)
		} else {
			err = worker.Fail(ctx, m.ID, StatusFailedTerminal, "nope", nil, actor)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	for cardID, want := range map[string]string{gone: kept, kept: "", "": "", "  ": ""} {
		got, err := worker.AppleContactsMergedCardTarget(ctx, cardID)
		if err != nil || got != want {
			t.Fatalf("target(%q) = %q, %v; want %q", cardID, got, err, want)
		}
	}
}
