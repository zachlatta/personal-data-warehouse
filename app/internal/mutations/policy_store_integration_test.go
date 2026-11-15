package mutations

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

// policyFixture is one isolated thermostat, policy and grant in the test
// database: every name carries a random suffix, so runs and parallel tests
// never see each other's rows.
type policyFixture struct {
	store    *PostgresStore
	service  *Service
	device   string
	policyID string
	tokenID  string
}

func newPolicyFixture(t *testing.T, mode string) policyFixture {
	t.Helper()
	store := testStore(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	f := policyFixture{store: store, service: NewService(store, Config{}), device: "DEV" + suffix, policyID: "heating-" + strings.ReplaceAll(suffix, "_", ""), tokenID: "tok_" + suffix}
	for _, statement := range []string{
		`CREATE SCHEMA IF NOT EXISTS base_resideo`,
		`CREATE TABLE IF NOT EXISTS base_resideo.thermostats (
			account text NOT NULL, device_id text NOT NULL, name text NOT NULL DEFAULT '', is_online bigint NOT NULL DEFAULT 1,
			indoor_temperature double precision NOT NULL DEFAULT 0, heat_setpoint double precision NOT NULL DEFAULT 0,
			cool_setpoint double precision NOT NULL DEFAULT 0, min_heat_setpoint double precision NOT NULL DEFAULT 50,
			max_heat_setpoint double precision NOT NULL DEFAULT 90, min_cool_setpoint double precision NOT NULL DEFAULT 50,
			max_cool_setpoint double precision NOT NULL DEFAULT 99, system_mode text NOT NULL DEFAULT 'Heat',
			operation_mode text NOT NULL DEFAULT '', setpoint_status text NOT NULL DEFAULT '', temperature_units text NOT NULL DEFAULT 'Fahrenheit',
			schedule_enabled bigint NOT NULL DEFAULT 0, last_polled_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (account, device_id))`,
		`CREATE TABLE IF NOT EXISTS base_resideo.thermostat_readings (
			account text NOT NULL, device_id text NOT NULL, observed_at timestamptz NOT NULL,
			heat_setpoint double precision NOT NULL DEFAULT 0, cool_setpoint double precision NOT NULL DEFAULT 0,
			PRIMARY KEY (account, device_id, observed_at))`,
	} {
		if _, err := store.db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("fixture DDL: %v", err)
		}
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO base_resideo.thermostats (account, device_id, name, indoor_temperature, heat_setpoint, cool_setpoint, last_polled_at)
		VALUES ($1, $2, 'Hall', 67, 68, 78, now() - interval '5 minutes')`, ResideoAccount, f.device); err != nil {
		t.Fatal(err)
	}
	f.insertPolicy(t, 1, mode, `{"device_id":{"in":["`+f.device+`"]},"heat_setpoint":{"min":55,"max":72},"cool_setpoint":{"absent":true},"hold":{"in":["permanent"]}}`)
	if _, err := execContext(ctx, store.db, `
		INSERT INTO @mutation_policy_grants (id, policy_id, token_id, label, expires_at, created_by)
		VALUES ($1, $2, $3, 'wf:heating-winter', now() + interval '30 days', 'test')`, "grant-"+suffix, f.policyID, f.tokenID); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f policyFixture) insertPolicy(t *testing.T, version int64, mode, match string) {
	t.Helper()
	if _, err := execContext(context.Background(), f.store.db, `
		INSERT INTO @mutation_policies (id, version, operation, account, match_json, deny_json, limits_json, mode, valid_from, valid_until, created_by, approved_by)
		VALUES ($1, $2, $3, $4, $5::jsonb, '{"heat_setpoint":{"below":50}}'::jsonb,
		        '{"max_per_day":8,"max_per_hour":1,"min_interval_minutes":30,"input_max_age_minutes":30,"pause_after_manual_change_hours":4}'::jsonb,
		        $6, now() - interval '1 day', now() + interval '30 days', 'test', 'app:test')`,
		f.policyID, version, ResideoSetThermostatOperation, ResideoAccount, match, mode); err != nil {
		t.Fatal(err)
	}
}

func (f policyFixture) propose(t *testing.T, tokenID string, heat float64) (ProposalResponse, error) {
	t.Helper()
	ctx := auth.WithPrincipal(context.Background(), auth.Principal{Scope: auth.ScopeAgent, TokenID: tokenID, ClientName: "wf:heating-winter"})
	return f.service.ProposeMutation(ctx, ProposeMutationInput{
		Title: "Heat " + randomSuffix(t), Reason: "isolated test",
		Mutations: []map[string]any{{
			"type": ResideoSetThermostatOperation, "device_id": f.device, "heat_setpoint": heat, "hold": "permanent", "reason": "away",
		}},
	})
}

func TestADryRunPolicyRecordsWouldApproveOnTheRequestAndAsAnEvent(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeDryRun)
	ctx := context.Background()
	proposal, err := f.propose(t, f.tokenID, 55)
	if err != nil {
		t.Fatal(err)
	}
	request, err := f.store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if err != nil {
		t.Fatal(err)
	}
	if request.Status != StatusPendingReview {
		t.Fatalf("a dry run must still ask Zach: status %s", request.Status)
	}
	if request.Policy["decision"] != PolicyDecisionWouldApprove || request.Policy["policy_id"] != f.policyID {
		t.Fatalf("policy = %#v", request.Policy)
	}
	var decision, eventType string
	if err := queryRowContext(ctx, f.store.db, `SELECT decision FROM @mutation_policy_evaluations WHERE request_id = $1`, request.ID).Scan(&decision); err != nil || decision != PolicyDecisionWouldApprove {
		t.Fatalf("evaluation row: %q %v", decision, err)
	}
	if err := queryRowContext(ctx, f.store.db, `SELECT event_type FROM @upstream_mutation_request_events WHERE request_id = $1 AND actor_type = 'policy'`, request.ID).Scan(&eventType); err != nil || eventType != "policy_evaluated" {
		t.Fatalf("event: %q %v", eventType, err)
	}
	// The list carries the badge without the whole context.
	listed, err := f.store.ListRequests(ctx, RequestFilter{Statuses: []string{StatusPendingReview}, Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range listed {
		if row.ID == request.ID {
			if row.Policy["decision"] != PolicyDecisionWouldApprove {
				t.Fatalf("list badge: %#v", row.Policy)
			}
			encoded, _ := json.Marshal(f.service.RequestJSON(row, false)["policy"])
			if len(encoded) < 10 {
				t.Fatalf("API row lost the policy: %s", encoded)
			}
		}
	}

	// The dry run counts toward its own limits: a second change inside the
	// hour would be refused for the rate, which is what dry run measures.
	second, err := f.propose(t, f.tokenID, 68)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := f.store.GetRequest(ctx, second.RequestID, AllMutations)
	if again.Policy["decision"] != PolicyDecisionReview {
		t.Fatalf("second change in the hour: %#v", again.Policy)
	}
}

func TestAnotherTokenAndAWideningValueAreRecordedAsReview(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeDryRun)
	ctx := context.Background()
	other, err := f.propose(t, "tok_someone_else", 55)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := f.store.GetRequest(ctx, other.RequestID, AllMutations)
	if request.Policy["decision"] != PolicyDecisionReview || request.RequestedByTokenID != "tok_someone_else" {
		t.Fatalf("policy = %#v", request.Policy)
	}
}

func TestAManualSetpointChangePausesThePolicy(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeDryRun)
	ctx := context.Background()
	if _, err := f.store.db.ExecContext(ctx, `
		INSERT INTO base_resideo.thermostat_readings (account, device_id, observed_at, heat_setpoint, cool_setpoint) VALUES
		($1, $2, now() - interval '3 hours', 68, 78),
		($1, $2, now() - interval '2 hours', 70, 78)`, ResideoAccount, f.device); err != nil {
		t.Fatal(err)
	}
	proposal, err := f.propose(t, f.tokenID, 55)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := f.store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if request.Policy["decision"] != PolicyDecisionReview {
		t.Fatalf("a hand change 2 hours ago must pause the policy: %#v", request.Policy)
	}
	if reasons, _ := request.Policy["reasons"].([]any); len(reasons) == 0 {
		t.Fatalf("no reason: %#v", request.Policy)
	}
}

func TestAnEnforcedDenyRuleRefusesTheProposal(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeEnforce)
	if _, err := f.propose(t, f.tokenID, 49); err == nil {
		t.Fatal("heat 49 must be refused by the deny rule")
	}
}

// Dry-run decisions count against the limits even after the policy is
// switched to enforce: a change that "would have" been approved a few
// minutes ago still uses the hour's one change, so turning enforcement on
// never lets more changes through than the limits allow. The safer reading.
func TestDryRunDecisionsCountAgainstLimitsAfterSwitchingToEnforce(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeDryRun)
	ctx := context.Background()
	first, err := f.propose(t, f.tokenID, 55)
	if err != nil {
		t.Fatal(err)
	}
	if request, _ := f.store.GetRequest(ctx, first.RequestID, AllMutations); request.Policy["decision"] != PolicyDecisionWouldApprove {
		t.Fatalf("dry run: %#v", request.Policy)
	}
	f.insertPolicy(t, 2, PolicyModeEnforce, `{"device_id":{"in":["`+f.device+`"]},"heat_setpoint":{"min":55,"max":72},"cool_setpoint":{"absent":true},"hold":{"in":["permanent"]}}`)
	second, err := f.propose(t, f.tokenID, 68)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := f.store.GetRequest(ctx, second.RequestID, AllMutations)
	if request.Status != StatusPendingReview || request.Policy["decision"] != PolicyDecisionReview {
		t.Fatalf("a change inside the hour after a dry-run approval was approved: %s %#v", request.Status, request.Policy)
	}
}

func TestAnEnforcedPolicyApprovesInTheProposalTransaction(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeEnforce)
	ctx := context.Background()
	proposal, err := f.propose(t, f.tokenID, 55)
	if err != nil {
		t.Fatal(err)
	}
	request, err := f.store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if err != nil {
		t.Fatal(err)
	}
	actor := PolicyActorPrefix + f.policyID + "@1"
	if request.Status != "approved" || request.ApprovedBy != actor || request.Mutations[0].ApprovedBy != actor || request.Mutations[0].Status != "approved" {
		t.Fatalf("request %s by %q; mutation %s by %q", request.Status, request.ApprovedBy, request.Mutations[0].Status, request.Mutations[0].ApprovedBy)
	}
	var actorType string
	if err := queryRowContext(ctx, f.store.db, `SELECT actor_type FROM @upstream_mutation_request_events WHERE request_id = $1 AND event_type = 'approved'`, request.ID).Scan(&actorType); err != nil || actorType != "policy" {
		t.Fatalf("approved event actor_type %q, %v", actorType, err)
	}
	// Another token's identical proposal waits for Zach.
	other, err := f.propose(t, "tok_not_granted", 68)
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := f.store.GetRequest(ctx, other.RequestID, AllMutations); again.Status != StatusPendingReview {
		t.Fatalf("an ungranted token was approved: %s", again.Status)
	}
}

func TestTwoConcurrentProposalsCannotBothPassTheHourlyLimit(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeEnforce)
	ctx := context.Background()
	results := make(chan string, 2)
	for _, heat := range []float64{55, 68} {
		go func(heat float64) {
			proposal, err := f.propose(t, f.tokenID, heat)
			if err != nil {
				results <- "error: " + err.Error()
				return
			}
			request, _ := f.store.GetRequest(ctx, proposal.RequestID, AllMutations)
			results <- request.Status
		}(heat)
	}
	statuses := map[string]int{}
	for range 2 {
		statuses[<-results]++
	}
	if statuses["approved"] != 1 || statuses[StatusPendingReview] != 1 {
		t.Fatalf("statuses = %v, want one approved and one for review", statuses)
	}
}

func TestUndoRestoresThePreviousSetpointAsAReviewerApproval(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeEnforce)
	ctx := context.Background()
	proposal, err := f.propose(t, f.tokenID, 55)
	if err != nil {
		t.Fatal(err)
	}
	// The worker ran it: succeeded, with the device state it found first.
	if _, err := execContext(ctx, f.store.db, `
		UPDATE @upstream_mutations SET status = 'succeeded', executed_at = now(),
		       result_json = '{"verified":true,"previous":{"heat_setpoint":68,"cool_setpoint":78,"setpoint_status":"PermanentHold"},"current":{"heat_setpoint":55}}'::jsonb
		 WHERE request_id = $1`, proposal.RequestID); err != nil {
		t.Fatal(err)
	}
	setRequestStatus(t, f.store, proposal.RequestID, "succeeded")
	request, _ := f.store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if !canUndo(request) {
		t.Fatal("a thermostat change that ran must offer Undo")
	}
	reviewer := auth.WithPrincipal(ctx, auth.Principal{Scope: auth.ScopeReviewer, TokenID: "phone", ClientName: "pixel"})
	undone, err := f.service.UndoRequest(reviewer, proposal.RequestID, "app:pixel")
	if err != nil {
		t.Fatal(err)
	}
	if undone.Status != "approved" || undone.ApprovedBy != "app:pixel" {
		t.Fatalf("undo request %s by %q", undone.Status, undone.ApprovedBy)
	}
	payload := undone.Mutations[0].Payload
	if payload["heat_setpoint"] != 68.0 || payload["hold"] != thermostatHoldPermanent || payload["device_id"] != f.device {
		t.Fatalf("undo payload = %#v", payload)
	}
	// Undo twice is the same request, not a second change.
	again, err := f.service.UndoRequest(reviewer, proposal.RequestID, "app:pixel")
	if err != nil || again.ID != undone.ID {
		t.Fatalf("second undo: %v %s vs %s", err, again.ID, undone.ID)
	}
}

func TestPolicyOutcomesAreReportedOnceAndThreeFailuresTurnThePolicyOff(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeEnforce)
	ctx := context.Background()
	// Each failed run is its own request, approved by the policy.
	var ids []string
	for i := range 3 {
		if i > 0 {
			// Clear the rate limits between runs; this test is about outcomes.
			if _, err := execContext(ctx, f.store.db, `DELETE FROM @mutation_policy_evaluations WHERE policy_id = $1`, f.policyID); err != nil {
				t.Fatal(err)
			}
		}
		proposal, err := f.propose(t, f.tokenID, float64(55+i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, proposal.RequestID)
		setRequestStatus(t, f.store, proposal.RequestID, "failed_terminal")
	}
	var mine []PolicyOutcome
	for range 5 {
		outcomes, err := f.store.ClaimPolicyOutcomes(ctx, 500)
		if err != nil {
			t.Fatal(err)
		}
		for _, outcome := range outcomes {
			if outcome.PolicyID == f.policyID {
				mine = append(mine, outcome)
			}
		}
	}
	kinds := map[string]int{}
	for _, outcome := range mine {
		kinds[outcome.Kind]++
	}
	if kinds[PolicyOutcomeFailed] != 3 || kinds[PolicyOutcomePolicyDisabled] != 1 {
		t.Fatalf("outcomes = %v (each reported once; the policy turned off once)", kinds)
	}
	var mode, createdBy string
	if err := queryRowContext(ctx, f.store.db, `SELECT mode, created_by FROM @mutation_policies WHERE id = $1 ORDER BY version DESC LIMIT 1`, f.policyID).Scan(&mode, &createdBy); err != nil {
		t.Fatal(err)
	}
	if mode != PolicyModeOff || createdBy != PolicyAutoOffActor {
		t.Fatalf("policy mode %s by %s", mode, createdBy)
	}
	// Now a change inside the old policy waits for Zach.
	if _, err := execContext(ctx, f.store.db, `DELETE FROM @mutation_policy_evaluations WHERE policy_id = $1`, f.policyID); err != nil {
		t.Fatal(err)
	}
	proposal, err := f.propose(t, f.tokenID, 60)
	if err != nil {
		t.Fatal(err)
	}
	if request, _ := f.store.GetRequest(ctx, proposal.RequestID, AllMutations); request.Status != StatusPendingReview {
		t.Fatalf("an off policy approved: %s", request.Status)
	}
}

func TestAKillSwitchRowSendsAProposalToReview(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeEnforce)
	ctx := context.Background()
	if err := f.store.SetAutoApprove(ctx, false, "app:test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.store.SetAutoApprove(context.Background(), true, "app:test") })
	proposal, err := f.propose(t, f.tokenID, 55)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := f.store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if request.Status != StatusPendingReview || request.Policy["decision"] != PolicyDecisionReview {
		t.Fatalf("kill switch off: %s %#v", request.Status, request.Policy)
	}
	overview, err := f.store.ListPolicies(ctx)
	if err != nil || overview.AutoApproveEnabled {
		t.Fatalf("overview %+v %v", overview.AutoApproveEnabled, err)
	}
}

// holdLock takes an advisory lock the way the worker's claim does (Python,
// upstream_mutation_policy.lock_policy_state) and returns its release.
func holdLock(t *testing.T, store *PostgresStore, key string) func() {
	t.Helper()
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		t.Fatal(err)
	}
	return func() { _ = tx.Rollback() }
}

// waitsFor runs write and reports whether it was still blocked after a pause
// while the lock was held, and that it finished once released.
func waitsFor(t *testing.T, release func(), write func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- write() }()
	select {
	case err := <-done:
		release()
		t.Fatalf("the write did not wait for the claim's lock (err %v)", err)
	case <-time.After(300 * time.Millisecond):
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the write never finished after the lock was released")
	}
}

func TestPolicyWritersWaitForTheClaimsLock(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeEnforce)
	ctx := context.Background()
	t.Cleanup(func() { _ = f.store.SetAutoApprove(context.Background(), true, "test") })
	waitsFor(t, holdLock(t, f.store, policyLockPrefix+policySettingsLockID), func() error {
		return f.store.SetAutoApprove(ctx, false, "app:test")
	})
	waitsFor(t, holdLock(t, f.store, policyLockPrefix+f.policyID), func() error {
		_, err := f.store.SetPolicyMode(ctx, f.policyID, PolicyModeOff, "app:test")
		return err
	})
	waitsFor(t, holdLock(t, f.store, policyLockPrefix+f.policyID), func() error {
		tx, err := f.store.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		for range PolicyAutoOffAfterFailures {
			if _, err := execContext(ctx, tx, `INSERT INTO @upstream_mutation_requests (id, status, approved_by, updated_at) VALUES ($1, 'failed_terminal', $2, now())`, randomSuffix(t), PolicyActorPrefix+f.policyID+"@1"); err != nil {
				return err
			}
		}
		// Back to enforce first, so the auto-off has a change to make.
		if _, err := setPolicyMode(ctx, tx, f.policyID, PolicyModeEnforce, "test", "", time.Now()); err != nil {
			return err
		}
		_, err = disablePolicyAfterFailures(ctx, tx, f.policyID)
		return err
	})
}

// The Python claim takes the same keys; a typo on either side would make the
// locks unrelated.
func TestTheClaimUsesTheSameLockKeys(t *testing.T) {
	source, err := os.ReadFile("../../../src/personal_data_warehouse/upstream_mutation_policy.py")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`POLICY_LOCK_PREFIX = "` + policyLockPrefix + `"`, `SETTINGS_LOCK_ID = "` + policySettingsLockID + `"`} {
		if !strings.Contains(string(source), want) {
			t.Fatalf("upstream_mutation_policy.py does not declare %s", want)
		}
	}
}

// ClaimPolicyOutcomes takes the locks of every policy it may turn off up
// front, in sorted order like every other writer, never one at a time in the
// order the requests happen to sort: while it waits for policy B it already
// holds A, though B's failures are the older ones.
func TestPolicyOutcomesTakePolicyLocksInSortedOrder(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeEnforce)
	ctx := context.Background()
	a, b := "aaa-"+strings.ReplaceAll(randomSuffix(t), "_", ""), "zzz-"+strings.ReplaceAll(randomSuffix(t), "_", "")
	for i, id := range []string{b, a} {
		if _, err := execContext(ctx, f.store.db, `
			INSERT INTO @mutation_policies (id, version, operation, account, mode) VALUES ($1, 1, $2, 'home', 'enforce')`, id, ResideoSetThermostatOperation); err != nil {
			t.Fatal(err)
		}
		for range PolicyAutoOffAfterFailures {
			if _, err := execContext(ctx, f.store.db, `
				INSERT INTO @upstream_mutation_requests (id, status, approved_by, updated_at)
				VALUES ($1, 'failed_terminal', $2, now() - make_interval(mins => $3))`, randomSuffix(t), PolicyActorPrefix+id+"@1", 30-i*10); err != nil {
				t.Fatal(err)
			}
		}
	}
	release := holdLock(t, f.store, policyLockPrefix+b)
	done := make(chan error, 1)
	go func() { _, err := f.store.ClaimPolicyOutcomes(ctx, 500); done <- err }()
	time.Sleep(300 * time.Millisecond)
	var free bool
	if err := f.store.db.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, policyLockPrefix+a).Scan(&free); err != nil {
		t.Fatal(err)
	}
	if free {
		_, _ = f.store.db.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, policyLockPrefix+a)
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if free {
		t.Fatal("while waiting for policy B the outcome loop did not hold policy A: locks were not taken in sorted order")
	}
}

// Reported outcomes and requests still waiting to run must not fill the page:
// a newer failure is found however many of those sort before it, so its push
// and the three-failure shutoff are never starved.
func TestPolicyOutcomesFilterBeforeTheLimit(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeEnforce)
	ctx := context.Background()
	actor := PolicyActorPrefix + f.policyID + "@1"
	for i := range 12 {
		id := "old-" + randomSuffix(t)
		status := "succeeded"
		if i%3 == 0 {
			status = "approved" // a scheduled step still waiting
		}
		if _, err := execContext(ctx, f.store.db, `
			INSERT INTO @upstream_mutation_requests (id, status, approved_by, updated_at) VALUES ($1, $2, $3, now() - interval '1 day')`, id, status, actor); err != nil {
			t.Fatal(err)
		}
		if status == "succeeded" {
			if _, err := execContext(ctx, f.store.db, `
				INSERT INTO @upstream_mutation_request_events (request_id, event_index, event_type, actor_type, actor_id)
				VALUES ($1, 0, 'policy_outcome_reported', 'policy', $2)`, id, actor); err != nil {
				t.Fatal(err)
			}
		}
	}
	newer := "new-" + randomSuffix(t)
	if _, err := execContext(ctx, f.store.db, `
		INSERT INTO @upstream_mutation_requests (id, title, status, approved_by, updated_at) VALUES ($1, 'Heat to away', 'failed_terminal', $2, now())`, newer, actor); err != nil {
		t.Fatal(err)
	}
	found := false
	for page := range 3 {
		outcomes, err := f.store.ClaimPolicyOutcomes(ctx, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range outcomes {
			if o.Request.ID == newer {
				found = true
			}
			if strings.HasPrefix(o.Request.ID, "old-") {
				t.Fatalf("page %d reported a row with nothing to report: %+v", page, o)
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Fatal("a newer failure was starved by reported and waiting rows")
	}
}

// An outcome is queued in the timeline notification outbox in the same
// transaction that marks it reported, never pushed in passing: the outbox
// worker delivers it with its own retries and leases.
func TestAPolicyOutcomeIsQueuedInTheNotificationOutbox(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeEnforce)
	ctx := context.Background()
	id := "ran-" + randomSuffix(t)
	if _, err := execContext(ctx, f.store.db, `
		INSERT INTO @upstream_mutation_requests (id, title, reason, status, approved_by, updated_at)
		VALUES ($1, 'Set the thermostat: heat to 55°', 'You left home.', 'succeeded', $2, now())`, id, PolicyActorPrefix+f.policyID+"@1"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ClaimPolicyOutcomes(ctx, 500); err != nil {
		t.Fatal(err)
	}
	var adapter, status string
	var payload []byte
	if err := queryRowContext(ctx, f.store.db, `SELECT adapter, status, payload FROM @notification_events WHERE payload->>'request_id' = $1`, id).Scan(&adapter, &status, &payload); err != nil {
		t.Fatalf("no outbox row: %v", err)
	}
	row := decodeJSONMap(payload)
	if row["actor"] != "PDW" {
		t.Fatalf("an outbox row needs an actor: %#v", row)
	}
	if adapter != PolicyOutcomeAdapter || status != "pending" || row["category"] != "mutation_applied" || row["kind"] != "mutation_applied" || row["route"] != "/mutations/"+id || row["quiet"] != true {
		t.Fatalf("outbox row %s %s %#v", adapter, status, row)
	}
	if !strings.Contains(stringFromAny(row["snippet"]), "Undo") {
		t.Fatalf("body %q", row["snippet"])
	}
	// Reported once: a second pass queues nothing more.
	if _, err := f.store.ClaimPolicyOutcomes(ctx, 500); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := queryRowContext(ctx, f.store.db, `SELECT count(*) FROM @notification_events WHERE payload->>'request_id' = $1`, id).Scan(&n); err != nil || n != 1 {
		t.Fatalf("outbox rows %d %v", n, err)
	}
}
