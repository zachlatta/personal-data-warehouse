package mutations

import (
	"context"
	"encoding/json"
	"testing"

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
	f := policyFixture{store: store, service: NewService(store, Config{}), device: "DEV" + suffix, policyID: "heating-" + suffix, tokenID: "tok_" + suffix}
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
