package mutations

import (
	"strings"
	"testing"
	"time"
)

var policyNow = time.Date(2026, 12, 1, 15, 0, 0, 0, time.UTC)

type fakeLedger struct {
	approvals    []time.Time
	manualChange time.Time
	manualErr    error
}

func (l *fakeLedger) ApprovalsSince(_ string, since time.Time) (int, error) {
	count := 0
	for _, at := range l.approvals {
		if !at.Before(since) {
			count++
		}
	}
	return count, nil
}

func (l *fakeLedger) LastApproval(string) (time.Time, bool, error) {
	var last time.Time
	for _, at := range l.approvals {
		if at.After(last) {
			last = at
		}
	}
	return last, !last.IsZero(), nil
}

func (l *fakeLedger) ManualChangeSince(_ Policy, _ string, since time.Time) (time.Time, bool, error) {
	if l.manualErr != nil {
		return time.Time{}, false, l.manualErr
	}
	if !l.manualChange.IsZero() && !l.manualChange.Before(since) {
		return l.manualChange, true, nil
	}
	return time.Time{}, false, nil
}

func floatPtr(v float64) *float64 { return &v }

// heatingPolicy is a synthetic thermostat policy for boundary tests.
func heatingPolicy(mode string) Policy {
	return Policy{
		ID: "heating-winter", Version: 1, Operation: ResideoSetThermostatOperation, Account: ResideoAccount,
		Match: map[string]FieldRule{
			"device_id":     {In: []any{"DEV1"}},
			"heat_setpoint": {Min: floatPtr(55), Max: floatPtr(72)},
			"cool_setpoint": {Absent: true},
			"hold":          {In: []any{"permanent"}},
		},
		Deny:       map[string]FieldRule{"heat_setpoint": {Below: floatPtr(50)}},
		Limits:     PolicyLimits{MaxPerDay: 8, MaxPerHour: 1, MinIntervalMinutes: 30, InputMaxAgeMinutes: 30, PauseAfterManualChangeHours: 4},
		Mode:       mode,
		ValidFrom:  time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC),
		ValidUntil: time.Date(2027, 4, 15, 0, 0, 0, 0, time.UTC),
	}
}

func heatingGrant() PolicyGrant {
	return PolicyGrant{ID: "grant-1", PolicyID: "heating-winter", TokenID: "tok_heat", ExpiresAt: time.Date(2027, 4, 15, 0, 0, 0, 0, time.UTC)}
}

func thermostatMutation(heat float64, hold string) storedMutation {
	return storedMutation{
		Provider: ResideoProvider, Operation: ResideoSetThermostatOperation, Account: ResideoAccount,
		Payload: map[string]any{"device_id": "DEV1", "heat_setpoint": heat, "hold": hold},
		Preview: map[string]any{"thermostat": map[string]any{"current": map[string]any{
			"as_of": policyNow.Add(-10 * time.Minute).Format(time.RFC3339), "heat_setpoint": 68.0,
		}}},
	}
}

func evaluate(t *testing.T, in PolicyInput) PolicyEvaluation {
	t.Helper()
	if in.Now.IsZero() {
		in.Now = policyNow
	}
	if in.Ledger == nil {
		in.Ledger = &fakeLedger{}
	}
	eval, err := EvaluatePolicy(in)
	if err != nil {
		t.Fatal(err)
	}
	return eval
}

func heatingInput(mode string, mutations ...storedMutation) PolicyInput {
	return PolicyInput{
		TokenID: "tok_heat", Mutations: mutations, AutoApproveEnabled: true,
		Policies: []Policy{heatingPolicy(mode)}, Grants: []PolicyGrant{heatingGrant()},
	}
}

func TestAChangeInsideTheHeatingPolicyIsApprovedOrWouldBe(t *testing.T) {
	if eval := evaluate(t, heatingInput(PolicyModeEnforce, thermostatMutation(55, "permanent"))); eval.Decision != PolicyDecisionApproved || eval.PolicyID != "heating-winter" {
		t.Fatalf("enforce: %+v", eval)
	}
	eval := evaluate(t, heatingInput(PolicyModeDryRun, thermostatMutation(68, "permanent")))
	if eval.Decision != PolicyDecisionWouldApprove || !strings.Contains(eval.Label(), "Would auto-approve") {
		t.Fatalf("dry run: %+v %q", eval, eval.Label())
	}
	if eval.Inputs["input_observed_at"] == nil || eval.Inputs["current_heat_setpoint"] != 68.0 {
		t.Fatalf("the audit must record what the decision read: %+v", eval.Inputs)
	}
}

func TestAChangeOutsideThePolicyGoesToReviewSayingWhy(t *testing.T) {
	for heat, want := range map[float64]string{74: "74 is above this policy's 72", 52: "52 is below this policy's 55"} {
		eval := evaluate(t, heatingInput(PolicyModeEnforce, thermostatMutation(heat, "permanent")))
		if eval.Decision != PolicyDecisionReview || !strings.Contains(strings.Join(eval.Reasons, "; "), want) {
			t.Fatalf("heat %g: %+v", heat, eval)
		}
	}
	eval := evaluate(t, heatingInput(PolicyModeEnforce, thermostatMutation(68, "temporary")))
	if eval.Decision != PolicyDecisionReview || !strings.Contains(strings.Join(eval.Reasons, "; "), "hold") {
		t.Fatalf("temporary hold: %+v", eval)
	}
}

func TestADenyRuleRefusesAnEnforcedProposalAndADryRunRecordsIt(t *testing.T) {
	if eval := evaluate(t, heatingInput(PolicyModeEnforce, thermostatMutation(49, "permanent"))); eval.Decision != PolicyDecisionRefused || !strings.Contains(eval.Reasons[0], "below 50") {
		t.Fatalf("enforce: %+v", eval)
	}
	if eval := evaluate(t, heatingInput(PolicyModeDryRun, thermostatMutation(49, "permanent"))); eval.Decision != PolicyDecisionWouldRefuse {
		t.Fatalf("dry run: %+v", eval)
	}
}

func TestATypeThatIsNotAutoApprovableIsNeverAutoApproved(t *testing.T) {
	// Even a policy that matches everything, granted to the proposer, cannot
	// approve a type the type rules mark not auto-approvable.
	if TypeRuleFor(GmailSendEmailOperation).AutoApprovable || TypeRuleFor("some.future_type").AutoApprovable {
		t.Fatal("only listed types may be auto-approvable")
	}
	policy := Policy{ID: "everything", Version: 1, Operation: GmailSendEmailOperation, Mode: PolicyModeEnforce}
	eval := evaluate(t, PolicyInput{
		TokenID: "tok_heat", AutoApproveEnabled: true,
		Mutations: []storedMutation{{Provider: "gmail", Operation: GmailSendEmailOperation, Account: "zach@example.test"}},
		Policies:  []Policy{policy},
		Grants:    []PolicyGrant{{ID: "g", PolicyID: "everything", TokenID: "tok_heat", ExpiresAt: policyNow.Add(time.Hour)}},
	})
	if eval.Decision != PolicyDecisionReview || !strings.Contains(eval.Reasons[0], "never auto-approved") {
		t.Fatalf("%+v", eval)
	}
	for operation, rule := range typeRules {
		if operation != ResideoSetThermostatOperation && rule.AutoApprovable {
			t.Fatalf("%s became auto-approvable; only the thermostat may be (protected path)", operation)
		}
	}
}

func TestAMixedBatchGoesToReviewWhole(t *testing.T) {
	inside := thermostatMutation(68, "permanent")
	outside := thermostatMutation(75, "permanent")
	if eval := evaluate(t, heatingInput(PolicyModeEnforce, inside, outside)); eval.Decision != PolicyDecisionReview {
		t.Fatalf("one mutation outside the policy approved the batch: %+v", eval)
	}
	archive := storedMutation{Provider: "gmail", Operation: GmailArchiveOperation, Account: "zach@example.test"}
	if eval := evaluate(t, heatingInput(PolicyModeEnforce, inside, archive)); eval.Decision != PolicyDecisionReview {
		t.Fatalf("a thermostat change carried an archive through: %+v", eval)
	}
}

func TestOnlyTheGrantedTokenIsApprovedNeverAChosenName(t *testing.T) {
	in := heatingInput(PolicyModeEnforce, thermostatMutation(68, "permanent"))
	in.TokenID = "tok_other_agent"
	if eval := evaluate(t, in); eval.Decision != PolicyDecisionReview || !strings.Contains(eval.Reasons[0], "no active grant") {
		t.Fatalf("%+v", eval)
	}
	in.TokenID = ""
	if eval := evaluate(t, in); eval.Decision != PolicyDecisionReview {
		t.Fatalf("the master secret has no grant: %+v", eval)
	}
	for _, grant := range []PolicyGrant{
		{ID: "g", PolicyID: "heating-winter", TokenID: "tok_heat", ExpiresAt: policyNow.Add(-time.Minute)},
		{ID: "g", PolicyID: "heating-winter", TokenID: "tok_heat", ExpiresAt: policyNow.Add(time.Hour), RevokedAt: policyNow.Add(-time.Hour)},
		{ID: "g", PolicyID: "heating-winter", TokenID: "tok_heat"},
	} {
		in := heatingInput(PolicyModeEnforce, thermostatMutation(68, "permanent"))
		in.Grants = []PolicyGrant{grant}
		if eval := evaluate(t, in); eval.Decision != PolicyDecisionReview {
			t.Fatalf("grant %+v approved: %+v", grant, eval)
		}
	}
}

func TestTheKillSwitchAndTheValidDatesSendEverythingToReview(t *testing.T) {
	in := heatingInput(PolicyModeEnforce, thermostatMutation(68, "permanent"))
	in.AutoApproveEnabled = false
	if eval := evaluate(t, in); eval.Decision != PolicyDecisionReview || !strings.Contains(eval.Reasons[0], "switched off") {
		t.Fatalf("%+v", eval)
	}
	in = heatingInput(PolicyModeEnforce, thermostatMutation(68, "permanent"))
	in.Now = time.Date(2027, 5, 1, 0, 0, 0, 0, time.UTC)
	if eval := evaluate(t, in); eval.Decision != PolicyDecisionReview {
		t.Fatalf("out of season: %+v", eval)
	}
	in = heatingInput(PolicyModeOff, thermostatMutation(68, "permanent"))
	if eval := evaluate(t, in); eval.Decision == PolicyDecisionApproved || eval.Decision == PolicyDecisionWouldApprove {
		t.Fatalf("an off policy decided: %+v", eval)
	}
}

func TestLimitsSendAChangeToReview(t *testing.T) {
	cases := map[string]*fakeLedger{
		"per hour":       {approvals: []time.Time{policyNow.Add(-40 * time.Minute)}},
		"min interval":   {approvals: []time.Time{policyNow.Add(-20 * time.Minute)}},
		"per day":        {approvals: []time.Time{policyNow.Add(-23 * time.Hour), policyNow.Add(-22 * time.Hour), policyNow.Add(-21 * time.Hour), policyNow.Add(-20 * time.Hour), policyNow.Add(-19 * time.Hour), policyNow.Add(-18 * time.Hour), policyNow.Add(-17 * time.Hour), policyNow.Add(-16 * time.Hour)}},
		"manual change":  {manualChange: policyNow.Add(-3 * time.Hour)},
		"history absent": {manualErr: ErrPolicyInputUnavailable},
	}
	for name, ledger := range cases {
		in := heatingInput(PolicyModeEnforce, thermostatMutation(68, "permanent"))
		in.Ledger = ledger
		if eval := evaluate(t, in); eval.Decision != PolicyDecisionReview {
			t.Fatalf("%s: %+v", name, eval)
		}
	}
	in := heatingInput(PolicyModeEnforce, thermostatMutation(68, "permanent"))
	in.Ledger = &fakeLedger{manualChange: policyNow.Add(-5 * time.Hour), approvals: []time.Time{policyNow.Add(-2 * time.Hour)}}
	if eval := evaluate(t, in); eval.Decision != PolicyDecisionApproved {
		t.Fatalf("a pause over 4 hours old and an approval 2 hours ago must not block: %+v", eval)
	}
	stale := thermostatMutation(68, "permanent")
	stale.Preview = map[string]any{"thermostat": map[string]any{"current": map[string]any{"as_of": policyNow.Add(-45 * time.Minute).Format(time.RFC3339)}}}
	if eval := evaluate(t, heatingInput(PolicyModeEnforce, stale)); eval.Decision != PolicyDecisionReview || !strings.Contains(strings.Join(eval.Reasons, ";"), "45 minutes old") {
		t.Fatalf("stale thermostat data: %+v", eval)
	}
}

func TestAPolicyRuleOnAnUnknownFieldNeverMatches(t *testing.T) {
	policy := heatingPolicy(PolicyModeEnforce)
	policy.Match["fan_mode"] = FieldRule{In: []any{"auto"}}
	in := heatingInput(PolicyModeEnforce, thermostatMutation(68, "permanent"))
	in.Policies = []Policy{policy}
	if eval := evaluate(t, in); eval.Decision != PolicyDecisionReview {
		t.Fatalf("%+v", eval)
	}
	policy = heatingPolicy(PolicyModeEnforce)
	policy.Unreadable = "match: unknown rule"
	in.Policies = []Policy{policy}
	if eval := evaluate(t, in); eval.Decision != PolicyDecisionReview {
		t.Fatalf("unreadable: %+v", eval)
	}
}

func TestPolicyJSONIsParsedStrictly(t *testing.T) {
	if _, err := parseFieldRules([]byte(`{"heat_setpoint":{"min":55,"max":72}}`), matchRuleKeys); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`{"heat_setpoint":{"minimum":55}}`,
		`{"heat_setpoint":{"min":72,"max":55}}`,
		`{"heat_setpoint":{}}`,
		`{"heat_setpoint":{"absent":false}}`,
		`{"hold":{"in":[]}}`,
		`[]`,
	} {
		if _, err := parseFieldRules([]byte(raw), matchRuleKeys); err == nil {
			t.Fatalf("%s parsed", raw)
		}
	}
	if _, err := parseFieldRules([]byte(`{"heat_setpoint":{"min":50}}`), denyRuleKeys); err == nil {
		t.Fatal("a deny rule takes below/above/in, not min")
	}
	if _, err := parsePolicyLimits([]byte(`{"max_per_day":8,"max_per_week":2}`)); err == nil {
		t.Fatal("an unknown limit must not be ignored")
	}
}

// Limits count changes. A request of four thermostat changes, each inside the
// policy, would otherwise pass a one-per-hour limit as one approval; a policy
// approves one change per request and a batch goes to Zach.
func TestABatchOfChangesInsideThePolicyGoesToReview(t *testing.T) {
	batch := []storedMutation{thermostatMutation(55, "permanent"), thermostatMutation(68, "permanent"), thermostatMutation(56, "permanent"), thermostatMutation(69, "permanent")}
	eval := evaluate(t, heatingInput(PolicyModeEnforce, batch...))
	if eval.Decision != PolicyDecisionReview || !strings.Contains(eval.Reasons[0], "one change per request") {
		t.Fatalf("a batch was approved: %+v", eval)
	}
	if eval := evaluate(t, heatingInput(PolicyModeDryRun, batch[:2]...)); eval.Decision != PolicyDecisionReview {
		t.Fatalf("a dry run would have approved a batch: %+v", eval)
	}
}
