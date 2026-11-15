package mutations

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

func widensOn(t *testing.T, old, next Policy) []string {
	t.Helper()
	_, widens := policyNarrowing(old, next)
	return widens
}

func TestLoweringTheMinimumFrom55To50IsNotANarrowing(t *testing.T) {
	old := heatingPolicy(PolicyModeEnforce)
	next := heatingPolicy(PolicyModeEnforce)
	next.Match["heat_setpoint"] = FieldRule{Min: floatPtr(50), Max: floatPtr(72)}
	if widens := widensOn(t, old, next); len(widens) == 0 || !strings.Contains(widens[0], "lower or removed minimum") {
		t.Fatalf("a lower minimum must widen: %v", widens)
	}
}

func TestOnlyProvableNarrowingsNarrow(t *testing.T) {
	base := func() Policy { return heatingPolicy(PolicyModeEnforce) }
	narrow := map[string]func(*Policy){
		"raise the minimum":  func(p *Policy) { p.Match["heat_setpoint"] = FieldRule{Min: floatPtr(58), Max: floatPtr(72)} },
		"lower the maximum":  func(p *Policy) { p.Match["heat_setpoint"] = FieldRule{Min: floatPtr(55), Max: floatPtr(70)} },
		"fewer values":       func(p *Policy) { p.Match["hold"] = FieldRule{In: []any{"permanent"}} },
		"a stricter deny":    func(p *Policy) { p.Deny["heat_setpoint"] = FieldRule{Below: floatPtr(52)} },
		"a new match rule":   func(p *Policy) { delete(p.Match, "device_id"); *p = heatingPolicy(PolicyModeEnforce) },
		"fewer per day":      func(p *Policy) { p.Limits.MaxPerDay = 4 },
		"a longer interval":  func(p *Policy) { p.Limits.MinIntervalMinutes = 60 },
		"fresher input":      func(p *Policy) { p.Limits.InputMaxAgeMinutes = 15 },
		"a longer pause":     func(p *Policy) { p.Limits.PauseAfterManualChangeHours = 6 },
		"a later start":      func(p *Policy) { p.ValidFrom = p.ValidFrom.Add(24 * time.Hour) },
		"an earlier end":     func(p *Policy) { p.ValidUntil = p.ValidUntil.Add(-24 * time.Hour) },
		"enforce to dry run": func(p *Policy) { p.Mode = PolicyModeDryRun },
		"enforce to off":     func(p *Policy) { p.Mode = PolicyModeOff },
		"unchanged":          func(p *Policy) {},
	}
	for name, change := range narrow {
		next := base()
		change(&next)
		if widens := widensOn(t, base(), next); len(widens) != 0 {
			t.Fatalf("%s should narrow: %v", name, widens)
		}
	}
	widen := map[string]func(*Policy){
		"raise the maximum":  func(p *Policy) { p.Match["heat_setpoint"] = FieldRule{Min: floatPtr(55), Max: floatPtr(74)} },
		"drop a match rule":  func(p *Policy) { delete(p.Match, "hold") },
		"a new value":        func(p *Policy) { p.Match["hold"] = FieldRule{In: []any{"permanent", "temporary"}} },
		"absent to set":      func(p *Policy) { p.Match["cool_setpoint"] = FieldRule{Min: floatPtr(70)} },
		"drop a deny rule":   func(p *Policy) { delete(p.Deny, "heat_setpoint") },
		"a looser deny":      func(p *Policy) { p.Deny["heat_setpoint"] = FieldRule{Below: floatPtr(45)} },
		"more per hour":      func(p *Policy) { p.Limits.MaxPerHour = 2 },
		"no daily limit":     func(p *Policy) { p.Limits.MaxPerDay = 0 },
		"a shorter interval": func(p *Policy) { p.Limits.MinIntervalMinutes = 10 },
		"staler input":       func(p *Policy) { p.Limits.InputMaxAgeMinutes = 60 },
		"a shorter pause":    func(p *Policy) { p.Limits.PauseAfterManualChangeHours = 1 },
		"an earlier start":   func(p *Policy) { p.ValidFrom = p.ValidFrom.Add(-24 * time.Hour) },
		"a later end":        func(p *Policy) { p.ValidUntil = p.ValidUntil.Add(24 * time.Hour) },
		"no end":             func(p *Policy) { p.ValidUntil = time.Time{} },
		"dry run to enforce": func(p *Policy) {},
		"another account":    func(p *Policy) { p.Account = "cabin" },
	}
	for name, change := range widen {
		old := base()
		next := base()
		change(&next)
		if name == "dry run to enforce" {
			old.Mode = PolicyModeDryRun
		}
		if widens := widensOn(t, old, next); len(widens) == 0 {
			t.Fatalf("%s must not count as a narrowing", name)
		}
	}
}

func TestAForbiddenTypeIsRefusedAtProposeTimeWithAClearError(t *testing.T) {
	store := &recordingStore{}
	service := NewService(store, Config{})
	for _, operation := range []string{"garage_door.open", "door_lock.unlock", "lock.set_state", "payment.send", "payments.transfer"} {
		_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
			Title: "x", Reason: "y", Mutations: []map[string]any{{"type": operation}},
		})
		if err == nil || !strings.Contains(err.Error(), "forbidden") || !strings.Contains(err.Error(), "agents may never propose") {
			t.Fatalf("%s: err = %v", operation, err)
		}
		if TypeRuleFor(operation).AgentProposable || TypeRuleFor(operation).AutoApprovable {
			t.Fatalf("%s must be neither proposable nor approvable", operation)
		}
	}
	if len(store.createCalls) != 0 {
		t.Fatal("a forbidden proposal reached the store")
	}
	if TypeRuleFor(PolicyChangeOperation).AutoApprovable || !TypeRuleFor(PolicyChangeOperation).AgentProposable {
		t.Fatal("a policy change is proposable and never auto-approvable")
	}
}

func TestAPolicySpecIsValidatedStrictly(t *testing.T) {
	good := map[string]any{
		"id": "heating-winter", "operation": ResideoSetThermostatOperation, "account": "home",
		"match":  map[string]any{"heat_setpoint": map[string]any{"min": 55, "max": 72}, "hold": map[string]any{"in": []any{"permanent"}}},
		"deny":   map[string]any{"heat_setpoint": map[string]any{"below": 50}},
		"limits": map[string]any{"max_per_day": 8, "max_per_hour": 1},
		"mode":   "dry_run", "valid_from": "2026-11-15T00:00:00-05:00", "valid_until": "2027-04-15T00:00:00-04:00",
	}
	if _, err := policyFromSpec(good); err != nil {
		t.Fatal(err)
	}
	mutate := func(key string, value any) map[string]any {
		out := map[string]any{}
		for k, v := range good {
			out[k] = v
		}
		if value == nil {
			delete(out, key)
		} else {
			out[key] = value
		}
		return out
	}
	for name, spec := range map[string]map[string]any{
		"never auto-approvable type": mutate("operation", GmailSendEmailOperation),
		"unknown field":              mutate("note", "x"),
		"unknown fact":               mutate("match", map[string]any{"fan_mode": map[string]any{"in": []any{"on"}}}),
		"no rate limits":             mutate("limits", map[string]any{"max_per_day": 8}),
		"no end":                     mutate("valid_until", nil),
		"bad id":                     mutate("id", "Heating Winter"),
	} {
		if _, err := policyFromSpec(spec); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
}

func TestPolicyChangesEndToEnd(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeEnforce)
	ctx := context.Background()
	agent := auth.WithPrincipal(ctx, auth.Principal{Scope: auth.ScopeAgent, TokenID: f.tokenID, ClientName: "wf:heating-winter"})
	propose := func(entry map[string]any) (Request, error) {
		entry["type"] = PolicyChangeOperation
		entry["reason"] = "test " + randomSuffix(t)
		proposal, err := f.service.ProposeMutation(agent, ProposeMutationInput{Title: "Policy " + randomSuffix(t), Reason: "test", Mutations: []map[string]any{entry}})
		if err != nil {
			return Request{}, err
		}
		return f.store.GetRequest(ctx, proposal.RequestID, AllMutations)
	}
	spec := func(min float64) map[string]any {
		return map[string]any{
			"id": f.policyID, "operation": ResideoSetThermostatOperation, "account": "home",
			"match":  map[string]any{"device_id": map[string]any{"in": []any{f.device}}, "heat_setpoint": map[string]any{"min": min, "max": 72}, "cool_setpoint": map[string]any{"absent": true}, "hold": map[string]any{"in": []any{"permanent"}}},
			"deny":   map[string]any{"heat_setpoint": map[string]any{"below": 50}},
			"limits": map[string]any{"max_per_day": 8, "max_per_hour": 1, "min_interval_minutes": 30, "input_max_age_minutes": 30, "pause_after_manual_change_hours": 4},
			"mode":   "enforce",
			// The fixture's window, narrowed by a minute on each side.
			"valid_from":  time.Now().Add(-24*time.Hour + time.Hour).UTC().Format(time.RFC3339),
			"valid_until": time.Now().Add(29 * 24 * time.Hour).UTC().Format(time.RFC3339),
		}
	}
	// Lowering the minimum widens: it waits for Zach, with the diff.
	lower, err := propose(map[string]any{"action": "put_policy", "policy": spec(50)})
	if err != nil {
		t.Fatal(err)
	}
	if lower.Status != StatusPendingReview {
		t.Fatalf("a widening applied itself: %s", lower.Status)
	}
	change := mapFromAny(lower.Mutations[0].Preview["policy_change"])
	if change["narrowing"] != false || !strings.Contains(strings.Join(stringSliceFromAny(change["widens"]), ";"), "minimum") {
		t.Fatalf("diff = %#v", change)
	}
	// Raising it narrows: applied at once as pdw:narrowing, a new version.
	raise, err := propose(map[string]any{"action": "put_policy", "policy": spec(58)})
	if err != nil {
		t.Fatal(err)
	}
	if raise.Status != "succeeded" || raise.ApprovedBy != NarrowingActor {
		t.Fatalf("a narrowing: %s by %q (%s)", raise.Status, raise.ApprovedBy, raise.Error)
	}
	current, _, err := currentPolicy(ctx, f.store.db, f.policyID)
	if err != nil || current.Version != 2 || *current.Match["heat_setpoint"].Min != 58 {
		t.Fatalf("current policy %+v %v", current, err)
	}
	// The pending widening was judged against v1; approving it now is refused.
	if _, err := f.store.ApproveRequest(ctx, lower.ID, "app:test"); err == nil || !strings.Contains(err.Error(), "changed since") {
		t.Fatalf("a stale change applied: %v", err)
	}
	// A new grant waits for Zach; Zach's approval applies it.
	grant, err := propose(map[string]any{"action": "grant", "grant": map[string]any{"id": "g-" + strings.ToLower(randomSuffix(t)[2:]), "policy_id": f.policyID, "token_id": "tok_new", "label": "wf:other", "expires_at": time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)}})
	if err != nil {
		t.Fatal(err)
	}
	if grant.Status != StatusPendingReview {
		t.Fatalf("a new grant applied itself: %s", grant.Status)
	}
	if _, err := f.store.ApproveRequest(ctx, grant.ID, "app:test"); err != nil {
		t.Fatal(err)
	}
	if approved, _ := f.store.GetRequest(ctx, grant.ID, AllMutations); approved.Status != "succeeded" {
		t.Fatalf("approved grant: %s", approved.Status)
	}
	// Turning things off narrows; turning auto-approval on does not.
	off, err := propose(map[string]any{"action": "set_mode", "policy_id": f.policyID, "mode": "off"})
	if err != nil || off.Status != "succeeded" {
		t.Fatalf("set_mode off: %v %s %s", err, off.Status, off.Error)
	}
	on, err := propose(map[string]any{"action": "set_mode", "policy_id": f.policyID, "mode": "enforce"})
	if err != nil || on.Status != StatusPendingReview {
		t.Fatalf("set_mode enforce: %v %s", err, on.Status)
	}
	switchOff, err := propose(map[string]any{"action": "set_auto_approve", "enabled": false})
	if err != nil || switchOff.Status != "succeeded" {
		t.Fatalf("kill switch: %v %s", err, switchOff.Status)
	}
	t.Cleanup(func() { _ = f.store.SetAutoApprove(context.Background(), true, "test") })
	switchOn, err := propose(map[string]any{"action": "set_auto_approve", "enabled": true})
	if err != nil || switchOn.Status != StatusPendingReview {
		t.Fatalf("switch on: %v %s", err, switchOn.Status)
	}
	// A mixed request is refused: policy changes travel alone.
	_, err = f.service.ProposeMutation(agent, ProposeMutationInput{Title: "mixed " + randomSuffix(t), Reason: "x", Mutations: []map[string]any{
		{"type": PolicyChangeOperation, "action": "set_auto_approve", "enabled": false, "reason": "x"},
		{"type": ResideoSetThermostatOperation, "device_id": f.device, "heat_setpoint": 60, "hold": "permanent", "reason": "x"},
	}})
	if err == nil || !strings.Contains(err.Error(), "policies only") {
		t.Fatalf("mixed: %v", err)
	}
}

// One pdw.mutation_policy proposal that creates a dry-run
// policy and grants it to a workflow token. It waits for Zach whole; his
// approval applies both, in order.
func TestANewPolicyAndItsGrantTravelInOneRequest(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeDryRun)
	ctx := context.Background()
	agent := auth.WithPrincipal(ctx, auth.Principal{Scope: auth.ScopeFull, TokenID: "tok_host", ClientName: "test-host"})
	id := "winter-" + strings.ToLower(strings.ReplaceAll(randomSuffix(t), "_", ""))
	proposal, err := f.service.ProposeMutation(agent, ProposeMutationInput{Title: "Heating policy " + id, Reason: "dry run first", Mutations: []map[string]any{
		{"type": PolicyChangeOperation, "action": "put_policy", "reason": "synthetic policy fixture", "policy": map[string]any{
			"id": id, "operation": ResideoSetThermostatOperation, "account": "home",
			"match":  map[string]any{"device_id": map[string]any{"in": []any{f.device}}, "heat_setpoint": map[string]any{"min": 55, "max": 72}, "cool_setpoint": map[string]any{"absent": true}, "hold": map[string]any{"in": []any{"permanent"}}},
			"deny":   map[string]any{"heat_setpoint": map[string]any{"below": 50}},
			"limits": map[string]any{"max_per_day": 8, "max_per_hour": 1, "min_interval_minutes": 30, "input_max_age_minutes": 30, "pause_after_manual_change_hours": 4},
			"mode":   "dry_run", "valid_from": "2026-11-15T00:00:00-05:00", "valid_until": "2027-04-15T00:00:00-04:00",
		}},
		{"type": PolicyChangeOperation, "action": "grant", "reason": "the heating workflow's own token", "grant": map[string]any{
			"id": id + "-wf", "policy_id": id, "token_id": "tok_heating_workflow", "label": "wf:heating-winter", "expires_at": "2027-04-15T00:00:00-04:00",
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := f.store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if request.Status != StatusPendingReview || len(request.Mutations) != 2 {
		t.Fatalf("status %s, %d mutations", request.Status, len(request.Mutations))
	}
	if _, err := f.store.ApproveRequest(ctx, proposal.RequestID, "app:test-phone"); err != nil {
		t.Fatal(err)
	}
	policy, found, err := currentPolicy(ctx, f.store.db, id)
	if err != nil || !found || policy.Version != 1 || policy.Mode != PolicyModeDryRun || policy.ApprovedBy != "app:test-phone" || policy.CreatedBy != "test-host" {
		t.Fatalf("policy %+v found=%v err=%v", policy, found, err)
	}
	grant, found, err := grantByID(ctx, f.store.db, id+"-wf")
	if err != nil || !found || grant.TokenID != "tok_heating_workflow" || !grant.Active(time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("grant %+v found=%v err=%v", grant, found, err)
	}
}

// Two agents shorten one grant at once (to 1 and to 2 days). Each is a
// narrowing of the 30-day grant it read, but applied in the wrong order the
// second would lengthen the first's. Judged again under the policy's lock,
// whichever runs second sees the other's result: the grant ends at the
// earlier expiry, and a change that no longer narrows waits for Zach.
func TestConcurrentGrantNarrowingsNeverLengthenTheGrant(t *testing.T) {
	f := newPolicyFixture(t, PolicyModeEnforce)
	ctx := context.Background()
	agent := auth.WithPrincipal(ctx, auth.Principal{Scope: auth.ScopeAgent, TokenID: f.tokenID, ClientName: "wf:heating-winter"})
	for round := range 8 {
		grantID := "race-" + strings.ToLower(strings.ReplaceAll(randomSuffix(t), "_", ""))
		if _, err := execContext(ctx, f.store.db, `
			INSERT INTO @mutation_policy_grants (id, policy_id, token_id, label, expires_at, created_by)
			VALUES ($1, $2, 'tok_race', 'race', now() + interval '30 days', 'test')`, grantID, f.policyID); err != nil {
			t.Fatal(err)
		}
		oneDay := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
		twoDays := oneDay.Add(24 * time.Hour)
		results := make(chan error, 2)
		start := make(chan struct{})
		for _, until := range []time.Time{oneDay, twoDays} {
			go func(until time.Time) {
				<-start
				_, err := f.service.ProposeMutation(agent, ProposeMutationInput{Title: "Shorten " + randomSuffix(t), Reason: "race", Mutations: []map[string]any{{
					"type": PolicyChangeOperation, "action": "grant", "reason": "shorten",
					"grant": map[string]any{"id": grantID, "policy_id": f.policyID, "token_id": "tok_race", "label": "race", "expires_at": until.Format(time.RFC3339)},
				}}})
				results <- err
			}(until)
		}
		close(start)
		for range 2 {
			if err := <-results; err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
		grant, _, err := grantByID(ctx, f.store.db, grantID)
		if err != nil {
			t.Fatal(err)
		}
		if !grant.ExpiresAt.Equal(oneDay) {
			t.Fatalf("round %d: the grant ends %s, want the earlier %s (a narrowing lengthened it)", round, grant.ExpiresAt, oneDay)
		}
	}
}

func TestANarrowingIsReportedQuietlyThroughTheOutbox(t *testing.T) {
	alert := PolicyOutcomeAlert(PolicyOutcome{Kind: PolicyOutcomeNarrowed, Request: Request{ID: "req-n", Title: "Turn heating-winter off", Reason: "Guests are staying."}})
	if !strings.HasPrefix(stringFromAny(alert["title"]), "Policy narrowed: ") || alert["quiet"] != true || alert["route"] != "/mutations/req-n" || alert["adapter"] != PolicyOutcomeAdapter {
		t.Fatalf("alert %#v", alert)
	}
}

// A widening whose old and new rules print alike (one value "a b" against
// two values "a" and "b") is still a widening: the verdict is never decided
// by how the diff is displayed.
func TestAWideningThatPrintsLikeTheOldRuleIsStillAWidening(t *testing.T) {
	old := heatingPolicy(PolicyModeEnforce)
	old.Match["hold"] = FieldRule{In: []any{"a b"}}
	next := heatingPolicy(PolicyModeEnforce)
	next.Match["hold"] = FieldRule{In: []any{"a", "b"}}
	if widens := widensOn(t, old, next); len(widens) == 0 {
		t.Fatal("new allowed values were taken as no change because they print alike")
	}
}

func TestAllowedValuesPrintUnambiguously(t *testing.T) {
	if got := describeRule(FieldRule{In: []any{"a b"}}); got != `one of ["a b"]` {
		t.Fatalf("%s", got)
	}
	if got := describeRule(FieldRule{In: []any{55.0, 68.0}}); got != `one of [55,68]` {
		t.Fatalf("%s", got)
	}
}
