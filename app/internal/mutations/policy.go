package mutations

// Policy-based auto-approval. A policy is a named, versioned rule for one
// mutation type that only Zach can create, widen or extend; a grant gives one
// authenticated token (a workflow's identity) the right to use it. PDW, not
// the agent, evaluates every proposal against the policies:
//
//  1. type rules (typeRules, in code): may an agent propose this type at
//     all, and may any policy ever approve it;
//  2. deny rules: parameter limits that refuse a proposal outright;
//  3. grant match: the proposer's token holds an active grant on a policy
//     whose match rules and limits all pass;
//  4. default: anything else goes to Zach's review, with the reason the
//     closest policy did not match.
//
// The decision is stored on the request (context.policy), as a request event
// and as a row of mutation_policy_evaluations. A policy in dry_run mode only
// records what it would have done.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	PolicyModeOff     = "off"
	PolicyModeDryRun  = "dry_run"
	PolicyModeEnforce = "enforce"

	// PolicyDecisionRefused: a deny rule of an enforced policy refused the
	// proposal; nothing was queued.
	PolicyDecisionRefused = "refused"
	// PolicyDecisionWouldRefuse: a dry-run policy's deny rule would have.
	PolicyDecisionWouldRefuse = "would_refuse"
	// PolicyDecisionReview: Zach decides, as without any policy.
	PolicyDecisionReview = "review"
	// PolicyDecisionWouldApprove: a dry-run policy would have approved it.
	PolicyDecisionWouldApprove = "would_approve"
	// PolicyDecisionApproved: an enforced policy approved it.
	PolicyDecisionApproved = "approved"

	// PolicyActorPrefix starts approved_by and the event actor of a policy
	// approval: policy:<id>@<version>.
	PolicyActorPrefix = "policy:"

	// AutoApproveSettingKey is the global kill switch's row in
	// mutation_policy_settings: {"enabled": false} sends everything to review.
	AutoApproveSettingKey = "auto_approve"
	// AutoApproveEnv overrides the settings row: PDW_MUTATION_AUTO_APPROVE=0
	// switches auto-approval off whatever the database says.
	AutoApproveEnv = "PDW_MUTATION_AUTO_APPROVE"
)

// TypeRule is what a mutation type allows whatever any policy says. It lives
// in code on purpose: a policy, which agents may propose changes to, cannot
// make a type approvable that is not.
type TypeRule struct {
	// AgentProposable false refuses the type at propose time.
	AgentProposable bool
	// AutoApprovable false means no policy can ever approve the type.
	AutoApprovable bool
}

// typeRules lists the types that differ from the default (proposable, never
// auto-approved). Every type not named here, including any type added later,
// needs Zach's approval until it is listed here as auto-approvable.
var typeRules = map[string]TypeRule{
	ResideoSetThermostatOperation: {AgentProposable: true, AutoApprovable: true},
	// A policy change: agents may propose one, no policy may approve one.
	// Only a change PDW proves a narrowing applies without Zach.
	PolicyChangeOperation: {AgentProposable: true, AutoApprovable: false},
}

// forbiddenCategories are kinds of change agents may never propose, whatever
// type a later commit names them with: unlocking a door, opening a garage,
// moving money. The key is the type's provider (the part before the dot).
var forbiddenCategories = map[string]string{
	"lock":        "door locks",
	"door_lock":   "door locks",
	"doorlock":    "door locks",
	"garage":      "garage doors",
	"garage_door": "garage doors",
	"payment":     "payments",
	"payments":    "payments",
	"bank":        "payments",
	"money":       "payments",
}

// TypeRuleFor returns the rule for an operation.
func TypeRuleFor(operation string) TypeRule {
	if rule, ok := typeRules[operation]; ok {
		return rule
	}
	if _, forbidden := ForbiddenCategory(operation); forbidden {
		return TypeRule{AgentProposable: false, AutoApprovable: false}
	}
	return TypeRule{AgentProposable: true, AutoApprovable: false}
}

// ForbiddenCategory names what a forbidden operation is (door locks, garage
// doors, payments), and false for any other operation.
func ForbiddenCategory(operation string) (string, bool) {
	provider, _, _ := strings.Cut(strings.ToLower(strings.TrimSpace(operation)), ".")
	category, ok := forbiddenCategories[provider]
	return category, ok
}

// FieldRule is one field's limit. In a policy's match rules it says what the
// field may be (min/max inclusive, a set of values, or absent); in its deny
// rules it says what refuses a proposal (below, above, or a set of values).
type FieldRule struct {
	Min    *float64 `json:"min,omitempty"`
	Max    *float64 `json:"max,omitempty"`
	In     []any    `json:"in,omitempty"`
	Absent bool     `json:"absent,omitempty"`
	Below  *float64 `json:"below,omitempty"`
	Above  *float64 `json:"above,omitempty"`
}

// PolicyLimits are the rate, freshness and pause limits. Zero means unset,
// except that an enforced policy without max_per_day and max_per_hour is
// refused when it is created (see validatePolicy).
type PolicyLimits struct {
	MaxPerDay                   int     `json:"max_per_day,omitempty"`
	MaxPerHour                  int     `json:"max_per_hour,omitempty"`
	MinIntervalMinutes          int     `json:"min_interval_minutes,omitempty"`
	InputMaxAgeMinutes          int     `json:"input_max_age_minutes,omitempty"`
	PauseAfterManualChangeHours float64 `json:"pause_after_manual_change_hours,omitempty"`
}

// Policy is one version of a policy. Rows are never updated: a change is a
// new version, so an old approval can always be explained.
type Policy struct {
	ID         string
	Version    int64
	Operation  string
	Account    string
	Match      map[string]FieldRule
	Deny       map[string]FieldRule
	Limits     PolicyLimits
	Mode       string
	ValidFrom  time.Time
	ValidUntil time.Time
	CreatedBy  string
	ApprovedBy string
	CreatedAt  time.Time
	// Unreadable is set when the stored JSON could not be parsed strictly;
	// such a policy approves nothing.
	Unreadable string
}

// Actor is approved_by for a mutation this version approved.
func (p Policy) Actor() string { return fmt.Sprintf("%s%s@%d", PolicyActorPrefix, p.ID, p.Version) }

// PolicyGrant gives one token the right to use one policy until it expires.
type PolicyGrant struct {
	ID        string
	PolicyID  string
	TokenID   string
	Label     string
	ExpiresAt time.Time
	RevokedAt time.Time
	RevokedBy string
	CreatedBy string
	CreatedAt time.Time
}

// Active reports whether the grant may be used at now. A grant with no
// expiry is never active: a forgotten grant must not live forever.
func (g PolicyGrant) Active(now time.Time) bool {
	if g.RevokedAt.Unix() > 0 {
		return false
	}
	return g.ExpiresAt.Unix() > 0 && now.Before(g.ExpiresAt)
}

var (
	matchRuleKeys = map[string]bool{"min": true, "max": true, "in": true, "absent": true}
	denyRuleKeys  = map[string]bool{"below": true, "above": true, "in": true}
	limitKeys     = map[string]bool{
		"max_per_day": true, "max_per_hour": true, "min_interval_minutes": true,
		"input_max_age_minutes": true, "pause_after_manual_change_hours": true,
	}
	policyModes = map[string]bool{PolicyModeOff: true, PolicyModeDryRun: true, PolicyModeEnforce: true}
)

// parseFieldRules reads match_json or deny_json strictly: an unknown rule key
// is an error, never ignored, because ignoring it would widen the policy.
func parseFieldRules(raw []byte, allowed map[string]bool) (map[string]FieldRule, error) {
	rules := map[string]FieldRule{}
	if len(bytes.TrimSpace(raw)) == 0 {
		return rules, nil
	}
	var fields map[string]map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("rules must be an object of field -> rule: %w", err)
	}
	for field, rule := range fields {
		for key := range rule {
			if !allowed[key] {
				return nil, fmt.Errorf("field %s has unknown rule %q", field, key)
			}
		}
		encoded, _ := json.Marshal(rule)
		var parsed FieldRule
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&parsed); err != nil {
			return nil, fmt.Errorf("field %s: %w", field, err)
		}
		if rule["absent"] != nil && !parsed.Absent {
			return nil, fmt.Errorf("field %s: absent may only be true", field)
		}
		if parsed.Min != nil && parsed.Max != nil && *parsed.Min > *parsed.Max {
			return nil, fmt.Errorf("field %s: min is above max", field)
		}
		if rule["in"] != nil && len(parsed.In) == 0 {
			return nil, fmt.Errorf("field %s: in must list at least one value", field)
		}
		if len(rule) == 0 {
			return nil, fmt.Errorf("field %s has an empty rule", field)
		}
		rules[field] = parsed
	}
	return rules, nil
}

func parsePolicyLimits(raw []byte) (PolicyLimits, error) {
	var limits PolicyLimits
	if len(bytes.TrimSpace(raw)) == 0 {
		return limits, nil
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(raw, &keys); err != nil {
		return limits, fmt.Errorf("limits must be an object: %w", err)
	}
	for key := range keys {
		if !limitKeys[key] {
			return limits, fmt.Errorf("unknown limit %q", key)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&limits); err != nil {
		return limits, err
	}
	if limits.MaxPerDay < 0 || limits.MaxPerHour < 0 || limits.MinIntervalMinutes < 0 || limits.InputMaxAgeMinutes < 0 || limits.PauseAfterManualChangeHours < 0 {
		return limits, errors.New("limits must not be negative")
	}
	return limits, nil
}

// PolicyFacts is what one mutation exposes to policies: the checkable fields
// (Facts) and the inputs a limit reads (thermostat poll time). A type that
// does not implement policyFactsFor exposes nothing, so no policy matches it.
type PolicyFacts struct {
	Facts map[string]any
	// Fields is every field this type can expose; a policy rule on any
	// other field cannot be checked and never matches.
	Fields map[string]bool
	// InputObservedAt is when the warehouse last read the state the
	// proposal was judged against (zero when unknown).
	InputObservedAt time.Time
	// Target identifies the one device or object a manual-change pause is
	// about; empty when the type has none.
	Target string
	Inputs map[string]any
}

// policyFactsFor returns a stored mutation's facts, and false for a type that
// exposes none.
func policyFactsFor(mutation storedMutation) (PolicyFacts, bool) {
	switch mutation.Operation {
	case ResideoSetThermostatOperation:
		return thermostatPolicyFacts(mutation)
	}
	return PolicyFacts{}, false
}

// matchesRule reports whether value satisfies a match rule; present says
// whether the field was set at all.
func matchesRule(rule FieldRule, value any, present bool) (bool, string) {
	if rule.Absent {
		if present {
			return false, "must not be set"
		}
		return true, ""
	}
	if !present {
		return false, "must be set"
	}
	if rule.Min != nil || rule.Max != nil {
		number, ok := numberValue(value)
		if !ok {
			return false, "is not a number"
		}
		if rule.Min != nil && number < *rule.Min {
			return false, fmt.Sprintf("%g is below this policy's %g", number, *rule.Min)
		}
		if rule.Max != nil && number > *rule.Max {
			return false, fmt.Sprintf("%g is above this policy's %g", number, *rule.Max)
		}
	}
	if len(rule.In) > 0 && !valueIn(value, rule.In) {
		return false, fmt.Sprintf("%v is not one of %v", value, rule.In)
	}
	return true, ""
}

// deniedBy reports whether a deny rule refuses value.
func deniedBy(rule FieldRule, value any, present bool) (bool, string) {
	if !present {
		return false, ""
	}
	if rule.Below != nil || rule.Above != nil {
		if number, ok := numberValue(value); ok {
			if rule.Below != nil && number < *rule.Below {
				return true, fmt.Sprintf("%g is below %g", number, *rule.Below)
			}
			if rule.Above != nil && number > *rule.Above {
				return true, fmt.Sprintf("%g is above %g", number, *rule.Above)
			}
		}
	}
	if len(rule.In) > 0 && valueIn(value, rule.In) {
		return true, fmt.Sprintf("%v is a denied value", value)
	}
	return false, ""
}

func numberValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, !math.IsNaN(typed)
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	}
	return 0, false
}

func valueIn(value any, set []any) bool {
	for _, candidate := range set {
		if a, ok := numberValue(value); ok {
			if b, ok := numberValue(candidate); ok && a == b {
				return true
			}
			continue
		}
		if fmt.Sprint(value) == fmt.Sprint(candidate) {
			return true
		}
	}
	return false
}

// policyMatch checks a policy's match rules against one mutation's facts.
func policyMatch(policy Policy, facts PolicyFacts) []string {
	var reasons []string
	fields := make([]string, 0, len(policy.Match))
	for field := range policy.Match {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	for _, field := range fields {
		if !facts.Fields[field] {
			reasons = append(reasons, fmt.Sprintf("this policy names %s, which %s does not expose", field, policy.Operation))
			continue
		}
		value, present := facts.Facts[field]
		if ok, why := matchesRule(policy.Match[field], value, present); !ok {
			reasons = append(reasons, field+" "+why)
		}
	}
	return reasons
}

// policyDenial returns the first deny rule that refuses the facts.
func policyDenial(policy Policy, facts PolicyFacts) string {
	fields := make([]string, 0, len(policy.Deny))
	for field := range policy.Deny {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	for _, field := range fields {
		value, present := facts.Facts[field]
		if denied, why := deniedBy(policy.Deny[field], value, present); denied {
			return fmt.Sprintf("policy %s refuses %s: %s", policy.ID, field, why)
		}
	}
	return ""
}

// PolicyLedger answers the questions a limit asks of the warehouse. The
// Postgres store answers them inside the proposal's transaction, under the
// policy's advisory lock, so two proposals cannot both pass one limit.
type PolicyLedger interface {
	// ApprovalsSince counts requests this policy approved (or, in dry run,
	// would have approved) at or after since.
	ApprovalsSince(policyID string, since time.Time) (int, error)
	// LastApproval is the newest such request's evaluation time.
	LastApproval(policyID string) (time.Time, bool, error)
	// ManualChangeSince reports a change to target at or after since that
	// this policy did not make (a hand change, or a change approved by hand).
	ManualChangeSince(policy Policy, target string, since time.Time) (time.Time, bool, error)
}

// PolicyEvaluation is the decision on one request.
type PolicyEvaluation struct {
	Decision      string
	PolicyID      string
	PolicyVersion int64
	Mode          string
	TokenID       string
	Reasons       []string
	Inputs        map[string]any
	EvaluatedAt   time.Time
}

// Relevant reports whether the decision is worth recording: a request no
// policy could ever touch carries no policy note.
func (e PolicyEvaluation) Relevant() bool { return e.Decision != "" }

// Label is the review's one-line badge.
func (e PolicyEvaluation) Label() string {
	switch e.Decision {
	case PolicyDecisionApproved:
		return fmt.Sprintf("Auto-approved by policy %s v%d", e.PolicyID, e.PolicyVersion)
	case PolicyDecisionWouldApprove:
		return fmt.Sprintf("Would auto-approve (policy %s v%d, dry run)", e.PolicyID, e.PolicyVersion)
	case PolicyDecisionWouldRefuse:
		return fmt.Sprintf("Would refuse (policy %s v%d, dry run)", e.PolicyID, e.PolicyVersion)
	case PolicyDecisionRefused:
		return "Refused by policy " + e.PolicyID
	case PolicyDecisionReview:
		if len(e.Reasons) > 0 {
			return "Needs your approval: " + e.Reasons[0]
		}
		return "Needs your approval"
	}
	return ""
}

// JSON is the stored and served shape (context.policy).
func (e PolicyEvaluation) JSON() map[string]any {
	reasons := e.Reasons
	if reasons == nil {
		reasons = []string{}
	}
	return map[string]any{
		"decision":       e.Decision,
		"label":          e.Label(),
		"policy_id":      e.PolicyID,
		"policy_version": e.PolicyVersion,
		"mode":           e.Mode,
		"token_id":       e.TokenID,
		"reasons":        reasons,
		"inputs":         e.Inputs,
		"evaluated_at":   e.EvaluatedAt.UTC().Format(time.RFC3339),
	}
}

// PolicyInput is everything EvaluatePolicy reads.
type PolicyInput struct {
	TokenID   string
	Mutations []storedMutation
	// Policies are the current version of every policy for the request's
	// operations; Grants every grant on them.
	Policies []Policy
	Grants   []PolicyGrant
	// AutoApproveEnabled is the global kill switch (settings row and env).
	AutoApproveEnabled bool
	Ledger             PolicyLedger
	Now                time.Time
}

// EvaluatePolicy decides a request. It only ever adds an approval a policy
// describes: an unreadable payload, an unknown field, a type no policy may
// approve or any failed limit sends the request to review.
func EvaluatePolicy(in PolicyInput) (PolicyEvaluation, error) {
	now := in.Now.UTC()
	eval := PolicyEvaluation{TokenID: in.TokenID, EvaluatedAt: now, Inputs: map[string]any{}}
	if len(in.Mutations) == 0 {
		return eval, nil
	}
	candidates := map[int][]Policy{}
	anyAutoType := false
	for index, mutation := range in.Mutations {
		if TypeRuleFor(mutation.Operation).AutoApprovable {
			anyAutoType = true
		}
		for _, policy := range in.Policies {
			if policy.Mode == PolicyModeOff || policy.Operation != mutation.Operation {
				continue
			}
			if policy.Account != "" && policy.Account != mutation.Account {
				continue
			}
			candidates[index] = append(candidates[index], policy)
		}
	}
	if !anyAutoType && len(candidates) == 0 {
		// Nothing here any policy could touch: no note on the request.
		return eval, nil
	}
	review := func(reason string, policy *Policy) (PolicyEvaluation, error) {
		eval.Decision = PolicyDecisionReview
		eval.Reasons = append(eval.Reasons, reason)
		if policy != nil {
			eval.PolicyID, eval.PolicyVersion, eval.Mode = policy.ID, policy.Version, policy.Mode
		}
		return eval, nil
	}

	facts := make([]PolicyFacts, len(in.Mutations))
	for index, mutation := range in.Mutations {
		f, ok := policyFactsFor(mutation)
		if !ok {
			f = PolicyFacts{Facts: map[string]any{}, Fields: map[string]bool{}}
		}
		facts[index] = f
	}

	// Deny rules first: an enforced deny refuses the whole proposal; a dry
	// run records that it would have.
	for index := range in.Mutations {
		for _, policy := range candidates[index] {
			if policy.Unreadable != "" {
				continue
			}
			if why := policyDenial(policy, facts[index]); why != "" {
				eval.PolicyID, eval.PolicyVersion, eval.Mode = policy.ID, policy.Version, policy.Mode
				eval.Reasons = []string{why}
				if policy.Mode == PolicyModeEnforce {
					eval.Decision = PolicyDecisionRefused
				} else {
					eval.Decision = PolicyDecisionWouldRefuse
				}
				return eval, nil
			}
		}
	}

	if !in.AutoApproveEnabled {
		return review("auto-approval is switched off", nil)
	}
	if strings.TrimSpace(in.TokenID) == "" {
		return review("the proposer did not authenticate with an issued token, so no policy grant can apply", nil)
	}

	// A policy approves one change per request. Its limits count changes,
	// so a batch (even one wholly inside the policy) goes to review rather
	// than slipping several changes past a one-per-hour limit as one.
	if len(in.Mutations) > 1 {
		return review(fmt.Sprintf("a request with %d changes needs your approval: a policy approves one change per request", len(in.Mutations)), nil)
	}

	// Every mutation must match one and the same policy: a mixed request goes
	// to review whole.
	var chosen *Policy
	var closest *Policy
	var closestReasons []string
	for index, mutation := range in.Mutations {
		if !TypeRuleFor(mutation.Operation).AutoApprovable {
			return review(fmt.Sprintf("%s is never auto-approved", mutation.Operation), nil)
		}
		var matched *Policy
		for i := range candidates[index] {
			policy := candidates[index][i]
			reasons := policyEligibility(policy, in.Grants, in.TokenID, facts[index], now)
			if len(reasons) == 0 {
				matched = &policy
				break
			}
			if closest == nil || len(reasons) < len(closestReasons) {
				closest, closestReasons = &policy, reasons
			}
		}
		if matched == nil {
			if closest != nil {
				eval.Reasons = closestReasons
				eval.Decision = PolicyDecisionReview
				eval.PolicyID, eval.PolicyVersion, eval.Mode = closest.ID, closest.Version, closest.Mode
				return eval, nil
			}
			return review(fmt.Sprintf("no policy covers %s on %s", mutation.Operation, mutation.Account), nil)
		}
		if chosen != nil && (chosen.ID != matched.ID || chosen.Version != matched.Version) {
			return review("the request's mutations match different policies; a request is auto-approved only when one policy covers all of it", nil)
		}
		chosen = matched
	}
	eval.PolicyID, eval.PolicyVersion, eval.Mode = chosen.ID, chosen.Version, chosen.Mode

	reasons, err := policyLimitFailures(*chosen, facts, in.Ledger, now, eval.Inputs)
	if err != nil {
		return eval, err
	}
	if len(reasons) > 0 {
		eval.Decision = PolicyDecisionReview
		eval.Reasons = reasons
		return eval, nil
	}
	if chosen.Mode == PolicyModeEnforce {
		eval.Decision = PolicyDecisionApproved
	} else {
		eval.Decision = PolicyDecisionWouldApprove
	}
	eval.Reasons = []string{fmt.Sprintf("inside policy %s v%d", chosen.ID, chosen.Version)}
	return eval, nil
}

// policyEligibility lists why policy cannot approve one mutation for token;
// empty means it can, limits aside.
func policyEligibility(policy Policy, grants []PolicyGrant, tokenID string, facts PolicyFacts, now time.Time) []string {
	if policy.Unreadable != "" {
		return []string{fmt.Sprintf("policy %s v%d cannot be read: %s", policy.ID, policy.Version, policy.Unreadable)}
	}
	var reasons []string
	granted := false
	for _, grant := range grants {
		if grant.PolicyID == policy.ID && grant.TokenID == tokenID && grant.Active(now) {
			granted = true
			break
		}
	}
	if !granted {
		reasons = append(reasons, fmt.Sprintf("the proposing token holds no active grant on policy %s", policy.ID))
	}
	if policy.ValidFrom.Unix() > 0 && now.Before(policy.ValidFrom) {
		reasons = append(reasons, fmt.Sprintf("policy %s starts %s", policy.ID, policy.ValidFrom.UTC().Format("2006-01-02")))
	}
	if policy.ValidUntil.Unix() > 0 && !now.Before(policy.ValidUntil) {
		reasons = append(reasons, fmt.Sprintf("policy %s ended %s", policy.ID, policy.ValidUntil.UTC().Format("2006-01-02")))
	}
	return append(reasons, policyMatch(policy, facts)...)
}

// policyLimitFailures checks the rate, freshness and pause limits, writing
// what it read into inputs for the audit.
func policyLimitFailures(policy Policy, facts []PolicyFacts, ledger PolicyLedger, now time.Time, inputs map[string]any) ([]string, error) {
	limits := policy.Limits
	var reasons []string
	if ledger == nil {
		return []string{"policy limits cannot be checked"}, nil
	}
	if limits.MaxPerDay > 0 {
		count, err := ledger.ApprovalsSince(policy.ID, now.Add(-24*time.Hour))
		if err != nil {
			return nil, err
		}
		inputs["approvals_last_day"] = count
		if count >= limits.MaxPerDay {
			reasons = append(reasons, fmt.Sprintf("policy %s already approved %d changes in the last day (limit %d)", policy.ID, count, limits.MaxPerDay))
		}
	}
	if limits.MaxPerHour > 0 {
		count, err := ledger.ApprovalsSince(policy.ID, now.Add(-time.Hour))
		if err != nil {
			return nil, err
		}
		inputs["approvals_last_hour"] = count
		if count >= limits.MaxPerHour {
			reasons = append(reasons, fmt.Sprintf("policy %s already approved %d changes in the last hour (limit %d)", policy.ID, count, limits.MaxPerHour))
		}
	}
	if limits.MinIntervalMinutes > 0 {
		last, ok, err := ledger.LastApproval(policy.ID)
		if err != nil {
			return nil, err
		}
		if ok {
			inputs["last_approval_at"] = last.UTC().Format(time.RFC3339)
			if next := last.Add(time.Duration(limits.MinIntervalMinutes) * time.Minute); now.Before(next) {
				reasons = append(reasons, fmt.Sprintf("policy %s needs %d minutes between changes; the last was at %s", policy.ID, limits.MinIntervalMinutes, last.UTC().Format(time.RFC3339)))
			}
		}
	}
	for _, f := range facts {
		if limits.InputMaxAgeMinutes > 0 {
			if f.InputObservedAt.IsZero() || f.InputObservedAt.Unix() <= 0 {
				reasons = append(reasons, "the state this change was judged against has no read time")
			} else {
				inputs["input_observed_at"] = f.InputObservedAt.UTC().Format(time.RFC3339)
				if age := now.Sub(f.InputObservedAt); age > time.Duration(limits.InputMaxAgeMinutes)*time.Minute {
					reasons = append(reasons, fmt.Sprintf("the thermostat data is %d minutes old (limit %d)", int(age.Minutes()), limits.InputMaxAgeMinutes))
				}
			}
		}
		for key, value := range f.Inputs {
			inputs[key] = value
		}
		if limits.PauseAfterManualChangeHours > 0 && f.Target != "" {
			window := time.Duration(limits.PauseAfterManualChangeHours * float64(time.Hour))
			at, found, err := ledger.ManualChangeSince(policy, f.Target, now.Add(-window))
			if errors.Is(err, ErrPolicyInputUnavailable) {
				reasons = append(reasons, "the thermostat's change history cannot be read, so a hand change cannot be ruled out")
				continue
			}
			if err != nil {
				return nil, err
			}
			if found {
				inputs["manual_change_at"] = at.UTC().Format(time.RFC3339)
				reasons = append(reasons, fmt.Sprintf("paused until %s: the setpoint was changed at %s by something other than policy %s", at.Add(window).UTC().Format(time.RFC3339), at.UTC().Format(time.RFC3339), policy.ID))
			}
		}
	}
	return reasons, nil
}
