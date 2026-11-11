package mutations

// pdw.mutation_policy: a change to the mutation policies themselves. An agent
// may propose one (it is not forbidden), but no policy can ever approve one
// (typeRules marks it not auto-approvable). PDW, not the agent, decides
// whether a change only narrows what may run without Zach: it compares the
// current state with the proposed one field by field, and only a provable
// narrowing (or a kill switch) applies at once, as pdw:narrowing, with Zach
// told. Anything else is reviewed as a diff. Applying a change is a write to
// PDW's own tables, done in the approving transaction; no worker claims it.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	PolicyChangeOperation = "pdw.mutation_policy"
	PolicyChangeProvider  = "pdw"
	PolicyChangeAccount   = "pdw"

	// NarrowingActor approves a change PDW proved only narrows.
	NarrowingActor = "pdw:narrowing"

	policyActionPut            = "put_policy"
	policyActionSetMode        = "set_mode"
	policyActionGrant          = "grant"
	policyActionRevokeGrant    = "revoke_grant"
	policyActionSetAutoApprove = "set_auto_approve"
)

var (
	policyIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,62}$`)
	policyActions   = map[string][]string{
		policyActionPut:            {"policy"},
		policyActionSetMode:        {"policy_id", "mode"},
		policyActionGrant:          {"grant"},
		policyActionRevokeGrant:    {"grant_id"},
		policyActionSetAutoApprove: {"enabled"},
	}
	policySpecKeys  = map[string]bool{"id": true, "operation": true, "account": true, "match": true, "deny": true, "limits": true, "mode": true, "valid_from": true, "valid_until": true}
	grantSpecKeys   = map[string]bool{"id": true, "policy_id": true, "token_id": true, "label": true, "expires_at": true}
	modeNarrowOrder = map[string]int{PolicyModeEnforce: 2, PolicyModeDryRun: 1, PolicyModeOff: 0}
)

// policyFieldsFor is every fact a type exposes to policies (resideo.go's
// thermostatPolicyFields for the thermostat); a type with none cannot have a
// policy.
func policyFieldsFor(operation string) map[string]bool {
	switch operation {
	case ResideoSetThermostatOperation:
		return thermostatPolicyFields
	}
	return nil
}

// policyChangeFromInput validates a pdw.mutation_policy entry's shape and
// returns its canonical payload. What it changes is judged later, against the
// current state (enrichPolicyChanges).
func policyChangeFromInput(mutation MutationInput) (map[string]any, error) {
	raw := mutation.Raw
	action := strings.TrimSpace(stringFromAny(raw["action"]))
	required, ok := policyActions[action]
	if !ok {
		return nil, fmt.Errorf("action must be one of put_policy, set_mode, grant, revoke_grant, set_auto_approve, not %q", action)
	}
	allowed := map[string]bool{"type": true, "operation": true, "account": true, "title": true, "reason": true, "action": true}
	for _, key := range required {
		allowed[key] = true
	}
	var unknown []string
	for key := range raw {
		if !allowed[key] {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("has unknown field(s) %s for action %s", strings.Join(unknown, ", "), action)
	}
	if account := normalizeAccount(mutation.Account); account != "" && account != PolicyChangeAccount {
		return nil, fmt.Errorf("account must be %q or omitted", PolicyChangeAccount)
	}
	if strings.TrimSpace(mutation.Reason) == "" {
		return nil, errors.New("must include reason: why the policy changes")
	}
	payload := map[string]any{"action": action}
	switch action {
	case policyActionPut:
		spec, ok := raw["policy"].(map[string]any)
		if !ok {
			return nil, errors.New("put_policy needs policy: {id, operation, account, match, deny, limits, mode, valid_from, valid_until}")
		}
		policy, err := policyFromSpec(spec)
		if err != nil {
			return nil, err
		}
		payload["policy"] = policySpec(policy)
	case policyActionSetMode:
		id := strings.TrimSpace(stringFromAny(raw["policy_id"]))
		mode := strings.TrimSpace(stringFromAny(raw["mode"]))
		if !policyIDPattern.MatchString(id) || !policyModes[mode] {
			return nil, errors.New("set_mode needs policy_id and mode (off, dry_run or enforce)")
		}
		payload["policy_id"], payload["mode"] = id, mode
	case policyActionGrant:
		spec, ok := raw["grant"].(map[string]any)
		if !ok {
			return nil, errors.New("grant needs grant: {id, policy_id, token_id, label, expires_at}")
		}
		grant, err := grantFromSpec(spec)
		if err != nil {
			return nil, err
		}
		payload["grant"] = grantSpec(grant)
	case policyActionRevokeGrant:
		id := strings.TrimSpace(stringFromAny(raw["grant_id"]))
		if !policyIDPattern.MatchString(id) {
			return nil, errors.New("revoke_grant needs grant_id")
		}
		payload["grant_id"] = id
	case policyActionSetAutoApprove:
		enabled, ok := raw["enabled"].(bool)
		if !ok {
			return nil, errors.New("set_auto_approve needs enabled: true or false")
		}
		payload["enabled"] = enabled
	}
	return payload, nil
}

func parseSpecTime(value any, field string) (time.Time, error) {
	text := strings.TrimSpace(stringFromAny(value))
	if text == "" {
		return time.Time{}, nil
	}
	at, err := time.Parse(time.RFC3339, text)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be an RFC 3339 time with a zone", field)
	}
	return at.UTC(), nil
}

// policyFromSpec reads a put_policy spec strictly.
func policyFromSpec(spec map[string]any) (Policy, error) {
	for key := range spec {
		if !policySpecKeys[key] {
			return Policy{}, fmt.Errorf("policy has unknown field %q", key)
		}
	}
	policy := Policy{
		ID:        strings.TrimSpace(stringFromAny(spec["id"])),
		Operation: strings.TrimSpace(stringFromAny(spec["operation"])),
		Account:   strings.TrimSpace(stringFromAny(spec["account"])),
		Mode:      strings.TrimSpace(stringFromAny(spec["mode"])),
	}
	if !policyIDPattern.MatchString(policy.ID) {
		return Policy{}, errors.New("policy id must be 2 to 63 lowercase letters, digits and dashes")
	}
	fields := policyFieldsFor(policy.Operation)
	if fields == nil || !TypeRuleFor(policy.Operation).AutoApprovable {
		return Policy{}, fmt.Errorf("%q can never be auto-approved, so it cannot have a policy", policy.Operation)
	}
	if policy.Account == "" {
		return Policy{}, errors.New("policy needs account")
	}
	encode := func(key string) []byte {
		if spec[key] == nil {
			return nil
		}
		data, _ := json.Marshal(spec[key])
		return data
	}
	var err error
	if policy.Match, err = parseFieldRules(encode("match"), matchRuleKeys); err != nil {
		return Policy{}, fmt.Errorf("match: %w", err)
	}
	if policy.Deny, err = parseFieldRules(encode("deny"), denyRuleKeys); err != nil {
		return Policy{}, fmt.Errorf("deny: %w", err)
	}
	for field := range policy.Match {
		if !fields[field] {
			return Policy{}, fmt.Errorf("match names %s, which %s does not expose", field, policy.Operation)
		}
	}
	for field := range policy.Deny {
		if !fields[field] {
			return Policy{}, fmt.Errorf("deny names %s, which %s does not expose", field, policy.Operation)
		}
	}
	if policy.Limits, err = parsePolicyLimits(encode("limits")); err != nil {
		return Policy{}, fmt.Errorf("limits: %w", err)
	}
	if !policyModes[policy.Mode] {
		return Policy{}, errors.New("mode must be off, dry_run or enforce")
	}
	if policy.ValidFrom, err = parseSpecTime(spec["valid_from"], "valid_from"); err != nil {
		return Policy{}, err
	}
	if policy.ValidUntil, err = parseSpecTime(spec["valid_until"], "valid_until"); err != nil {
		return Policy{}, err
	}
	if policy.Mode != PolicyModeOff {
		// A live policy ends by itself and cannot run unbounded.
		if policy.ValidUntil.IsZero() || policy.ValidFrom.IsZero() || !policy.ValidUntil.After(policy.ValidFrom) {
			return Policy{}, errors.New("a policy that is not off needs valid_from before valid_until")
		}
		if policy.Limits.MaxPerDay <= 0 || policy.Limits.MaxPerHour <= 0 {
			return Policy{}, errors.New("a policy that is not off needs limits max_per_day and max_per_hour")
		}
		if len(policy.Match) == 0 {
			return Policy{}, errors.New("a policy that is not off needs match rules")
		}
	}
	return policy, nil
}

// policySpec is the canonical stored spec of a policy.
func policySpec(policy Policy) map[string]any {
	spec := map[string]any{
		"id": policy.ID, "operation": policy.Operation, "account": policy.Account,
		"match": policy.Match, "deny": policy.Deny, "limits": policy.Limits, "mode": policy.Mode,
	}
	if !policy.ValidFrom.IsZero() && policy.ValidFrom.Unix() > 0 {
		spec["valid_from"] = policy.ValidFrom.UTC().Format(time.RFC3339)
	}
	if !policy.ValidUntil.IsZero() && policy.ValidUntil.Unix() > 0 {
		spec["valid_until"] = policy.ValidUntil.UTC().Format(time.RFC3339)
	}
	// Round-trip so the stored payload holds plain JSON values.
	data, _ := json.Marshal(spec)
	out := map[string]any{}
	_ = json.Unmarshal(data, &out)
	return out
}

func grantFromSpec(spec map[string]any) (PolicyGrant, error) {
	for key := range spec {
		if !grantSpecKeys[key] {
			return PolicyGrant{}, fmt.Errorf("grant has unknown field %q", key)
		}
	}
	grant := PolicyGrant{
		ID:       strings.TrimSpace(stringFromAny(spec["id"])),
		PolicyID: strings.TrimSpace(stringFromAny(spec["policy_id"])),
		TokenID:  strings.TrimSpace(stringFromAny(spec["token_id"])),
		Label:    strings.TrimSpace(stringFromAny(spec["label"])),
	}
	if !policyIDPattern.MatchString(grant.ID) || !policyIDPattern.MatchString(grant.PolicyID) || grant.TokenID == "" {
		return PolicyGrant{}, errors.New("grant needs id, policy_id and token_id (an issued token's id, never a client name)")
	}
	expires, err := parseSpecTime(spec["expires_at"], "expires_at")
	if err != nil {
		return PolicyGrant{}, err
	}
	if expires.IsZero() {
		return PolicyGrant{}, errors.New("a grant needs expires_at: a forgotten grant must not live forever")
	}
	grant.ExpiresAt = expires
	return grant, nil
}

func grantSpec(grant PolicyGrant) map[string]any {
	return map[string]any{
		"id": grant.ID, "policy_id": grant.PolicyID, "token_id": grant.TokenID, "label": grant.Label,
		"expires_at": grant.ExpiresAt.UTC().Format(time.RFC3339),
	}
}

// policyNarrowing compares an existing policy version with a proposed one
// field by field. It returns the diff and, for every
// field that does not provably narrow, why. No reasons means the new version
// allows only what the old one did.
func policyNarrowing(old, next Policy) (diff []map[string]any, widens []string) {
	note := func(field string, before, after any, narrowing bool, why string) {
		// The verdict comes first: an unchanged display never hides a
		// widening (two rules can print alike and still differ).
		if narrowing && fmt.Sprint(before) == fmt.Sprint(after) {
			return
		}
		verdict := "narrows"
		if !narrowing {
			verdict = "widens"
			widens = append(widens, field+": "+why)
		}
		diff = append(diff, map[string]any{"field": field, "before": before, "after": after, "verdict": verdict})
	}
	note("operation", old.Operation, next.Operation, old.Operation == next.Operation, "a different type is a new policy")
	note("account", old.Account, next.Account, old.Account == next.Account, "a different account is a new policy")
	note("mode", old.Mode, next.Mode, modeNarrowOrder[next.Mode] <= modeNarrowOrder[old.Mode], "only enforce → dry_run → off narrows")

	fields := map[string]bool{}
	for field := range old.Match {
		fields[field] = true
	}
	for field := range next.Match {
		fields[field] = true
	}
	for _, field := range sortedKeys(fields) {
		before, hadBefore := old.Match[field]
		after, hasAfter := next.Match[field]
		switch {
		case hadBefore && !hasAfter:
			note("match."+field, describeRule(before), "any", false, "the rule is removed")
		case !hadBefore && hasAfter:
			note("match."+field, "any", describeRule(after), true, "")
		default:
			ok, why := matchRuleNarrows(before, after)
			note("match."+field, describeRule(before), describeRule(after), ok, why)
		}
	}
	denyFields := map[string]bool{}
	for field := range old.Deny {
		denyFields[field] = true
	}
	for field := range next.Deny {
		denyFields[field] = true
	}
	for _, field := range sortedKeys(denyFields) {
		before, hadBefore := old.Deny[field]
		after, hasAfter := next.Deny[field]
		switch {
		case hadBefore && !hasAfter:
			note("deny."+field, describeRule(before), "none", false, "a deny rule is removed")
		case !hadBefore && hasAfter:
			note("deny."+field, "none", describeRule(after), true, "")
		default:
			ok, why := denyRuleNarrows(before, after)
			note("deny."+field, describeRule(before), describeRule(after), ok, why)
		}
	}
	atMost := func(field string, before, after int) {
		// Zero is "no limit": loosening to it widens.
		ok := before == 0 || (after > 0 && after <= before)
		note("limits."+field, before, after, ok, "a higher or removed maximum")
	}
	atMost("max_per_day", old.Limits.MaxPerDay, next.Limits.MaxPerDay)
	atMost("max_per_hour", old.Limits.MaxPerHour, next.Limits.MaxPerHour)
	atMost("input_max_age_minutes", old.Limits.InputMaxAgeMinutes, next.Limits.InputMaxAgeMinutes)
	note("limits.min_interval_minutes", old.Limits.MinIntervalMinutes, next.Limits.MinIntervalMinutes,
		next.Limits.MinIntervalMinutes >= old.Limits.MinIntervalMinutes, "a shorter interval")
	note("limits.pause_after_manual_change_hours", old.Limits.PauseAfterManualChangeHours, next.Limits.PauseAfterManualChangeHours,
		next.Limits.PauseAfterManualChangeHours >= old.Limits.PauseAfterManualChangeHours, "a shorter pause")
	note("valid_from", formatSpecTime(old.ValidFrom), formatSpecTime(next.ValidFrom),
		!next.ValidFrom.Before(old.ValidFrom), "an earlier start")
	untilOK := old.ValidUntil.Unix() <= 0 || (next.ValidUntil.Unix() > 0 && !next.ValidUntil.After(old.ValidUntil))
	note("valid_until", formatSpecTime(old.ValidUntil), formatSpecTime(next.ValidUntil), untilOK, "a later or removed end")
	return diff, widens
}

func matchRuleNarrows(before, after FieldRule) (bool, string) {
	if before.Absent || after.Absent {
		if before.Absent && after.Absent {
			return true, ""
		}
		return false, "switching between absent and set is not comparable"
	}
	if before.Min != nil && (after.Min == nil || *after.Min < *before.Min) {
		return false, "a lower or removed minimum"
	}
	if before.Max != nil && (after.Max == nil || *after.Max > *before.Max) {
		return false, "a higher or removed maximum"
	}
	if len(before.In) > 0 {
		if len(after.In) == 0 {
			return false, "the allowed values are removed"
		}
		for _, value := range after.In {
			if !valueIn(value, before.In) {
				return false, fmt.Sprintf("%v was not allowed before", value)
			}
		}
	}
	return true, ""
}

func denyRuleNarrows(before, after FieldRule) (bool, string) {
	if before.Below != nil && (after.Below == nil || *after.Below < *before.Below) {
		return false, "denies less below"
	}
	if before.Above != nil && (after.Above == nil || *after.Above > *before.Above) {
		return false, "denies less above"
	}
	for _, value := range before.In {
		if !valueIn(value, after.In) {
			return false, fmt.Sprintf("%v is no longer denied", value)
		}
	}
	return true, ""
}

func describeRule(rule FieldRule) string {
	var parts []string
	if rule.Absent {
		parts = append(parts, "not set")
	}
	if rule.Min != nil {
		parts = append(parts, fmt.Sprintf("min %g", *rule.Min))
	}
	if rule.Max != nil {
		parts = append(parts, fmt.Sprintf("max %g", *rule.Max))
	}
	if rule.Below != nil {
		parts = append(parts, fmt.Sprintf("below %g", *rule.Below))
	}
	if rule.Above != nil {
		parts = append(parts, fmt.Sprintf("above %g", *rule.Above))
	}
	if len(rule.In) > 0 {
		// JSON, so "a b" and "a", "b" print differently and numbers stay numbers.
		values, _ := json.Marshal(rule.In)
		parts = append(parts, "one of "+string(values))
	}
	return strings.Join(parts, ", ")
}

func formatSpecTime(at time.Time) string {
	if at.Unix() <= 0 {
		return "none"
	}
	return at.UTC().Format(time.RFC3339)
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// policyChangeReview is the review of one change: the diff, whether PDW
// proved it a narrowing, and why not.
type policyChangeReview struct {
	Narrowing bool
	Summary   string
	Diff      []map[string]any
	Widens    []string
	// BaseVersion is the policy version the diff was computed against.
	BaseVersion int64
	// Base fingerprints the state the diff was computed against (the
	// policy's version, the grant row, or the kill switch). Under the lock,
	// a change whose base moved is judged again, never applied blind.
	Base string
}

func (r policyChangeReview) preview() map[string]any {
	diff := r.Diff
	if diff == nil {
		diff = []map[string]any{}
	}
	return map[string]any{"policy_change": map[string]any{
		"narrowing": r.Narrowing, "summary": r.Summary, "diff": diff, "widens": nonNilStrings(r.Widens), "base_version": r.BaseVersion, "base": r.Base,
	}}
}

// reviewPolicyChange judges one change against the current state.
// createdHere names the policies an earlier put_policy in the same request
// creates, so a grant on a new policy can travel with it.
func reviewPolicyChange(ctx context.Context, tx sqlQueryer, payload map[string]any, createdHere map[string]bool) (policyChangeReview, error) {
	review, err := judgePolicyChange(ctx, tx, payload, createdHere)
	if err != nil {
		return review, err
	}
	review.Base, err = policyChangeBase(ctx, tx, payload)
	return review, err
}

// policyChangeBase fingerprints the state a change is judged against.
func policyChangeBase(ctx context.Context, tx sqlQueryer, payload map[string]any) (string, error) {
	switch stringFromAny(payload["action"]) {
	case policyActionPut, policyActionSetMode:
		id := stringFromAny(mapFromAny(payload["policy"])["id"]) + stringFromAny(payload["policy_id"])
		policy, found, err := currentPolicy(ctx, tx, id)
		if err != nil || !found {
			return "policy:none", err
		}
		return fmt.Sprintf("policy:v%d", policy.Version), nil
	case policyActionGrant, policyActionRevokeGrant:
		id := stringFromAny(mapFromAny(payload["grant"])["id"]) + stringFromAny(payload["grant_id"])
		grant, found, err := grantByID(ctx, tx, id)
		if err != nil || !found {
			return "grant:none", err
		}
		return fmt.Sprintf("grant:%s|%s|%s|%d|%d", grant.PolicyID, grant.TokenID, grant.Label, grant.ExpiresAt.Unix(), grant.RevokedAt.Unix()), nil
	case policyActionSetAutoApprove:
		rows, err := queryContext(ctx, tx, `SELECT value::text FROM @mutation_policy_settings WHERE key = $1`, AutoApproveSettingKey)
		if err != nil {
			return "", err
		}
		defer rows.Close()
		value := "none"
		if rows.Next() {
			if err := rows.Scan(&value); err != nil {
				return "", err
			}
		}
		return "settings:" + value, rows.Err()
	}
	return "", nil
}

// policyChangeLocks takes, in the order every policy writer and the claim
// use (kill switch, then policy ids sorted), the locks the changes of a
// request touch, before any of them is judged under them.
func policyChangeLocks(ctx context.Context, tx *sql.Tx, payloads []map[string]any) error {
	settings := false
	var ids []string
	for _, payload := range payloads {
		switch stringFromAny(payload["action"]) {
		case policyActionSetAutoApprove:
			settings = true
		case policyActionPut:
			ids = append(ids, stringFromAny(mapFromAny(payload["policy"])["id"]))
		case policyActionSetMode:
			ids = append(ids, stringFromAny(payload["policy_id"]))
		case policyActionGrant:
			ids = append(ids, stringFromAny(mapFromAny(payload["grant"])["policy_id"]))
		case policyActionRevokeGrant:
			grant, found, err := grantByID(ctx, tx, stringFromAny(payload["grant_id"]))
			if err != nil {
				return err
			}
			if found {
				ids = append(ids, grant.PolicyID)
			}
		}
	}
	if settings {
		if err := lockPolicySettings(ctx, tx); err != nil {
			return err
		}
	}
	return lockPolicies(ctx, tx, ids)
}

func judgePolicyChange(ctx context.Context, tx sqlQueryer, payload map[string]any, createdHere map[string]bool) (policyChangeReview, error) {
	switch stringFromAny(payload["action"]) {
	case policyActionPut:
		next, err := policyFromSpec(mapFromAny(payload["policy"]))
		if err != nil {
			return policyChangeReview{}, err
		}
		current, found, err := currentPolicy(ctx, tx, next.ID)
		if err != nil {
			return policyChangeReview{}, err
		}
		if !found {
			return policyChangeReview{Summary: "creates policy " + next.ID + " (only Zach creates a policy)",
				Diff:   []map[string]any{{"field": "policy", "before": "none", "after": policySpec(next), "verdict": "widens"}},
				Widens: []string{"a new policy"}}, nil
		}
		if current.Unreadable != "" {
			return policyChangeReview{Summary: "replaces an unreadable policy", Widens: []string{"the current version cannot be compared"}, BaseVersion: current.Version}, nil
		}
		diff, widens := policyNarrowing(current, next)
		summary := fmt.Sprintf("changes policy %s (v%d → v%d)", next.ID, current.Version, current.Version+1)
		return policyChangeReview{Narrowing: len(widens) == 0, Summary: summary, Diff: diff, Widens: widens, BaseVersion: current.Version}, nil
	case policyActionSetMode:
		id, mode := stringFromAny(payload["policy_id"]), stringFromAny(payload["mode"])
		current, found, err := currentPolicy(ctx, tx, id)
		if err != nil {
			return policyChangeReview{}, err
		}
		if !found {
			return policyChangeReview{}, invalidProposalInput(fmt.Errorf("no policy %s", id))
		}
		narrows := modeNarrowOrder[mode] <= modeNarrowOrder[current.Mode]
		review := policyChangeReview{Narrowing: narrows, Summary: fmt.Sprintf("sets policy %s to %s", id, mode), BaseVersion: current.Version,
			Diff: []map[string]any{{"field": "mode", "before": current.Mode, "after": mode, "verdict": map[bool]string{true: "narrows", false: "widens"}[narrows]}}}
		if !narrows {
			review.Widens = []string{"mode: only enforce → dry_run → off narrows"}
		}
		return review, nil
	case policyActionGrant:
		grant, err := grantFromSpec(mapFromAny(payload["grant"]))
		if err != nil {
			return policyChangeReview{}, err
		}
		if _, found, err := currentPolicy(ctx, tx, grant.PolicyID); err != nil {
			return policyChangeReview{}, err
		} else if !found && !createdHere[grant.PolicyID] {
			return policyChangeReview{}, invalidProposalInput(fmt.Errorf("no policy %s (create it with put_policy earlier in this request)", grant.PolicyID))
		}
		existing, found, err := grantByID(ctx, tx, grant.ID)
		if err != nil {
			return policyChangeReview{}, err
		}
		if !found {
			return policyChangeReview{Summary: fmt.Sprintf("grants policy %s to token %s (%s)", grant.PolicyID, grant.TokenID, grant.Label),
				Diff:   []map[string]any{{"field": "grant", "before": "none", "after": grantSpec(grant), "verdict": "widens"}},
				Widens: []string{"a new grant"}}, nil
		}
		// Only an earlier expiry on the same grant narrows.
		narrows := existing.PolicyID == grant.PolicyID && existing.TokenID == grant.TokenID && existing.Active(time.Now()) && !grant.ExpiresAt.After(existing.ExpiresAt)
		review := policyChangeReview{Narrowing: narrows, Summary: "changes grant " + grant.ID,
			Diff: []map[string]any{{"field": "grant", "before": grantSpec(existing), "after": grantSpec(grant), "verdict": map[bool]string{true: "narrows", false: "widens"}[narrows]}}}
		if !narrows {
			review.Widens = []string{"only an earlier expiry of an active grant narrows"}
		}
		return review, nil
	case policyActionRevokeGrant:
		id := stringFromAny(payload["grant_id"])
		if _, found, err := grantByID(ctx, tx, id); err != nil {
			return policyChangeReview{}, err
		} else if !found {
			return policyChangeReview{}, invalidProposalInput(fmt.Errorf("no grant %s", id))
		}
		return policyChangeReview{Narrowing: true, Summary: "revokes grant " + id,
			Diff: []map[string]any{{"field": "grant", "before": "active", "after": "revoked", "verdict": "narrows"}}}, nil
	case policyActionSetAutoApprove:
		enabled, _ := payload["enabled"].(bool)
		if enabled {
			return policyChangeReview{Summary: "switches automatic approvals on", Widens: []string{"switching on"},
				Diff: []map[string]any{{"field": "auto_approve", "before": "off", "after": "on", "verdict": "widens"}}}, nil
		}
		return policyChangeReview{Narrowing: true, Summary: "switches every automatic approval off",
			Diff: []map[string]any{{"field": "auto_approve", "before": "on", "after": "off", "verdict": "narrows"}}}, nil
	}
	return policyChangeReview{}, errors.New("unknown policy change")
}

func currentPolicy(ctx context.Context, q sqlQueryer, id string) (Policy, bool, error) {
	rows, err := queryContext(ctx, q, `SELECT `+policyColumns+` FROM @mutation_policies WHERE id = $1 ORDER BY version DESC LIMIT 1`, id)
	if err != nil {
		return Policy{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return Policy{}, false, rows.Err()
	}
	policy, err := scanPolicy(rows)
	return policy, err == nil, err
}

func grantByID(ctx context.Context, q sqlQueryer, id string) (PolicyGrant, bool, error) {
	rows, err := queryContext(ctx, q, `SELECT `+grantColumns+` FROM @mutation_policy_grants WHERE id = $1`, id)
	if err != nil {
		return PolicyGrant{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return PolicyGrant{}, false, rows.Err()
	}
	grant, err := scanGrant(rows)
	return grant, err == nil, err
}

// enrichPolicyChanges judges every policy change in a proposal and puts the
// diff in its preview. A request that changes policies holds nothing else.
func (s *PostgresStore) enrichPolicyChanges(ctx context.Context, mutations []storedMutation) ([]storedMutation, error) {
	policyChanges := 0
	createdHere := map[string]bool{}
	for index := range mutations {
		if mutations[index].Operation != PolicyChangeOperation {
			continue
		}
		policyChanges++
		review, err := reviewPolicyChange(ctx, s.db, mutations[index].Payload, createdHere)
		if err != nil {
			return nil, invalidProposalInput(fmt.Errorf("mutation %d %w", index, err))
		}
		mutations[index].Preview = review.preview()
		if stringFromAny(mutations[index].Payload["action"]) == policyActionPut {
			createdHere[stringFromAny(mapFromAny(mutations[index].Payload["policy"])["id"])] = true
		}
	}
	if policyChanges > 0 && policyChanges != len(mutations) {
		return nil, invalidProposalInput(errors.New("a pdw.mutation_policy request changes policies only; propose other mutations separately"))
	}
	return mutations, nil
}

// allNarrowings reports whether every mutation is a policy change PDW proved
// narrows.
func allNarrowings(mutations []storedMutation) bool {
	if len(mutations) == 0 {
		return false
	}
	for _, mutation := range mutations {
		if mutation.Operation != PolicyChangeOperation {
			return false
		}
		change := mapFromAny(mutation.Preview["policy_change"])
		if narrowing, _ := change["narrowing"].(bool); !narrowing {
			return false
		}
	}
	return true
}

// applyPolicyChanges applies every approved pdw.mutation_policy mutation of a
// request inside the approving transaction and marks it succeeded. A narrowing
// is judged again here, under the policy's lock, against the state it is
// applied to: one that no longer narrows is refused, never applied.
func applyPolicyChanges(ctx context.Context, tx *sql.Tx, requestID, actor string, requireNarrowing bool, now time.Time) error {
	rows, err := queryContext(ctx, tx, `
		SELECT id, payload_json::text, preview_json::text, requested_by FROM @upstream_mutations
		WHERE request_id = $1 AND operation = $2 AND status = 'approved'
		ORDER BY request_index`, requestID, PolicyChangeOperation)
	if err != nil {
		return err
	}
	type change struct {
		id, requestedBy  string
		payload, preview map[string]any
	}
	var changes []change
	for rows.Next() {
		var c change
		var payload, preview string
		if err := rows.Scan(&c.id, &payload, &preview, &c.requestedBy); err != nil {
			rows.Close()
			return err
		}
		c.payload, c.preview = decodeJSONMap([]byte(payload)), decodeJSONMap([]byte(preview))
		changes = append(changes, c)
	}
	rows.Close()
	if len(changes) == 0 {
		return nil
	}
	payloads := make([]map[string]any, 0, len(changes))
	for _, c := range changes {
		payloads = append(payloads, c.payload)
	}
	// Every lock first, then judge each change again under them: a grant,
	// policy or switch can only change between judging and writing by
	// waiting for this transaction.
	if err := policyChangeLocks(ctx, tx, payloads); err != nil {
		return err
	}
	for _, c := range changes {
		// Applied in request order: a policy put earlier exists by now.
		review, err := reviewPolicyChange(ctx, tx, c.payload, nil)
		if err != nil {
			return err
		}
		if requireNarrowing && !review.Narrowing {
			return fmt.Errorf("mutation %s no longer only narrows: %s", c.id, strings.Join(review.Widens, "; "))
		}
		// A narrowing was just judged against the current state under these
		// locks. A change Zach approved was judged when he read its diff: if
		// that state moved since, he approved something else.
		if base := stringFromAny(mapFromAny(c.preview["policy_change"])["base"]); !requireNarrowing && base != review.Base && !createdEarlierInRequest(c.payload, payloads) {
			return fmt.Errorf("the state mutation %s was judged against changed since it was proposed (%s then, %s now); propose it again", c.id, base, review.Base)
		}
		result, err := applyPolicyChange(ctx, tx, c.payload, c.requestedBy, actor, requestID, now)
		if err != nil {
			return err
		}
		encoded, _ := json.Marshal(result)
		if _, err := execContext(ctx, tx, `
			UPDATE @upstream_mutations SET status = 'succeeded', result_json = $2::jsonb, executed_at = $3, observed_at = $3, updated_at = $3
			WHERE id = $1`, c.id, string(encoded), now); err != nil {
			return err
		}
		if err := appendMutationEvent(ctx, tx, c.id, "succeeded", "pdw", actor, map[string]any{"result": result}); err != nil {
			return err
		}
	}
	_, err = execContext(ctx, tx, `
		UPDATE @upstream_mutation_requests SET status = 'succeeded', executed_at = $2, observed_at = $2, updated_at = $2
		WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM @upstream_mutations WHERE request_id = $1 AND status <> 'succeeded')`, requestID, now)
	return err
}

func applyPolicyChange(ctx context.Context, tx *sql.Tx, payload map[string]any, createdBy, approvedBy, requestID string, now time.Time) (map[string]any, error) {
	switch stringFromAny(payload["action"]) {
	case policyActionPut:
		policy, err := policyFromSpec(mapFromAny(payload["policy"]))
		if err != nil {
			return nil, err
		}
		spec := policySpec(policy)
		match, _ := json.Marshal(spec["match"])
		deny, _ := json.Marshal(spec["deny"])
		limits, _ := json.Marshal(spec["limits"])
		var version int64
		if err := queryRowContext(ctx, tx, `
			INSERT INTO @mutation_policies (id, version, operation, account, match_json, deny_json, limits_json, mode,
			                                valid_from, valid_until, created_by, approved_by, change_request_id, created_at)
			VALUES ($1, COALESCE((SELECT max(version) FROM @mutation_policies WHERE id = $1), 0) + 1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7,
			        $8, $9, $10, $11, $12, $13)
			RETURNING version`, policy.ID, policy.Operation, policy.Account, string(match), string(deny), string(limits), policy.Mode,
			scheduleTime(policy.ValidFrom), scheduleTime(policy.ValidUntil), createdBy, approvedBy, requestID, now).Scan(&version); err != nil {
			return nil, err
		}
		return map[string]any{"policy_id": policy.ID, "version": version}, nil
	case policyActionSetMode:
		id, mode := stringFromAny(payload["policy_id"]), stringFromAny(payload["mode"])
		changed, err := setPolicyMode(ctx, tx, id, mode, approvedBy, "mode set by request "+requestID, now)
		return map[string]any{"policy_id": id, "mode": mode, "changed": changed}, err
	case policyActionGrant:
		grant, err := grantFromSpec(mapFromAny(payload["grant"]))
		if err != nil {
			return nil, err
		}
		if _, err := execContext(ctx, tx, `
			INSERT INTO @mutation_policy_grants AS existing (id, policy_id, token_id, label, expires_at, created_by, change_request_id, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
			ON CONFLICT (id) DO UPDATE SET expires_at = EXCLUDED.expires_at, label = EXCLUDED.label, updated_at = EXCLUDED.updated_at
			  WHERE existing.policy_id = EXCLUDED.policy_id AND existing.token_id = EXCLUDED.token_id`,
			grant.ID, grant.PolicyID, grant.TokenID, grant.Label, grant.ExpiresAt, createdBy, requestID, now); err != nil {
			return nil, err
		}
		return map[string]any{"grant_id": grant.ID}, nil
	case policyActionRevokeGrant:
		id := stringFromAny(payload["grant_id"])
		if _, err := execContext(ctx, tx, `
			UPDATE @mutation_policy_grants SET revoked_at = $2, revoked_by = $3, updated_at = $2
			WHERE id = $1 AND revoked_at <= '1970-01-01 00:00:00+00'::timestamptz`, id, now, approvedBy); err != nil {
			return nil, err
		}
		return map[string]any{"grant_id": id, "revoked": true}, nil
	case policyActionSetAutoApprove:
		enabled, _ := payload["enabled"].(bool)
		value, _ := json.Marshal(map[string]any{"enabled": enabled})
		if _, err := execContext(ctx, tx, `
			INSERT INTO @mutation_policy_settings (key, value, updated_by, updated_at) VALUES ($1, $2::jsonb, $3, $4)
			ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_by = EXCLUDED.updated_by, updated_at = EXCLUDED.updated_at`,
			AutoApproveSettingKey, string(value), approvedBy, now); err != nil {
			return nil, err
		}
		return map[string]any{"auto_approve": enabled}, nil
	}
	return nil, errors.New("unknown policy change")
}

// createdEarlierInRequest reports whether a grant's policy is put by another
// change of the same request: its base was "none" at proposal by design.
func createdEarlierInRequest(payload map[string]any, payloads []map[string]any) bool {
	if stringFromAny(payload["action"]) != policyActionGrant {
		return false
	}
	policyID := stringFromAny(mapFromAny(payload["grant"])["policy_id"])
	for _, other := range payloads {
		if stringFromAny(other["action"]) == policyActionPut && stringFromAny(mapFromAny(other["policy"])["id"]) == policyID {
			return true
		}
	}
	return false
}

// approveAsNarrowing approves a request of provable narrowings and applies
// them, as pdw:narrowing. It first takes the locks and judges every change
// again against the state as it is now: when a concurrent change moved that
// state (two agents shortening one grant at once), a change that no longer
// narrows stays pending for Zach with its fresh diff, instead of applying.
func approveAsNarrowing(ctx context.Context, tx *sql.Tx, requestID string, now time.Time) error {
	rows, err := queryContext(ctx, tx, `
		SELECT id, payload_json::text, preview_json::text FROM @upstream_mutations
		WHERE request_id = $1 AND operation = $2 ORDER BY request_index`, requestID, PolicyChangeOperation)
	if err != nil {
		return err
	}
	type pending struct {
		id               string
		payload, preview map[string]any
	}
	var all []pending
	for rows.Next() {
		var p pending
		var payload, preview string
		if err := rows.Scan(&p.id, &payload, &preview); err != nil {
			rows.Close()
			return err
		}
		p.payload, p.preview = decodeJSONMap([]byte(payload)), decodeJSONMap([]byte(preview))
		all = append(all, p)
	}
	rows.Close()
	payloads := make([]map[string]any, 0, len(all))
	for _, p := range all {
		payloads = append(payloads, p.payload)
	}
	if err := policyChangeLocks(ctx, tx, payloads); err != nil {
		return err
	}
	stillNarrows := true
	for _, p := range all {
		review, err := reviewPolicyChange(ctx, tx, p.payload, nil)
		if err != nil {
			return err
		}
		// Judged now, under the locks, against the state it will change:
		// that is the judgement that counts, whatever the proposal saw.
		encoded, _ := json.Marshal(review.preview())
		if review.Narrowing {
			if _, err := execContext(ctx, tx, `UPDATE @upstream_mutations SET preview_json = $2::jsonb WHERE id = $1`, p.id, string(encoded)); err != nil {
				return err
			}
			continue
		}
		stillNarrows = false
		if _, err := execContext(ctx, tx, `UPDATE @upstream_mutations SET preview_json = $2::jsonb WHERE id = $1`, p.id, string(encoded)); err != nil {
			return err
		}
	}
	if !stillNarrows {
		return nil
	}
	if _, err := execContext(ctx, tx, `
		UPDATE @upstream_mutation_requests SET status = 'approved', approved_by = $1, approved_at = $2, updated_at = $2 WHERE id = $3`,
		NarrowingActor, now, requestID); err != nil {
		return err
	}
	pendingIDs, err := pendingMutationIDsForUpdate(ctx, tx, requestID)
	if err != nil {
		return err
	}
	if _, err := execContext(ctx, tx, `
		UPDATE @upstream_mutations SET status = 'approved', approved_by = $1, approved_at = $2, updated_at = $2
		WHERE request_id = $3 AND status = 'pending_review'`, NarrowingActor, now, requestID); err != nil {
		return err
	}
	for _, mutationID := range pendingIDs {
		if err := appendMutationEvent(ctx, tx, mutationID, "approved", "pdw", NarrowingActor, map[string]any{"request_id": requestID, "why": "PDW proved this change only narrows"}); err != nil {
			return err
		}
	}
	if err := appendRequestEvent(ctx, tx, requestID, "approved", "pdw", NarrowingActor, map[string]any{"approved_mutation_ids": pendingIDs}); err != nil {
		return err
	}
	return applyPolicyChanges(ctx, tx, requestID, NarrowingActor, true, now)
}
