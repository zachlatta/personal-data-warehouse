package mutations

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// The policy tables (ops schema, app-only like the request and event
// tables). The Python ensure path (ensure_upstream_mutation_tables) declares
// the same DDL, so whichever bootstraps a database first gives it this shape.
var mutationPolicySchemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS @mutation_policies (
		id text NOT NULL,
		version bigint NOT NULL,
		operation text NOT NULL DEFAULT '',
		account text NOT NULL DEFAULT '',
		match_json jsonb NOT NULL DEFAULT '{}'::jsonb,
		deny_json jsonb NOT NULL DEFAULT '{}'::jsonb,
		limits_json jsonb NOT NULL DEFAULT '{}'::jsonb,
		mode text NOT NULL DEFAULT 'off',
		valid_from timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		valid_until timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		created_by text NOT NULL DEFAULT '',
		approved_by text NOT NULL DEFAULT '',
		change_request_id text NOT NULL DEFAULT '',
		note text NOT NULL DEFAULT '',
		created_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (id, version)
	)`,
	`CREATE INDEX IF NOT EXISTS mutation_policies_operation_idx ON @mutation_policies (operation, id, version)`,
	`CREATE TABLE IF NOT EXISTS @mutation_policy_grants (
		id text PRIMARY KEY,
		policy_id text NOT NULL,
		token_id text NOT NULL,
		label text NOT NULL DEFAULT '',
		expires_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		revoked_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		revoked_by text NOT NULL DEFAULT '',
		created_by text NOT NULL DEFAULT '',
		change_request_id text NOT NULL DEFAULT '',
		created_at timestamptz NOT NULL DEFAULT now(),
		updated_at timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS mutation_policy_grants_policy_idx ON @mutation_policy_grants (policy_id, token_id)`,
	`CREATE TABLE IF NOT EXISTS @mutation_policy_evaluations (
		request_id text NOT NULL,
		phase text NOT NULL DEFAULT 'propose',
		policy_id text NOT NULL DEFAULT '',
		policy_version bigint NOT NULL DEFAULT 0,
		token_id text NOT NULL DEFAULT '',
		decision text NOT NULL DEFAULT '',
		reasons_json jsonb NOT NULL DEFAULT '[]'::jsonb,
		inputs_json jsonb NOT NULL DEFAULT '{}'::jsonb,
		evaluated_at timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS mutation_policy_evaluations_policy_idx ON @mutation_policy_evaluations (policy_id, evaluated_at)`,
	`CREATE INDEX IF NOT EXISTS mutation_policy_evaluations_request_idx ON @mutation_policy_evaluations (request_id, evaluated_at)`,
	`CREATE TABLE IF NOT EXISTS @mutation_policy_settings (
		key text PRIMARY KEY,
		value jsonb NOT NULL DEFAULT 'null'::jsonb,
		updated_by text NOT NULL DEFAULT '',
		updated_at timestamptz NOT NULL DEFAULT now()
	)`,
}

// ErrPolicyInputUnavailable means a limit's input could not be read; the
// request goes to review rather than failing.
var ErrPolicyInputUnavailable = errors.New("policy input unavailable")

const policyColumns = `id, version, operation, account, match_json, deny_json, limits_json, mode,
	valid_from, valid_until, created_by, approved_by, created_at`

func scanPolicy(row scanner) (Policy, error) {
	var policy Policy
	var match, deny, limits []byte
	if err := row.Scan(&policy.ID, &policy.Version, &policy.Operation, &policy.Account, &match, &deny, &limits,
		&policy.Mode, &policy.ValidFrom, &policy.ValidUntil, &policy.CreatedBy, &policy.ApprovedBy, &policy.CreatedAt); err != nil {
		return Policy{}, err
	}
	var problems []string
	var err error
	if policy.Match, err = parseFieldRules(match, matchRuleKeys); err != nil {
		problems = append(problems, "match: "+err.Error())
	}
	if policy.Deny, err = parseFieldRules(deny, denyRuleKeys); err != nil {
		problems = append(problems, "deny: "+err.Error())
	}
	if policy.Limits, err = parsePolicyLimits(limits); err != nil {
		problems = append(problems, "limits: "+err.Error())
	}
	if !policyModes[policy.Mode] {
		problems = append(problems, fmt.Sprintf("unknown mode %q", policy.Mode))
	}
	policy.Unreadable = strings.Join(problems, "; ")
	return policy, nil
}

const grantColumns = `id, policy_id, token_id, label, expires_at, revoked_at, revoked_by, created_by, created_at`

func scanGrant(row scanner) (PolicyGrant, error) {
	var grant PolicyGrant
	err := row.Scan(&grant.ID, &grant.PolicyID, &grant.TokenID, &grant.Label, &grant.ExpiresAt, &grant.RevokedAt,
		&grant.RevokedBy, &grant.CreatedBy, &grant.CreatedAt)
	return grant, err
}

// currentPolicies returns the newest version of every policy for operations,
// locking those rows FOR SHARE so a concurrent change waits for this
// transaction.
func currentPolicies(ctx context.Context, tx *sql.Tx, operations []string) ([]Policy, error) {
	rows, err := queryContext(ctx, tx, `
		SELECT `+policyColumns+` FROM @mutation_policies AS policy
		WHERE operation = ANY($1)
		  AND version = (SELECT max(version) FROM @mutation_policies AS newer WHERE newer.id = policy.id)
		ORDER BY id
		FOR SHARE
	`, operations)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var policies []Policy
	for rows.Next() {
		policy, err := scanPolicy(rows)
		if err != nil {
			return nil, err
		}
		policies = append(policies, policy)
	}
	return policies, rows.Err()
}

func grantsForPolicies(ctx context.Context, tx *sql.Tx, policyIDs []string) ([]PolicyGrant, error) {
	if len(policyIDs) == 0 {
		return nil, nil
	}
	rows, err := queryContext(ctx, tx, `SELECT `+grantColumns+` FROM @mutation_policy_grants WHERE policy_id = ANY($1) ORDER BY id FOR SHARE`, policyIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var grants []PolicyGrant
	for rows.Next() {
		grant, err := scanGrant(rows)
		if err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

// autoApproveEnabled reads the global kill switch: the environment variable
// wins, then the settings row (locked FOR SHARE); no row means on.
func autoApproveEnabled(ctx context.Context, tx *sql.Tx) (bool, error) {
	if value := strings.TrimSpace(os.Getenv(AutoApproveEnv)); value == "0" || strings.EqualFold(value, "false") || strings.EqualFold(value, "off") {
		return false, nil
	}
	var raw []byte
	err := queryRowContext(ctx, tx, `SELECT value FROM @mutation_policy_settings WHERE key = $1 FOR SHARE`, AutoApproveSettingKey).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	var setting struct {
		Enabled *bool `json:"enabled"`
	}
	if err := json.Unmarshal(raw, &setting); err != nil || setting.Enabled == nil {
		// An unreadable switch is off: it can only send things to review.
		return false, nil
	}
	return *setting.Enabled, nil
}

// lockPolicies takes each policy's transaction-scoped advisory lock in a
// fixed order, so the limits two concurrent proposals count are serialized.
func lockPolicies(ctx context.Context, tx *sql.Tx, policyIDs []string) error {
	ids := append([]string(nil), policyIDs...)
	sort.Strings(ids)
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('pdw_mutation_policy:' || $1, 0))`, id); err != nil {
			return err
		}
	}
	return nil
}

// txPolicyLedger answers limits inside the proposal's transaction.
type txPolicyLedger struct {
	ctx context.Context
	tx  *sql.Tx
}

func (l txPolicyLedger) ApprovalsSince(policyID string, since time.Time) (int, error) {
	var count int
	err := queryRowContext(l.ctx, l.tx, `
		SELECT count(*) FROM @mutation_policy_evaluations
		WHERE policy_id = $1 AND phase = 'propose' AND decision IN ('approved', 'would_approve') AND evaluated_at >= $2
	`, policyID, since).Scan(&count)
	return count, err
}

func (l txPolicyLedger) LastApproval(policyID string) (time.Time, bool, error) {
	var at sql.NullTime
	err := queryRowContext(l.ctx, l.tx, `
		SELECT max(evaluated_at) FROM @mutation_policy_evaluations
		WHERE policy_id = $1 AND phase = 'propose' AND decision IN ('approved', 'would_approve')
	`, policyID).Scan(&at)
	if err != nil || !at.Valid {
		return time.Time{}, false, err
	}
	return at.Time, true, nil
}

// ManualChangeSince finds a setpoint change in base_resideo.thermostat_readings
// (one row per state change) at or after since that no mutation this policy
// approved explains: a change at the thermostat, in the Resideo app, or a PDW
// change someone else proposed or Zach approved by hand. The Python
// claim-time check (upstream_mutation_policy.py) runs the same query.
func (l txPolicyLedger) ManualChangeSince(policy Policy, target string, since time.Time) (time.Time, bool, error) {
	var regclass sql.NullString
	if err := l.tx.QueryRowContext(l.ctx, `SELECT to_regclass($1)::text`, qualifiedRelation("resideo_thermostat_readings")).Scan(&regclass); err != nil {
		return time.Time{}, false, err
	}
	if !regclass.Valid {
		return time.Time{}, false, ErrPolicyInputUnavailable
	}
	var at time.Time
	err := queryRowContext(l.ctx, l.tx, thermostatManualChangeSQL, ResideoAccount, target, since, PolicyActorPrefix+policy.ID+"@%").Scan(&at)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return at, true, nil
}

// thermostatManualChangeSQL: $1 account, $2 device, $3 since, $4 the policy's
// approved_by pattern. A change is explained when a succeeded mutation this
// policy approved set that value and ran within 30 minutes before the
// reading (or 5 after: the executor records its own read-back first).
const thermostatManualChangeSQL = `
	WITH readings AS (
		SELECT observed_at, heat_setpoint, cool_setpoint,
		       lag(heat_setpoint) OVER (ORDER BY observed_at) AS previous_heat,
		       lag(cool_setpoint) OVER (ORDER BY observed_at) AS previous_cool
		FROM @resideo_thermostat_readings
		WHERE account = $1 AND device_id = $2 AND observed_at >= $3::timestamptz - interval '7 days'
	)
	SELECT readings.observed_at
	FROM readings
	WHERE readings.observed_at >= $3
	  AND readings.previous_heat IS NOT NULL
	  AND (readings.heat_setpoint IS DISTINCT FROM readings.previous_heat
	       OR readings.cool_setpoint IS DISTINCT FROM readings.previous_cool)
	  AND NOT EXISTS (
		SELECT 1 FROM @upstream_mutations AS mutation
		WHERE mutation.operation = 'resideo.set_thermostat'
		  AND mutation.status = 'succeeded'
		  AND mutation.approved_by LIKE $4
		  AND mutation.payload_json->>'device_id' = $2
		  AND (NOT (mutation.payload_json ? 'heat_setpoint')
		       OR (mutation.payload_json->>'heat_setpoint')::float8 = readings.heat_setpoint)
		  AND (NOT (mutation.payload_json ? 'cool_setpoint')
		       OR (mutation.payload_json->>'cool_setpoint')::float8 = readings.cool_setpoint)
		  AND readings.observed_at BETWEEN mutation.executed_at - interval '30 minutes'
		                               AND mutation.executed_at + interval '5 minutes'
	  )
	ORDER BY readings.observed_at DESC
	LIMIT 1`

// qualifiedRelation is the quoted "schema"."name" for a catalog id, which
// to_regclass accepts.
func qualifiedRelation(logicalID string) string { return warehouse.SQLRelation(logicalID) }

// evaluateRequestPolicy loads the policy state for normalized under the
// policies' advisory locks and decides the request.
func evaluateRequestPolicy(ctx context.Context, tx *sql.Tx, tokenID string, normalized []storedMutation, now time.Time) (PolicyEvaluation, error) {
	operations := map[string]bool{}
	for _, mutation := range normalized {
		operations[mutation.Operation] = true
	}
	ops := make([]string, 0, len(operations))
	for op := range operations {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	// Lock first, then read: the advisory lock is what makes the counters
	// below exact for this policy.
	var candidateIDs []string
	rows, err := queryContext(ctx, tx, `SELECT DISTINCT id FROM @mutation_policies WHERE operation = ANY($1)`, ops)
	if err != nil {
		return PolicyEvaluation{}, err
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return PolicyEvaluation{}, err
		}
		candidateIDs = append(candidateIDs, id)
	}
	rows.Close()
	if err := lockPolicies(ctx, tx, candidateIDs); err != nil {
		return PolicyEvaluation{}, err
	}
	policies, err := currentPolicies(ctx, tx, ops)
	if err != nil {
		return PolicyEvaluation{}, err
	}
	ids := make([]string, 0, len(policies))
	for _, policy := range policies {
		ids = append(ids, policy.ID)
	}
	grants, err := grantsForPolicies(ctx, tx, ids)
	if err != nil {
		return PolicyEvaluation{}, err
	}
	enabled, err := autoApproveEnabled(ctx, tx)
	if err != nil {
		return PolicyEvaluation{}, err
	}
	return EvaluatePolicy(PolicyInput{
		TokenID:            tokenID,
		Mutations:          normalized,
		Policies:           policies,
		Grants:             grants,
		AutoApproveEnabled: enabled,
		Ledger:             txPolicyLedger{ctx: ctx, tx: tx},
		Now:                now,
	})
}

// recordPolicyEvaluation stores the decision three ways: a row of
// mutation_policy_evaluations, context.policy on the request, and a request
// event.
func recordPolicyEvaluation(ctx context.Context, tx *sql.Tx, requestID, phase string, eval PolicyEvaluation) error {
	reasons, _ := json.Marshal(nonNilStrings(eval.Reasons))
	inputs, _ := json.Marshal(eval.Inputs)
	if _, err := execContext(ctx, tx, `
		INSERT INTO @mutation_policy_evaluations (request_id, phase, policy_id, policy_version, token_id, decision, reasons_json, inputs_json, evaluated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9)
	`, requestID, phase, eval.PolicyID, eval.PolicyVersion, eval.TokenID, eval.Decision, string(reasons), string(inputs), eval.EvaluatedAt); err != nil {
		return err
	}
	encoded, _ := json.Marshal(eval.JSON())
	if _, err := execContext(ctx, tx, `
		UPDATE @upstream_mutation_requests
		   SET context_json = jsonb_set(COALESCE(context_json, '{}'::jsonb), '{policy}', $2::jsonb, true)
		 WHERE id = $1
	`, requestID, string(encoded)); err != nil {
		return err
	}
	actor := "pdw-policy-engine"
	if eval.PolicyID != "" {
		actor = fmt.Sprintf("%s%s@%d", PolicyActorPrefix, eval.PolicyID, eval.PolicyVersion)
	}
	return appendRequestEvent(ctx, tx, requestID, "policy_evaluated", "policy", actor, map[string]any{"phase": phase, "policy": eval.JSON()})
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
