package mutations

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// What happened to a policy-approved request after the proposal, reported to
// Zach once each by the app's policy outcome loop (Service.RunPolicyOutcomes):
// a change that ran gets a quiet push with Undo; one the worker's claim-time
// check sent back to review gets the ordinary review alert; three failed runs
// in a row turn the policy off, and that is said too.
const (
	PolicyOutcomeApplied        = "applied"
	PolicyOutcomeFailed         = "failed"
	PolicyOutcomeSentBack       = "sent_back"
	PolicyOutcomePolicyDisabled = "policy_disabled"

	// PolicyAutoOffAfterFailures is how many failed runs in a row turn a
	// policy off.
	PolicyAutoOffAfterFailures = 3
	// PolicyAutoOffActor is created_by on the version that turned it off.
	PolicyAutoOffActor = "pdw:auto-off"
)

// PolicyOutcomeAdapter is the notification outbox adapter of these reports
// (ops.notification_events.adapter); server.renderTimelineNotification draws it.
const PolicyOutcomeAdapter = "mutation_policy"

// PolicyOutcome is one report.
type PolicyOutcome struct {
	Kind     string
	Request  Request
	PolicyID string
	Detail   string
}

var policyFailedStatuses = map[string]bool{"failed_terminal": true, "blocked_missing_credentials": true}

// ClaimPolicyOutcomes marks each outcome not yet reported as reported and
// queues its alert in the notification outbox, in one transaction (FOR UPDATE
// SKIP LOCKED, so two app replicas never report one twice), and returns what
// it queued. Only requests touched in the last two days are scanned.
func (s *PostgresStore) ClaimPolicyOutcomes(ctx context.Context, limit int) ([]PolicyOutcome, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.EnsureTables(ctx); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	// Only rows with something to report are selected, then limited: rows
	// already reported or still waiting to run never fill a page and starve
	// a newer outcome.
	rows, err := queryContext(ctx, tx, `
		SELECT request.id, request.status, request.approved_by, request.title, request.reason, request.error,
		       flags.recheck_index, flags.reported_recheck, flags.outcome_reported
		FROM @upstream_mutation_requests AS request
		CROSS JOIN LATERAL (
			SELECT COALESCE(max(e.event_index) FILTER (WHERE e.event_type = 'policy_recheck_failed'), -1) AS recheck_index,
			       COALESCE(max((e.event_json->>'recheck_index')::bigint) FILTER (WHERE e.event_type = 'policy_recheck_reported'), -1) AS reported_recheck,
			       COALESCE(bool_or(e.event_type = 'policy_outcome_reported'), false) AS outcome_reported
			FROM @upstream_mutation_request_events AS e
			WHERE e.request_id = request.id
		) AS flags
		WHERE request.updated_at >= now() - interval '2 days'
		  AND (starts_with(request.approved_by, 'policy:') OR request.status = 'pending_review')
		  AND ((flags.recheck_index > flags.reported_recheck AND request.status = 'pending_review')
		       OR (NOT flags.outcome_reported AND starts_with(request.approved_by, 'policy:')
		           AND request.status IN ('succeeded', 'observed', 'failed_terminal', 'blocked_missing_credentials')))
		ORDER BY request.updated_at
		LIMIT $1
		FOR UPDATE OF request SKIP LOCKED
	`, limit)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		id, status, approvedBy string
		title, reason, errText string
		recheck, reported      int64
		outcomeReported        bool
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.status, &c.approvedBy, &c.title, &c.reason, &c.errText, &c.recheck, &c.reported, &c.outcomeReported); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Every policy this pass may turn off is locked now, sorted, like every
	// other policy writer (lockPolicies), never one at a time in request
	// order, which could deadlock against a writer locking two policies.
	var mayDisable []string
	for _, c := range candidates {
		if policyFailedStatuses[c.status] && strings.HasPrefix(c.approvedBy, PolicyActorPrefix) {
			mayDisable = append(mayDisable, policyIDFromActor(c.approvedBy))
		}
	}
	if err := lockPolicies(ctx, tx, mayDisable); err != nil {
		return nil, err
	}
	var outcomes []PolicyOutcome
	for _, c := range candidates {
		switch {
		case c.recheck > c.reported && c.status == StatusPendingReview:
			if err := appendRequestEvent(ctx, tx, c.id, "policy_recheck_reported", "policy", "pdw-policy-engine", map[string]any{"recheck_index": c.recheck}); err != nil {
				return nil, err
			}
			outcomes = append(outcomes, PolicyOutcome{Kind: PolicyOutcomeSentBack, Request: Request{ID: c.id}})
		case !c.outcomeReported && strings.HasPrefix(c.approvedBy, PolicyActorPrefix) && (undoableStatuses[c.status] || policyFailedStatuses[c.status]):
			policyID := policyIDFromActor(c.approvedBy)
			kind := PolicyOutcomeApplied
			if policyFailedStatuses[c.status] {
				kind = PolicyOutcomeFailed
			}
			if err := appendRequestEvent(ctx, tx, c.id, "policy_outcome_reported", "policy", c.approvedBy, map[string]any{"status": c.status, "kind": kind}); err != nil {
				return nil, err
			}
			outcomes = append(outcomes, PolicyOutcome{Kind: kind, Request: Request{ID: c.id}, PolicyID: policyID})
			if kind == PolicyOutcomeFailed {
				disabled, err := disablePolicyAfterFailures(ctx, tx, policyID)
				if err != nil {
					return nil, err
				}
				if disabled != "" {
					outcomes = append(outcomes, PolicyOutcome{Kind: PolicyOutcomePolicyDisabled, Request: Request{ID: c.id}, PolicyID: policyID, Detail: disabled})
				}
			}
		}
	}
	// Each report goes to the timeline notification outbox in this same
	// transaction: it is delivered by the outbox worker (fan-out, leases,
	// retries, receipts), so a crash or a push failure loses nothing.
	text := map[string]candidate{}
	for _, c := range candidates {
		text[c.id] = c
	}
	for i := range outcomes {
		c := text[outcomes[i].Request.ID]
		outcomes[i].Request.Title, outcomes[i].Request.Reason, outcomes[i].Request.Error = c.title, c.reason, c.errText
		if err := queuePolicyOutcome(ctx, tx, outcomes[i], i); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return outcomes, nil
}

// policyIDFromActor reads <id> out of policy:<id>@<version>.
func policyIDFromActor(actor string) string {
	rest := strings.TrimPrefix(actor, PolicyActorPrefix)
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		return rest[:at]
	}
	return rest
}

// disablePolicyAfterFailures turns policyID off when its last
// PolicyAutoOffAfterFailures finished runs all failed, by writing a new version
// with mode off. It returns the reason, or "" when nothing changed.
func disablePolicyAfterFailures(ctx context.Context, tx *sql.Tx, policyID string) (string, error) {
	rows, err := queryContext(ctx, tx, `
		SELECT status FROM @upstream_mutation_requests
		WHERE starts_with(approved_by, $1) AND status IN ('succeeded', 'observed', 'failed_terminal', 'blocked_missing_credentials')
		ORDER BY updated_at DESC
		LIMIT $2
	`, PolicyActorPrefix+policyID+"@", PolicyAutoOffAfterFailures)
	if err != nil {
		return "", err
	}
	failures := 0
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			rows.Close()
			return "", err
		}
		if policyFailedStatuses[status] {
			failures++
		}
	}
	rows.Close()
	if failures < PolicyAutoOffAfterFailures {
		return "", nil
	}
	reason := fmt.Sprintf("turned off after %d failed runs in a row", PolicyAutoOffAfterFailures)
	if err := lockPolicies(ctx, tx, []string{policyID}); err != nil {
		return "", err
	}
	changed, err := setPolicyMode(ctx, tx, policyID, PolicyModeOff, PolicyAutoOffActor, reason, time.Now().UTC())
	if err != nil || !changed {
		return "", err
	}
	return reason, nil
}

// setPolicyMode writes a new version of policyID that differs only in mode.
// The caller holds the policy's lock (lockPolicies).
// It reports false when the current version already has that mode.
func setPolicyMode(ctx context.Context, tx *sql.Tx, policyID, mode, actor, note string, now time.Time) (bool, error) {
	if !policyModes[mode] {
		return false, fmt.Errorf("mode must be off, dry_run or enforce, not %q", mode)
	}
	result, err := execContext(ctx, tx, `
		INSERT INTO @mutation_policies (id, version, operation, account, match_json, deny_json, limits_json, mode,
		                                valid_from, valid_until, created_by, approved_by, change_request_id, note, created_at)
		SELECT id, version + 1, operation, account, match_json, deny_json, limits_json, $2,
		       valid_from, valid_until, $3, $3, '', $4, $5
		FROM @mutation_policies AS current
		WHERE id = $1
		  AND version = (SELECT max(version) FROM @mutation_policies WHERE id = $1)
		  AND mode <> $2
	`, policyID, mode, actor, note, now)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected > 0, err
}

// PolicyOutcomeAlert is what Zach hears about one outcome: a quiet alert with
// Undo when a change ran, the review alert again when the claim sent it back,
// and an ordinary alert when it failed or a policy was turned off. It is the
// payload of the outbox row (title, snippet and route like a timeline event,
// plus the category, kind and request_id the phone's action buttons need).
func PolicyOutcomeAlert(outcome PolicyOutcome) map[string]any {
	request := outcome.Request
	alert := map[string]any{
		"adapter": PolicyOutcomeAdapter, "source": "pdw_mutations", "actor": "PDW", "request_id": request.ID,
		"route": "/mutations/" + request.ID, "category": "link", "kind": "mutation_" + outcome.Kind, "quiet": false,
		"policy_id": outcome.PolicyID,
	}
	firstNonEmpty := func(values ...string) string {
		for _, value := range values {
			if strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		}
		return ""
	}
	switch outcome.Kind {
	case PolicyOutcomeApplied:
		alert["title"] = request.Title
		alert["snippet"] = firstNonEmpty(request.Reason, "Approved by policy "+outcome.PolicyID+".") + " Tap Undo to put it back."
		alert["category"], alert["kind"], alert["quiet"] = "mutation_applied", "mutation_applied", true
	case PolicyOutcomeSentBack:
		alert["title"] = "Back for your review: " + request.Title
		alert["snippet"] = firstNonEmpty(request.Error, "an automatic approval no longer held when it was about to run")
		alert["category"], alert["kind"] = "mutation_review", "mutation_request"
	case PolicyOutcomePolicyDisabled:
		alert["title"] = "Policy " + outcome.PolicyID + " turned off"
		alert["snippet"] = "It " + outcome.Detail + "; changes it covered now come to you for review."
	default:
		alert["title"] = "Did not run: " + request.Title
		alert["snippet"] = firstNonEmpty(request.Error, "An automatic change did not run.")
	}
	return alert
}

// queuePolicyOutcome inserts one report into the notification outbox. The
// event id is unique per report, so a retried transaction queues it once.
func queuePolicyOutcome(ctx context.Context, tx *sql.Tx, outcome PolicyOutcome, index int) error {
	alert := PolicyOutcomeAlert(outcome)
	var seq int64
	if err := queryRowContext(ctx, tx, `SELECT COALESCE(max(event_index), 0) FROM @upstream_mutation_request_events WHERE request_id = $1`, outcome.Request.ID).Scan(&seq); err != nil {
		return err
	}
	eventID := fmt.Sprintf("%s:%s:%d:%d", outcome.Request.ID, outcome.Kind, seq, index)
	alert["event_id"] = eventID
	payload, err := json.Marshal(alert)
	if err != nil {
		return err
	}
	_, err = execContext(ctx, tx, `
		INSERT INTO @notification_events (adapter, event_id, source, priority, event_ts, landed_at, payload)
		VALUES ($1, $2, 'pdw_mutations', 'self', now(), now(), $3::jsonb)
		ON CONFLICT (adapter, event_id) DO NOTHING`, PolicyOutcomeAdapter, eventID, string(payload))
	return err
}
