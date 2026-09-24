package mutations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Request statuses the agent-facing lifecycle reads and writes. The rest of
// the vocabulary (approved, executing, succeeded, observed, rejected, the
// failure statuses) is written by the reviewer surfaces and the workers and
// stays as bare strings where they write it.
const (
	StatusPendingReview = "pending_review"
	// StatusWithdrawn is terminal and agent-written: the proposer took the
	// request back before a human decided on it. It is deliberately not
	// `rejected`, which is a human's decision and is kept as that record.
	StatusWithdrawn = "withdrawn"
)

// RequestStateError says the request is in a state that refuses what the
// agent asked for — most importantly, that it has already been approved or
// has already run, so proposing it again would repeat its effect. The tool
// surfaces it as invalid input (HTTP 400 / an MCP error result) rather than a
// server failure, because the caller is a model and the message is the
// instruction it needs.
type RequestStateError struct {
	RequestID string
	Status    string
	Revision  int64
	Message   string
}

func (e *RequestStateError) Error() string { return e.Message }

// requestLock is the row-locked view of a request that every agent-side
// transition decides on. It is taken FOR UPDATE, the same lock ApproveRequest
// and RejectRequest take, so an agent withdrawing a request and a human
// approving it serialize: whichever commits second sees the other's status
// and is refused.
type requestLock struct {
	ID           string
	Status       string
	Revision     int64
	SupersededBy string
	Replaces     string
}

func lockRequest(ctx context.Context, tx *sql.Tx, id string) (requestLock, error) {
	var lock requestLock
	err := queryRowContext(ctx, tx, `
		SELECT id, status, revision, superseded_by_request_id, replaces_request_id
		FROM @upstream_mutation_requests
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&lock.ID, &lock.Status, &lock.Revision, &lock.SupersededBy, &lock.Replaces)
	if errors.Is(err, sql.ErrNoRows) {
		return requestLock{}, ErrNotFound
	}
	return lock, err
}

func validateWithdrawInput(id string, input WithdrawInput) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("withdraw needs the request id being withdrawn")
	}
	if strings.TrimSpace(input.Reason) == "" {
		return errors.New("withdraw needs a reason: say why the request no longer stands, so the reviewer and the audit trail know")
	}
	if strings.TrimSpace(input.ReplacedBy) == strings.TrimSpace(id) {
		return errors.New("a request cannot be replaced by itself")
	}
	if input.ExpectedRevision < 0 {
		return errors.New("expected_revision must be a positive revision number")
	}
	return nil
}

// refusalForStatus words the refusal for an agent, by what the request's
// state means. `verb` is "withdrawn" or "replaced".
func refusalForStatus(lock requestLock, verb string) *RequestStateError {
	var message string
	switch lock.Status {
	case "approved":
		message = fmt.Sprintf("request %s was approved by a reviewer and is queued to run, so it cannot be %s; do not propose the same change again — if the approved payload is wrong, say so rather than duplicating it", lock.ID, verb)
	case "executing":
		message = fmt.Sprintf("request %s is executing right now, so it cannot be %s; proposing it again would repeat its effect", lock.ID, verb)
	case "succeeded", "observed":
		message = fmt.Sprintf("request %s already ran (status %s), so it cannot be %s; a replacement would repeat its effect — propose a follow-up correction if one is needed", lock.ID, lock.Status, verb)
	case "rejected":
		message = fmt.Sprintf("request %s was denied by a reviewer and stays as the record of that decision, so it cannot be %s", lock.ID, verb)
	case StatusWithdrawn:
		if lock.SupersededBy != "" {
			message = fmt.Sprintf("request %s is already withdrawn and replaced by %s", lock.ID, lock.SupersededBy)
		} else {
			message = fmt.Sprintf("request %s is already withdrawn", lock.ID)
		}
	default:
		message = fmt.Sprintf("request %s is %s; only a request still waiting for review can be %s", lock.ID, lock.Status, verb)
	}
	return &RequestStateError{RequestID: lock.ID, Status: lock.Status, Revision: lock.Revision, Message: message}
}

// checkPendingRevision is the stale-version guard. A pending request whose
// revision is still 1 is exactly as the agent left it, so no version needs
// stating. Once a reviewer has edited or trimmed it the revision has moved,
// and the agent must name the revision it read: withdrawing a request a human
// is mid-way through fixing, on the strength of a version that no longer
// exists, is the race this refuses.
func checkPendingRevision(lock requestLock, expected int64, verb string) error {
	if lock.Status != StatusPendingReview {
		return refusalForStatus(lock, verb)
	}
	if expected == 0 && lock.Revision > 1 {
		return &RequestStateError{RequestID: lock.ID, Status: lock.Status, Revision: lock.Revision, Message: fmt.Sprintf(
			"request %s is at revision %d: a reviewer changed it after it was proposed; read the current request and pass expected_revision %d to have it %s as the version you saw",
			lock.ID, lock.Revision, lock.Revision, verb)}
	}
	if expected != 0 && expected != lock.Revision {
		return &RequestStateError{RequestID: lock.ID, Status: lock.Status, Revision: lock.Revision, Message: fmt.Sprintf(
			"request %s is at revision %d, not %d: it changed since it was read; read it again before having it %s",
			lock.ID, lock.Revision, expected, verb)}
	}
	return nil
}

// WithdrawRequest takes a pending request back on the proposer's behalf.
//
// Only a request still waiting for review can be withdrawn: an approved one
// is queued to run, an executing one is running, a finished one has run, and
// a denied one is the reviewer's record. The row lock is the same one
// ApproveRequest takes, so a withdrawal and an approval racing each other
// resolve to exactly one of the two. The status is agent-written and
// terminal; the reason is required and kept on the row and in the event log,
// and an optional replacement is linked both ways.
func (s *PostgresStore) WithdrawRequest(ctx context.Context, id string, input WithdrawInput) (Request, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.EnsureTables(ctx); err != nil {
		return Request{}, err
	}
	id = strings.TrimSpace(id)
	input.Reason = strings.TrimSpace(input.Reason)
	input.ReplacedBy = strings.TrimSpace(input.ReplacedBy)
	if err := validateWithdrawInput(id, input); err != nil {
		return Request{}, err
	}
	if input.Actor == "" {
		input.Actor = defaultRequestedBy
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Request{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	lock, err := lockRequest(ctx, tx, id)
	if err != nil {
		return Request{}, err
	}
	if err := checkPendingRevision(lock, input.ExpectedRevision, "withdrawn"); err != nil {
		return Request{}, err
	}
	if input.ReplacedBy != "" {
		replacement, err := lockRequest(ctx, tx, input.ReplacedBy)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return Request{}, fmt.Errorf("replacement request %s does not exist", input.ReplacedBy)
			}
			return Request{}, err
		}
		if err := checkReplacementIsLive(replacement); err != nil {
			return Request{}, err
		}
	}
	now := time.Now().UTC()
	if err := withdrawPendingRows(ctx, tx, lock, input, now); err != nil {
		return Request{}, err
	}
	if input.ReplacedBy != "" {
		if err := linkReplaces(ctx, tx, input.ReplacedBy, lock, input.Reason, input.Actor); err != nil {
			return Request{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Request{}, err
	}
	committed = true
	return s.GetRequest(ctx, id)
}

// checkReplacementIsLive refuses a replacement pointer at a request that is
// itself out of the picture: a withdrawn or denied replacement would be a
// dangling chain that reads "dealt with" and was not.
func checkReplacementIsLive(replacement requestLock) error {
	switch replacement.Status {
	case StatusWithdrawn, "rejected":
		return &RequestStateError{RequestID: replacement.ID, Status: replacement.Status, Revision: replacement.Revision, Message: fmt.Sprintf(
			"replacement request %s is %s, so it cannot stand in for anything", replacement.ID, replacement.Status)}
	}
	return nil
}

// withdrawPendingRows flips a locked pending request and its pending mutations
// to withdrawn and writes the events. The caller has already checked the
// status and revision.
func withdrawPendingRows(ctx context.Context, tx *sql.Tx, lock requestLock, input WithdrawInput, now time.Time) error {
	pendingIDs, err := pendingMutationIDsForUpdate(ctx, tx, lock.ID)
	if err != nil {
		return err
	}
	if _, err := execContext(ctx, tx, `
		UPDATE @upstream_mutation_requests
		   SET status = $1,
		       error = $2,
		       withdrawn_by = $3,
		       withdrawn_at = $4,
		       superseded_by_request_id = CASE WHEN $5 <> '' THEN $5 ELSE superseded_by_request_id END,
		       revision = revision + 1,
		       updated_at = $4
		 WHERE id = $6
	`, StatusWithdrawn, input.Reason, input.Actor, now, input.ReplacedBy, lock.ID); err != nil {
		return err
	}
	if _, err := execContext(ctx, tx, `
		UPDATE @upstream_mutations
		   SET status = $1, error = $2, updated_at = $3
		 WHERE request_id = $4 AND status = 'pending_review'
	`, StatusWithdrawn, input.Reason, now, lock.ID); err != nil {
		return err
	}
	for _, mutationID := range pendingIDs {
		if err := appendMutationEvent(ctx, tx, mutationID, "withdrawn", "agent", input.Actor, map[string]any{
			"request_id":  lock.ID,
			"reason":      input.Reason,
			"replaced_by": input.ReplacedBy,
		}); err != nil {
			return err
		}
	}
	return appendRequestEvent(ctx, tx, lock.ID, "withdrawn", "agent", input.Actor, map[string]any{
		"reason":            input.Reason,
		"replaced_by":       input.ReplacedBy,
		"expected_revision": input.ExpectedRevision,
		"revision_before":   lock.Revision,
		"mutation_ids":      pendingIDs,
	})
}

// linkReplaces records on the replacement which request it replaces. The
// column is written once — a request replaces one earlier request — and the
// event is appended only when the link is new, so a retried call that already
// linked the two writes nothing twice.
func linkReplaces(ctx context.Context, tx *sql.Tx, newID string, old requestLock, reason string, actor string) error {
	result, err := execContext(ctx, tx, `
		UPDATE @upstream_mutation_requests
		   SET replaces_request_id = $1, updated_at = $2
		 WHERE id = $3 AND replaces_request_id = ''
	`, old.ID, time.Now().UTC(), newID)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n == 0 {
		return err
	}
	return appendRequestEvent(ctx, tx, newID, "replaces", "agent", actor, map[string]any{
		"request_id":            old.ID,
		"status_at_replacement": old.Status,
		"reason":                reason,
	})
}

// applyReplacement closes out the request a new proposal replaces, inside the
// proposal's own transaction. What "closes out" means depends on where the
// old request got to:
//
//   - still pending: it is withdrawn, so a reviewer cannot approve both and
//     send the same thing twice (the 2026-08-14 calendar batch: v1 approved,
//     v2 approved, a third request to delete the duplicates);
//   - failed, credential-blocked, or denied: it stays what it is and gains
//     the link, the same record a human's "Mark superseded" writes;
//   - already withdrawn without a replacement: it gains the link;
//   - approved, executing, or finished: the proposal is refused outright and
//     nothing is created — the old request will run or has run, and a second
//     copy would repeat its effect.
//
// A replacement that already points at newID is a no-op, so a retried call
// after a timeout converges instead of failing.
func applyReplacement(ctx context.Context, tx *sql.Tx, newID string, replacement RequestReplacement, actor string, now time.Time) error {
	oldID := strings.TrimSpace(replacement.RequestID)
	if oldID == newID {
		return &RequestStateError{RequestID: newID, Status: "", Message: fmt.Sprintf("request %s is the same proposal as %s (identical content), so it cannot replace it; change what is proposed or withdraw it with a reason", newID, oldID)}
	}
	old, err := lockRequest(ctx, tx, oldID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return &RequestStateError{RequestID: oldID, Message: fmt.Sprintf("replaces_request_id %s does not exist", oldID)}
		}
		return err
	}
	if old.SupersededBy == newID {
		return linkReplaces(ctx, tx, newID, old, replacement.Reason, actor)
	}
	switch old.Status {
	case StatusPendingReview:
		if err := checkPendingRevision(old, replacement.ExpectedRevision, "replaced"); err != nil {
			return err
		}
		if err := withdrawPendingRows(ctx, tx, old, WithdrawInput{
			Reason:           replacement.Reason,
			ReplacedBy:       newID,
			ExpectedRevision: replacement.ExpectedRevision,
			Actor:            actor,
		}, now); err != nil {
			return err
		}
	case "failed_terminal", "failed_retryable", "blocked_missing_credentials", "rejected", StatusWithdrawn:
		if old.SupersededBy != "" {
			return &RequestStateError{RequestID: old.ID, Status: old.Status, Revision: old.Revision, Message: fmt.Sprintf(
				"request %s is already replaced by %s; replace that one instead", old.ID, old.SupersededBy)}
		}
		if _, err := execContext(ctx, tx, `
			UPDATE @upstream_mutation_requests
			   SET superseded_by_request_id = $2, revision = revision + 1, updated_at = $3
			 WHERE id = $1
		`, old.ID, newID, now); err != nil {
			return err
		}
		if err := appendRequestEvent(ctx, tx, old.ID, "superseded", "agent", actor, map[string]any{
			"superseded_by":   newID,
			"status_at_close": old.Status,
			"reason":          replacement.Reason,
		}); err != nil {
			return err
		}
	default:
		return refusalForStatus(old, "replaced")
	}
	return linkReplaces(ctx, tx, newID, old, replacement.Reason, actor)
}

// applyReplacementStandalone is applyReplacement in its own transaction, for
// the path where CreateRequest found the proposal already stored (same
// content, so the same idempotency key) and there is no proposal transaction
// to ride in.
func (s *PostgresStore) applyReplacementStandalone(ctx context.Context, newID string, replacement RequestReplacement, actor string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := applyReplacement(ctx, tx, newID, replacement, actor, time.Now().UTC()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}
