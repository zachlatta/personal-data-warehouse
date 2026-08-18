package mutations

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// requestIsSupersedable answers whether a request has run out of road. A
// request that can still be approved, retried, or observed must not be closed
// out — marking it superseded would hide live work. Only a request nothing else
// will ever move qualifies.
func requestIsSupersedable(status string) bool {
	switch strings.TrimSpace(status) {
	case "failed_terminal", "failed_retryable", "blocked_missing_credentials":
		return true
	default:
		return false
	}
}

// SupersedeRequest records that a dead request was replaced by a later one.
//
// It does not rewrite the failure: the request really did fail, and the status
// stays as it was. What it adds is the link a reader needs to see the failure
// was dealt with, instead of a terminal red row sitting in the review list with
// no indication that anything was ever done about it.
func (s *PostgresStore) SupersedeRequest(ctx context.Context, id string, supersededBy string, actor string) (Request, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	id = strings.TrimSpace(id)
	supersededBy = strings.TrimSpace(supersededBy)
	if err := validateSupersedeInput(id, supersededBy); err != nil {
		return Request{}, err
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

	status, err := requestStatusForUpdate(ctx, tx, id)
	if err != nil {
		return Request{}, err
	}
	if !requestIsSupersedable(status) {
		return Request{}, fmt.Errorf("request %s is %s; only a request that can no longer run can be superseded", id, status)
	}
	// A dangling pointer would be worse than no pointer, so the replacement has
	// to exist before the link is written.
	if _, err := requestStatusForUpdate(ctx, tx, supersededBy); err != nil {
		if errors.Is(err, ErrNotFound) {
			return Request{}, fmt.Errorf("superseding request %s does not exist", supersededBy)
		}
		return Request{}, err
	}

	now := time.Now().UTC()
	if _, err := execContext(ctx, tx, `
		UPDATE @upstream_mutation_requests
		SET superseded_by_request_id = $2, revision = revision + 1, updated_at = $3
		WHERE id = $1
	`, id, supersededBy, now); err != nil {
		return Request{}, err
	}
	if err := appendRequestEvent(ctx, tx, id, "superseded", "human", actor, map[string]any{
		"superseded_by":   supersededBy,
		"status_at_close": status,
	}); err != nil {
		return Request{}, err
	}
	if err := tx.Commit(); err != nil {
		return Request{}, err
	}
	committed = true
	return s.GetRequest(ctx, id)
}

func validateSupersedeInput(id string, supersededBy string) error {
	if id == "" {
		return errors.New("supersede needs the request id being closed out")
	}
	if supersededBy == "" {
		return errors.New("supersede needs the request id that replaced this one")
	}
	if id == supersededBy {
		return errors.New("a request cannot supersede itself")
	}
	return nil
}
