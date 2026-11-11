package mutations

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

// Cancel stops an approved request whose remaining mutations have not been
// claimed yet: each still-waiting mutation ends cancelled and never runs. A
// reviewer can cancel any request (the review's Cancel); the token that
// proposed a request can cancel that request (the cancel_mutation tool), since
// cancelling only ever stops something. A trip plan whose dates moved is
// cancelled and proposed again.

const cancelMutationDescription = "Cancel an approved mutation request before it runs: every mutation in it that is still waiting (a scheduled step whose not_before has not come, or one not yet picked up) ends cancelled and never runs. Only the token that proposed the request can cancel it this way, and only while none of its mutations is running. Requires a reason. Use it when a scheduled plan no longer stands (a trip was cancelled or moved), then propose the new plan. A pending request is withdrawn with withdraw_mutation instead."

// CancelMutationInput is the cancel_mutation tool's input.
type CancelMutationInput struct {
	RequestID string `json:"request_id" jsonschema:"the approved request to cancel"`
	Reason    string `json:"reason" jsonschema:"required: why the request no longer stands; shown to Zach and kept in the audit log"`
}

// CancelInput is who cancels and why.
type CancelInput struct {
	Actor  string
	Reason string
	// ProposerTokenID, when set, limits the cancel to a request that token
	// proposed; empty means a reviewer is cancelling.
	ProposerTokenID string
}

type cancelStore interface {
	CancelRequest(ctx context.Context, id string, input CancelInput) (Request, error)
}

// cancellableStatuses are the mutation statuses a cancel stops.
var cancellableStatuses = []string{"approved", "failed_retryable"}

func (s *PostgresStore) CancelRequest(ctx context.Context, id string, input CancelInput) (Request, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.EnsureTables(ctx); err != nil {
		return Request{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Request{}, err
	}
	defer func() { _ = tx.Rollback() }()
	// Lock order, the same as the worker's claim, send-back and status
	// rollup (postgres.py): the request's mutations first, then the request.
	// The claim locks rows FOR UPDATE SKIP LOCKED: a row being claimed right
	// now is waited for here, then seen as executing (or sent back) and
	// refused.
	rows, err := queryContext(ctx, tx, `SELECT id, status FROM @upstream_mutations WHERE request_id = $1 ORDER BY request_index, id FOR UPDATE`, id)
	if err != nil {
		return Request{}, err
	}
	type mutationRow struct{ id, status string }
	var locked []mutationRow
	for rows.Next() {
		var row mutationRow
		if err := rows.Scan(&row.id, &row.status); err != nil {
			rows.Close()
			return Request{}, err
		}
		locked = append(locked, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Request{}, err
	}
	var status, tokenID string
	err = queryRowContext(ctx, tx, `SELECT status, requested_by_token_id FROM @upstream_mutation_requests WHERE id = $1 FOR UPDATE`, id).Scan(&status, &tokenID)
	if errors.Is(err, sql.ErrNoRows) {
		return Request{}, ErrNotFound
	}
	if err != nil {
		return Request{}, err
	}
	if input.ProposerTokenID != "" && (tokenID == "" || tokenID != input.ProposerTokenID) {
		return Request{}, &RequestStateError{RequestID: id, Status: status, Message: "only the token that proposed this request (or Zach) can cancel it"}
	}
	var waiting []string
	running := false
	for _, row := range locked {
		switch row.status {
		case "approved", "failed_retryable":
			waiting = append(waiting, row.id)
		case "executing":
			running = true
		}
	}
	if running {
		return Request{}, &RequestStateError{RequestID: id, Status: status, Message: "a mutation in this request is running now and cannot be recalled; Undo it after it finishes"}
	}
	if len(waiting) == 0 {
		return Request{}, &RequestStateError{RequestID: id, Status: status, Message: fmt.Sprintf("request %s has nothing waiting to run (status %s); a pending request is withdrawn or denied, not cancelled", id, status)}
	}
	now := time.Now().UTC()
	reason := strings.TrimSpace(input.Reason)
	if _, err := execContext(ctx, tx, `
		UPDATE @upstream_mutations SET status = 'cancelled', error = $2, updated_at = $3
		WHERE id = ANY($1)
	`, waiting, "cancelled by "+input.Actor+": "+reason, now); err != nil {
		return Request{}, err
	}
	actorType := "human"
	if input.ProposerTokenID != "" {
		actorType = "agent"
	}
	for _, mutationID := range waiting {
		if err := appendMutationEvent(ctx, tx, mutationID, "cancelled", actorType, input.Actor, map[string]any{"request_id": id, "reason": reason}); err != nil {
			return Request{}, err
		}
	}
	if err := appendRequestEvent(ctx, tx, id, "cancelled", actorType, input.Actor, map[string]any{"cancelled_mutation_ids": waiting, "reason": reason}); err != nil {
		return Request{}, err
	}
	// What the request is now, by the ladder every rollup uses
	// (DeriveRequestStatus): cancelled when nothing in it ran, otherwise
	// the outcome of the steps that did.
	statusRows, err := queryContext(ctx, tx, `SELECT id, status FROM @upstream_mutations WHERE request_id = $1 ORDER BY request_index`, id)
	if err != nil {
		return Request{}, err
	}
	var statuses []string
	byID := map[string]any{}
	for statusRows.Next() {
		var mutationID, mutationStatus string
		if err := statusRows.Scan(&mutationID, &mutationStatus); err != nil {
			statusRows.Close()
			return Request{}, err
		}
		statuses = append(statuses, mutationStatus)
		byID[mutationID] = mutationStatus
	}
	statusRows.Close()
	result, _ := json.Marshal(map[string]any{"mutation_statuses": byID})
	if _, err := execContext(ctx, tx, `
		UPDATE @upstream_mutation_requests
		   SET status = $2, result_json = COALESCE(result_json, '{}'::jsonb) || $3::jsonb, error = $4, updated_at = $5
		 WHERE id = $1
	`, id, DeriveRequestStatus(statuses, status), string(result), "cancelled: "+reason, now); err != nil {
		return Request{}, err
	}
	if err := tx.Commit(); err != nil {
		return Request{}, err
	}
	return s.GetRequest(ctx, id, AllMutations)
}

// CancelRequest cancels as a reviewer (actor) or, with proposerTokenID, as
// the token that proposed the request.
func (s *Service) CancelRequest(ctx context.Context, id string, input CancelInput) (Request, error) {
	store, ok := s.store.(cancelStore)
	if !ok {
		return Request{}, errors.New("cancelling needs the Postgres store")
	}
	if strings.TrimSpace(id) == "" {
		return Request{}, invalidProposalInput(errors.New("request_id is required"))
	}
	if strings.TrimSpace(input.Reason) == "" {
		return Request{}, invalidProposalInput(errors.New("reason is required: say why the request no longer stands"))
	}
	request, err := store.CancelRequest(ctx, strings.TrimSpace(id), input)
	if errors.Is(err, ErrNotFound) {
		return Request{}, invalidProposalInput(fmt.Errorf("no mutation request %s", id))
	}
	return request, err
}

// CancelMutation is the agent tool: only the proposing token may cancel.
func (s *Service) CancelMutation(ctx context.Context, in CancelMutationInput) (WithdrawResponse, error) {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok || principal.TokenID == "" {
		return WithdrawResponse{}, invalidProposalInput(errors.New("cancel_mutation needs an issued token: only the token that proposed a request can cancel it"))
	}
	request, err := s.CancelRequest(ctx, in.RequestID, CancelInput{Actor: requestActor(ctx), Reason: in.Reason, ProposerTokenID: principal.TokenID})
	if err != nil {
		return WithdrawResponse{}, err
	}
	return WithdrawResponse{RequestID: request.ID, Status: request.Status, Revision: request.Revision, ReviewURL: s.requestURL(request.ID)}, nil
}

// canCancel reports whether the review should offer Cancel: something in the
// request still waits to run and nothing is running.
func canCancel(request Request) bool {
	if len(request.Mutations) == 0 || len(request.Mutations) < request.MutationCount {
		return false
	}
	waiting := false
	for _, mutation := range request.Mutations {
		switch mutation.Status {
		case "executing":
			return false
		case "approved", "failed_retryable":
			waiting = true
		}
	}
	return waiting
}
