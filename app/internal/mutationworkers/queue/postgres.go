package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// PostgresStore reproduces the Python warehouse's claim / reclaim / complete /
// fail methods over ops.upstream_mutation_operations, statement for statement.
// Relation names are resolved through the catalog's @logical_id markers.
type PostgresStore struct {
	db      *sql.DB
	url     string
	timeout time.Duration
	now     func() time.Time
}

// NormalizePostgresURL mirrors config.normalize_postgres_url: the Python
// psycopg2 dialect spellings are accepted and rewritten to postgresql://.
func NormalizePostgresURL(value string) string {
	normalized := strings.TrimSpace(value)
	switch {
	case normalized == "":
		return ""
	case strings.HasPrefix(normalized, "postgres://"):
		return "postgresql://" + strings.TrimPrefix(normalized, "postgres://")
	case strings.HasPrefix(normalized, "postgresql+psycopg2://"):
		return "postgresql://" + strings.TrimPrefix(normalized, "postgresql+psycopg2://")
	}
	return normalized
}

// OpenPostgres connects and pings.
func OpenPostgres(ctx context.Context, databaseURL string, timeout time.Duration) (*PostgresStore, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	databaseURL = NormalizePostgresURL(databaseURL)
	if databaseURL == "" {
		return nil, errors.New("POSTGRES_DATABASE_URL must be set")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &PostgresStore{db: db, url: databaseURL, timeout: timeout, now: func() time.Time { return time.Now().UTC() }}, nil
}

// Close releases the pool.
func (s *PostgresStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// EnsureTables provisions the mutation tables through the app's own store,
// which owns their DDL, so the two ensure paths cannot disagree.
func (s *PostgresStore) EnsureTables(ctx context.Context) error {
	store, err := mutations.NewPostgresStore(s.url, s.timeout)
	if err != nil {
		return err
	}
	defer store.Close()
	return store.EnsureTables(ctx)
}

func (s *PostgresStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.timeout)
}

func expand(statement string) string { return warehouse.ExpandRelations(statement) }

// ReclaimStaleExecuting implements Store.
func (s *PostgresStore) ReclaimStaleExecuting(ctx context.Context, staleAfter time.Duration, idempotent []Operation, actorID string) (int, error) {
	if len(idempotent) == 0 {
		return 0, nil
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	now := s.now()
	cutoff := now.Add(-staleAfter)
	providers := make([]string, 0, len(idempotent))
	operations := make([]string, 0, len(idempotent))
	for _, op := range idempotent {
		providers = append(providers, op.Provider)
		operations = append(operations, op.Operation)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	rows, err := tx.QueryContext(ctx, expand(`
		WITH candidates AS (
			SELECT id, request_id, claimed_by, attempt_count
			FROM @upstream_mutations
			WHERE status = 'executing'
			  AND claimed_at < $1
			  AND (provider, operation) IN (
			      SELECT * FROM UNNEST($2::text[], $3::text[])
			  )
			FOR UPDATE SKIP LOCKED
		)
		UPDATE @upstream_mutations AS mutation
		   SET status = 'approved',
		       claimed_by = '',
		       claimed_at = '1970-01-01 00:00:00+00'::timestamptz,
		       updated_at = $4
		  FROM candidates
		 WHERE mutation.id = candidates.id
		RETURNING
			mutation.id,
			mutation.request_id,
			candidates.claimed_by AS previous_claimed_by,
			candidates.attempt_count
	`), cutoff, providers, operations, now)
	if err != nil {
		return 0, err
	}
	type reclaimed struct {
		id, requestID, previousClaimedBy string
		attemptCount                     int64
	}
	var reclaimedRows []reclaimed
	for rows.Next() {
		var r reclaimed
		if err := rows.Scan(&r.id, &r.requestID, &r.previousClaimedBy, &r.attemptCount); err != nil {
			rows.Close()
			return 0, err
		}
		reclaimedRows = append(reclaimedRows, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	requestIDs := map[string]bool{}
	for _, r := range reclaimedRows {
		if err := appendEvent(ctx, tx, r.id, "reclaimed", "dagster", actorID, map[string]any{
			"previous_claimed_by": r.previousClaimedBy,
			"attempt_count":       r.attemptCount,
			"stale_after_seconds": int64(staleAfter / time.Second),
		}, now); err != nil {
			return 0, err
		}
		if r.requestID != "" {
			requestIDs[r.requestID] = true
		}
	}
	if err := s.refreshRequests(ctx, tx, requestIDs); err != nil {
		return 0, err
	}
	return len(reclaimedRows), tx.Commit()
}

// ClaimApproved implements Store.
func (s *PostgresStore) ClaimApproved(ctx context.Context, limit int, claimedBy string, providers []string) ([]Mutation, error) {
	if limit <= 0 {
		return nil, nil
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	now := s.now()
	included := make([]string, 0, len(providers))
	for _, provider := range providers {
		if strings.TrimSpace(provider) != "" {
			included = append(included, provider)
		}
	}
	providerFilter := ""
	args := []any{ClaimableStatuses}
	if len(included) > 0 {
		providerFilter = "AND provider = ANY($2)"
		args = append(args, included)
	}
	n := len(args)
	args = append(args, limit, claimedBy, now, now)
	statement := fmt.Sprintf(`
		WITH candidates AS (
			SELECT id
			FROM @upstream_mutations
			WHERE status = ANY($1)
			  %s
			ORDER BY approved_at ASC, created_at ASC, id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $%d
		)
		UPDATE @upstream_mutations AS mutation
		   SET status = 'executing',
		       claimed_by = $%d,
		       claimed_at = $%d,
		       updated_at = $%d,
		       attempt_count = attempt_count + 1
		  FROM candidates
		 WHERE mutation.id = candidates.id
		RETURNING mutation.id, mutation.request_id, mutation.provider, mutation.operation,
		          mutation.account, mutation.status, mutation.attempt_count, mutation.payload_json::text
	`, providerFilter, n+1, n+2, n+3, n+4)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	rows, err := tx.QueryContext(ctx, expand(statement), args...)
	if err != nil {
		return nil, err
	}
	var claimed []Mutation
	for rows.Next() {
		var m Mutation
		var payload string
		if err := rows.Scan(&m.ID, &m.RequestID, &m.Provider, &m.Operation, &m.Account, &m.Status, &m.AttemptCount, &payload); err != nil {
			rows.Close()
			return nil, err
		}
		m.Payload = decodeJSONMap(payload)
		claimed = append(claimed, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	requestIDs := map[string]bool{}
	for _, m := range claimed {
		if err := appendEvent(ctx, tx, m.ID, "claimed", "dagster", claimedBy, map[string]any{"attempt_count": m.AttemptCount}, now); err != nil {
			return nil, err
		}
		if m.RequestID != "" {
			requestIDs[m.RequestID] = true
		}
	}
	if err := s.refreshRequests(ctx, tx, requestIDs); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

// Complete implements Store.
func (s *PostgresStore) Complete(ctx context.Context, mutationID string, resultJSON map[string]any, actorID string) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	now := s.now()
	if resultJSON == nil {
		resultJSON = map[string]any{}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx, expand(`
		UPDATE @upstream_mutations
		   SET status = 'succeeded',
		       result_json = $1::jsonb,
		       error = '',
		       executed_at = $2,
		       updated_at = $3
		 WHERE id = $4
	`), jsonString(resultJSON), now, now, mutationID); err != nil {
		return err
	}
	if err := appendEvent(ctx, tx, mutationID, "executed", "dagster", actorID, resultJSON, now); err != nil {
		return err
	}
	if err := s.refreshRequestOf(ctx, tx, mutationID); err != nil {
		return err
	}
	return tx.Commit()
}

// Fail implements Store.
func (s *PostgresStore) Fail(ctx context.Context, mutationID string, status string, errorText string, resultJSON map[string]any, actorID string) error {
	switch status {
	case StatusFailedRetryable, StatusFailedTerminal, StatusBlockedMissingCredentials:
	default:
		return fmt.Errorf("unsupported failure status: %s", status)
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	now := s.now()
	if resultJSON == nil {
		resultJSON = map[string]any{}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx, expand(`
		UPDATE @upstream_mutations
		   SET status = $1,
		       error = $2,
		       result_json = $3::jsonb,
		       updated_at = $4
		 WHERE id = $5
	`), status, errorText, jsonString(resultJSON), now, mutationID); err != nil {
		return err
	}
	if err := appendEvent(ctx, tx, mutationID, "failed", "dagster", actorID, map[string]any{
		"status": status,
		"error":  errorText,
		"result": resultJSON,
	}, now); err != nil {
		return err
	}
	if err := s.refreshRequestOf(ctx, tx, mutationID); err != nil {
		return err
	}
	return tx.Commit()
}

func appendEvent(ctx context.Context, tx *sql.Tx, mutationID, eventType, actorType, actorID string, event map[string]any, now time.Time) error {
	_, err := tx.ExecContext(ctx, expand(`
		INSERT INTO @upstream_mutation_events (
			mutation_id, event_index, event_type, actor_type, actor_id, event_json, created_at
		)
		SELECT $1, COALESCE(max(event_index) + 1, 0), $2, $3, $4, $5::jsonb, $6
		FROM @upstream_mutation_events
		WHERE mutation_id = $7
	`), mutationID, eventType, actorType, actorID, jsonString(event), now, mutationID)
	return err
}

func (s *PostgresStore) refreshRequestOf(ctx context.Context, tx *sql.Tx, mutationID string) error {
	var requestID string
	err := tx.QueryRowContext(ctx, expand(`SELECT request_id FROM @upstream_mutations WHERE id = $1`), mutationID).Scan(&requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if requestID == "" {
		return nil
	}
	return s.refreshRequestStatus(ctx, tx, requestID)
}

func (s *PostgresStore) refreshRequests(ctx context.Context, tx *sql.Tx, requestIDs map[string]bool) error {
	ids := make([]string, 0, len(requestIDs))
	for id := range requestIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := s.refreshRequestStatus(ctx, tx, id); err != nil {
			return err
		}
	}
	return nil
}

// refreshRequestStatus is _refresh_upstream_mutation_request_status: the
// request's status is derived from its mutations' statuses in a fixed
// precedence, with the newest executed/observed stamps rolled up.
func (s *PostgresStore) refreshRequestStatus(ctx context.Context, tx *sql.Tx, requestID string) error {
	var requestStatus string
	var requestExecutedAt, requestObservedAt time.Time
	err := tx.QueryRowContext(ctx, expand(`SELECT status, executed_at, observed_at FROM @upstream_mutation_requests WHERE id = $1`), requestID).
		Scan(&requestStatus, &requestExecutedAt, &requestObservedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, expand(`
		SELECT id, status, executed_at, observed_at
		FROM @upstream_mutations
		WHERE request_id = $1
		ORDER BY request_index ASC, created_at ASC, id ASC
	`), requestID)
	if err != nil {
		return err
	}
	type row struct {
		id, status             string
		executedAt, observedAt time.Time
	}
	var mutationRows []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.status, &r.executedAt, &r.observedAt); err != nil {
			rows.Close()
			return err
		}
		mutationRows = append(mutationRows, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(mutationRows) == 0 {
		return nil
	}
	var active []string
	statuses := map[string]string{}
	executedAt, observedAt := requestExecutedAt, requestObservedAt
	for i, r := range mutationRows {
		statuses[r.id] = r.status
		if r.status != "rejected" {
			active = append(active, r.status)
		}
		if i == 0 || r.executedAt.After(executedAt) {
			executedAt = r.executedAt
		}
		if i == 0 || r.observedAt.After(observedAt) {
			observedAt = r.observedAt
		}
	}
	status := DeriveRequestStatus(active, requestStatus)
	now := s.now()
	_, err = tx.ExecContext(ctx, expand(`
		UPDATE @upstream_mutation_requests
		   SET status = $1,
		       result_json = $2::jsonb,
		       executed_at = CASE WHEN $3 > executed_at THEN $3 ELSE executed_at END,
		       observed_at = CASE WHEN $4 > observed_at THEN $4 ELSE observed_at END,
		       updated_at = $5
		 WHERE id = $6
	`), status, jsonString(map[string]any{"mutation_statuses": statuses}), executedAt, observedAt, now, requestID)
	return err
}

// DeriveRequestStatus is the precedence ladder the Python request refresh
// applies to the non-rejected mutation statuses of one request.
func DeriveRequestStatus(active []string, current string) string {
	if len(active) == 0 {
		return "rejected"
	}
	anyIs := func(want string) bool {
		for _, s := range active {
			if s == want {
				return true
			}
		}
		return false
	}
	allIn := func(want ...string) bool {
		for _, s := range active {
			ok := false
			for _, w := range want {
				if s == w {
					ok = true
					break
				}
			}
			if !ok {
				return false
			}
		}
		return true
	}
	switch {
	case anyIs("pending_review"):
		return "pending_review"
	case anyIs("executing"):
		return "executing"
	case anyIs("approved"):
		return "approved"
	case anyIs("failed_retryable"):
		return "failed_retryable"
	case anyIs("blocked_missing_credentials"):
		return "blocked_missing_credentials"
	case anyIs("failed_terminal"):
		return "failed_terminal"
	case allIn("observed"):
		return "observed"
	case allIn("succeeded", "observed"):
		return "succeeded"
	}
	return current
}

func jsonString(value map[string]any) string {
	if value == nil {
		return "{}"
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func decodeJSONMap(data string) map[string]any {
	out := map[string]any{}
	if strings.TrimSpace(data) == "" {
		return out
	}
	if err := json.Unmarshal([]byte(data), &out); err != nil {
		return map[string]any{}
	}
	return out
}

// AppleContactsMergedCardTarget is the card a deleted Apple Contacts card was
// merged into, per the mutation ledger ("" when nobody merged it). Only a
// SUCCEEDED apple_contacts.merge_contacts counts: a proposed or failed merge
// says nothing about where the card went. The newest one wins.
func (s *PostgresStore) AppleContactsMergedCardTarget(ctx context.Context, cardID string) (string, error) {
	cardID = strings.TrimSpace(cardID)
	if cardID == "" {
		return "", nil
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var target sql.NullString
	err := s.db.QueryRowContext(ctx, expand(`
		SELECT payload_json ->> 'keep_card_id' AS keep_card_id
		FROM @upstream_mutations
		WHERE provider = 'apple_contacts'
		  AND operation = 'apple_contacts.merge_contacts'
		  AND status = 'succeeded'
		  AND payload_json -> 'merge_card_ids' ? $1
		ORDER BY executed_at DESC, id DESC
		LIMIT 1
	`), cardID).Scan(&target)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(target.String), nil
}
