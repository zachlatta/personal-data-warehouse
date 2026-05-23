package mutations

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresStore struct {
	db      *sql.DB
	timeout time.Duration
}

type storedMutation struct {
	Provider  string
	Operation string
	Account   string
	Title     string
	Reason    string
	Payload   map[string]any
	Preview   map[string]any
}

func NewPostgresStore(databaseURL string, timeout time.Duration) (*PostgresStore, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &PostgresStore{db: db, timeout: timeout}, nil
}

func (s *PostgresStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *PostgresStore) EnsureTables(ctx context.Context) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	for _, statement := range upstreamMutationSchemaStatements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) CreateRequest(ctx context.Context, input CreateRequestInput) (Request, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.EnsureTables(ctx); err != nil {
		return Request{}, err
	}
	normalized, err := normalizeForStorage(input)
	if err != nil {
		return Request{}, err
	}
	idempotencyKey, err := requestIdempotencyKey(input, normalized)
	if err != nil {
		return Request{}, err
	}
	if existing, err := s.getRequestByIdempotencyKey(ctx, idempotencyKey); err != nil {
		return Request{}, err
	} else if existing.ID != "" {
		return existing, nil
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

	now := time.Now().UTC()
	requestID, err := randomID("req")
	if err != nil {
		return Request{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO upstream_mutation_requests (
			id, status, title, reason, context_json, result_json, idempotency_key,
			requested_by, created_at, updated_at
		)
		VALUES ($1, 'pending_review', $2, $3, $4::jsonb, '{}'::jsonb, $5, $6, $7, $8)
	`, requestID, input.Title, input.Reason, jsonString(input.Context), idempotencyKey, input.RequestedBy, now, now); err != nil {
		return Request{}, err
	}
	if err := appendRequestEvent(ctx, tx, requestID, "created", "agent", input.RequestedBy, map[string]any{
		"title":          input.Title,
		"reason":         input.Reason,
		"context":        input.Context,
		"mutation_count": len(normalized),
	}); err != nil {
		return Request{}, err
	}
	for index, mutation := range normalized {
		mutationID, err := randomID("mut")
		if err != nil {
			return Request{}, err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO upstream_mutations (
				id, request_id, request_index, provider, operation, account, status, title, reason,
				payload_json, preview_json, result_json, idempotency_key,
				requested_by, created_at, updated_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, 'pending_review', $7, $8, $9::jsonb, $10::jsonb, '{}'::jsonb, '', $11, $12, $13)
		`, mutationID, requestID, index, mutation.Provider, mutation.Operation, mutation.Account, mutation.Title, mutation.Reason, jsonString(mutation.Payload), jsonString(mutation.Preview), input.RequestedBy, now, now); err != nil {
			return Request{}, err
		}
		if err := appendMutationEvent(ctx, tx, mutationID, "created", "agent", input.RequestedBy, map[string]any{
			"request_id":    requestID,
			"request_index": index,
			"title":         mutation.Title,
			"reason":        mutation.Reason,
			"payload":       mutation.Payload,
			"preview":       mutation.Preview,
		}); err != nil {
			return Request{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Request{}, err
	}
	committed = true
	return s.GetRequest(ctx, requestID)
}

func (s *PostgresStore) ListRequests(ctx context.Context, filter RequestFilter) ([]Request, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.EnsureTables(ctx); err != nil {
		return nil, err
	}
	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	args := []any{}
	where := ""
	if len(filter.Statuses) > 0 {
		placeholders := make([]string, 0, len(filter.Statuses))
		for _, status := range filter.Statuses {
			args = append(args, status)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		where = "WHERE request.status IN (" + strings.Join(placeholders, ", ") + ")"
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT request.id, request.status, request.title, request.reason, request.context_json,
		       request.result_json, request.error, request.idempotency_key, request.revision,
		       request.requested_by, request.approved_by, request.created_at, request.updated_at,
		       request.approved_at, request.executed_at, request.observed_at,
		       count(mutation.id)::bigint AS mutation_count
		FROM upstream_mutation_requests AS request
		LEFT JOIN upstream_mutations AS mutation ON mutation.request_id = request.id
		%s
		GROUP BY request.id
		ORDER BY request.created_at DESC, request.id DESC
		LIMIT $%d
	`, where, len(args)), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	requests := []Request{}
	for rows.Next() {
		request, err := scanRequest(rows)
		if err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return requests, nil
}

func (s *PostgresStore) GetRequest(ctx context.Context, id string) (Request, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.EnsureTables(ctx); err != nil {
		return Request{}, err
	}
	request, err := s.getRequest(ctx, id)
	if err != nil {
		return Request{}, err
	}
	mutations, err := s.listMutationsForRequest(ctx, id)
	if err != nil {
		return Request{}, err
	}
	mutations, err = s.enrichGmailThreadPreviews(ctx, mutations)
	if err != nil {
		return Request{}, err
	}
	request.Mutations = mutations
	if request.MutationCount == 0 {
		request.MutationCount = len(mutations)
	}
	return request, nil
}

func (s *PostgresStore) ApproveRequest(ctx context.Context, id string, actor string) (Request, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.EnsureTables(ctx); err != nil {
		return Request{}, err
	}
	if actor == "" {
		actor = reviewerActorID
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
	if status != "pending_review" {
		return Request{}, fmt.Errorf("cannot approve request with status %s", status)
	}
	pendingIDs, err := pendingMutationIDsForUpdate(ctx, tx, id)
	if err != nil {
		return Request{}, err
	}
	if len(pendingIDs) == 0 {
		return Request{}, errors.New("cannot approve a request without pending mutations")
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		UPDATE upstream_mutation_requests
		   SET status = 'approved', approved_by = $1, approved_at = $2, updated_at = $3
		 WHERE id = $4
	`, actor, now, now, id); err != nil {
		return Request{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE upstream_mutations
		   SET status = 'approved', approved_by = $1, approved_at = $2, updated_at = $3
		 WHERE request_id = $4 AND status = 'pending_review'
	`, actor, now, now, id); err != nil {
		return Request{}, err
	}
	for _, mutationID := range pendingIDs {
		if err := appendMutationEvent(ctx, tx, mutationID, "approved", "human", actor, map[string]any{"request_id": id}); err != nil {
			return Request{}, err
		}
	}
	if err := appendRequestEvent(ctx, tx, id, "approved", "human", actor, map[string]any{"approved_mutation_ids": pendingIDs}); err != nil {
		return Request{}, err
	}
	if err := tx.Commit(); err != nil {
		return Request{}, err
	}
	committed = true
	return s.GetRequest(ctx, id)
}

func (s *PostgresStore) RejectRequest(ctx context.Context, id string, actor string, reason string) (Request, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.EnsureTables(ctx); err != nil {
		return Request{}, err
	}
	if actor == "" {
		actor = reviewerActorID
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
	if status != "pending_review" {
		return Request{}, fmt.Errorf("cannot reject request with status %s", status)
	}
	pendingIDs, err := pendingMutationIDsForUpdate(ctx, tx, id)
	if err != nil {
		return Request{}, err
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		UPDATE upstream_mutation_requests
		   SET status = 'rejected', error = $1, updated_at = $2
		 WHERE id = $3
	`, reason, now, id); err != nil {
		return Request{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE upstream_mutations
		   SET status = 'rejected', error = $1, updated_at = $2
		 WHERE request_id = $3 AND status = 'pending_review'
	`, reason, now, id); err != nil {
		return Request{}, err
	}
	for _, mutationID := range pendingIDs {
		if err := appendMutationEvent(ctx, tx, mutationID, "rejected", "human", actor, map[string]any{"request_id": id, "reason": reason}); err != nil {
			return Request{}, err
		}
	}
	if err := appendRequestEvent(ctx, tx, id, "rejected", "human", actor, map[string]any{"reason": reason}); err != nil {
		return Request{}, err
	}
	if err := tx.Commit(); err != nil {
		return Request{}, err
	}
	committed = true
	return s.GetRequest(ctx, id)
}

func (s *PostgresStore) getRequestByIdempotencyKey(ctx context.Context, key string) (Request, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM upstream_mutation_requests WHERE idempotency_key = $1`, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return Request{}, nil
	}
	if err != nil {
		return Request{}, err
	}
	return s.GetRequest(ctx, id)
}

func (s *PostgresStore) getRequest(ctx context.Context, id string) (Request, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT request.id, request.status, request.title, request.reason, request.context_json,
		       request.result_json, request.error, request.idempotency_key, request.revision,
		       request.requested_by, request.approved_by, request.created_at, request.updated_at,
		       request.approved_at, request.executed_at, request.observed_at,
		       count(mutation.id)::bigint AS mutation_count
		FROM upstream_mutation_requests AS request
		LEFT JOIN upstream_mutations AS mutation ON mutation.request_id = request.id
		WHERE request.id = $1
		GROUP BY request.id
	`, id)
	request, err := scanRequest(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Request{}, ErrNotFound
	}
	return request, err
}

func (s *PostgresStore) listMutationsForRequest(ctx context.Context, requestID string) ([]Mutation, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, request_id, request_index, provider, operation, account, status, title, reason,
		       payload_json, preview_json, result_json, error, idempotency_key, revision, attempt_count,
		       requested_by, approved_by, claimed_by, claimed_at, created_at, updated_at, approved_at,
		       executed_at, observed_at
		FROM upstream_mutations
		WHERE request_id = $1
		ORDER BY request_index ASC, created_at ASC, id ASC
	`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	mutations := []Mutation{}
	for rows.Next() {
		mutation, err := scanMutation(rows)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, mutation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return mutations, nil
}

func (s *PostgresStore) enrichGmailThreadPreviews(ctx context.Context, mutations []Mutation) ([]Mutation, error) {
	targets := gmailThreadPreviewTargets(mutations)
	if len(targets) == 0 {
		return mutations, nil
	}
	args := make([]any, 0, len(targets)*2)
	values := make([]string, 0, len(targets))
	for _, target := range targets {
		args = append(args, target.Account, target.ThreadID)
		values = append(values, fmt.Sprintf("($%d, $%d)", len(args)-1, len(args)))
	}

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		WITH wanted(account, thread_id) AS (
			VALUES %s
		)
		SELECT
			message.account,
			message.thread_id,
			message.message_id,
			message.subject,
			message.from_address,
			COALESCE(array_to_json(message.to_addresses)::text, '[]') AS to_addresses_json,
			COALESCE(array_to_json(message.cc_addresses)::text, '[]') AS cc_addresses_json,
			COALESCE(array_to_json(message.label_ids)::text, '[]') AS label_ids_json,
			message.internal_date,
			message.snippet,
			substring(
				COALESCE(
					NULLIF(message.body_markdown_clean, ''),
					NULLIF(message.body_markdown, ''),
					NULLIF(message.body_text, ''),
					message.snippet,
					''
				)
				from 1 for 1600
			) AS preview_text,
			count(*) OVER (PARTITION BY message.account, message.thread_id)::bigint AS message_count,
			count(*) FILTER (WHERE 'INBOX' = ANY(message.label_ids)) OVER (PARTITION BY message.account, message.thread_id)::bigint AS inbox_message_count
		FROM gmail_messages AS message
		JOIN wanted ON wanted.account = message.account AND wanted.thread_id = message.thread_id
		WHERE message.is_deleted = 0
		  AND NOT ('TRASH' = ANY(message.label_ids))
		  AND NOT ('SPAM' = ANY(message.label_ids))
		ORDER BY message.account ASC, message.thread_id ASC, message.internal_date ASC, message.message_id ASC
	`, strings.Join(values, ", ")), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	previewRows := []gmailThreadPreviewRow{}
	for rows.Next() {
		var row gmailThreadPreviewRow
		var toAddressesJSON, ccAddressesJSON, labelIDsJSON string
		var messageCount, inboxMessageCount int64
		if err := rows.Scan(
			&row.Account,
			&row.ThreadID,
			&row.MessageID,
			&row.Subject,
			&row.FromAddress,
			&toAddressesJSON,
			&ccAddressesJSON,
			&labelIDsJSON,
			&row.InternalDate,
			&row.Snippet,
			&row.PreviewText,
			&messageCount,
			&inboxMessageCount,
		); err != nil {
			return nil, err
		}
		row.ToAddresses = decodeJSONStringArray(toAddressesJSON)
		row.CCAddresses = decodeJSONStringArray(ccAddressesJSON)
		row.LabelIDs = decodeJSONStringArray(labelIDsJSON)
		row.MessageCount = int(messageCount)
		row.InboxMessageCount = int(inboxMessageCount)
		previewRows = append(previewRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return applyGmailThreadPreviewRows(mutations, previewRows), nil
}

func (s *PostgresStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, s.timeout)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanRequest(row scanner) (Request, error) {
	var request Request
	var contextJSON, resultJSON []byte
	var mutationCount int64
	err := row.Scan(
		&request.ID,
		&request.Status,
		&request.Title,
		&request.Reason,
		&contextJSON,
		&resultJSON,
		&request.Error,
		&request.IdempotencyKey,
		&request.Revision,
		&request.RequestedBy,
		&request.ApprovedBy,
		&request.CreatedAt,
		&request.UpdatedAt,
		&request.ApprovedAt,
		&request.ExecutedAt,
		&request.ObservedAt,
		&mutationCount,
	)
	if err != nil {
		return Request{}, err
	}
	request.Context = decodeJSONMap(contextJSON)
	request.Result = decodeJSONMap(resultJSON)
	request.MutationCount = int(mutationCount)
	return request, nil
}

func scanMutation(row scanner) (Mutation, error) {
	var mutation Mutation
	var payloadJSON, previewJSON, resultJSON []byte
	err := row.Scan(
		&mutation.ID,
		&mutation.RequestID,
		&mutation.RequestIndex,
		&mutation.Provider,
		&mutation.Operation,
		&mutation.Account,
		&mutation.Status,
		&mutation.Title,
		&mutation.Reason,
		&payloadJSON,
		&previewJSON,
		&resultJSON,
		&mutation.Error,
		&mutation.IdempotencyKey,
		&mutation.Revision,
		&mutation.AttemptCount,
		&mutation.RequestedBy,
		&mutation.ApprovedBy,
		&mutation.ClaimedBy,
		&mutation.ClaimedAt,
		&mutation.CreatedAt,
		&mutation.UpdatedAt,
		&mutation.ApprovedAt,
		&mutation.ExecutedAt,
		&mutation.ObservedAt,
	)
	if err != nil {
		return Mutation{}, err
	}
	mutation.Payload = decodeJSONMap(payloadJSON)
	mutation.Preview = decodeJSONMap(previewJSON)
	mutation.Result = decodeJSONMap(resultJSON)
	return mutation, nil
}

func normalizeForStorage(input CreateRequestInput) ([]storedMutation, error) {
	out := []storedMutation{}
	for index, mutation := range input.Mutations {
		account := normalizeAccount(mutation.Account)
		reason := optionalTitle(mutation.Reason, input.Reason)
		switch mutation.Type {
		case GmailArchiveOperation, GmailUnarchiveOperation:
			archive := mutation.Type == GmailArchiveOperation
			for _, threadID := range normalizeStringSlice(mutation.ThreadIDs) {
				titleVerb := "Archive"
				payload := map[string]any{"thread_ids": []string{threadID}, "remove_label_ids": []string{"INBOX"}}
				if !archive {
					titleVerb = "Unarchive"
					payload = map[string]any{"thread_ids": []string{threadID}, "add_label_ids": []string{"INBOX"}}
				}
				out = append(out, storedMutation{
					Provider:  "gmail",
					Operation: mutation.Type,
					Account:   account,
					Title:     optionalTitle(mutation.Title, titleVerb+": "+threadID),
					Reason:    reason,
					Payload:   payload,
					Preview: map[string]any{
						"thread_count": 1,
						"threads":      []map[string]any{{"thread_id": threadID}},
						"context":      input.Context,
					},
				})
			}
		case GmailSendEmailOperation:
			deliveryMode, err := normalizeDeliveryMode(mutation.DeliveryMode)
			if err != nil {
				return nil, err
			}
			message := normalizeMessageForStorage(mutation.Message)
			out = append(out, storedMutation{
				Provider:  "gmail",
				Operation: GmailSendEmailOperation,
				Account:   account,
				Title:     optionalTitle(mutation.Title, emailRequestTitle(message)),
				Reason:    reason,
				Payload: map[string]any{
					"delivery_mode": deliveryMode,
					"message":       message,
				},
				Preview: map[string]any{
					"email": map[string]any{
						"mode":          emailPreviewMode(message),
						"delivery_mode": deliveryMode,
						"to":            message["to"],
						"cc":            message["cc"],
						"bcc":           message["bcc"],
						"subject":       message["subject"],
						"body_text":     message["body_text"],
						"body_html":     message["body_html"],
					},
					"context": input.Context,
				},
			})
		case GooglePeopleContactsOperation, ContactsBatchMutationOperation:
			for operationIndex, operation := range mutation.Operations {
				op := cloneMap(operation)
				out = append(out, storedMutation{
					Provider:  "google_people",
					Operation: ContactsBatchMutationOperation,
					Account:   account,
					Title:     optionalTitle(mutation.Title, contactMutationTitle(op, operationIndex)),
					Reason:    reason,
					Payload:   map[string]any{"operations": []map[string]any{op}},
					Preview: map[string]any{
						"operation_count": 1,
						"operations":      []map[string]any{contactOperationPreview(op, operationIndex)},
						"context":         input.Context,
					},
				})
			}
		default:
			return nil, fmt.Errorf("mutation %d has unsupported type %q", index, mutation.Type)
		}
	}
	return out, nil
}

func normalizeMessageForStorage(message map[string]any) map[string]any {
	out := map[string]any{
		"to":        stringSliceFromAny(message["to"]),
		"cc":        stringSliceFromAny(message["cc"]),
		"bcc":       stringSliceFromAny(message["bcc"]),
		"subject":   strings.TrimSpace(stringFromAny(message["subject"])),
		"body_text": stringFromAny(message["body_text"]),
		"body_html": stringFromAny(message["body_html"]),
	}
	if replyToThreadID := strings.TrimSpace(stringFromAny(message["reply_to_thread_id"])); replyToThreadID != "" {
		out["reply_to_thread_id"] = replyToThreadID
	}
	if inReplyTo := strings.TrimSpace(stringFromAny(message["in_reply_to"])); inReplyTo != "" {
		out["in_reply_to"] = inReplyTo
	}
	if references := stringSliceFromAny(message["references"]); len(references) > 0 {
		out["references"] = references
	}
	return out
}

func emailPreviewMode(message map[string]any) string {
	if strings.TrimSpace(stringFromAny(message["reply_to_thread_id"])) != "" {
		return "reply"
	}
	return "new_thread"
}

func contactMutationTitle(operation map[string]any, index int) string {
	op := strings.TrimSpace(stringFromAny(operation["op"]))
	resource := strings.TrimSpace(stringFromAny(operation["resource_name"]))
	switch op {
	case "create_contact":
		return "Create contact"
	case "update_contact":
		if resource != "" {
			return "Update contact: " + resource
		}
		return "Update contact"
	case "delete_contact":
		if resource != "" {
			return "Delete contact: " + resource
		}
		return "Delete contact"
	default:
		return fmt.Sprintf("Contact mutation %d", index+1)
	}
}

func contactOperationPreview(operation map[string]any, index int) map[string]any {
	preview := cloneMap(operation)
	preview["op_index"] = index
	return preview
}

func requestIdempotencyKey(input CreateRequestInput, mutations []storedMutation) (string, error) {
	data, err := json.Marshal(map[string]any{
		"title":     input.Title,
		"reason":    input.Reason,
		"mutations": mutations,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func randomID(prefix string) (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(data[:]), nil
}

func requestStatusForUpdate(ctx context.Context, tx *sql.Tx, id string) (string, error) {
	var status string
	err := tx.QueryRowContext(ctx, `SELECT status FROM upstream_mutation_requests WHERE id = $1 FOR UPDATE`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return status, err
}

func pendingMutationIDsForUpdate(ctx context.Context, tx *sql.Tx, requestID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id
		FROM upstream_mutations
		WHERE request_id = $1 AND status = 'pending_review'
		ORDER BY request_index ASC, created_at ASC, id ASC
		FOR UPDATE
	`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func appendRequestEvent(ctx context.Context, tx *sql.Tx, requestID string, eventType string, actorType string, actorID string, event map[string]any) error {
	var index int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(event_index), -1) + 1 FROM upstream_mutation_request_events WHERE request_id = $1`, requestID).Scan(&index); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO upstream_mutation_request_events (request_id, event_index, event_type, actor_type, actor_id, event_json)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
	`, requestID, index, eventType, actorType, actorID, jsonString(event))
	return err
}

func appendMutationEvent(ctx context.Context, tx *sql.Tx, mutationID string, eventType string, actorType string, actorID string, event map[string]any) error {
	var index int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(event_index), -1) + 1 FROM upstream_mutation_events WHERE mutation_id = $1`, mutationID).Scan(&index); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO upstream_mutation_events (mutation_id, event_index, event_type, actor_type, actor_id, event_json)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
	`, mutationID, index, eventType, actorType, actorID, jsonString(event))
	return err
}

func jsonString(value map[string]any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func decodeJSONMap(data []byte) map[string]any {
	if len(data) == 0 {
		return map[string]any{}
	}
	out := map[string]any{}
	if err := json.Unmarshal(data, &out); err != nil {
		return map[string]any{}
	}
	return out
}

func decodeJSONStringArray(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	out := []string{}
	if err := json.Unmarshal([]byte(value), &out); err == nil {
		return normalizeStringSlice(out)
	}
	var anyValues []any
	if err := json.Unmarshal([]byte(value), &anyValues); err != nil {
		return nil
	}
	return stringSliceFromAny(anyValues)
}

var upstreamMutationSchemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS upstream_mutation_requests (
		id text PRIMARY KEY,
		status text NOT NULL DEFAULT 'pending_review',
		title text NOT NULL DEFAULT '',
		reason text NOT NULL DEFAULT '',
		context_json jsonb NOT NULL DEFAULT '{}'::jsonb,
		result_json jsonb NOT NULL DEFAULT '{}'::jsonb,
		error text NOT NULL DEFAULT '',
		idempotency_key text NOT NULL DEFAULT '',
		revision bigint NOT NULL DEFAULT 1,
		requested_by text NOT NULL DEFAULT '',
		approved_by text NOT NULL DEFAULT '',
		created_at timestamptz NOT NULL DEFAULT now(),
		updated_at timestamptz NOT NULL DEFAULT now(),
		approved_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		executed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		observed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz
	)`,
	`CREATE TABLE IF NOT EXISTS upstream_mutations (
		id text PRIMARY KEY,
		request_id text NOT NULL DEFAULT '',
		request_index bigint NOT NULL DEFAULT 0,
		provider text NOT NULL DEFAULT '',
		operation text NOT NULL DEFAULT '',
		account text NOT NULL DEFAULT '',
		status text NOT NULL DEFAULT 'pending_review',
		title text NOT NULL DEFAULT '',
		reason text NOT NULL DEFAULT '',
		payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
		preview_json jsonb NOT NULL DEFAULT '{}'::jsonb,
		result_json jsonb NOT NULL DEFAULT '{}'::jsonb,
		error text NOT NULL DEFAULT '',
		idempotency_key text NOT NULL DEFAULT '',
		revision bigint NOT NULL DEFAULT 1,
		attempt_count bigint NOT NULL DEFAULT 0,
		requested_by text NOT NULL DEFAULT '',
		approved_by text NOT NULL DEFAULT '',
		claimed_by text NOT NULL DEFAULT '',
		claimed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		created_at timestamptz NOT NULL DEFAULT now(),
		updated_at timestamptz NOT NULL DEFAULT now(),
		approved_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		executed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
		observed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz
	)`,
	`ALTER TABLE upstream_mutations ADD COLUMN IF NOT EXISTS request_id text NOT NULL DEFAULT ''`,
	`ALTER TABLE upstream_mutations ADD COLUMN IF NOT EXISTS request_index bigint NOT NULL DEFAULT 0`,
	`CREATE TABLE IF NOT EXISTS upstream_mutation_events (
		mutation_id text NOT NULL,
		event_index bigint NOT NULL,
		event_type text NOT NULL DEFAULT '',
		actor_type text NOT NULL DEFAULT '',
		actor_id text NOT NULL DEFAULT '',
		event_json jsonb NOT NULL DEFAULT '{}'::jsonb,
		created_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (mutation_id, event_index)
	)`,
	`CREATE TABLE IF NOT EXISTS upstream_mutation_request_events (
		request_id text NOT NULL,
		event_index bigint NOT NULL,
		event_type text NOT NULL DEFAULT '',
		actor_type text NOT NULL DEFAULT '',
		actor_id text NOT NULL DEFAULT '',
		event_json jsonb NOT NULL DEFAULT '{}'::jsonb,
		created_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (request_id, event_index)
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS upstream_mutation_requests_idempotency_idx ON upstream_mutation_requests (idempotency_key) WHERE idempotency_key != ''`,
	`CREATE INDEX IF NOT EXISTS upstream_mutation_requests_status_updated_idx ON upstream_mutation_requests (status, updated_at)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS upstream_mutations_idempotency_idx ON upstream_mutations (idempotency_key) WHERE idempotency_key != ''`,
	`CREATE INDEX IF NOT EXISTS upstream_mutations_request_idx ON upstream_mutations (request_id, request_index, created_at, id)`,
	`CREATE INDEX IF NOT EXISTS upstream_mutations_status_updated_idx ON upstream_mutations (status, updated_at)`,
	`CREATE INDEX IF NOT EXISTS upstream_mutation_request_events_request_idx ON upstream_mutation_request_events (request_id, event_index)`,
	`CREATE INDEX IF NOT EXISTS upstream_mutation_events_mutation_idx ON upstream_mutation_events (mutation_id, event_index)`,
}
