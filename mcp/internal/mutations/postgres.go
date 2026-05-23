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
	"html"
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

type gmailSignature struct {
	HTML string
	Text string
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
	normalized = s.enrichGmailEmailSignatures(ctx, normalized)
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

func (s *PostgresStore) UpdateGmailEmailMutation(ctx context.Context, requestID string, mutationID string, input UpdateGmailEmailMutationInput, actor string) (Mutation, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.EnsureTables(ctx); err != nil {
		return Mutation{}, err
	}
	if actor == "" {
		actor = reviewerActorID
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Mutation{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	requestStatus, err := requestStatusForUpdate(ctx, tx, requestID)
	if err != nil {
		return Mutation{}, err
	}
	if requestStatus != "pending_review" {
		return Mutation{}, fmt.Errorf("cannot edit mutation for request with status %s", requestStatus)
	}
	mutation, err := mutationForUpdate(ctx, tx, requestID, mutationID)
	if err != nil {
		return Mutation{}, err
	}
	if mutation.Status != "pending_review" {
		return Mutation{}, fmt.Errorf("cannot edit mutation with status %s", mutation.Status)
	}
	if mutation.Provider != "gmail" || mutation.Operation != GmailSendEmailOperation {
		return Mutation{}, fmt.Errorf("cannot edit mutation with operation %s", mutation.Operation)
	}
	payload, preview, title, err := updatedGmailEmailPayload(mutation, input)
	if err != nil {
		return Mutation{}, err
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		UPDATE upstream_mutations
		   SET title = $1,
		       payload_json = $2::jsonb,
		       preview_json = $3::jsonb,
		       revision = revision + 1,
		       updated_at = $4
		 WHERE id = $5
	`, title, jsonString(payload), jsonString(preview), now, mutationID); err != nil {
		return Mutation{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE upstream_mutation_requests
		   SET revision = revision + 1,
		       updated_at = $1
		 WHERE id = $2
	`, now, requestID); err != nil {
		return Mutation{}, err
	}
	message := mapFromAny(payload["message"])
	if err := appendMutationEvent(ctx, tx, mutationID, "edited", "human", actor, map[string]any{
		"request_id":    requestID,
		"delivery_mode": payload["delivery_mode"],
		"message":       message,
	}); err != nil {
		return Mutation{}, err
	}
	if err := appendRequestEvent(ctx, tx, requestID, "mutation_edited", "human", actor, map[string]any{
		"mutation_id":   mutationID,
		"delivery_mode": payload["delivery_mode"],
	}); err != nil {
		return Mutation{}, err
	}
	if err := tx.Commit(); err != nil {
		return Mutation{}, err
	}
	committed = true
	mutation.Title = title
	mutation.Payload = payload
	mutation.Preview = preview
	mutation.Revision++
	mutation.UpdatedAt = now
	return mutation, nil
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
			substring(COALESCE(message.body_html, '') from 1 for 200000) AS body_html,
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
			&row.BodyHTML,
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

func (s *PostgresStore) enrichGmailEmailSignatures(ctx context.Context, mutations []storedMutation) []storedMutation {
	if s == nil || s.db == nil {
		return mutations
	}
	signatures := map[string]gmailSignature{}
	for index := range mutations {
		mutation := mutations[index]
		if mutation.Provider != "gmail" || mutation.Operation != GmailSendEmailOperation {
			continue
		}
		message := mapFromAny(mutation.Payload["message"])
		if gmailEmailHasSignature(message) {
			continue
		}
		account := normalizeAccount(mutation.Account)
		if account == "" {
			continue
		}
		signature, ok := signatures[account]
		if !ok {
			var err error
			signature, err = s.latestGmailSignature(ctx, account)
			if err != nil {
				signature = gmailSignature{}
			}
			signatures[account] = signature
		}
		if signature.Empty() {
			continue
		}
		variants := normalizeStoredEmailVariants(mutation.Payload["variants"])
		if len(variants) > 0 {
			for variantIndex, variant := range variants {
				variantMessage := mapFromAny(variant["message"])
				if !gmailEmailHasSignature(variantMessage) {
					variantMessage = appendGmailSignatureToMessage(variantMessage, signature)
				}
				variants[variantIndex]["message"] = variantMessage
				if stringFromAny(variant["id"]) == stringFromAny(mutation.Payload["selected_variant_id"]) {
					message = variantMessage
				}
			}
			if stringFromAny(mutation.Payload["selected_variant_id"]) == "" {
				mutation.Payload["selected_variant_id"] = stringFromAny(variants[0]["id"])
				message = mapFromAny(variants[0]["message"])
			}
			mutations[index].Payload["variants"] = variants
		} else {
			message = appendGmailSignatureToMessage(message, signature)
		}
		mutations[index].Payload["message"] = message
		previewEmail := mapFromAny(mutations[index].Preview["email"])
		for key, value := range message {
			previewEmail[key] = value
		}
		previewEmail["delivery_mode"] = mutations[index].Payload["delivery_mode"]
		previewEmail["mode"] = emailPreviewMode(message)
		previewEmail["signature_source"] = "gmail_messages.sent"
		if len(variants) > 0 {
			previewEmail["variants"] = variants
			previewEmail["selected_variant_id"] = mutations[index].Payload["selected_variant_id"]
		}
		mutations[index].Preview["email"] = previewEmail
	}
	return mutations
}

func (s *PostgresStore) latestGmailSignature(ctx context.Context, account string) (gmailSignature, error) {
	var bodyHTML, bodyText string
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(body_html, ''), COALESCE(body_text, '')
		FROM gmail_messages
		WHERE account = $1
		  AND is_deleted = 0
		  AND 'SENT' = ANY(label_ids)
		  AND position('gmail_signature' in lower(COALESCE(body_html, ''))) > 0
		ORDER BY internal_date DESC
		LIMIT 1
	`, account).Scan(&bodyHTML, &bodyText)
	if errors.Is(err, sql.ErrNoRows) {
		return gmailSignature{}, nil
	}
	if err != nil {
		return gmailSignature{}, err
	}
	signature := gmailSignature{
		HTML: extractGmailSignatureHTML(bodyHTML),
		Text: extractGmailSignatureText(bodyText),
	}
	if signature.Text == "" && signature.HTML != "" {
		signature.Text = htmlFragmentText(signature.HTML)
	}
	return signature, nil
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
			variants, err := normalizeEmailVariantInputs(message, mutation.EmailVariants)
			if err != nil {
				return nil, err
			}
			selectedVariantID := ""
			if len(variants) > 0 {
				selectedVariantID = stringFromAny(variants[0]["id"])
				message = mapFromAny(variants[0]["message"])
			}
			payload := map[string]any{
				"delivery_mode": deliveryMode,
				"message":       message,
			}
			if len(variants) > 0 {
				payload["variants"] = variants
				payload["selected_variant_id"] = selectedVariantID
			}
			previewEmail := map[string]any{
				"mode":          emailPreviewMode(message),
				"delivery_mode": deliveryMode,
				"to":            message["to"],
				"cc":            message["cc"],
				"bcc":           message["bcc"],
				"subject":       message["subject"],
				"body_text":     message["body_text"],
				"body_html":     message["body_html"],
			}
			if len(variants) > 0 {
				previewEmail["variants"] = variants
				previewEmail["selected_variant_id"] = selectedVariantID
			}
			out = append(out, storedMutation{
				Provider:  "gmail",
				Operation: GmailSendEmailOperation,
				Account:   account,
				Title:     optionalTitle(mutation.Title, gmailEmailRequestTitle(deliveryMode, message)),
				Reason:    reason,
				Payload:   payload,
				Preview: map[string]any{
					"email":   previewEmail,
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

func normalizeEmailVariantInputs(baseMessage map[string]any, variants []GmailEmailVariantInput) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(variants))
	seenIDs := map[string]bool{}
	for index, variant := range variants {
		title, err := normalizeEmailVariantTitle(variant.Title)
		if err != nil {
			return nil, err
		}
		message := emailVariantMessage(baseMessage, variant)
		normalizedMessage := normalizeMessageForStorage(message)
		id := emailVariantID(index)
		if seenIDs[id] {
			return nil, fmt.Errorf("duplicate email variant id %q", id)
		}
		seenIDs[id] = true
		out = append(out, map[string]any{
			"id":      id,
			"title":   title,
			"message": normalizedMessage,
		})
	}
	return out, nil
}

func normalizeStoredEmailVariants(value any) []map[string]any {
	rawVariants := mapSliceFromAny(value)
	out := make([]map[string]any, 0, len(rawVariants))
	for index, raw := range rawVariants {
		title := strings.TrimSpace(stringFromAny(raw["title"]))
		if title == "" {
			title = fmt.Sprintf("Variant %d", index+1)
		}
		id := strings.TrimSpace(stringFromAny(raw["id"]))
		if id == "" {
			id = emailVariantID(index)
		}
		message := normalizeMessageForStorage(mapFromAny(raw["message"]))
		out = append(out, map[string]any{
			"id":      id,
			"title":   title,
			"message": message,
		})
	}
	return out
}

func emailVariantMessage(baseMessage map[string]any, variant GmailEmailVariantInput) map[string]any {
	message := cloneMap(baseMessage)
	for key, value := range variant.Message {
		message[key] = value
	}
	if values := normalizeStringSlice(variant.To); len(values) > 0 {
		message["to"] = values
	}
	if values := normalizeStringSlice(variant.CC); len(values) > 0 {
		message["cc"] = values
	}
	if values := normalizeStringSlice(variant.BCC); len(values) > 0 {
		message["bcc"] = values
	}
	if subject := strings.TrimSpace(variant.Subject); subject != "" {
		message["subject"] = subject
	}
	if variant.BodyText != "" {
		message["body_text"] = variant.BodyText
	}
	if variant.BodyHTML != "" {
		message["body_html"] = variant.BodyHTML
	}
	if replyToThreadID := strings.TrimSpace(variant.ReplyToThreadID); replyToThreadID != "" {
		message["reply_to_thread_id"] = replyToThreadID
	}
	if inReplyTo := strings.TrimSpace(variant.InReplyTo); inReplyTo != "" {
		message["in_reply_to"] = inReplyTo
	}
	if references := normalizeStringSlice(variant.References); len(references) > 0 {
		message["references"] = references
	}
	return message
}

func normalizeEmailVariantTitle(value string) (string, error) {
	words := strings.Fields(value)
	if len(words) != 2 {
		return "", errors.New("Gmail email variant title must be exactly two words")
	}
	title := strings.Join(words, " ")
	if len([]rune(title)) > 32 {
		return "", errors.New("Gmail email variant title must be 32 characters or fewer")
	}
	return title, nil
}

func emailVariantID(index int) string {
	return fmt.Sprintf("variant_%d", index+1)
}

func updatedGmailEmailPayload(mutation Mutation, input UpdateGmailEmailMutationInput) (map[string]any, map[string]any, string, error) {
	deliveryMode, err := normalizeDeliveryMode(input.DeliveryMode)
	if err != nil {
		return nil, nil, "", err
	}
	existingPayload := cloneMap(mutation.Payload)
	variants := normalizeStoredEmailVariants(existingPayload["variants"])
	selectedVariantID := strings.TrimSpace(input.SelectedVariantID)
	if selectedVariantID == "" {
		selectedVariantID = strings.TrimSpace(stringFromAny(existingPayload["selected_variant_id"]))
	}
	if selectedVariantID == "" && len(variants) > 0 {
		selectedVariantID = stringFromAny(variants[0]["id"])
	}
	mergedMessage := mapFromAny(existingPayload["message"])
	selectedVariantIndex := -1
	for index, variant := range variants {
		if stringFromAny(variant["id"]) == selectedVariantID {
			mergedMessage = mapFromAny(variant["message"])
			selectedVariantIndex = index
			break
		}
	}
	for key, value := range input.Message {
		mergedMessage[key] = value
	}
	message := normalizeMessageForStorage(mergedMessage)
	if !hasAnyRecipient(message) {
		return nil, nil, "", errors.New("Gmail email mutation must include at least one recipient")
	}
	if strings.TrimSpace(stringFromAny(message["subject"])) == "" {
		return nil, nil, "", errors.New("Gmail email mutation must include subject")
	}
	if strings.TrimSpace(stringFromAny(message["body_text"])) == "" && strings.TrimSpace(stringFromAny(message["body_html"])) == "" {
		return nil, nil, "", errors.New("Gmail email mutation must include body_text or body_html")
	}

	payload := existingPayload
	payload["delivery_mode"] = deliveryMode
	payload["message"] = message
	if len(variants) > 0 {
		if selectedVariantIndex < 0 {
			return nil, nil, "", fmt.Errorf("unknown Gmail email variant %q", selectedVariantID)
		}
		variants[selectedVariantIndex]["message"] = message
		payload["variants"] = variants
		payload["selected_variant_id"] = selectedVariantID
	}

	preview := cloneMap(mutation.Preview)
	previewEmail := cloneMap(message)
	previewEmail["mode"] = emailPreviewMode(message)
	previewEmail["delivery_mode"] = deliveryMode
	if len(variants) > 0 {
		previewEmail["variants"] = variants
		previewEmail["selected_variant_id"] = selectedVariantID
	}
	preview["email"] = previewEmail

	return payload, preview, gmailEmailRequestTitle(deliveryMode, message), nil
}

func (signature gmailSignature) Empty() bool {
	return strings.TrimSpace(signature.HTML) == "" && strings.TrimSpace(signature.Text) == ""
}

func gmailEmailHasSignature(message map[string]any) bool {
	bodyHTML := strings.ToLower(stringFromAny(message["body_html"]))
	bodyText := strings.ReplaceAll(stringFromAny(message["body_text"]), "\r\n", "\n")
	return strings.Contains(bodyHTML, "gmail_signature") ||
		strings.Contains(bodyHTML, "zach@hackclub.com") && strings.Contains(bodyHTML, "hackclub.com/donate") ||
		strings.Contains(bodyText, "\n--\n") ||
		strings.HasPrefix(strings.TrimSpace(bodyText), "--\n")
}

func appendGmailSignatureToMessage(message map[string]any, signature gmailSignature) map[string]any {
	out := cloneMap(message)
	bodyText := stringFromAny(out["body_text"])
	bodyHTML := strings.TrimSpace(stringFromAny(out["body_html"]))
	signatureHTML := strings.TrimSpace(signature.HTML)
	signatureText := strings.TrimSpace(signature.Text)
	if signatureText == "" && signatureHTML != "" {
		signatureText = htmlFragmentText(signatureHTML)
	}
	if bodyHTML == "" && signatureHTML != "" {
		bodyHTML = emailPlainTextToHTML(bodyText)
	}
	if signatureHTML != "" {
		out["body_html"] = joinEmailHTML(bodyHTML, signatureHTML)
	}
	if signatureText != "" {
		out["body_text"] = joinEmailText(bodyText, signatureText)
	}
	return out
}

func joinEmailHTML(bodyHTML string, signatureHTML string) string {
	bodyHTML = strings.TrimSpace(bodyHTML)
	signatureHTML = strings.TrimSpace(signatureHTML)
	if bodyHTML == "" {
		return signatureHTML
	}
	if signatureHTML == "" {
		return bodyHTML
	}
	return bodyHTML + `<div><br></div>` + signatureHTML
}

func joinEmailText(bodyText string, signatureText string) string {
	bodyText = strings.TrimRight(strings.ReplaceAll(bodyText, "\r\n", "\n"), "\n")
	signatureText = strings.TrimSpace(strings.ReplaceAll(signatureText, "\r\n", "\n"))
	if bodyText == "" {
		return signatureText + "\n"
	}
	if signatureText == "" {
		return bodyText + "\n"
	}
	return bodyText + "\n\n" + signatureText + "\n"
}

func emailPlainTextToHTML(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "<div><br></div>"
	}
	paragraphs := strings.Split(strings.ReplaceAll(value, "\r\n", "\n"), "\n\n")
	out := strings.Builder{}
	for paragraphIndex, paragraph := range paragraphs {
		lines := normalizeStringSlice(strings.Split(paragraph, "\n"))
		if len(lines) == 0 {
			continue
		}
		if out.Len() > 0 && paragraphIndex > 0 {
			out.WriteString("<div><br></div>")
		}
		out.WriteString("<div>")
		for index, line := range lines {
			if index > 0 {
				out.WriteString("<br>")
			}
			out.WriteString(html.EscapeString(line))
		}
		out.WriteString("</div>")
	}
	if out.Len() == 0 {
		return "<div><br></div>"
	}
	return out.String()
}

func extractGmailSignatureHTML(bodyHTML string) string {
	lower := strings.ToLower(bodyHTML)
	marker := strings.Index(lower, "gmail_signature")
	if marker < 0 {
		return ""
	}
	start := strings.LastIndex(lower[:marker], "<div")
	if start < 0 {
		return ""
	}
	end := matchingDivEnd(lower, start)
	if end <= start || end > len(bodyHTML) {
		return ""
	}
	return strings.TrimSpace(bodyHTML[start:end])
}

func matchingDivEnd(lowerHTML string, start int) int {
	depth := 0
	cursor := start
	for cursor < len(lowerHTML) {
		nextOpen := strings.Index(lowerHTML[cursor:], "<div")
		nextClose := strings.Index(lowerHTML[cursor:], "</div")
		if nextOpen < 0 && nextClose < 0 {
			return -1
		}
		if nextOpen >= 0 && (nextClose < 0 || nextOpen < nextClose) {
			depth++
			cursor += nextOpen + len("<div")
			continue
		}
		depth--
		cursor += nextClose
		tagEnd := strings.Index(lowerHTML[cursor:], ">")
		if tagEnd < 0 {
			return -1
		}
		cursor += tagEnd + 1
		if depth == 0 {
			return cursor
		}
	}
	return -1
}

func extractGmailSignatureText(bodyText string) string {
	bodyText = strings.ReplaceAll(bodyText, "\r\n", "\n")
	for _, delimiter := range []string{"\n--\n", "\n-- \n"} {
		index := strings.Index(bodyText, delimiter)
		if index < 0 {
			continue
		}
		signature := bodyText[index+1:]
		if quoteIndex := strings.Index(signature, "\nOn "); quoteIndex >= 0 && strings.Contains(signature[quoteIndex:], " wrote:") {
			signature = signature[:quoteIndex]
		}
		return strings.TrimSpace(signature)
	}
	return ""
}

func htmlFragmentText(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	lower := strings.ToLower(value)
	out := strings.Builder{}
	for index := 0; index < len(value); {
		if value[index] != '<' {
			out.WriteByte(value[index])
			index++
			continue
		}
		end := strings.IndexByte(value[index:], '>')
		if end < 0 {
			break
		}
		tag := lower[index : index+end+1]
		if strings.HasPrefix(tag, "<br") || strings.HasPrefix(tag, "</div") || strings.HasPrefix(tag, "</p") {
			out.WriteByte('\n')
		}
		index += end + 1
	}
	lines := normalizeStringSlice(strings.Split(html.UnescapeString(out.String()), "\n"))
	return strings.Join(lines, "\n")
}

func emailPreviewMode(message map[string]any) string {
	if strings.TrimSpace(stringFromAny(message["reply_to_thread_id"])) != "" {
		return "reply"
	}
	return "new_thread"
}

func gmailEmailRequestTitle(deliveryMode string, message map[string]any) string {
	if deliveryMode == "draft" {
		subject := strings.TrimSpace(stringFromAny(message["subject"]))
		if subject == "" {
			return "Create draft"
		}
		return "Create draft: " + subject
	}
	return emailRequestTitle(message)
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

func mutationForUpdate(ctx context.Context, tx *sql.Tx, requestID string, mutationID string) (Mutation, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT id, request_id, request_index, provider, operation, account, status, title, reason,
		       payload_json, preview_json, result_json, error, idempotency_key, revision, attempt_count,
		       requested_by, approved_by, claimed_by, claimed_at, created_at, updated_at, approved_at,
		       executed_at, observed_at
		FROM upstream_mutations
		WHERE request_id = $1 AND id = $2
		FOR UPDATE
	`, requestID, mutationID)
	mutation, err := scanMutation(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Mutation{}, ErrNotFound
	}
	return mutation, err
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
