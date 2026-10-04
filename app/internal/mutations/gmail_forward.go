package mutations

import (
	"context"
	"errors"
	"fmt"
	"html"
	"strings"
	"time"
)

// A forward is a gmail.send_email whose message names forward_message_id,
// the way a reply names reply_to_thread_id: one email type, so variants,
// drafts, the reviewer's edit-before-approve and the signature all work on it
// unchanged. At proposal time the original is read from the warehouse and
// written into the body exactly as Gmail's own Forward composes it (note,
// signature, then the "Forwarded message" header block over the original),
// so the reviewer approves the words that go out. The original's files are
// not snapshotted — the executor re-reads the immutable original from Gmail
// and re-attaches them — and the preview lists them so the review says what
// will be sent.

// gmailForwardMarker opens Gmail's forwarded-message block, in both the HTML
// and the plain-text body. Its presence is what makes enrichment idempotent.
const gmailForwardMarker = "---------- Forwarded message ---------"

type gmailMessageKey struct {
	Account   string
	MessageID string
}

// gmailForwardRow is the original message a forward carries.
type gmailForwardRow struct {
	Account          string
	MessageID        string
	ThreadID         string
	RFC822MessageID  string
	ReferencesHeader string
	FromHeader       string
	ToHeader         string
	CCHeader         string
	Subject          string
	InternalDate     time.Time
	BodyHTML         string
	BodyText         string
	Attachments      []gmailForwardAttachment
}

type gmailForwardAttachment struct {
	Filename    string
	ContentType string
	Size        int64
}

func gmailForwardMessageID(message map[string]any) string {
	return strings.TrimSpace(stringFromAny(message["forward_message_id"]))
}

// validateGmailForward holds a forward to one meaning: an email is a reply or
// a forward, never both, and the variants of a forward are alternative
// wordings of forwarding one message, not of forwarding different ones.
func validateGmailForward(messages []map[string]any) error {
	forwardIDs := map[string]bool{}
	forwarding := 0
	for index, message := range messages {
		forwardID := gmailForwardMessageID(message)
		if forwardID == "" {
			continue
		}
		forwarding++
		forwardIDs[forwardID] = true
		if strings.TrimSpace(stringFromAny(message["reply_to_thread_id"])) != "" {
			return fmt.Errorf("Gmail email variant %d sets both reply_to_thread_id and forward_message_id; an email is a reply or a forward, not both", index+1)
		}
	}
	if len(forwardIDs) > 1 || (forwarding > 0 && forwarding < len(messages)) {
		return errors.New("every variant of a forward must forward the same forward_message_id")
	}
	return nil
}

func (s *PostgresStore) enrichGmailEmailForwards(ctx context.Context, mutations []storedMutation) ([]storedMutation, error) {
	targets := gmailForwardTargets(mutations)
	if len(targets) == 0 {
		return mutations, nil
	}
	if s == nil || s.db == nil {
		return nil, errors.New("cannot read the message to forward: the warehouse is not connected")
	}
	rows, err := s.gmailForwardRows(ctx, targets)
	if err != nil {
		return nil, fmt.Errorf("read the Gmail message to forward: %w", err)
	}
	return applyGmailForwardRows(mutations, rows)
}

func gmailForwardTargets(mutations []storedMutation) []gmailMessageKey {
	targets := []gmailMessageKey{}
	seen := map[gmailMessageKey]bool{}
	for _, mutation := range mutations {
		if mutation.Provider != "gmail" || mutation.Operation != GmailSendEmailOperation {
			continue
		}
		account := normalizeAccount(mutation.Account)
		for _, message := range gmailEmailPayloadMessages(mutation.Payload) {
			key := gmailMessageKey{Account: account, MessageID: gmailForwardMessageID(message)}
			if key.MessageID == "" || seen[key] {
				continue
			}
			targets = append(targets, key)
			seen[key] = true
		}
	}
	return targets
}

func gmailEmailPayloadMessages(payload map[string]any) []map[string]any {
	messages := []map[string]any{mapFromAny(payload["message"])}
	for _, variant := range normalizeStoredEmailVariants(payload["variants"]) {
		messages = append(messages, mapFromAny(variant["message"]))
	}
	return messages
}

func (s *PostgresStore) gmailForwardRows(ctx context.Context, targets []gmailMessageKey) ([]gmailForwardRow, error) {
	args := make([]any, 0, len(targets)*2)
	values := make([]string, 0, len(targets))
	for _, target := range targets {
		args = append(args, target.Account, target.MessageID)
		values = append(values, fmt.Sprintf("($%d, $%d)", len(args)-1, len(args)))
	}
	wanted := strings.Join(values, ", ")

	rows, err := queryContext(ctx, s.db, fmt.Sprintf(`
		WITH wanted(account, message_id) AS (
			VALUES %s
		)
		SELECT
			message.account,
			message.message_id,
			message.thread_id,
			COALESCE(message.rfc822_message_id, ''),
			COALESCE(message.subject, ''),
			COALESCE(message.from_address, ''),
			message.internal_date,
			COALESCE(message.body_html, ''),
			COALESCE(NULLIF(message.body_text, ''), NULLIF(message.body_markdown_full, ''), ''),
			-- The headers as the sender wrote them (display names included):
			-- the forwarded block quotes them, and References threads the forward.
			COALESCE((
				SELECT jsonb_object_agg(lower(header ->> 'name'), header ->> 'value')
				FROM jsonb_array_elements(COALESCE(message.payload_json::jsonb #> '{payload,headers}', '[]'::jsonb)) AS header
				WHERE lower(header ->> 'name') IN ('from', 'to', 'cc', 'references')
			), '{}'::jsonb)::text
		FROM @gmail_messages AS message
		JOIN wanted ON wanted.account = message.account AND wanted.message_id = message.message_id
		WHERE message.is_deleted = 0
	`, wanted), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []gmailForwardRow{}
	for rows.Next() {
		var row gmailForwardRow
		var fromAddress, headersJSON string
		if err := rows.Scan(&row.Account, &row.MessageID, &row.ThreadID, &row.RFC822MessageID, &row.Subject, &fromAddress,
			&row.InternalDate, &row.BodyHTML, &row.BodyText, &headersJSON); err != nil {
			return nil, err
		}
		headers := decodeJSONMap([]byte(headersJSON))
		row.FromHeader = strings.TrimSpace(stringFromAny(headers["from"]))
		if row.FromHeader == "" {
			row.FromHeader = fromAddress
		}
		row.ToHeader = stringFromAny(headers["to"])
		row.CCHeader = stringFromAny(headers["cc"])
		row.ReferencesHeader = stringFromAny(headers["references"])
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	attachments, err := queryContext(ctx, s.db, fmt.Sprintf(`
		WITH wanted(account, message_id) AS (
			VALUES %s
		)
		SELECT attachment.account, attachment.message_id, attachment.filename,
		       COALESCE(attachment.mime_type, ''), COALESCE(attachment.size, 0)
		FROM @gmail_attachments AS attachment
		JOIN wanted ON wanted.account = attachment.account AND wanted.message_id = attachment.message_id
		WHERE attachment.is_deleted = 0 AND attachment.filename <> ''
		ORDER BY attachment.account, attachment.message_id, attachment.part_id
	`, wanted), args...)
	if err != nil {
		return nil, err
	}
	defer attachments.Close()
	byKey := map[gmailMessageKey][]gmailForwardAttachment{}
	for attachments.Next() {
		var key gmailMessageKey
		var attachment gmailForwardAttachment
		if err := attachments.Scan(&key.Account, &key.MessageID, &attachment.Filename, &attachment.ContentType, &attachment.Size); err != nil {
			return nil, err
		}
		byKey[key] = append(byKey[key], attachment)
	}
	if err := attachments.Err(); err != nil {
		return nil, err
	}
	for index := range out {
		out[index].Attachments = byKey[gmailMessageKey{Account: out[index].Account, MessageID: out[index].MessageID}]
	}
	return out, nil
}

// applyGmailForwardRows writes each forwarded original into its message (and
// every variant's), and refuses a forward of a message the warehouse does not
// hold: an empty forward is a proposal the agent must fix, not a review.
func applyGmailForwardRows(mutations []storedMutation, rows []gmailForwardRow) ([]storedMutation, error) {
	rowsByKey := map[gmailMessageKey]gmailForwardRow{}
	for _, row := range rows {
		rowsByKey[gmailMessageKey{Account: normalizeAccount(row.Account), MessageID: strings.TrimSpace(row.MessageID)}] = row
	}
	out := make([]storedMutation, len(mutations))
	copy(out, mutations)
	for index := range out {
		mutation := &out[index]
		if mutation.Provider != "gmail" || mutation.Operation != GmailSendEmailOperation {
			continue
		}
		account := normalizeAccount(mutation.Account)
		payload := cloneMap(mutation.Payload)
		message := mapFromAny(payload["message"])
		forwardID := gmailForwardMessageID(message)
		if forwardID == "" {
			continue
		}
		row, ok := rowsByKey[gmailMessageKey{Account: account, MessageID: forwardID}]
		if !ok {
			return nil, invalidProposalInput(fmt.Errorf(
				"forward_message_id %q is not a message in base_gmail.messages for %s; forward the Gmail message_id of a synced message (a gmail search hit's source_pk.message_id)",
				forwardID, account))
		}
		deliveryMode := stringFromAny(payload["delivery_mode"])
		titleWasDefault := mutation.Title == gmailEmailRequestTitle(deliveryMode, message)

		message = gmailMessageWithForward(message, row)
		variants := normalizeStoredEmailVariants(payload["variants"])
		selectedVariantID := strings.TrimSpace(stringFromAny(payload["selected_variant_id"]))
		for variantIndex, variant := range variants {
			variantMessage := gmailMessageWithForward(mapFromAny(variant["message"]), row)
			variants[variantIndex]["message"] = variantMessage
			if stringFromAny(variant["id"]) == selectedVariantID {
				message = variantMessage
			}
		}
		if len(variants) > 0 {
			payload["variants"] = variants
		}
		payload["message"] = message
		mutation.Payload = payload
		if titleWasDefault {
			mutation.Title = gmailEmailRequestTitle(deliveryMode, message)
		}
		syncGmailEmailPreviewFromPayload(mutation, message, variants)
		mutation.Preview["forward"] = gmailForwardPreview(row)
	}
	return out, nil
}

func gmailMessageWithForward(message map[string]any, row gmailForwardRow) map[string]any {
	out := cloneMap(message)
	if strings.TrimSpace(stringFromAny(out["subject"])) == "" {
		out["subject"] = gmailForwardSubject(row.Subject)
	}
	bodyText := stringFromAny(out["body_text"])
	bodyHTML := strings.TrimSpace(stringFromAny(out["body_html"]))
	if !strings.Contains(bodyHTML, gmailForwardMarker) {
		if bodyHTML == "" {
			// A text-only edit may already carry the forwarded block as text;
			// the HTML gets Gmail's block from the original, not a text copy.
			note := bodyText
			if index := strings.Index(note, gmailForwardMarker); index >= 0 {
				note = note[:index]
			}
			bodyHTML = emailPlainTextToHTML(note)
		}
		if strings.TrimSpace(htmlFragmentText(bodyHTML)) == "" {
			// An empty line to type in above the forwarded block, as Gmail's
			// composer leaves; it also keeps the block splittable as the quote.
			bodyHTML = "<div><br></div>"
		}
		out["body_html"] = joinEmailHTML(bodyHTML, gmailForwardHTML(row))
	}
	if !strings.Contains(bodyText, gmailForwardMarker) {
		out["body_text"] = joinEmailText(bodyText, gmailForwardText(row))
	}
	if parent := strings.TrimSpace(row.RFC822MessageID); parent != "" {
		if strings.TrimSpace(stringFromAny(out["in_reply_to"])) == "" {
			out["in_reply_to"] = parent
		}
		references := stringSliceFromAny(out["references"])
		if len(references) == 0 {
			references = splitMessageIDHeader(row.ReferencesHeader)
		}
		references, _ = appendUniqueMessageID(references, parent)
		out["references"] = references
	}
	return out
}

func gmailForwardSubject(subject string) string {
	subject = strings.TrimSpace(subject)
	if strings.HasPrefix(strings.ToLower(subject), "fwd:") {
		return subject
	}
	return strings.TrimSpace("Fwd: " + subject)
}

type gmailForwardHeaderLine struct {
	Label string
	Value string
}

func gmailForwardHeaderLines(row gmailForwardRow) []gmailForwardHeaderLine {
	lines := []gmailForwardHeaderLine{{"From", strings.TrimSpace(row.FromHeader)}}
	if !row.InternalDate.IsZero() {
		lines = append(lines, gmailForwardHeaderLine{"Date", row.InternalDate.Format("Mon, Jan 2, 2006 at 3:04 PM")})
	}
	lines = append(lines, gmailForwardHeaderLine{"Subject", strings.TrimSpace(row.Subject)})
	if to := strings.TrimSpace(row.ToHeader); to != "" {
		lines = append(lines, gmailForwardHeaderLine{"To", to})
	}
	if cc := strings.TrimSpace(row.CCHeader); cc != "" {
		lines = append(lines, gmailForwardHeaderLine{"Cc", cc})
	}
	return lines
}

func gmailForwardHTML(row gmailForwardRow) string {
	body := strings.TrimSpace(htmlBodyFragment(row.BodyHTML))
	if body == "" {
		body = emailPlainTextToHTML(row.BodyText)
	}
	var out strings.Builder
	out.WriteString(`<div class="gmail_quote gmail_quote_container"><div dir="ltr" class="gmail_attr">`)
	out.WriteString(gmailForwardMarker)
	out.WriteString("<br>")
	for _, line := range gmailForwardHeaderLines(row) {
		out.WriteString(html.EscapeString(line.Label + ": " + line.Value))
		out.WriteString("<br>")
	}
	out.WriteString(`</div><br><br>`)
	out.WriteString(body)
	out.WriteString(`</div>`)
	return out.String()
}

func gmailForwardText(row gmailForwardRow) string {
	body := strings.TrimSpace(row.BodyText)
	if body == "" {
		body = htmlEmailText(row.BodyHTML)
	}
	lines := []string{gmailForwardMarker}
	for _, line := range gmailForwardHeaderLines(row) {
		lines = append(lines, line.Label+": "+line.Value)
	}
	return strings.Join(lines, "\n") + "\n\n" + body
}

// gmailForwardPreview is what the reviewer is told about the original: whose
// message it is and which files will go with it.
func gmailForwardPreview(row gmailForwardRow) map[string]any {
	attachments := make([]map[string]any, 0, len(row.Attachments))
	for _, attachment := range row.Attachments {
		attachments = append(attachments, map[string]any{
			"filename":     attachment.Filename,
			"content_type": attachment.ContentType,
			"size":         attachment.Size,
		})
	}
	preview := map[string]any{
		"message_id":  row.MessageID,
		"thread_id":   row.ThreadID,
		"from":        strings.TrimSpace(row.FromHeader),
		"to":          strings.TrimSpace(row.ToHeader),
		"cc":          strings.TrimSpace(row.CCHeader),
		"subject":     strings.TrimSpace(row.Subject),
		"attachments": attachments,
	}
	if !row.InternalDate.IsZero() {
		preview["date"] = row.InternalDate.UTC().Format(time.RFC3339)
	}
	return preview
}
