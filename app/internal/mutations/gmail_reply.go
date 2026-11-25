package mutations

import (
	"context"
	"errors"
	"fmt"
	"html"
	"strings"
	"time"
)

// A reply is a gmail.send_email whose message names its parent:
// reply_to_message_id (that message, a gmail search hit's
// source_pk.message_id) or reply_to_thread_id alone (the thread's latest
// message). At proposal time the parent is read from the warehouse and
// everything that threads the reply is derived from it: the Gmail thread id
// the executor files it under, In-Reply-To, References, and the quoted
// history below the signature. The parent is then pinned as
// reply_to_message_id, so a reviewer edit re-derives the same headers even
// after newer mail lands in the thread.
//
// Agents may not write In-Reply-To or References themselves. On 2026-11-24
// three approved replies did, without a thread id, to avoid quoting a private
// aside that was the thread's latest message; Gmail filed each as a new
// conversation. reply_to_message_id is what they needed.

// gmailReplyParentRow is the message a reply answers.
type gmailReplyParentRow struct {
	Account          string
	MessageID        string
	ThreadID         string
	RFC822MessageID  string
	ReferencesHeader string
	InReplyToHeader  string
	FromAddress      string
	InternalDate     time.Time
	BodyHTML         string
	BodyText         string
}

func gmailReplyToMessageID(message map[string]any) string {
	return strings.TrimSpace(stringFromAny(message["reply_to_message_id"]))
}

func gmailReplyToThreadID(message map[string]any) string {
	return strings.TrimSpace(stringFromAny(message["reply_to_thread_id"]))
}

func gmailMessageIsReply(message map[string]any) bool {
	return gmailReplyToMessageID(message) != "" || gmailReplyToThreadID(message) != ""
}

// refuseHandBuiltReplyHeaders turns away the headers PDW derives, naming the
// field that replaces them.
func refuseHandBuiltReplyHeaders(message map[string]any) error {
	for _, field := range []string{"in_reply_to", "references"} {
		value, ok := message[field]
		if !ok {
			continue
		}
		if strings.TrimSpace(stringFromAny(value)) == "" && len(stringSliceFromAny(value)) == 0 {
			continue
		}
		return fmt.Errorf("sets %s; PDW derives In-Reply-To, References and the Gmail thread from the message replied to: set reply_to_message_id to that message's Gmail message_id (a gmail search hit's source_pk.message_id), or reply_to_thread_id to reply to the thread's latest message", field)
	}
	return nil
}

type gmailReplyTarget struct {
	Account   string
	ThreadID  string
	MessageID string
}

func gmailReplyTargets(mutations []storedMutation) []gmailReplyTarget {
	targets := []gmailReplyTarget{}
	seen := map[gmailReplyTarget]bool{}
	for _, mutation := range mutations {
		if mutation.Provider != "gmail" || mutation.Operation != GmailSendEmailOperation {
			continue
		}
		account := normalizeAccount(mutation.Account)
		for _, message := range gmailEmailPayloadMessages(mutation.Payload) {
			target := gmailReplyTarget{Account: account, MessageID: gmailReplyToMessageID(message)}
			if target.MessageID == "" {
				target.ThreadID = gmailReplyToThreadID(message)
			}
			if target.MessageID == "" && target.ThreadID == "" || seen[target] {
				continue
			}
			targets = append(targets, target)
			seen[target] = true
		}
	}
	return targets
}

func (s *PostgresStore) enrichGmailEmailReplies(ctx context.Context, mutations []storedMutation) ([]storedMutation, error) {
	targets := gmailReplyTargets(mutations)
	if len(targets) == 0 {
		return mutations, nil
	}
	if s == nil || s.db == nil {
		return nil, errors.New("cannot read the message to reply to: the warehouse is not connected")
	}
	rows, err := s.gmailReplyParentRows(ctx, targets)
	if err != nil {
		return nil, fmt.Errorf("read the Gmail message to reply to: %w", err)
	}
	return applyGmailReplyParentRows(mutations, rows)
}

// gmailReplyParentRows reads each named message by its primary key, and each
// thread's latest message through (account, thread_id, internal_date DESC).
func (s *PostgresStore) gmailReplyParentRows(ctx context.Context, targets []gmailReplyTarget) ([]gmailReplyParentRow, error) {
	messageArgs, threadArgs := []any{}, []any{}
	messageValues, threadValues := []string{}, []string{}
	for _, target := range targets {
		if target.MessageID != "" {
			messageArgs = append(messageArgs, target.Account, target.MessageID)
			messageValues = append(messageValues, fmt.Sprintf("($%d, $%d)", len(messageArgs)-1, len(messageArgs)))
			continue
		}
		threadArgs = append(threadArgs, target.Account, target.ThreadID)
		threadValues = append(threadValues, fmt.Sprintf("($%d, $%d)", len(threadArgs)-1, len(threadArgs)))
	}
	const columns = `
			message.account,
			message.message_id,
			message.thread_id,
			message.rfc822_message_id,
			COALESCE((
				SELECT header ->> 'value'
				FROM jsonb_array_elements(COALESCE(message.payload_json::jsonb #> '{payload,headers}', '[]'::jsonb)) AS header
				WHERE lower(header ->> 'name') = 'references'
				LIMIT 1
			), '') AS references_header,
			COALESCE((
				SELECT header ->> 'value'
				FROM jsonb_array_elements(COALESCE(message.payload_json::jsonb #> '{payload,headers}', '[]'::jsonb)) AS header
				WHERE lower(header ->> 'name') = 'in-reply-to'
				LIMIT 1
			), '') AS in_reply_to_header,
			COALESCE(message.from_address, '') AS from_address,
			message.internal_date,
			COALESCE(message.body_html, '') AS body_html,
			COALESCE(message.body_text, '') AS body_text`
	// A reply needs the parent's Message-ID; a draft, a trashed or a spam
	// message is never what a reply answers.
	const answerable = `
			message.is_deleted = 0
			AND COALESCE(message.rfc822_message_id, '') != ''
			AND NOT ('DRAFT' = ANY(message.label_ids))`
	out := []gmailReplyParentRow{}
	if len(messageValues) > 0 {
		rows, err := s.scanGmailReplyParentRows(ctx, fmt.Sprintf(`
			WITH wanted(account, message_id) AS (VALUES %s)
			SELECT %s
			FROM @gmail_messages AS message
			JOIN wanted ON wanted.account = message.account AND wanted.message_id = message.message_id
			WHERE %s
		`, strings.Join(messageValues, ", "), columns, answerable), messageArgs)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	if len(threadValues) > 0 {
		rows, err := s.scanGmailReplyParentRows(ctx, fmt.Sprintf(`
			WITH wanted(account, thread_id) AS (VALUES %s)
			SELECT latest.*
			FROM wanted
			CROSS JOIN LATERAL (
				SELECT %s
				FROM @gmail_messages AS message
				WHERE message.account = wanted.account
				  AND message.thread_id = wanted.thread_id
				  AND %s
				  AND NOT ('TRASH' = ANY(message.label_ids))
				  AND NOT ('SPAM' = ANY(message.label_ids))
				ORDER BY message.internal_date DESC, message.message_id DESC
				LIMIT 1
			) AS latest
		`, strings.Join(threadValues, ", "), columns, answerable), threadArgs)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

func (s *PostgresStore) scanGmailReplyParentRows(ctx context.Context, query string, args []any) ([]gmailReplyParentRow, error) {
	rows, err := queryContext(ctx, s.db, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []gmailReplyParentRow{}
	for rows.Next() {
		var row gmailReplyParentRow
		if err := rows.Scan(&row.Account, &row.MessageID, &row.ThreadID, &row.RFC822MessageID, &row.ReferencesHeader,
			&row.InReplyToHeader, &row.FromAddress, &row.InternalDate, &row.BodyHTML, &row.BodyText); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// applyGmailReplyParentRows threads and quotes every reply (and every
// variant) from its parent, and refuses a reply whose parent the warehouse
// does not hold: Gmail would file it as a new conversation.
func applyGmailReplyParentRows(mutations []storedMutation, rows []gmailReplyParentRow) ([]storedMutation, error) {
	byMessage := map[gmailMessageKey]gmailReplyParentRow{}
	latestByThread := map[gmailThreadPreviewKey]gmailReplyParentRow{}
	for _, row := range rows {
		account := normalizeAccount(row.Account)
		byMessage[gmailMessageKey{Account: account, MessageID: strings.TrimSpace(row.MessageID)}] = row
		key := gmailThreadPreviewKey{Account: account, ThreadID: strings.TrimSpace(row.ThreadID)}
		if latest, ok := latestByThread[key]; !ok || row.InternalDate.After(latest.InternalDate) ||
			(row.InternalDate.Equal(latest.InternalDate) && row.MessageID > latest.MessageID) {
			latestByThread[key] = row
		}
	}
	parentFor := func(account string, message map[string]any) (gmailReplyParentRow, error) {
		threadID := gmailReplyToThreadID(message)
		if messageID := gmailReplyToMessageID(message); messageID != "" {
			row, ok := byMessage[gmailMessageKey{Account: account, MessageID: messageID}]
			if !ok {
				return row, invalidProposalInput(fmt.Errorf(
					"reply_to_message_id %q is not a message in base_gmail.messages for %s; reply to the Gmail message_id of a synced message (a gmail search hit's source_pk.message_id)",
					messageID, account))
			}
			if threadID != "" && threadID != row.ThreadID {
				return row, invalidProposalInput(fmt.Errorf(
					"reply_to_message_id %q is in Gmail thread %s, not reply_to_thread_id %q; drop reply_to_thread_id, the message names its thread",
					messageID, row.ThreadID, threadID))
			}
			return row, nil
		}
		row, ok := latestByThread[gmailThreadPreviewKey{Account: account, ThreadID: threadID}]
		if !ok {
			return row, invalidProposalInput(fmt.Errorf(
				"reply_to_thread_id %q has no synced message in base_gmail.messages for %s to reply to; use a synced thread_id, or reply_to_message_id",
				threadID, account))
		}
		return row, nil
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
		variants := normalizeStoredEmailVariants(payload["variants"])
		changed := false
		reply := func(message map[string]any) (map[string]any, error) {
			if !gmailMessageIsReply(message) {
				return message, nil
			}
			row, err := parentFor(account, message)
			if err != nil {
				return nil, err
			}
			changed = true
			return gmailMessageWithReplyParent(message, row), nil
		}

		var err error
		if len(variants) > 0 {
			selectedVariantID := strings.TrimSpace(stringFromAny(payload["selected_variant_id"]))
			if selectedVariantID == "" {
				selectedVariantID = stringFromAny(variants[0]["id"])
			}
			for variantIndex, variant := range variants {
				variantMessage, err := reply(mapFromAny(variant["message"]))
				if err != nil {
					return nil, err
				}
				variants[variantIndex]["message"] = variantMessage
				if stringFromAny(variant["id"]) == selectedVariantID {
					message = variantMessage
				}
			}
			if changed {
				payload["selected_variant_id"] = selectedVariantID
				payload["variants"] = variants
			}
		} else if message, err = reply(message); err != nil {
			return nil, err
		}
		if !changed {
			continue
		}
		payload["message"] = message
		mutation.Payload = payload
		syncGmailEmailPreviewFromPayload(mutation, message, variants)
	}
	return out, nil
}

// gmailMessageWithReplyParent pins the parent and derives the thread, the
// headers and (once) the quote from it.
func gmailMessageWithReplyParent(message map[string]any, row gmailReplyParentRow) map[string]any {
	out := cloneMap(message)
	out["reply_to_message_id"] = strings.TrimSpace(row.MessageID)
	out["reply_to_thread_id"] = strings.TrimSpace(row.ThreadID)
	parent := strings.TrimSpace(row.RFC822MessageID)
	out["in_reply_to"] = parent
	references := splitMessageIDHeader(row.ReferencesHeader)
	if len(references) == 0 {
		references = splitMessageIDHeader(row.InReplyToHeader)
	}
	references, _ = appendUniqueMessageID(references, parent)
	out["references"] = references
	if quoted, ok := gmailMessageWithReplyQuote(out, row); ok {
		out = quoted
	}
	return out
}

func gmailEmailHasReplyQuote(message map[string]any) bool {
	bodyHTML := strings.ToLower(stringFromAny(message["body_html"]))
	if strings.Contains(bodyHTML, "gmail_quote") {
		return true
	}
	bodyText := strings.ReplaceAll(stringFromAny(message["body_text"]), "\r\n", "\n")
	return strings.Contains(bodyText, "\nOn ") && strings.Contains(bodyText, " wrote:")
}

func gmailMessageWithReplyQuote(message map[string]any, row gmailReplyParentRow) (map[string]any, bool) {
	if gmailEmailHasReplyQuote(message) {
		return message, false
	}
	quoteHTML := gmailReplyQuoteHTML(row)
	quoteText := gmailReplyQuoteText(row)
	if quoteHTML == "" && quoteText == "" {
		return message, false
	}
	out := cloneMap(message)
	bodyText := stringFromAny(out["body_text"])
	bodyHTML := strings.TrimSpace(stringFromAny(out["body_html"]))
	if bodyHTML == "" && bodyText != "" {
		bodyHTML = emailPlainTextToHTML(bodyText)
	}
	if quoteHTML != "" {
		out["body_html"] = joinEmailHTML(bodyHTML, quoteHTML)
	}
	if quoteText != "" {
		out["body_text"] = joinEmailText(bodyText, quoteText)
	}
	return out, true
}

func gmailReplyQuoteHTML(row gmailReplyParentRow) string {
	bodyHTML := strings.TrimSpace(htmlBodyFragment(row.BodyHTML))
	if bodyHTML == "" {
		bodyHTML = emailPlainTextToHTML(row.BodyText)
	}
	if strings.TrimSpace(htmlFragmentText(bodyHTML)) == "" && !strings.Contains(strings.ToLower(bodyHTML), "<img") {
		return ""
	}
	return `<div class="gmail_quote gmail_quote_container"><div dir="ltr" class="gmail_attr">` +
		html.EscapeString(gmailReplyQuoteAttribution(row)) +
		`<br></div><blockquote class="gmail_quote" style="margin:0px 0px 0px 0.8ex;border-left:1px solid rgb(204,204,204);padding-left:1ex">` +
		bodyHTML +
		`</blockquote></div>`
}

func gmailReplyQuoteText(row gmailReplyParentRow) string {
	bodyText := strings.TrimSpace(row.BodyText)
	if bodyText == "" {
		bodyText = htmlEmailText(row.BodyHTML)
	}
	if bodyText == "" {
		return ""
	}
	return gmailReplyQuoteAttribution(row) + "\n" + quoteEmailText(bodyText)
}

func gmailReplyQuoteAttribution(row gmailReplyParentRow) string {
	from := strings.TrimSpace(row.FromAddress)
	if from == "" {
		from = "someone"
	}
	if row.InternalDate.IsZero() {
		return "On " + from + " wrote:"
	}
	return "On " + row.InternalDate.Format("Mon, Jan 2, 2006 at 3:04 PM") + ", " + from + " wrote:"
}
