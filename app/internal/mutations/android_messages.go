package mutations

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Android Messages sends are executed on Zach's phone, not in the cloud worker:
// SMS and RCS have no server API, and the only way to send as Zach is Google
// Messages itself on his Pixel. The proposal, review and edit halves live here
// with every other mutation type; the executor is `pdw mutations
// android-messages` on porygon, which claims exactly this provider and asks the
// phone (over its root SSH login) to send through Messages' own no-UI send
// service. Google Messages picks RCS or SMS/MMS for the conversation itself,
// exactly as when Zach taps send.

// androidMessagesTextLimit bounds a proposed text. RCS accepts far more, but a
// text longer than this is almost certainly a document pasted into the wrong
// tool, and an SMS fallback would split it into dozens of parts.
const androidMessagesTextLimit = 4000

func isAndroidMessagesSendMutation(mutation Mutation) bool {
	return mutation.Operation == AndroidMessagesSendMessageOperation
}

// normalizeAndroidMessagesRecipient is the phone number Messages keys a
// one-to-one conversation by, in E.164 form. Email addresses were iMessage
// handles: a Pixel cannot text them, so they are refused rather than sent as
// something else.
func normalizeAndroidMessagesRecipient(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", errors.New("must include recipient: an E.164 phone number (+13105550123)")
	}
	if strings.Contains(trimmed, "@") {
		return "", fmt.Errorf("recipient %q is an email address; texts from the Pixel go to phone numbers only (an email was an iMessage handle)", value)
	}
	return normalizeAppleMessagesRecipient(trimmed)
}

// validateAndroidMessagesSendMessage rejects at proposal time what the
// executor could only discover after a human had already approved it.
func validateAndroidMessagesSendMessage(mutation MutationInput) error {
	if _, err := normalizeAndroidMessagesRecipient(mutation.Recipient); err != nil {
		return err
	}
	if service := strings.ToLower(strings.TrimSpace(mutation.Service)); service != "" && service != "auto" {
		return fmt.Errorf("service %q is not supported: Google Messages picks RCS or SMS for the conversation itself", mutation.Service)
	}
	return validateTextMessageText(mutation.Text)
}

func validateTextMessageText(text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("must include text: the message to send")
	}
	if count := utf8.RuneCountInString(text); count > androidMessagesTextLimit {
		return fmt.Errorf("text is %d characters; at most %d", count, androidMessagesTextLimit)
	}
	return nil
}

func androidMessagesPayload(mutation MutationInput) map[string]any {
	recipient, _ := normalizeAndroidMessagesRecipient(mutation.Recipient)
	return map[string]any{
		"recipient": recipient,
		"text":      strings.TrimRight(mutation.Text, " \t\r\n"),
	}
}

func androidMessagesTitle(mutation MutationInput) string {
	recipient, err := normalizeAndroidMessagesRecipient(mutation.Recipient)
	if err != nil {
		recipient = strings.TrimSpace(mutation.Recipient)
	}
	return "Text " + recipient
}

const androidMessagesEffect = "Sends this text as Zach from Google Messages on his Pixel (RCS when the conversation has it, otherwise SMS). It cannot be unsent once delivered."

// androidMessagesPreview is the proposal-time preview: exactly who and exactly
// what. The recipient's name and the recent conversation are hydrated on read
// (hydrateAndroidMessagesPreviews), because a contact card can change after
// proposal.
func androidMessagesPreview(payload map[string]any) map[string]any {
	return map[string]any{
		"recipient":      stringFromAny(payload["recipient"]),
		"recipient_kind": "phone",
		"text":           stringFromAny(payload["text"]),
		"effect":         androidMessagesEffect,
	}
}

// updatedTextMessagePayload applies a reviewer's edit of a pending text: the
// words only. The recipient was validated at proposal time; a wrong
// recipient is a deny, not a redirect.
func updatedTextMessagePayload(mutation Mutation, input UpdateTextMessageMutationInput) (map[string]any, map[string]any, string, error) {
	text := strings.TrimRight(input.Text, " \t\r\n")
	if err := validateTextMessageText(text); err != nil {
		return nil, nil, "", fmt.Errorf("text message %w", err)
	}
	payload := cloneMap(mutation.Payload)
	payload["text"] = text
	preview := cloneMap(mutation.Preview)
	message := cloneMap(mapFromAny(preview["text_message"]))
	if len(message) == 0 {
		message = androidMessagesPreview(payload)
	}
	message["text"] = text
	message["edited"] = true
	preview["text_message"] = message
	return payload, preview, mutation.Title, nil
}

type textMessageTarget struct {
	account   string
	recipient string
}

func androidMessagesTargets(mutations []Mutation) []textMessageTarget {
	seen := map[textMessageTarget]bool{}
	out := []textMessageTarget{}
	for _, mutation := range mutations {
		if !isAndroidMessagesSendMutation(mutation) {
			continue
		}
		target := textMessageTarget{account: mutation.Account, recipient: stringFromAny(mutation.Payload["recipient"])}
		if target.recipient == "" || seen[target] {
			continue
		}
		seen[target] = true
		out = append(out, target)
	}
	return out
}

// textMessageRecipientInfo is what the warehouse knows about a recipient: the
// contact card(s) the number belongs to, and the newest messages exchanged with
// it on either phone.
type textMessageRecipientInfo struct {
	names  []string
	recent []map[string]any
}

const textMessageRecentLimit = 6

// hydrateAndroidMessagesPreviews names the recipient from the contact book and
// shows the latest messages exchanged with that number, from the Pixel
// (base_android_messages, since the switch) and the iPhone before it
// (base_apple_messages), so the reviewer approves a text to a person, not to a
// number. It belongs on the read path: cards and the conversation change after
// proposal. Failure is non-fatal; the review then shows the bare number,
// flagged as unchecked.
func (s *PostgresStore) hydrateAndroidMessagesPreviews(ctx context.Context, mutations []Mutation) []Mutation {
	if s == nil || s.db == nil {
		return mutations
	}
	targets := androidMessagesTargets(mutations)
	if len(targets) == 0 {
		return mutations
	}
	infos := map[textMessageTarget]*textMessageRecipientInfo{}
	for _, target := range targets {
		info := &textMessageRecipientInfo{}
		nameRows, err := queryContext(ctx, s.db, `
			SELECT DISTINCT display_name
			FROM @clean_contact_points
			WHERE normalized_value = $1 AND COALESCE(display_name, '') <> ''
			ORDER BY display_name
			LIMIT 5
		`, strings.TrimPrefix(target.recipient, "+"))
		if err != nil {
			return mutations
		}
		for nameRows.Next() {
			var name string
			if err := nameRows.Scan(&name); err == nil {
				info.names = append(info.names, name)
			}
		}
		nameRows.Close()
		infos[target] = info
		messageRows, err := queryContext(ctx, s.db, `
			SELECT message_at, is_from_me, service, body FROM (
			    SELECT message.message_at, message.is_from_me, message.protocol_name AS service, LEFT(message.body_text, 300) AS body
			    FROM @android_messages AS message
			    WHERE message.account = $1 AND message.chat_key = $2 AND message.is_group = 0
			      AND COALESCE(message.body_text, '') <> ''
			    UNION ALL
			    SELECT message.message_at, message.is_from_me, message.service, LEFT(message.body_text, 300)
			    FROM @apple_messages AS message
			    WHERE message.account = $1
			      AND message.is_deleted = 0
			      AND message.handle_id IN (
			          SELECT handle.handle_id FROM @apple_message_handles AS handle
			          WHERE handle.account = $1 AND lower(handle.address) = lower($2)
			      )
			      AND COALESCE(message.body_text, '') <> ''
			) AS conversation
			ORDER BY message_at DESC
			LIMIT $3
		`, target.account, target.recipient, textMessageRecentLimit)
		if err != nil {
			continue
		}
		for messageRows.Next() {
			var at time.Time
			var fromMe int64
			var service, body string
			if err := messageRows.Scan(&at, &fromMe, &service, &body); err != nil {
				continue
			}
			info.recent = append(info.recent, map[string]any{
				"message_at": at.UTC().Format(time.RFC3339),
				"is_from_me": fromMe == 1,
				"service":    service,
				"text":       body,
			})
		}
		messageRows.Close()
	}
	return applyTextMessageRecipientInfo(mutations, infos)
}

func applyTextMessageRecipientInfo(mutations []Mutation, infos map[textMessageTarget]*textMessageRecipientInfo) []Mutation {
	for index := range mutations {
		mutation := &mutations[index]
		if !isAndroidMessagesSendMutation(*mutation) {
			continue
		}
		target := textMessageTarget{account: mutation.Account, recipient: stringFromAny(mutation.Payload["recipient"])}
		info, ok := infos[target]
		if !ok {
			continue
		}
		preview := cloneMap(mutation.Preview)
		message := cloneMap(mapFromAny(preview["text_message"]))
		if len(message) == 0 {
			message = androidMessagesPreview(mutation.Payload)
		}
		// The words may have been edited since proposal; the payload is what runs.
		message["text"] = stringFromAny(mutation.Payload["text"])
		names := append([]string{}, info.names...)
		message["recipient_names"] = names
		if len(names) > 0 {
			message["recipient_name"] = strings.Join(names, " / ")
		} else {
			delete(message, "recipient_name")
		}
		recent := append([]map[string]any{}, info.recent...)
		message["recent_messages"] = recent
		warnings := []string{}
		if len(names) == 0 {
			warnings = append(warnings, "This number is not on any contact card.")
		}
		if len(recent) == 0 {
			warnings = append(warnings, "No earlier messages with this number are in the warehouse: this would be a first text.")
		}
		message["warnings"] = warnings
		preview["text_message"] = message
		mutation.Preview = preview
	}
	return mutations
}
