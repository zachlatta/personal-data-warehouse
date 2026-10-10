package mutations

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Apple Messages sends are executed on a Mac, not in the cloud worker: iMessage and
// SMS have no server API, and the only way to send as Zach is Messages.app itself on
// a Mac signed in to his Apple ID (and, for SMS, paired with his iPhone). The proposal
// and review halves live here with every other mutation type; the executor is
// `pdw mutations apple-messages` on that Mac, which claims exactly this provider.

// appleMessagesTextLimit bounds a proposed text. iMessage itself accepts far more, but
// a text longer than this is almost certainly a document pasted into the wrong tool,
// and an SMS fallback would split it into dozens of parts.
const appleMessagesTextLimit = 4000

// appleMessagesServices are the values `service` may take. "auto" sends in the
// conversation Zach already has with the recipient, whatever its service, and only
// falls back to iMessage when there is none.
var appleMessagesServices = map[string]string{
	"":         "auto",
	"auto":     "auto",
	"imessage": "iMessage",
	"sms":      "SMS",
}

var (
	appleMessagesEmailPattern = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)
	appleMessagesPhoneStrip   = regexp.MustCompile(`[\s().\-]`)
	appleMessagesE164Pattern  = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)
)

func isAppleMessagesMutation(mutation Mutation) bool {
	return mutation.Provider == AppleMessagesProvider || mutation.Operation == AppleMessagesSendMessageOperation
}

// normalizeAppleMessagesRecipient turns what an agent pasted into the handle
// Messages keys a conversation by: an E.164 phone number (+13105550123) or a
// lowercased email address. A ten-digit number is read as North American, because
// that is the only ambiguous shape Zach's contacts hold; anything else must carry
// its country code. It refuses rather than guesses: a wrong normalization is a text
// to a stranger.
func normalizeAppleMessagesRecipient(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", errors.New("must include recipient: an E.164 phone number (+13105550123) or an iMessage email address")
	}
	if strings.Contains(trimmed, "@") {
		email := strings.ToLower(strings.TrimPrefix(trimmed, "mailto:"))
		if !appleMessagesEmailPattern.MatchString(email) {
			return "", fmt.Errorf("recipient %q is not an email address", value)
		}
		return email, nil
	}
	phone := appleMessagesPhoneStrip.ReplaceAllString(strings.TrimPrefix(trimmed, "tel:"), "")
	if !strings.HasPrefix(phone, "+") {
		switch {
		case len(phone) == 10 && isAllDigits(phone):
			phone = "+1" + phone
		case len(phone) == 11 && strings.HasPrefix(phone, "1") && isAllDigits(phone):
			phone = "+" + phone
		default:
			return "", fmt.Errorf("recipient %q is not a phone number Messages can address: write it in E.164 form with its country code (+13105550123)", value)
		}
	}
	if !appleMessagesE164Pattern.MatchString(phone) {
		return "", fmt.Errorf("recipient %q is not an E.164 phone number (+13105550123)", value)
	}
	return phone, nil
}

func isAllDigits(value string) bool {
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return value != ""
}

func normalizeAppleMessagesService(value string) (string, error) {
	service, ok := appleMessagesServices[strings.ToLower(strings.TrimSpace(value))]
	if !ok {
		return "", fmt.Errorf("service %q must be auto, iMessage, or SMS", value)
	}
	return service, nil
}

// validateAppleMessagesSendMessage rejects at proposal time what the executor could
// only discover after a human had already approved it.
func validateAppleMessagesSendMessage(mutation MutationInput) error {
	if _, err := normalizeAppleMessagesRecipient(mutation.Recipient); err != nil {
		return err
	}
	if _, err := normalizeAppleMessagesService(mutation.Service); err != nil {
		return err
	}
	text := strings.TrimSpace(mutation.Text)
	if text == "" {
		return errors.New("must include text: the message to send")
	}
	if count := utf8.RuneCountInString(mutation.Text); count > appleMessagesTextLimit {
		return fmt.Errorf("text is %d characters; at most %d", count, appleMessagesTextLimit)
	}
	return nil
}

func appleMessagesPayload(mutation MutationInput) map[string]any {
	recipient, _ := normalizeAppleMessagesRecipient(mutation.Recipient)
	service, _ := normalizeAppleMessagesService(mutation.Service)
	return map[string]any{
		"recipient": recipient,
		"service":   service,
		"text":      strings.TrimRight(mutation.Text, " \t\r\n"),
	}
}

func appleMessagesTitle(mutation MutationInput) string {
	recipient, err := normalizeAppleMessagesRecipient(mutation.Recipient)
	if err != nil {
		recipient = strings.TrimSpace(mutation.Recipient)
	}
	return "Text " + recipient
}

// appleMessagesPreview is the proposal-time preview: exactly who and exactly what.
// The recipient's name and the recent conversation are hydrated on read
// (hydrateAppleMessagesPreviews), because a contact card can change after proposal.
func appleMessagesPreview(payload map[string]any) map[string]any {
	recipient := stringFromAny(payload["recipient"])
	kind := "phone"
	if strings.Contains(recipient, "@") {
		kind = "email"
	}
	return map[string]any{
		"recipient":      recipient,
		"recipient_kind": kind,
		"service":        stringFromAny(payload["service"]),
		"text":           stringFromAny(payload["text"]),
		"effect":         "Sends this text as Zach from Messages on his Mac. It cannot be unsent once delivered.",
	}
}

// appleMessagesContactKey is the marts_contacts.contact_points.normalized_value a
// recipient matches: digits with the country code for a phone, the lowercased
// address for an email.
func appleMessagesContactKey(recipient string) string {
	if strings.Contains(recipient, "@") {
		return strings.ToLower(recipient)
	}
	return strings.TrimPrefix(recipient, "+")
}

type appleMessagesTarget struct {
	account   string
	recipient string
}

func appleMessagesTargets(mutations []Mutation) []appleMessagesTarget {
	seen := map[appleMessagesTarget]bool{}
	out := []appleMessagesTarget{}
	for _, mutation := range mutations {
		if !isAppleMessagesMutation(mutation) {
			continue
		}
		target := appleMessagesTarget{account: mutation.Account, recipient: stringFromAny(mutation.Payload["recipient"])}
		if target.recipient == "" || seen[target] {
			continue
		}
		seen[target] = true
		out = append(out, target)
	}
	return out
}

// appleMessagesRecipientInfo is what the warehouse knows about a recipient: the
// contact card(s) the handle belongs to, and the newest messages exchanged with it.
type appleMessagesRecipientInfo struct {
	names  []string
	recent []map[string]any
}

const appleMessagesRecentLimit = 6

// hydrateAppleMessagesPreviews names the recipient from the contact book and shows
// the latest messages exchanged with that handle, so the reviewer approves a text
// to a person, not to a number. It belongs on the read path: cards and the
// conversation change after proposal. Failure is non-fatal; the review then shows
// the bare handle, flagged as unknown.
func (s *PostgresStore) hydrateAppleMessagesPreviews(ctx context.Context, mutations []Mutation) []Mutation {
	if s == nil || s.db == nil {
		return mutations
	}
	targets := appleMessagesTargets(mutations)
	if len(targets) == 0 {
		return mutations
	}
	infos := map[appleMessagesTarget]*appleMessagesRecipientInfo{}
	for _, target := range targets {
		info := &appleMessagesRecipientInfo{}
		infos[target] = info
		nameRows, err := queryContext(ctx, s.db, `
			SELECT DISTINCT display_name
			FROM @clean_contact_points
			WHERE normalized_value = $1 AND COALESCE(display_name, '') <> ''
			ORDER BY display_name
			LIMIT 5
		`, appleMessagesContactKey(target.recipient))
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
		messageRows, err := queryContext(ctx, s.db, `
			SELECT message.message_at, message.is_from_me, message.service, LEFT(message.body_text, 300)
			FROM @apple_messages AS message
			WHERE message.account = $1
			  AND message.is_deleted = 0
			  AND message.handle_id IN (
			      SELECT handle.handle_id FROM @apple_message_handles AS handle
			      WHERE handle.account = $1 AND lower(handle.address) = lower($2)
			  )
			  AND COALESCE(message.body_text, '') <> ''
			ORDER BY message.message_at DESC
			LIMIT $3
		`, target.account, target.recipient, appleMessagesRecentLimit)
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
	return applyAppleMessagesRecipientInfo(mutations, infos)
}

func applyAppleMessagesRecipientInfo(mutations []Mutation, infos map[appleMessagesTarget]*appleMessagesRecipientInfo) []Mutation {
	for index := range mutations {
		mutation := &mutations[index]
		if !isAppleMessagesMutation(*mutation) {
			continue
		}
		target := appleMessagesTarget{account: mutation.Account, recipient: stringFromAny(mutation.Payload["recipient"])}
		info, ok := infos[target]
		if !ok {
			continue
		}
		preview := cloneMap(mutation.Preview)
		message := cloneMap(mapFromAny(preview["text_message"]))
		if len(message) == 0 {
			message = appleMessagesPreview(mutation.Payload)
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
			warnings = append(warnings, "This number or address is not on any contact card.")
		}
		if len(recent) == 0 {
			warnings = append(warnings, "No earlier messages with this handle are in the warehouse: this would be a first text.")
		}
		message["warnings"] = warnings
		preview["text_message"] = message
		mutation.Preview = preview
	}
	return mutations
}
