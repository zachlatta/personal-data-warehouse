package mutations

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Shared Messages handle helpers. apple_messages.mark_read (apple_messages_read.go)
// still runs on the Mac; sending a text moved to the Pixel
// (android_messages.send_message, android_messages.go) when Zach switched phones on
// 2026-10-23, and apple_messages.send_message is no longer accepted.

var (
	appleMessagesEmailPattern = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)
	appleMessagesPhoneStrip   = regexp.MustCompile(`[\s().\-]`)
	appleMessagesE164Pattern  = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)
)

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
