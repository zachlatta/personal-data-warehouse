// Package androidmessages applies approved android_messages.send_message
// mutations: one text, as Zach, from Google Messages on his Pixel.
//
// SMS and RCS have no server API, and the Pixel is the only thing that can
// send as Zach. The work is split in two:
//
//   - On porygon, `pdw mutations android-messages` is the resident queue
//     worker (the same queue package the Apple workers use). It claims
//     approved rows and, for each, runs the device half on the phone over its
//     root SSH login, handing it the request on stdin.
//   - On the phone, `pdw mutations android-messages-device send` (the pdw
//     binary of the pdw-android-messages Magisk module) does the send itself:
//     it starts Google Messages' own no-UI send service, the one the dialer
//     uses for "reply with a message" (RESPOND_VIA_MESSAGE to
//     NoConfirmationSmsSendService). That needs no screen and works while the
//     phone is locked, and Messages sends over RCS or SMS exactly as when
//     Zach taps send.
//
// Messages has no idempotency key, and a text cannot be unsent, so the rule
// every branch serves is "one approval sends one message": before sending, the
// phone looks in Messages' database (bugle_db, read-only) for an outgoing
// message with the same text to the same number since the approval, and
// treats a match as the send having already happened. A failure that may have
// sent (a dropped SSH session) is retryable only because of that check.
package androidmessages

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const (
	// DefaultDatabasePath is Google Messages' database on the phone.
	DefaultDatabasePath = "/data/data/com.google.android.apps.messaging/databases/bugle_db"

	// SendService is Messages' background send service. Its intent filter
	// is RESPOND_VIA_MESSAGE on sms:/smsto:, guarded by
	// SEND_RESPOND_VIA_MESSAGE, which root holds.
	SendService      = "com.google.android.apps.messaging/.shared.NoConfirmationSmsSendService"
	SendAction       = "android.intent.action.RESPOND_VIA_MESSAGE"
	SendTextExtraKey = "android.intent.extra.TEXT"
)

// Outcomes the device reports. The porygon half maps them onto queue
// statuses.
const (
	OutcomeSent        = "sent"
	OutcomeAlreadySent = "already_sent"
	// OutcomeFailed: Messages recorded the message as failed. Not retried:
	// the failed copy is in the conversation, and a retry would put a second
	// copy beside it if Messages later re-sends it.
	OutcomeFailed = "failed"
	// OutcomeRetry: nothing was sent (the database could not be read, or the
	// send service refused), so the next attempt may try again.
	OutcomeRetry = "retry"
	// OutcomeRefused: the request itself is wrong; nothing was sent.
	OutcomeRefused = "refused"
)

var (
	// verifyWait bounds how long the device waits for Messages to finish a
	// send. Messages writes the row as it queues the send and updates its
	// status when the network answers; RCS answers in a second or two.
	verifyWait = 30 * time.Second
	verifyPoll = time.Second
)

// DeviceRequest is what porygon sends the phone, as JSON on stdin.
type DeviceRequest struct {
	MutationID string    `json:"mutation_id"`
	Recipient  string    `json:"recipient"`
	Text       string    `json:"text"`
	Since      time.Time `json:"since"`
}

// SentMessage is one outgoing row of bugle_db.
type SentMessage struct {
	BugleID   int64  `json:"bugle_id"`
	Protocol  string `json:"protocol"`
	Status    int64  `json:"status"`
	State     string `json:"state"`
	SentAt    string `json:"sent_at,omitempty"`
	ErrorCode int64  `json:"error_code,omitempty"`
}

// DeviceResult is what the phone prints, as JSON on stdout.
type DeviceResult struct {
	Outcome  string       `json:"outcome"`
	Error    string       `json:"error,omitempty"`
	Message  *SentMessage `json:"message,omitempty"`
	Verified bool         `json:"verified"`
	Note     string       `json:"note,omitempty"`
}

// Messages' message_status for an outgoing message (below 100; 100 and up
// are incoming messages and conversation events).
func outgoingState(status int64) string {
	switch status {
	case 1:
		return "sent"
	case 2:
		return "delivered"
	case 8, 9:
		return "failed"
	default:
		return "sending"
	}
}

// Bugle is the slice of Messages' database the device reads.
type Bugle interface {
	// FindSent returns the newest outgoing message with this text (ignoring
	// surrounding whitespace) in the one-to-one conversation with this
	// number, queued at or after since.
	FindSent(recipient, text string, since time.Time) (SentMessage, bool, error)
}

// Device sends one text through Google Messages.
type Device struct {
	Bugle Bugle
	// SendText asks Messages to send; nil uses the RESPOND_VIA_MESSAGE
	// service through `am`.
	SendText func(recipient, text string) error
	Now      func() time.Time
	Sleep    func(time.Duration)
}

// Send applies one request. It never panics on bad input: every problem is
// a result the porygon half can record.
func (d *Device) Send(request DeviceRequest) DeviceResult {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Sleep == nil {
		d.Sleep = time.Sleep
	}
	if d.SendText == nil {
		d.SendText = amSendText
	}
	recipient, err := NormalizeRecipient(request.Recipient)
	if err != nil {
		return DeviceResult{Outcome: OutcomeRefused, Error: err.Error()}
	}
	text := strings.TrimSpace(request.Text)
	if text == "" {
		return DeviceResult{Outcome: OutcomeRefused, Error: "the request has no text"}
	}
	since := request.Since
	if since.IsZero() {
		since = d.Now().Add(-24 * time.Hour)
	}
	if existing, found, err := d.Bugle.FindSent(recipient, text, since); err != nil {
		return DeviceResult{Outcome: OutcomeRetry, Error: fmt.Sprintf("could not read Messages' database to check for an earlier send, so nothing was sent: %v", err)}
	} else if found {
		return DeviceResult{Outcome: OutcomeAlreadySent, Message: &existing, Verified: true}
	}

	started := d.Now()
	if err := d.SendText(recipient, request.Text); err != nil {
		return DeviceResult{Outcome: OutcomeRetry, Error: fmt.Sprintf("Messages' send service refused the request, so nothing was sent: %v", err)}
	}

	deadline := started.Add(verifyWait)
	var latest *SentMessage
	for {
		sent, found, err := d.Bugle.FindSent(recipient, text, started.Add(-time.Minute))
		if err == nil && found {
			latest = &sent
			switch sent.State {
			case "failed":
				return DeviceResult{Outcome: OutcomeFailed, Message: latest, Verified: true,
					Error: fmt.Sprintf("Messages marked the text failed (status %d, error %d); open Messages on the Pixel to retry or delete it", sent.Status, sent.ErrorCode)}
			case "sent", "delivered":
				return DeviceResult{Outcome: OutcomeSent, Message: latest, Verified: true}
			}
		}
		if !d.Now().Before(deadline) {
			break
		}
		d.Sleep(verifyPoll)
	}
	if latest != nil {
		// Queued in Messages, which keeps trying on its own (no signal, the
		// recipient's RCS is slow). Sending again would make a second copy.
		return DeviceResult{Outcome: OutcomeSent, Message: latest, Verified: false,
			Note: fmt.Sprintf("Messages queued the text but had not finished sending it after %ds; it keeps trying on its own", int(verifyWait.Seconds()))}
	}
	return DeviceResult{Outcome: OutcomeSent, Verified: false,
		Note: fmt.Sprintf("Messages accepted the send but the text did not appear in its database within %ds", int(verifyWait.Seconds()))}
}

// NormalizeRecipient is the E.164 number Messages keys a conversation by. A
// bare ten-digit number is read as +1; anything else must carry its country
// code. It refuses rather than guesses.
func NormalizeRecipient(value string) (string, error) {
	phone := strings.TrimPrefix(strings.TrimSpace(value), "tel:")
	phone = strings.NewReplacer(" ", "", "(", "", ")", "", ".", "", "-", "").Replace(phone)
	if !strings.HasPrefix(phone, "+") {
		switch {
		case len(phone) == 10 && allDigits(phone):
			phone = "+1" + phone
		case len(phone) == 11 && strings.HasPrefix(phone, "1") && allDigits(phone):
			phone = "+" + phone
		default:
			return "", fmt.Errorf("recipient %q is not an E.164 phone number", value)
		}
	}
	if len(phone) < 8 || len(phone) > 16 || phone[1] == '0' || !allDigits(phone[1:]) {
		return "", fmt.Errorf("recipient %q is not an E.164 phone number", value)
	}
	return phone, nil
}

func allDigits(value string) bool {
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return value != ""
}

// amSendText starts Messages' send service as root. The text travels as an
// argv element, never through a shell.
func amSendText(recipient, text string) error {
	am := "am"
	if _, err := exec.LookPath(am); err != nil {
		am = "/system/bin/am"
	}
	out, err := exec.Command(am, "startservice", "--user", "0",
		"-a", SendAction,
		"-d", "smsto:"+recipient,
		"--es", SendTextExtraKey, text,
		"-n", SendService,
	).CombinedOutput()
	output := strings.TrimSpace(string(out))
	if err != nil {
		return fmt.Errorf("%v: %s", err, output)
	}
	// am exits 0 even when the service could not be started.
	if strings.Contains(output, "Error:") || strings.Contains(output, "Not allowed") {
		return errors.New(output)
	}
	return nil
}

// BugleDB reads the live bugle_db, opened read-only for every query so each
// one sees what Messages has written since.
type BugleDB struct {
	Path string
}

// The conversation is the one-to-one chat with the number: Messages puts the
// other party's number on the conversation row (for a note-to-self as well),
// and lists them as its only participant.
const findSentQuery = `
SELECT
  m._id,
  COALESCE(m.message_protocol, -1),
  COALESCE(m.message_status, -1),
  COALESCE(m.sent_timestamp, 0),
  COALESCE(m.sms_error_code, 0)
FROM messages m
JOIN conversations c ON c._id = m.conversation_id
JOIN parts p ON p.message_id = m._id
WHERE m.received_timestamp >= ?1
  AND m.message_status >= 0 AND m.message_status < 100
  AND COALESCE(c.rcs_group_id, '') = ''
  AND c.participant_count <= 1
  AND (
    c.participant_normalized_destination = ?2
    OR EXISTS (
      SELECT 1 FROM conversation_participants cp
      JOIN participants pp ON pp._id = cp.participant_id
      WHERE cp.conversation_id = c._id AND pp.normalized_destination = ?2
    )
  )
  AND trim(p.text, ' ' || char(9) || char(10) || char(13)) = ?3
ORDER BY m._id DESC
LIMIT 1`

// FindSent implements Bugle.
func (b BugleDB) FindSent(recipient, text string, since time.Time) (SentMessage, bool, error) {
	path := b.Path
	if path == "" {
		path = DefaultDatabasePath
	}
	// Opening the database when Messages has not (no -wal/-shm beside it)
	// would have root create them, owned by root, and lock Messages out of
	// its own database.
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); err != nil {
			return SentMessage{}, false, fmt.Errorf("%s%s is absent (Messages has not opened its database; is the phone unlocked since boot?)", path, suffix)
		}
	}
	db, err := common.OpenSQLiteReadOnly(path)
	if err != nil {
		return SentMessage{}, false, err
	}
	defer db.Close()
	var id, protocol, status, sentMS, errorCode int64
	err = db.QueryRow(findSentQuery, since.UnixMilli(), recipient, strings.TrimSpace(text)).Scan(&id, &protocol, &status, &sentMS, &errorCode)
	if errors.Is(err, sql.ErrNoRows) {
		return SentMessage{}, false, nil
	}
	if err != nil {
		return SentMessage{}, false, err
	}
	message := SentMessage{BugleID: id, Protocol: protocolName(protocol), Status: status, State: outgoingState(status)}
	if errorCode > 0 {
		// Messages stores -1 (or 0) for "no error".
		message.ErrorCode = errorCode
	}
	if sentMS > 0 {
		message.SentAt = time.UnixMilli(sentMS).UTC().Format(time.RFC3339)
	}
	return message, true, nil
}

func protocolName(protocol int64) string {
	switch protocol {
	case 0:
		return "sms"
	case 1:
		return "mms"
	case 3:
		return "rcs"
	}
	return fmt.Sprintf("protocol_%d", protocol)
}
