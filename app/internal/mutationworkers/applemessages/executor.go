// Package applemessages applies approved apple_messages.send_message and
// apple_messages.mark_read mutations through Messages.app on the Mac that syncs
// Apple Messages. Mark-read lives in markread.go.
//
// iMessage and SMS have no server API: the only way to send a text as Zach is to
// ask Messages.app, on a Mac signed in to his Apple ID (and paired with his
// iPhone for SMS), over AppleScript. So the cloud mutation worker deliberately
// does not claim this provider; `pdw mutations apple-messages` does.
//
// Messages has no idempotency key, and a text cannot be unsent once delivered,
// so the rule every branch here serves is "one approval sends one message":
// before each attempt the executor looks in chat.db for an outgoing message with
// the same text to the same handle since the approval, and treats a match as the
// send having already happened. A failure that may have sent (a timeout) is
// retryable only because of that check.
package applemessages

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applescript"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

const (
	Provider             = "apple_messages"
	SendMessageOperation = "apple_messages.send_message"

	// MessagesBundleID is Messages.app (its historical name is MobileSMS).
	MessagesBundleID = "com.apple.MobileSMS"

	// LockID is this provider's own advisory lock: the queues are disjoint by
	// provider, so sharing the Notes or Contacts lock would make a long batch
	// there hold up a text for no reason.
	LockID int64 = 7_403_111_853

	// AllowedRecipientsEnv, when set, is the only set of handles the executor
	// will send to; anything else fails terminal before Messages is asked.
	// It is the testing guard: an empty value means no restriction.
	AllowedRecipientsEnv = "APPLE_MESSAGES_SEND_ALLOWED_RECIPIENTS"
)

var (
	phoneStrip  = regexp.MustCompile(`[\s().\-]`)
	e164Pattern = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)
	// verifyWait bounds how long the executor waits for a sent message to
	// appear in chat.db. Messages writes the row as it sends, so a few
	// seconds is generous; past it the send is recorded unverified.
	verifyWait = 15 * time.Second
	verifyPoll = time.Second
)

// SentMessage is one outgoing row of chat.db.
type SentMessage struct {
	GUID        string
	Service     string
	SentAt      time.Time
	IsSent      bool
	IsDelivered bool
	Error       int64
}

// Chat is a one-to-one conversation in chat.db.
type Chat struct {
	GUID    string
	Service string
}

// Store is the slice of chat.db the executor reads.
type Store interface {
	// FindSent returns the newest outgoing message with exactly this text to
	// this handle at or after since, or ok=false.
	FindSent(handle, text string, since time.Time) (SentMessage, bool, error)
	// OneToOneChat returns the most recently active one-to-one chat with this
	// handle, restricted to service unless service is "auto".
	OneToOneChat(handle, service string) (Chat, bool, error)
	// UnreadCount counts incoming unread messages in the one-to-one chats with
	// this identifier dated at or before through.
	UnreadCount(chat string, through time.Time) (int, error)
}

// Executor executes apple_messages.send_message mutations.
type Executor struct {
	runner    applescript.Runner
	store     Store
	readiness func() error
	allowed   map[string]bool
	now       func() time.Time
	sleep     func(time.Duration)
	// opener shows a conversation in Messages (mark_read); screenLocked
	// reports whether Messages' window could be on screen at all.
	opener       func(url string) error
	screenLocked func() (bool, error)
}

// NewExecutor builds an executor; a nil runner uses osascript and a nil store
// reads the live chat.db.
func NewExecutor(runner applescript.Runner, store Store) *Executor {
	if runner == nil {
		runner = applescript.Run
	}
	if store == nil {
		store = NewChatDBStore("")
	}
	return &Executor{runner: runner, store: store, now: time.Now, sleep: time.Sleep, opener: OpenURL}
}

// WithReadiness installs the check run before every send (launching
// Messages in the background).
// WithScreenLock sets the lock check mark_read runs before opening Messages.
func (e *Executor) WithScreenLock(locked func() (bool, error)) *Executor {
	e.screenLocked = locked
	return e
}

func (e *Executor) WithReadiness(readiness func() error) *Executor {
	e.readiness = readiness
	return e
}

// WithAllowedRecipients restricts sends to these handles. An empty list means
// no restriction. Entries that do not normalize are ignored, never widened.
func (e *Executor) WithAllowedRecipients(values []string) *Executor {
	allowed := map[string]bool{}
	for _, value := range values {
		if handle, err := NormalizeHandle(value); err == nil {
			allowed[handle] = true
		}
	}
	if len(allowed) == 0 && len(nonEmpty(values)) > 0 {
		// Every entry was malformed: fail closed rather than open.
		allowed[""] = true
	}
	e.allowed = allowed
	return e
}

// AllowedRecipientsFromEnv reads AllowedRecipientsEnv.
func AllowedRecipientsFromEnv(getenv func(string) string) []string {
	if getenv == nil {
		getenv = os.Getenv
	}
	return nonEmpty(strings.Split(getenv(AllowedRecipientsEnv), ","))
}

func nonEmpty(values []string) []string {
	out := []string{}
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, strings.TrimSpace(value))
		}
	}
	return out
}

// NormalizeHandle mirrors the proposal-side normalization: E.164 for a phone
// (a bare ten-digit number is +1), lowercase for an email.
func NormalizeHandle(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", errors.New("empty recipient")
	}
	if strings.Contains(trimmed, "@") {
		return strings.ToLower(strings.TrimPrefix(trimmed, "mailto:")), nil
	}
	phone := phoneStrip.ReplaceAllString(strings.TrimPrefix(trimmed, "tel:"), "")
	if !strings.HasPrefix(phone, "+") {
		switch {
		case len(phone) == 10:
			phone = "+1" + phone
		case len(phone) == 11 && strings.HasPrefix(phone, "1"):
			phone = "+" + phone
		}
	}
	if !e164Pattern.MatchString(phone) {
		return "", fmt.Errorf("recipient %q is not an E.164 phone number or an email address", value)
	}
	return phone, nil
}

// Execute implements queue.Executor.
func (e *Executor) Execute(mutation queue.Mutation) queue.Result {
	if mutation.Provider == Provider && mutation.Operation == MarkReadOperation {
		if mutation.Payload == nil {
			mutation.Payload = map[string]any{}
		}
		return e.markRead(mutation)
	}
	if mutation.Provider != Provider || mutation.Operation != SendMessageOperation {
		// Never burn an unrecognized row to failed_terminal: a newer worker
		// may understand it.
		return queue.Result{
			Status: queue.StatusFailedRetryable,
			Error:  fmt.Sprintf("unsupported mutation operation %s.%s; deferring", mutation.Provider, mutation.Operation),
		}
	}
	payload := mutation.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	handle, err := NormalizeHandle(stringValue(payload["recipient"]))
	if err != nil {
		return queue.Result{Status: queue.StatusFailedTerminal, Error: err.Error()}
	}
	text := stringValue(payload["text"])
	if strings.TrimSpace(text) == "" {
		return queue.Result{Status: queue.StatusFailedTerminal, Error: "payload has no text"}
	}
	service := stringValue(payload["service"])
	switch service {
	case "", "auto":
		service = "auto"
	case "iMessage", "SMS":
	default:
		return queue.Result{Status: queue.StatusFailedTerminal, Error: fmt.Sprintf("unknown service %q", service)}
	}
	if len(e.allowed) > 0 && !e.allowed[handle] {
		return queue.Result{
			Status: queue.StatusFailedTerminal,
			Error:  fmt.Sprintf("%s is not in %s on this Mac; nothing was sent", handle, AllowedRecipientsEnv),
		}
	}

	since := mutation.ApprovedAt
	if since.IsZero() {
		since = e.now().Add(-24 * time.Hour)
	}
	// Messages stamps a row with its own clock; a minute of slack covers a
	// Mac and a database that disagree slightly.
	since = since.Add(-time.Minute)
	if existing, found, err := e.store.FindSent(handle, text, since); err != nil {
		return queue.Result{
			Status: queue.StatusFailedRetryable,
			Error:  fmt.Sprintf("could not read chat.db to check for an earlier send, so nothing was sent: %v", err),
		}
	} else if found {
		result := sentResult(handle, existing)
		result["already_sent"] = true
		return queue.Result{Status: queue.StatusSucceeded, ResultJSON: result}
	}

	if e.readiness != nil {
		if err := e.readiness(); err != nil {
			return queue.Result{Status: queue.StatusFailedRetryable, Error: err.Error()}
		}
	}

	chat, haveChat, err := e.store.OneToOneChat(handle, service)
	if err != nil {
		return queue.Result{
			Status: queue.StatusFailedRetryable,
			Error:  fmt.Sprintf("could not read chat.db to find the conversation, so nothing was sent: %v", err),
		}
	}
	sendStarted := e.now()
	sentVia := ""
	var sendErr error
	if haveChat {
		_, sendErr = e.runner(chatScript(chat.GUID, text))
		sentVia = "chat:" + chat.GUID
		if sendErr != nil && isMissingObject(sendErr.Error()) {
			// The chat id did not resolve in Messages, so nothing was sent;
			// address the handle directly instead.
			haveChat = false
		}
	}
	if !haveChat {
		participantService := service
		if participantService == "auto" {
			participantService = "iMessage"
		}
		_, sendErr = e.runner(participantScript(handle, participantService, text))
		sentVia = "participant:" + participantService
	}
	if sendErr != nil {
		if errors.Is(sendErr, applescript.ErrTimeout) {
			// It may have sent. The retry looks in chat.db first.
			return queue.Result{
				Status: queue.StatusFailedRetryable,
				Error:  fmt.Sprintf("Messages did not answer within %ds; the next attempt checks chat.db before sending", int(applescript.DefaultScriptTimeout.Seconds())),
			}
		}
		status, message := applescript.Classify(sendErr.Error(), "Messages")
		return queue.Result{Status: status, Error: message, ResultJSON: map[string]any{"recipient": handle, "via": sentVia}}
	}

	// Verify: the row Messages wrote for this send.
	deadline := sendStarted.Add(verifyWait)
	for {
		sent, found, err := e.store.FindSent(handle, text, sendStarted.Add(-time.Minute))
		if err == nil && found {
			result := sentResult(handle, sent)
			result["via"] = sentVia
			if sent.Error != 0 {
				// Messages recorded the send as failed. Do not retry: the
				// failed row is in the conversation, and a retry would put a
				// second copy beside it if it later goes through.
				return queue.Result{
					Status:     queue.StatusFailedTerminal,
					Error:      fmt.Sprintf("Messages recorded the message with error %d (not delivered); open Messages on the Mac to retry or delete it", sent.Error),
					ResultJSON: result,
				}
			}
			result["verified"] = true
			return queue.Result{Status: queue.StatusSucceeded, ResultJSON: result}
		}
		if !e.now().Before(deadline) {
			break
		}
		e.sleep(verifyPoll)
	}
	return queue.Result{Status: queue.StatusSucceeded, ResultJSON: map[string]any{
		"recipient": handle,
		"via":       sentVia,
		"verified":  false,
		"note":      "Messages accepted the send but the message did not appear in chat.db within 15s",
	}}
}

func sentResult(handle string, sent SentMessage) map[string]any {
	return map[string]any{
		"recipient":    handle,
		"message_guid": sent.GUID,
		"service":      sent.Service,
		"sent_at":      sent.SentAt.UTC().Format(time.RFC3339),
		"is_sent":      sent.IsSent,
		"is_delivered": sent.IsDelivered,
		"error_code":   sent.Error,
	}
}

func isMissingObject(message string) bool {
	return strings.Contains(message, "-1728") || strings.Contains(message, "-1700") || strings.Contains(message, "-1719")
}

func stringValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

// chatScript sends to an existing conversation by its chat.db guid, which is
// also Messages' AppleScript chat id.
func chatScript(chatGUID, text string) string {
	return applescript.Dedent(fmt.Sprintf(`
		tell application "Messages"
		with timeout of %d seconds
		set targetChat to chat id %s
		send %s to targetChat
		end timeout
		end tell
		return "ok"
	`, applescript.InScriptTimeoutSeconds, applescript.String(chatGUID), applescript.String(text)))
}

// participantScript addresses the handle on the named service when Zach has
// no conversation with it yet.
func participantScript(handle, service, text string) string {
	serviceType := "iMessage"
	if service == "SMS" {
		serviceType = "SMS"
	}
	return applescript.Dedent(fmt.Sprintf(`
		tell application "Messages"
		with timeout of %d seconds
		set targetService to 1st account whose service type = %s
		set targetParticipant to participant %s of targetService
		send %s to targetParticipant
		end timeout
		end tell
		return "ok"
	`, applescript.InScriptTimeoutSeconds, serviceType, applescript.String(handle), applescript.String(text)))
}
