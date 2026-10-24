// Package applemessages applies approved apple_messages.mark_read mutations
// through Messages.app on the Mac that syncs Apple Messages (markread.go).
//
// It used to send texts too (apple_messages.send_message, over AppleScript).
// Zach moved to a Pixel on 2026-10-23 and texts are sent from the phone now
// (mutationworkers/androidmessages), so a send that reaches this worker is
// refused terminal with nothing sent.
package applemessages

import (
	"errors"
	"fmt"
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
)

var (
	phoneStrip  = regexp.MustCompile(`[\s().\-]`)
	e164Pattern = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)
	// verifyPoll is how often mark_read re-reads chat.db.
	verifyPoll = time.Second
)

// Chat is a one-to-one conversation in chat.db.
type Chat struct {
	GUID    string
	Service string
}

// Store is the slice of chat.db the executor reads.
type Store interface {
	// OneToOneChat returns the most recently active one-to-one chat with this
	// handle, restricted to service unless service is "auto".
	OneToOneChat(handle, service string) (Chat, bool, error)
	// UnreadCount counts incoming unread messages in the one-to-one chats with
	// this identifier dated at or before through.
	UnreadCount(chat string, through time.Time) (int, error)
}

// Executor executes apple_messages.mark_read mutations.
type Executor struct {
	runner    applescript.Runner
	store     Store
	readiness func() error
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
	// apple_messages.send_message: retired. The proposal API no longer
	// accepts it; an older approved row must not text from the Mac.
	return queue.Result{
		Status: queue.StatusFailedTerminal,
		Error:  "apple_messages.send_message is retired: texts are sent from the Pixel now (android_messages.send_message); nothing was sent",
	}
}

func stringValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}
