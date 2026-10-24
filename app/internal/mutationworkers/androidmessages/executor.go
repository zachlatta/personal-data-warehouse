package androidmessages

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

const (
	Provider             = "android_messages"
	SendMessageOperation = "android_messages.send_message"

	// LockID is this provider's own advisory lock.
	LockID int64 = 7_403_111_871

	// AllowedRecipientsEnv, when set, is the only set of numbers the worker
	// will send to; anything else fails terminal before the phone is asked.
	// It is the testing guard: an empty value means no restriction.
	AllowedRecipientsEnv = "ANDROID_MESSAGES_SEND_ALLOWED_RECIPIENTS"
	// SSHHostEnv is the phone's SSH host (default pichu, root login).
	SSHHostEnv = "ANDROID_MESSAGES_SSH_HOST"
	// DevicePDWEnv is the pdw binary on the phone (default: the
	// pdw-android-messages Magisk module's, which keeps itself updated).
	DevicePDWEnv = "ANDROID_MESSAGES_DEVICE_PDW"

	DefaultSSHHost   = "pichu"
	DefaultDevicePDW = "/data/adb/modules/pdw-android-messages/bin/pdw"
)

// transportTimeout bounds one SSH round trip: connecting, the duplicate
// check, the send and the device's own verifyWait.
var transportTimeout = 2 * time.Minute

// Transport runs the device half with this request on stdin and returns
// what it printed.
type Transport func(request []byte) ([]byte, error)

// Spec is the Android Messages worker. Nothing is reclaimed from a stale
// `executing` claim: a send may be mid-flight, so a stuck send waits for a
// human exactly like gmail.send_email.
var Spec = queue.Spec{
	DisplayName: "Android Messages",
	Command:     "android-messages",
	Provider:    Provider,
	WorkerName:  "android_messages_mutation_worker",
	LockID:      LockID,
	EnvPrefix:   "ANDROID_MESSAGES",
	Idempotent:  nil,
	NewExecutor: func(queue.Store) queue.Executor {
		getenv := queue.Getenv(os.Getenv)
		return NewExecutor(SSHTransport(getenv)).WithAllowedRecipients(AllowedRecipientsFromEnv(getenv))
	},
}

// Run is the `pdw mutations android-messages` entry point.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string, cfg ingestclient.Config) int {
	return Spec.Run(args, stdin, stdout, stderr, getenv, cfg)
}

// SSHTransport reaches the phone over its root SSH login.
func SSHTransport(getenv func(string) string) Transport {
	host := strings.TrimSpace(getenv(SSHHostEnv))
	if host == "" {
		host = DefaultSSHHost
	}
	devicePDW := strings.TrimSpace(getenv(DevicePDWEnv))
	if devicePDW == "" {
		devicePDW = DefaultDevicePDW
	}
	return func(request []byte) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), transportTimeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "ssh",
			"-o", "BatchMode=yes",
			"-o", "ConnectTimeout=20",
			"-o", "ServerAliveInterval=10",
			host,
			devicePDW, "mutations", "android-messages-device", "send")
		cmd.Stdin = bytes.NewReader(request)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return stdout.Bytes(), fmt.Errorf("ssh %s: %v: %s", host, err, strings.TrimSpace(stderr.String()))
		}
		return stdout.Bytes(), nil
	}
}

// AllowedRecipientsFromEnv reads the comma-separated testing allowlist.
func AllowedRecipientsFromEnv(getenv func(string) string) []string {
	var out []string
	for _, value := range strings.Split(getenv(AllowedRecipientsEnv), ",") {
		if value = strings.TrimSpace(value); value != "" {
			out = append(out, value)
		}
	}
	return out
}

// Executor executes android_messages.send_message mutations through the
// phone.
type Executor struct {
	transport Transport
	allowed   map[string]bool
	now       func() time.Time
}

// NewExecutor builds an executor around a transport to the phone.
func NewExecutor(transport Transport) *Executor {
	return &Executor{transport: transport, now: time.Now}
}

// WithAllowedRecipients restricts sends to these numbers. An empty list
// means no restriction. Entries that do not normalize are ignored, never
// widened; a list naming nobody valid fails closed.
func (e *Executor) WithAllowedRecipients(values []string) *Executor {
	allowed := map[string]bool{}
	for _, value := range values {
		if number, err := NormalizeRecipient(value); err == nil {
			allowed[number] = true
		}
	}
	if len(allowed) == 0 && len(values) > 0 {
		allowed[""] = true
	}
	e.allowed = allowed
	return e
}

// Execute implements queue.Executor.
func (e *Executor) Execute(mutation queue.Mutation) queue.Result {
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
	recipient, err := NormalizeRecipient(stringValue(payload["recipient"]))
	if err != nil {
		return queue.Result{Status: queue.StatusFailedTerminal, Error: err.Error()}
	}
	text := stringValue(payload["text"])
	if strings.TrimSpace(text) == "" {
		return queue.Result{Status: queue.StatusFailedTerminal, Error: "payload has no text"}
	}
	if len(e.allowed) > 0 && !e.allowed[recipient] {
		return queue.Result{
			Status: queue.StatusFailedTerminal,
			Error:  fmt.Sprintf("%s is not in %s; nothing was sent", recipient, AllowedRecipientsEnv),
		}
	}
	since := mutation.ApprovedAt
	if since.IsZero() {
		since = e.now().Add(-24 * time.Hour)
	}
	// Messages stamps a row with the phone's clock; a minute of slack covers
	// a phone and a warehouse that disagree slightly.
	since = since.Add(-time.Minute)
	request, err := json.Marshal(DeviceRequest{MutationID: mutation.ID, Recipient: recipient, Text: text, Since: since.UTC()})
	if err != nil {
		return queue.Result{Status: queue.StatusFailedTerminal, Error: err.Error()}
	}
	out, transportErr := e.transport(request)
	var result DeviceResult
	if err := json.Unmarshal(bytes.TrimSpace(out), &result); err != nil || result.Outcome == "" {
		// The phone was unreachable, or the session dropped mid-send. Either
		// way the next attempt checks Messages' database before sending.
		detail := "no result"
		if transportErr != nil {
			detail = transportErr.Error()
		} else if err != nil {
			detail = "unreadable result: " + strings.TrimSpace(string(out))
		}
		return queue.Result{
			Status: queue.StatusFailedRetryable,
			Error:  "could not reach the Pixel (" + detail + "); the next attempt checks Messages for an earlier send before sending",
		}
	}
	resultJSON := map[string]any{"recipient": recipient, "verified": result.Verified}
	if result.Message != nil {
		resultJSON["bugle_id"] = result.Message.BugleID
		resultJSON["protocol"] = result.Message.Protocol
		resultJSON["bugle_status"] = result.Message.Status
		resultJSON["state"] = result.Message.State
		if result.Message.SentAt != "" {
			resultJSON["sent_at"] = result.Message.SentAt
		}
		if result.Message.ErrorCode != 0 {
			resultJSON["error_code"] = result.Message.ErrorCode
		}
	}
	if result.Note != "" {
		resultJSON["note"] = result.Note
	}
	switch result.Outcome {
	case OutcomeSent:
		return queue.Result{Status: queue.StatusSucceeded, ResultJSON: resultJSON}
	case OutcomeAlreadySent:
		resultJSON["already_sent"] = true
		return queue.Result{Status: queue.StatusSucceeded, ResultJSON: resultJSON}
	case OutcomeFailed, OutcomeRefused:
		return queue.Result{Status: queue.StatusFailedTerminal, Error: result.Error, ResultJSON: resultJSON}
	default:
		return queue.Result{Status: queue.StatusFailedRetryable, Error: result.Error, ResultJSON: resultJSON}
	}
}

func stringValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}
