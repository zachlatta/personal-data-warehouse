package androidmessages

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// fakeBugle returns found rows in order (then the last repeats); a zero
// BugleID is "not found".
type fakeBugle struct {
	rows   []SentMessage
	calls  int
	err    error
	sinces []time.Time
	texts  []string
}

func (f *fakeBugle) FindSent(recipient, text string, since time.Time) (SentMessage, bool, error) {
	f.calls++
	f.sinces = append(f.sinces, since)
	f.texts = append(f.texts, text)
	if f.err != nil {
		return SentMessage{}, false, f.err
	}
	index := min(f.calls-1, len(f.rows)-1)
	if index < 0 || f.rows[index].BugleID == 0 {
		return SentMessage{}, false, nil
	}
	return f.rows[index], true, nil
}

type deviceHarness struct {
	bugle   *fakeBugle
	sends   []string
	sendErr error
}

func (h *deviceHarness) device() *Device {
	clock := time.Date(2026, 10, 24, 22, 0, 0, 0, time.UTC)
	return &Device{
		Bugle: h.bugle,
		SendText: func(recipient, text string) error {
			h.sends = append(h.sends, recipient+"|"+text)
			return h.sendErr
		},
		Now:   func() time.Time { return clock },
		Sleep: func(d time.Duration) { clock = clock.Add(d) },
	}
}

var approved = time.Date(2026, 10, 24, 21, 59, 0, 0, time.UTC)

func request(text string) DeviceRequest {
	return DeviceRequest{MutationID: "m1", Recipient: "+13105550123", Text: text, Since: approved}
}

func TestDeviceSendsOnceAndWaitsForMessagesToFinish(t *testing.T) {
	sending := SentMessage{BugleID: 7, Protocol: "rcs", Status: 5, State: "sending"}
	sent := SentMessage{BugleID: 7, Protocol: "rcs", Status: 2, State: "delivered"}
	h := &deviceHarness{bugle: &fakeBugle{rows: []SentMessage{{}, sending, sent}}}
	result := h.device().Send(request("On my way\n"))
	if result.Outcome != OutcomeSent || !result.Verified || result.Message.BugleID != 7 || result.Message.Protocol != "rcs" {
		t.Fatalf("result = %+v", result)
	}
	if len(h.sends) != 1 || h.sends[0] != "+13105550123|On my way\n" {
		t.Fatalf("sends = %q", h.sends)
	}
	if !h.bugle.sinces[0].Equal(approved) {
		t.Fatalf("the duplicate check looked back to %v, want the approval", h.bugle.sinces[0])
	}
	if h.bugle.texts[0] != "On my way" {
		t.Fatalf("the duplicate check compared %q, want the trimmed text", h.bugle.texts[0])
	}
}

func TestDeviceDoesNotSendWhatMessagesAlreadyHolds(t *testing.T) {
	h := &deviceHarness{bugle: &fakeBugle{rows: []SentMessage{{BugleID: 3, Protocol: "sms", Status: 1, State: "sent"}}}}
	result := h.device().Send(request("hi"))
	if result.Outcome != OutcomeAlreadySent || len(h.sends) != 0 {
		t.Fatalf("result = %+v, sends = %q", result, h.sends)
	}
}

func TestDeviceSendsNothingWhenItCannotCheckFirst(t *testing.T) {
	h := &deviceHarness{bugle: &fakeBugle{err: errors.New("bugle_db-wal is absent")}}
	result := h.device().Send(request("hi"))
	if result.Outcome != OutcomeRetry || len(h.sends) != 0 || !strings.Contains(result.Error, "nothing was sent") {
		t.Fatalf("result = %+v, sends = %q", result, h.sends)
	}
}

func TestDeviceReportsAFailedSendTerminally(t *testing.T) {
	h := &deviceHarness{bugle: &fakeBugle{rows: []SentMessage{{}, {BugleID: 9, Protocol: "sms", Status: 8, State: "failed", ErrorCode: 1}}}}
	result := h.device().Send(request("hi"))
	if result.Outcome != OutcomeFailed || !strings.Contains(result.Error, "failed") {
		t.Fatalf("result = %+v", result)
	}
}

func TestDeviceLeavesASlowSendToMessages(t *testing.T) {
	h := &deviceHarness{bugle: &fakeBugle{rows: []SentMessage{{}, {BugleID: 9, Protocol: "rcs", Status: 5, State: "sending"}}}}
	result := h.device().Send(request("hi"))
	if result.Outcome != OutcomeSent || result.Verified || !strings.Contains(result.Note, "keeps trying") || len(h.sends) != 1 {
		t.Fatalf("result = %+v, sends = %q", result, h.sends)
	}
}

func TestDeviceRetriesWhenTheSendServiceRefuses(t *testing.T) {
	h := &deviceHarness{bugle: &fakeBugle{rows: []SentMessage{{}}}, sendErr: errors.New("Error: Not found; no service started.")}
	if result := h.device().Send(request("hi")); result.Outcome != OutcomeRetry {
		t.Fatalf("result = %+v", result)
	}
}

func TestDeviceRefusesABadRequest(t *testing.T) {
	for _, bad := range []DeviceRequest{{Recipient: "Ada", Text: "hi"}, {Recipient: "+13105550123", Text: "  "}} {
		h := &deviceHarness{bugle: &fakeBugle{}}
		if result := h.device().Send(bad); result.Outcome != OutcomeRefused || len(h.sends) != 0 {
			t.Fatalf("%+v: result = %+v", bad, result)
		}
	}
}

func TestNormalizeRecipient(t *testing.T) {
	for input, want := range map[string]string{"(310) 555-0123": "+13105550123", "13105550123": "+13105550123", "+447700900123": "+447700900123", "tel:+13105550123": "+13105550123"} {
		if got, err := NormalizeRecipient(input); err != nil || got != want {
			t.Fatalf("NormalizeRecipient(%q) = %q, %v", input, got, err)
		}
	}
	for _, bad := range []string{"", "someone@example.com", "5550123", "+0123456789", "447700900123"} {
		if got, err := NormalizeRecipient(bad); err == nil {
			t.Fatalf("NormalizeRecipient(%q) = %q, want an error", bad, got)
		}
	}
}

// seedBugle builds the slice of Messages' schema FindSent reads, in WAL mode
// with a writer left open (as Messages keeps it).
func seedBugle(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bugle_db")
	db, err := common.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ms := approved.UnixMilli()
	statements := []string{
		`PRAGMA journal_mode=WAL`,
		`CREATE TABLE conversations (_id INTEGER PRIMARY KEY, participant_count INT, participant_normalized_destination TEXT, rcs_group_id TEXT)`,
		`CREATE TABLE participants (_id INTEGER PRIMARY KEY, normalized_destination TEXT)`,
		`CREATE TABLE conversation_participants (conversation_id INT, participant_id INT)`,
		`CREATE TABLE messages (_id INTEGER PRIMARY KEY, conversation_id INT, message_protocol INT, message_status INT, sent_timestamp INT, received_timestamp INT, sms_error_code INT)`,
		`CREATE TABLE parts (_id INTEGER PRIMARY KEY, message_id INT, text TEXT)`,
		`INSERT INTO participants VALUES (1, '+13105550123'), (2, '+13105550199')`,
		// 1: one-to-one with +13105550123; 2: a group with them in it; 3: a
		// one-to-one keyed only through its participant row.
		`INSERT INTO conversations VALUES (1, 1, '+13105550123', NULL), (2, 2, NULL, 'rcs-group-1'), (3, 1, NULL, '')`,
		`INSERT INTO conversation_participants VALUES (1, 1), (2, 1), (2, 2), (3, 2)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	insert := func(id, conversation, status int, at int64, text string) {
		if _, err := db.Exec(`INSERT INTO messages VALUES (?, ?, 3, ?, ?, ?, 0)`, id, conversation, status, at, at); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO parts VALUES (?, ?, ?)`, id, id, text); err != nil {
			t.Fatal(err)
		}
	}
	insert(1, 1, 2, ms-48*3600*1000, "hi")   // before the window
	insert(2, 1, 100, ms+60_000, "hi")       // incoming
	insert(3, 2, 2, ms+120_000, "hi")        // the group
	insert(4, 1, 2, ms+180_000, "hi\n")      // the send
	insert(5, 3, 1, ms+240_000, " see you ") // keyed through participants
	return path
}

func TestBugleFindSentMatchesOnlyTheOneToOneOutgoingCopy(t *testing.T) {
	bugle := BugleDB{Path: seedBugle(t)}
	sent, found, err := bugle.FindSent("+13105550123", "hi", approved)
	if err != nil || !found || sent.BugleID != 4 || sent.State != "delivered" || sent.Protocol != "rcs" {
		t.Fatalf("FindSent = %+v %v %v", sent, found, err)
	}
	if other, found, _ := bugle.FindSent("+13105550199", "see you", approved); !found || other.BugleID != 5 || other.State != "sent" {
		t.Fatalf("participant-keyed FindSent = %+v %v", other, found)
	}
	if _, found, _ := bugle.FindSent("+13105550123", "something else", approved); found {
		t.Fatal("different text must not match")
	}
	if _, found, _ := bugle.FindSent("+13105550123", "hi", approved.Add(5*time.Minute)); found {
		t.Fatal("a send before the window must not match")
	}
	if _, found, _ := bugle.FindSent("+13105550199", "hi", approved); found {
		t.Fatal("a group message must not count as a one-to-one send")
	}
}

func TestBugleRefusesADatabaseMessagesHasNotOpened(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bugle_db")
	db, err := common.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE messages (_id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if _, _, err := (BugleDB{Path: path}).FindSent("+13105550123", "hi", approved); err == nil || !strings.Contains(err.Error(), "-wal") {
		t.Fatalf("err = %v", err)
	}
}

func mutation(recipient, text string) queue.Mutation {
	return queue.Mutation{ID: "m1", Provider: Provider, Operation: SendMessageOperation, Account: "you@example.com",
		ApprovedAt: approved, Payload: map[string]any{"recipient": recipient, "text": text}}
}

func fakeTransport(result any, err error, requests *[]DeviceRequest) Transport {
	return func(body []byte) ([]byte, error) {
		var request DeviceRequest
		_ = json.Unmarshal(body, &request)
		*requests = append(*requests, request)
		if text, ok := result.(string); ok {
			return []byte(text), err
		}
		out, _ := json.Marshal(result)
		return out, err
	}
}

func TestExecutorMapsTheDeviceResult(t *testing.T) {
	message := &SentMessage{BugleID: 7, Protocol: "rcs", Status: 2, State: "delivered"}
	cases := []struct {
		result DeviceResult
		status string
	}{
		{DeviceResult{Outcome: OutcomeSent, Message: message, Verified: true}, queue.StatusSucceeded},
		{DeviceResult{Outcome: OutcomeAlreadySent, Message: message, Verified: true}, queue.StatusSucceeded},
		{DeviceResult{Outcome: OutcomeFailed, Message: message, Error: "failed"}, queue.StatusFailedTerminal},
		{DeviceResult{Outcome: OutcomeRefused, Error: "bad"}, queue.StatusFailedTerminal},
		{DeviceResult{Outcome: OutcomeRetry, Error: "locked"}, queue.StatusFailedRetryable},
	}
	for _, tc := range cases {
		var requests []DeviceRequest
		got := NewExecutor(fakeTransport(tc.result, nil, &requests)).Execute(mutation("(310) 555-0123", "hi"))
		if got.Status != tc.status {
			t.Fatalf("%s: status = %s (%+v)", tc.result.Outcome, got.Status, got)
		}
		if len(requests) != 1 || requests[0].Recipient != "+13105550123" || requests[0].Text != "hi" || !requests[0].Since.Equal(approved.Add(-time.Minute)) {
			t.Fatalf("request = %+v", requests)
		}
		if tc.result.Outcome == OutcomeAlreadySent && got.ResultJSON["already_sent"] != true {
			t.Fatalf("already-sent result = %+v", got.ResultJSON)
		}
		if tc.result.Message != nil && got.ResultJSON["protocol"] != "rcs" {
			t.Fatalf("result JSON = %+v", got.ResultJSON)
		}
	}
}

func TestAnUnreachablePhoneIsRetryable(t *testing.T) {
	var requests []DeviceRequest
	got := NewExecutor(fakeTransport("", errors.New("ssh: connect to host pichu port 22: Operation timed out"), &requests)).Execute(mutation("+13105550123", "hi"))
	if got.Status != queue.StatusFailedRetryable || !strings.Contains(got.Error, "checks Messages for an earlier send") {
		t.Fatalf("result = %+v", got)
	}
}

func TestTheAllowlistRefusesAnyoneElseBeforeThePhoneIsAsked(t *testing.T) {
	var requests []DeviceRequest
	executor := NewExecutor(fakeTransport(DeviceResult{Outcome: OutcomeSent}, nil, &requests)).WithAllowedRecipients([]string{"+13105550199"})
	if got := executor.Execute(mutation("+13105550123", "hi")); got.Status != queue.StatusFailedTerminal || len(requests) != 0 {
		t.Fatalf("result = %+v, requests = %d", got, len(requests))
	}
	if got := executor.Execute(mutation("+13105550199", "hi")); got.Status != queue.StatusSucceeded {
		t.Fatalf("allowed recipient result = %+v", got)
	}
	closed := NewExecutor(fakeTransport(DeviceResult{Outcome: OutcomeSent}, nil, &requests)).WithAllowedRecipients([]string{"nobody"})
	if got := closed.Execute(mutation("+13105550199", "hi")); got.Status != queue.StatusFailedTerminal {
		t.Fatalf("a malformed allowlist must fail closed: %+v", got)
	}
}

func TestUnknownOperationsAreDeferredNotBurned(t *testing.T) {
	var requests []DeviceRequest
	m := mutation("+13105550123", "hi")
	m.Operation = "android_messages.unsend"
	if got := NewExecutor(fakeTransport(DeviceResult{Outcome: OutcomeSent}, nil, &requests)).Execute(m); got.Status != queue.StatusFailedRetryable || len(requests) != 0 {
		t.Fatalf("result = %+v", got)
	}
}
