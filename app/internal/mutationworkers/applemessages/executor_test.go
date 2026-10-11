package applemessages

import (
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applescript"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

type fakeStore struct {
	sent      []SentMessage // returned by FindSent in order; then the last repeats
	findCalls int
	findErr   error
	chat      Chat
	haveChat  bool
	chatErr   error
	sinces    []time.Time
	// unread is returned by UnreadCount in order; then the last repeats.
	unread      []int
	unreadCalls int
	unreadErr   error
}

func (f *fakeStore) FindSent(handle, text string, since time.Time) (SentMessage, bool, error) {
	f.findCalls++
	f.sinces = append(f.sinces, since)
	if f.findErr != nil {
		return SentMessage{}, false, f.findErr
	}
	index := f.findCalls - 1
	if index >= len(f.sent) {
		index = len(f.sent) - 1
	}
	if index < 0 || f.sent[index].GUID == "" {
		return SentMessage{}, false, nil
	}
	return f.sent[index], true, nil
}

func (f *fakeStore) UnreadCount(chat string, through time.Time) (int, error) {
	f.unreadCalls++
	if f.unreadErr != nil {
		return 0, f.unreadErr
	}
	index := f.unreadCalls - 1
	if index >= len(f.unread) {
		index = len(f.unread) - 1
	}
	if index < 0 {
		return 0, nil
	}
	return f.unread[index], nil
}

func (f *fakeStore) OneToOneChat(handle, service string) (Chat, bool, error) {
	return f.chat, f.haveChat, f.chatErr
}

type recordingRunner struct {
	scripts []string
	errs    []error
}

func (r *recordingRunner) run(script string) (string, error) {
	r.scripts = append(r.scripts, script)
	if len(r.errs) >= len(r.scripts) {
		return "", r.errs[len(r.scripts)-1]
	}
	return "ok", nil
}

func sendMutation(recipient, text string) queue.Mutation {
	return queue.Mutation{
		ID: "m1", Provider: Provider, Operation: SendMessageOperation, Account: "you@example.com",
		ApprovedAt: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC),
		Payload:    map[string]any{"recipient": recipient, "text": text, "service": "auto"},
	}
}

func testExecutor(runner *recordingRunner, store *fakeStore) *Executor {
	executor := NewExecutor(runner.run, store)
	clock := time.Date(2026, 10, 10, 12, 1, 0, 0, time.UTC)
	executor.now = func() time.Time { return clock }
	executor.sleep = func(d time.Duration) { clock = clock.Add(d) }
	return executor
}

var delivered = SentMessage{GUID: "GUID-1", Service: "iMessage", SentAt: time.Date(2026, 10, 10, 12, 1, 1, 0, time.UTC), IsSent: true}

func TestSendsInTheExistingConversationAndRecordsTheSentRow(t *testing.T) {
	runner := &recordingRunner{}
	store := &fakeStore{sent: []SentMessage{{}, delivered}, chat: Chat{GUID: "any;-;+13105550123", Service: "iMessage"}, haveChat: true}
	result := testExecutor(runner, store).Execute(sendMutation("+13105550123", `Say "hi"`+"\nbye"))
	if result.Status != queue.StatusSucceeded {
		t.Fatalf("status = %s (%s)", result.Status, result.Error)
	}
	if len(runner.scripts) != 1 || !strings.Contains(runner.scripts[0], `chat id "any;-;+13105550123"`) {
		t.Fatalf("expected one send to the existing chat, got %q", runner.scripts)
	}
	if !strings.Contains(runner.scripts[0], `send "Say \"hi\"" & linefeed & "bye"`) {
		t.Fatalf("text was not escaped as an AppleScript expression: %s", runner.scripts[0])
	}
	if result.ResultJSON["message_guid"] != "GUID-1" || result.ResultJSON["verified"] != true {
		t.Fatalf("result = %#v", result.ResultJSON)
	}
	if !store.sinces[0].Before(time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("the already-sent check must reach back to the approval, got %s", store.sinces[0])
	}
}

func TestAnAlreadySentMessageIsNotSentAgain(t *testing.T) {
	runner := &recordingRunner{}
	store := &fakeStore{sent: []SentMessage{delivered}}
	result := testExecutor(runner, store).Execute(sendMutation("+13105550123", "hi"))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["already_sent"] != true {
		t.Fatalf("result = %+v", result)
	}
	if len(runner.scripts) != 0 {
		t.Fatalf("Messages must not be asked again: %q", runner.scripts)
	}
}

func TestAnUnreadableChatDBSendsNothing(t *testing.T) {
	runner := &recordingRunner{}
	store := &fakeStore{findErr: errors.New("authorization denied")}
	result := testExecutor(runner, store).Execute(sendMutation("+13105550123", "hi"))
	if result.Status != queue.StatusFailedRetryable || len(runner.scripts) != 0 {
		t.Fatalf("result = %+v, scripts = %d", result, len(runner.scripts))
	}
}

func TestTheAllowlistRefusesAnyoneElseBeforeMessagesIsAsked(t *testing.T) {
	runner := &recordingRunner{}
	store := &fakeStore{sent: []SentMessage{{}}}
	executor := testExecutor(runner, store).WithAllowedRecipients([]string{"(310) 555-0100"})
	result := executor.Execute(sendMutation("+13105550123", "hi"))
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, AllowedRecipientsEnv) {
		t.Fatalf("result = %+v", result)
	}
	if len(runner.scripts) != 0 || store.findCalls != 0 {
		t.Fatalf("a refused recipient must not reach Messages or chat.db")
	}
	allowed := executor.Execute(sendMutation("+1 310 555 0100", "hi"))
	if allowed.Status != queue.StatusSucceeded {
		t.Fatalf("the allowlisted handle should send: %+v", allowed)
	}
}

func TestAMalformedAllowlistFailsClosed(t *testing.T) {
	runner := &recordingRunner{}
	executor := testExecutor(runner, &fakeStore{sent: []SentMessage{{}}}).WithAllowedRecipients([]string{"nonsense"})
	if result := executor.Execute(sendMutation("+13105550123", "hi")); result.Status != queue.StatusFailedTerminal {
		t.Fatalf("result = %+v", result)
	}
	if len(runner.scripts) != 0 {
		t.Fatal("nothing may be sent under an allowlist that names nobody")
	}
}

func TestANewHandleIsAddressedAsAParticipant(t *testing.T) {
	runner := &recordingRunner{}
	store := &fakeStore{sent: []SentMessage{{}, delivered}}
	result := testExecutor(runner, store).Execute(sendMutation("someone@example.com", "hi"))
	if result.Status != queue.StatusSucceeded {
		t.Fatalf("result = %+v", result)
	}
	if len(runner.scripts) != 1 || !strings.Contains(runner.scripts[0], `participant "someone@example.com" of targetService`) ||
		!strings.Contains(runner.scripts[0], "service type = iMessage") {
		t.Fatalf("scripts = %q", runner.scripts)
	}
}

func TestAChatIDMessagesDoesNotKnowFallsBackToTheParticipant(t *testing.T) {
	runner := &recordingRunner{errs: []error{errors.New("Can't get chat id \"x\". (-1728)")}}
	store := &fakeStore{sent: []SentMessage{{}, delivered}, chat: Chat{GUID: "x", Service: "SMS"}, haveChat: true}
	result := testExecutor(runner, store).Execute(sendMutation("+13105550123", "hi"))
	if result.Status != queue.StatusSucceeded || len(runner.scripts) != 2 || !strings.Contains(runner.scripts[1], "participant") {
		t.Fatalf("result = %+v scripts = %q", result, runner.scripts)
	}
}

func TestATimeoutIsRetryableBecauseTheRetryChecksChatDBFirst(t *testing.T) {
	runner := &recordingRunner{errs: []error{applescript.ErrTimeout}}
	result := testExecutor(runner, &fakeStore{sent: []SentMessage{{}}}).Execute(sendMutation("+13105550123", "hi"))
	if result.Status != queue.StatusFailedRetryable {
		t.Fatalf("result = %+v", result)
	}
}

func TestAMissingAutomationGrantIsBlocked(t *testing.T) {
	runner := &recordingRunner{errs: []error{errors.New("Not authorized to send Apple events to Messages. (-1743)")}}
	result := testExecutor(runner, &fakeStore{sent: []SentMessage{{}}}).Execute(sendMutation("+13105550123", "hi"))
	if result.Status != queue.StatusBlockedMissingCredentials {
		t.Fatalf("result = %+v", result)
	}
}

func TestAFailedSendRecordedByMessagesIsTerminal(t *testing.T) {
	failed := delivered
	failed.Error = 22
	runner := &recordingRunner{}
	result := testExecutor(runner, &fakeStore{sent: []SentMessage{{}, failed}}).Execute(sendMutation("+13105550123", "hi"))
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "error 22") {
		t.Fatalf("result = %+v", result)
	}
}

func TestASendThatNeverAppearsIsRecordedUnverified(t *testing.T) {
	runner := &recordingRunner{}
	result := testExecutor(runner, &fakeStore{sent: []SentMessage{{}}}).Execute(sendMutation("+13105550123", "hi"))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["verified"] != false {
		t.Fatalf("result = %+v", result)
	}
	if len(runner.scripts) != 1 {
		t.Fatalf("an unverified send must not be repeated: %d scripts", len(runner.scripts))
	}
}

func TestUnknownOperationsAreDeferredNotBurned(t *testing.T) {
	mutation := sendMutation("+13105550123", "hi")
	mutation.Operation = "apple_messages.unsend"
	if result := testExecutor(&recordingRunner{}, &fakeStore{}).Execute(mutation); result.Status != queue.StatusFailedRetryable {
		t.Fatalf("result = %+v", result)
	}
}

func TestChatDBLookupsReadTheMessagesSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chat.db")
	db, err := common.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	approved := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	statements := []string{
		`CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT)`,
		`CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, chat_identifier TEXT, service_name TEXT, style INTEGER)`,
		`CREATE TABLE message (ROWID INTEGER PRIMARY KEY, guid TEXT, text TEXT, attributedBody BLOB, handle_id INTEGER, service TEXT, date INTEGER, is_from_me INTEGER, is_sent INTEGER, is_delivered INTEGER, error INTEGER)`,
		`CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER, message_date INTEGER)`,
		`INSERT INTO handle VALUES (1, '+13105550123')`,
		`INSERT INTO chat VALUES (1, 'SMS;-;+13105550123', '+13105550123', 'SMS', 45), (2, 'any;-;+13105550123', '+13105550123', 'iMessage', 45), (3, 'chat123', 'chat123', 'iMessage', 43)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	insert := func(rowid int, guid, text string, handle int, at time.Time, fromMe int, chat int) {
		if _, err := db.Exec(`INSERT INTO message VALUES (?, ?, ?, NULL, ?, 'iMessage', ?, ?, 1, 1, 0)`, rowid, guid, text, handle, toAppleNanos(at), fromMe); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO chat_message_join VALUES (?, ?, ?)`, chat, rowid, toAppleNanos(at)); err != nil {
			t.Fatal(err)
		}
	}
	insert(1, "OLD", "hi", 1, approved.Add(-48*time.Hour), 1, 1)
	insert(2, "INCOMING", "hi", 1, approved.Add(time.Minute), 0, 2)
	insert(3, "SENT", "hi", 0, approved.Add(2*time.Minute), 1, 2) // handle_id 0: found through the chat
	insert(4, "GROUP", "hi", 0, approved.Add(3*time.Minute), 1, 3)
	// Modern macOS leaves message.text NULL and keeps the body only in the
	// attributedBody archive: this is how every AppleScript send on porygon
	// was stored on 2026-10-10, and a text-only match never saw them.
	archived, _ := hex.DecodeString(plainAttributedBodyHex)
	if _, err := db.Exec(`INSERT INTO message VALUES (5, 'ARCHIVED', NULL, ?, 1, 'iMessage', ?, 1, 1, 1, 0)`, archived, toAppleNanos(approved.Add(4*time.Minute))); err != nil {
		t.Fatal(err)
	}
	db.Close()

	store := NewChatDBStore(path)
	sent, found, err := store.FindSent("+13105550123", "hi", approved)
	if err != nil || !found || sent.GUID != "SENT" || !sent.IsDelivered {
		t.Fatalf("FindSent = %+v %v %v", sent, found, err)
	}
	archivedSent, found, err := store.FindSent("+13105550123", "Hello from Messages", approved)
	if err != nil || !found || archivedSent.GUID != "ARCHIVED" {
		t.Fatalf("a sent row whose body lives only in attributedBody must match: %+v %v %v", archivedSent, found, err)
	}
	if _, found, _ := store.FindSent("+13105550123", "something else", approved); found {
		t.Fatal("different text must not match")
	}
	if _, found, _ := store.FindSent("+13105550123", "hi", approved.Add(5*time.Minute)); found {
		t.Fatal("a send before the window must not match")
	}
	chat, ok, err := store.OneToOneChat("+13105550123", "auto")
	if err != nil || !ok || chat.GUID != "any;-;+13105550123" {
		t.Fatalf("auto chat = %+v %v %v (want the most recently active)", chat, ok, err)
	}
	chat, ok, _ = store.OneToOneChat("+13105550123", "SMS")
	if !ok || chat.GUID != "SMS;-;+13105550123" {
		t.Fatalf("SMS chat = %+v", chat)
	}
	if _, ok, _ := store.OneToOneChat("+13105550999", "auto"); ok {
		t.Fatal("no chat expected for an unknown handle")
	}
}

// plainAttributedBodyHex is the "plain" fixture from
// uploaders/applemessages/testdata/attributed_bodies.jsonl: an NSArchiver blob
// of "Hello from Messages" produced by Foundation.
const plainAttributedBodyHex = "040b73747265616d747970656481e803840140848484124e5341747472696275746564537472696e67008484084e534f626a656374008592848484084e53537472696e67019484012b1348656c6c6f2066726f6d204d6573736167657386840269490113928484840c4e5344696374696f6e6172790094840169008686"
