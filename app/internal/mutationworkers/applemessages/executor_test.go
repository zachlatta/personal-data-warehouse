package applemessages

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

type fakeStore struct {
	chat     Chat
	haveChat bool
	chatErr  error
	// unread is returned by UnreadCount in order; then the last repeats.
	unread      []int
	unreadCalls int
	unreadErr   error
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

func TestARetiredSendIsRefusedWithoutAskingMessages(t *testing.T) {
	runner := &recordingRunner{}
	result := testExecutor(runner, &fakeStore{haveChat: true}).Execute(sendMutation("+13105550123", "hi"))
	if result.Status != queue.StatusFailedTerminal || !strings.Contains(result.Error, "android_messages.send_message") {
		t.Fatalf("result = %+v", result)
	}
	if len(runner.scripts) != 0 {
		t.Fatalf("Messages was asked to run %d scripts for a retired send", len(runner.scripts))
	}
}

func TestUnknownOperationsAreDeferredNotBurned(t *testing.T) {
	mutation := sendMutation("+13105550123", "hi")
	mutation.Operation = "apple_messages.unsend"
	if result := testExecutor(&recordingRunner{}, &fakeStore{}).Execute(mutation); result.Status != queue.StatusFailedRetryable {
		t.Fatalf("result = %+v", result)
	}
}

func TestOneToOneChatReadsTheMessagesSchema(t *testing.T) {
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
	insert(3, "GROUP", "hi", 0, approved.Add(3*time.Minute), 1, 3)
	db.Close()

	store := NewChatDBStore(path)
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
