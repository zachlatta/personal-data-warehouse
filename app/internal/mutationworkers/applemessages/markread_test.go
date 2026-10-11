package applemessages

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

func markReadMutation(chat string) queue.Mutation {
	return queue.Mutation{
		ID: "m2", Provider: Provider, Operation: MarkReadOperation, Account: "you@example.com",
		Payload: map[string]any{"chat": chat},
	}
}

type markReadHarness struct {
	runner  *recordingRunner
	store   *fakeStore
	opened  []string
	locked  bool
	windows string
}

func (h *markReadHarness) executor() *Executor {
	runner := func(script string) (string, error) {
		h.runner.scripts = append(h.runner.scripts, script)
		if strings.Contains(script, "count of windows") {
			return h.windows, nil
		}
		return "ok", nil
	}
	executor := NewExecutor(runner, h.store)
	clock := time.Date(2026, 10, 11, 3, 0, 0, 0, time.UTC)
	executor.now = func() time.Time { return clock }
	executor.sleep = func(d time.Duration) { clock = clock.Add(d) }
	executor.opener = func(url string) error { h.opened = append(h.opened, url); return nil }
	executor.screenLocked = func() (bool, error) { return h.locked, nil }
	return executor
}

func newHarness(unread ...int) *markReadHarness {
	return &markReadHarness{
		runner:  &recordingRunner{},
		store:   &fakeStore{unread: unread, chat: Chat{GUID: "SMS;-;72975", Service: "SMS"}, haveChat: true},
		windows: "0",
	}
}

func TestMarkReadOpensTheConversationWaitsForChatDBAndClosesTheWindow(t *testing.T) {
	h := newHarness(3, 3, 0)
	result := h.executor().Execute(markReadMutation("72975"))
	if result.Status != queue.StatusSucceeded {
		t.Fatalf("status = %s (%s)", result.Status, result.Error)
	}
	if len(h.opened) != 1 || h.opened[0] != "sms:72975" {
		t.Fatalf("opened = %v", h.opened)
	}
	if result.ResultJSON["marked_read"] != 3 || result.ResultJSON["verified"] != true || result.ResultJSON["window_closed"] != true {
		t.Fatalf("result = %#v", result.ResultJSON)
	}
	if last := h.runner.scripts[len(h.runner.scripts)-1]; !strings.Contains(last, "close every window") {
		t.Fatalf("the window must be closed so later texts are not read silently; last script %q", last)
	}
}

func TestMarkReadLeavesAWindowZachHadOpen(t *testing.T) {
	h := newHarness(1, 0)
	h.windows = "1"
	result := h.executor().Execute(markReadMutation("+1 (310) 555-0123"))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["window_closed"] != false {
		t.Fatalf("result = %s %#v", result.Status, result.ResultJSON)
	}
	if h.opened[0] != "sms:+13105550123" {
		t.Fatalf("opened = %v", h.opened)
	}
	for _, script := range h.runner.scripts {
		if strings.Contains(script, "close every window") {
			t.Fatal("closed a window that was already open")
		}
	}
}

func TestMarkReadWithNothingUnreadOpensNothing(t *testing.T) {
	h := newHarness(0)
	result := h.executor().Execute(markReadMutation("72975"))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["already_read"] != true || len(h.opened) != 0 {
		t.Fatalf("result = %s %#v opened=%v", result.Status, result.ResultJSON, h.opened)
	}
}

func TestMarkReadWaitsWhileTheScreenIsLocked(t *testing.T) {
	h := newHarness(2)
	h.locked = true
	result := h.executor().Execute(markReadMutation("72975"))
	if result.Status != queue.StatusFailedRetryable || !strings.Contains(result.Error, "locked") || len(h.opened) != 0 {
		t.Fatalf("result = %s %q opened=%v", result.Status, result.Error, h.opened)
	}
}

func TestMarkReadThatChangesNothingIsRetryable(t *testing.T) {
	h := newHarness(2)
	result := h.executor().Execute(markReadMutation("72975"))
	if result.Status != queue.StatusFailedRetryable || !strings.Contains(result.Error, "still has 2 unread") {
		t.Fatalf("result = %s %q", result.Status, result.Error)
	}
}

func TestMarkReadPartialProgressSucceedsUnverified(t *testing.T) {
	h := newHarness(5, 1)
	result := h.executor().Execute(markReadMutation("72975"))
	if result.Status != queue.StatusSucceeded || result.ResultJSON["verified"] != false || result.ResultJSON["still_unread"] != 1 {
		t.Fatalf("result = %s %#v", result.Status, result.ResultJSON)
	}
}

func TestMarkReadRefusesGroupsAndUnknownConversations(t *testing.T) {
	h := newHarness(1)
	if result := h.executor().Execute(markReadMutation("chat123456")); result.Status != queue.StatusFailedTerminal {
		t.Fatalf("group: %s", result.Status)
	}
	h = newHarness(1)
	h.store.haveChat = false
	if result := h.executor().Execute(markReadMutation("+13105550123")); result.Status != queue.StatusFailedTerminal || len(h.opened) != 0 {
		t.Fatalf("unknown chat: %s opened=%v", result.Status, h.opened)
	}
	h = newHarness(1)
	h.store.unreadErr = errors.New("authorization denied")
	if result := h.executor().Execute(markReadMutation("72975")); result.Status != queue.StatusFailedRetryable {
		t.Fatalf("unreadable chat.db: %s", result.Status)
	}
}

func TestNormalizeChatKeepsEmailsThatStartWithChat(t *testing.T) {
	if got, err := NormalizeChat("ChatBot@Example.com"); err != nil || got != "chatbot@example.com" {
		t.Fatalf("NormalizeChat = %q, %v", got, err)
	}
}

func TestScreenLockedFromIORegistry(t *testing.T) {
	locked := []byte("<dict>\n\t<key>CGSSessionScreenIsLocked</key>\n\t<true/>\n</dict>")
	unlocked := []byte("<dict>\n\t<key>kCGSSessionOnConsoleKey</key>\n\t<true/>\n</dict>")
	if !screenLockedFromIORegistry(locked) || screenLockedFromIORegistry(unlocked) {
		t.Fatal("lock flag misread")
	}
}

func TestUnreadCountReadsTheMessagesSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chat.db")
	db, err := common.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 11, 3, 0, 0, 0, time.UTC)
	for _, statement := range []string{
		`CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, chat_identifier TEXT, service_name TEXT, style INTEGER)`,
		`CREATE TABLE message (ROWID INTEGER PRIMARY KEY, is_from_me INTEGER, is_read INTEGER, item_type INTEGER, associated_message_type INTEGER, date INTEGER)`,
		`CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER, message_date INTEGER)`,
		`INSERT INTO chat VALUES (1, 'SMS;-;72975', '72975', 'SMS', 45), (2, 'iMessage;+;chat1', 'chat1', 'iMessage', 43)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	insert := func(rowid, fromMe, read, itemType, assoc int, at time.Time, chat int) {
		if _, err := db.Exec(`INSERT INTO message VALUES (?, ?, ?, ?, ?, ?)`, rowid, fromMe, read, itemType, assoc, toAppleNanos(at)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO chat_message_join VALUES (?, ?, ?)`, chat, rowid, toAppleNanos(at)); err != nil {
			t.Fatal(err)
		}
	}
	insert(1, 0, 0, 0, 0, now.Add(-time.Hour), 1)    // counts
	insert(2, 0, 1, 0, 0, now.Add(-time.Hour), 1)    // read
	insert(3, 1, 0, 0, 0, now.Add(-time.Hour), 1)    // mine
	insert(4, 0, 0, 0, 2000, now.Add(-time.Hour), 1) // tapback
	insert(5, 0, 0, 0, 0, now.Add(time.Hour), 1)     // after through
	insert(6, 0, 0, 0, 0, now.Add(-time.Hour), 2)    // group
	db.Close()
	count, err := NewChatDBStore(path).UnreadCount("72975", now)
	if err != nil || count != 1 {
		t.Fatalf("UnreadCount = %d, %v", count, err)
	}
}
