package applemessages

import (
	"database/sql"
	"errors"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultChatDBPath is the Messages store the apple-messages uploader reads.
const DefaultChatDBPath = "~/Library/Messages/chat.db"

// appleEpoch is chat.db's time origin: message.date is nanoseconds since
// 2001-01-01 UTC.
var appleEpoch = time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)

func toAppleNanos(t time.Time) int64 { return t.Sub(appleEpoch).Nanoseconds() }

func fromAppleNanos(n int64) time.Time { return appleEpoch.Add(time.Duration(n)).UTC() }

// ChatDBStore reads the live chat.db read-only. These are point lookups for
// one handle, not the uploader's whole-store scan, so they read the live file
// (WAL included, which is what makes a just-sent message visible) rather than
// a snapshot.
type ChatDBStore struct {
	path string
}

// NewChatDBStore opens nothing yet; each lookup opens and closes the store so
// a resident worker never pins Messages' database.
func NewChatDBStore(path string) *ChatDBStore {
	if path == "" {
		path = DefaultChatDBPath
	}
	return &ChatDBStore{path: common.ExpandUser(path)}
}

func (s *ChatDBStore) open() (*sql.DB, error) {
	db, err := common.OpenSQLiteReadOnly(s.path)
	if err != nil {
		return nil, &common.PermissionError{Path: s.path, Err: err}
	}
	return db, nil
}

// FindSent implements Store.
func (s *ChatDBStore) FindSent(handle, text string, since time.Time) (SentMessage, bool, error) {
	db, err := s.open()
	if err != nil {
		return SentMessage{}, false, err
	}
	defer db.Close()
	return findSent(db, handle, text, since)
}

// OneToOneChat implements Store.
func (s *ChatDBStore) OneToOneChat(handle, service string) (Chat, bool, error) {
	db, err := s.open()
	if err != nil {
		return Chat{}, false, err
	}
	defer db.Close()
	return oneToOneChat(db, handle, service)
}

// findSent matches an outgoing message by its handle (a one-to-one message
// carries the recipient's handle) or by the one-to-one chat it was sent in
// (newer "any;-;" chats sometimes leave handle_id at 0 on the sender's row).
func findSent(db *sql.DB, handle, text string, since time.Time) (SentMessage, bool, error) {
	row := db.QueryRow(`
		SELECT m.guid, COALESCE(m.service, ''), m.date, COALESCE(m.is_sent, 0), COALESCE(m.is_delivered, 0), COALESCE(m.error, 0)
		FROM message AS m
		WHERE m.is_from_me = 1
		  AND m.date >= ?
		  AND m.text = ?
		  AND (
		      m.handle_id IN (SELECT ROWID FROM handle WHERE lower(id) = lower(?))
		      OR m.ROWID IN (
		          SELECT cmj.message_id
		          FROM chat_message_join AS cmj
		          JOIN chat AS c ON c.ROWID = cmj.chat_id
		          WHERE lower(c.chat_identifier) = lower(?) AND c.style = 45
		      )
		  )
		ORDER BY m.date DESC
		LIMIT 1
	`, toAppleNanos(since), text, handle, handle)
	var sent SentMessage
	var date, isSent, isDelivered int64
	if err := row.Scan(&sent.GUID, &sent.Service, &date, &isSent, &isDelivered, &sent.Error); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SentMessage{}, false, nil
		}
		return SentMessage{}, false, err
	}
	sent.SentAt = fromAppleNanos(date)
	sent.IsSent = isSent == 1
	sent.IsDelivered = isDelivered == 1
	return sent, true, nil
}

// oneToOneChat picks the most recently active one-to-one chat (style 45) with
// the handle: that is the conversation Zach would reply in by hand, on
// whatever service it already uses.
func oneToOneChat(db *sql.DB, handle, service string) (Chat, bool, error) {
	serviceFilter := ""
	args := []any{handle}
	if service != "" && service != "auto" {
		serviceFilter = "AND c.service_name = ?"
		args = append(args, service)
	}
	row := db.QueryRow(`
		SELECT c.guid, COALESCE(c.service_name, '')
		FROM chat AS c
		WHERE lower(c.chat_identifier) = lower(?) AND c.style = 45 `+serviceFilter+`
		ORDER BY COALESCE((
		    SELECT max(m.date) FROM chat_message_join AS cmj
		    JOIN message AS m ON m.ROWID = cmj.message_id
		    WHERE cmj.chat_id = c.ROWID
		), 0) DESC
		LIMIT 1
	`, args...)
	var chat Chat
	if err := row.Scan(&chat.GUID, &chat.Service); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Chat{}, false, nil
		}
		return Chat{}, false, err
	}
	return chat, true, nil
}
