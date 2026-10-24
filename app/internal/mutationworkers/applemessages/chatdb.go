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

// OneToOneChat implements Store.
func (s *ChatDBStore) OneToOneChat(handle, service string) (Chat, bool, error) {
	db, err := s.open()
	if err != nil {
		return Chat{}, false, err
	}
	defer db.Close()
	return oneToOneChat(db, handle, service)
}

// findSentCandidateLimit bounds how many outgoing rows to the handle since
// approval are decoded. A person texted hundreds of times in the window since
// one approval is not a real shape; past the bound the check fails rather than
// guess.
const findSentCandidateLimit = 500

// findSent matches an outgoing message by its handle (a one-to-one message
// carries the recipient's handle) or by the one-to-one chat it was sent in
// (newer "any;-;" chats sometimes leave handle_id at 0 on the sender's row).
//
// The body is compared in Go, not SQL, because modern macOS leaves
// message.text NULL and keeps the words only in the attributedBody archive:
// on 2026-10-10 every AppleScript send on porygon was stored that way, and a
// `text = ?` match saw none of them — which is both an unverified result and
// a broken already-sent check.
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
