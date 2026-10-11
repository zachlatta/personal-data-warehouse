package applemessages

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/applescript"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

// MarkReadOperation marks one one-to-one conversation read.
//
// Messages has no AppleScript verb for read state, and the private IMCore
// API that has one is closed to unentitled processes while SIP is on. What
// does work is what Zach does by hand: show the conversation in Messages'
// window. Messages then marks it read in chat.db and syncs that through
// iCloud. Two consequences shape this executor:
//
//   - The window must actually be on screen. While the Mac's screen is locked,
//     opening the conversation changes nothing (tested on porygon 2026-10-11:
//     `open sms:<handle>` under the lock screen left the message unread), so a
//     locked screen is failed_retryable and the row runs once it is unlocked.
//   - A conversation left on screen keeps marking new texts read, so the window
//     is closed again afterwards unless Messages already had one open.
const MarkReadOperation = "apple_messages.mark_read"

var (
	shortCodePattern = regexp.MustCompile(`^[0-9]{3,8}$`)
	groupChatPattern = regexp.MustCompile(`^chat[0-9]+$`)
	markReadWait     = 20 * time.Second
)

// NormalizeChat is the executor's twin of the proposal-side normalization: a
// short code passes through, anything else must be a handle.
func NormalizeChat(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if shortCodePattern.MatchString(trimmed) {
		return trimmed, nil
	}
	if groupChatPattern.MatchString(strings.ToLower(trimmed)) || strings.Contains(trimmed, ";+;") {
		return "", fmt.Errorf("chat %q is a group conversation; only one-to-one conversations can be marked read", value)
	}
	return NormalizeHandle(trimmed)
}

func (e *Executor) markRead(mutation queue.Mutation) queue.Result {
	chat, err := NormalizeChat(stringValue(mutation.Payload["chat"]))
	if err != nil {
		return queue.Result{Status: queue.StatusFailedTerminal, Error: err.Error()}
	}
	before, err := e.store.UnreadCount(chat, e.now().Add(time.Minute))
	if err != nil {
		return queue.Result{Status: queue.StatusFailedRetryable, Error: fmt.Sprintf("could not read chat.db to count unread messages: %v", err)}
	}
	if before == 0 {
		return queue.Result{Status: queue.StatusSucceeded, ResultJSON: map[string]any{"chat": chat, "marked_read": 0, "already_read": true}}
	}
	if _, found, err := e.store.OneToOneChat(chat, "auto"); err != nil {
		return queue.Result{Status: queue.StatusFailedRetryable, Error: fmt.Sprintf("could not read chat.db to find the conversation: %v", err)}
	} else if !found {
		return queue.Result{Status: queue.StatusFailedTerminal, Error: fmt.Sprintf("no one-to-one conversation with %s on this Mac", chat)}
	}
	if e.screenLocked != nil {
		locked, err := e.screenLocked()
		if err != nil {
			return queue.Result{Status: queue.StatusFailedRetryable, Error: fmt.Sprintf("could not tell whether the screen is locked: %v", err)}
		}
		if locked {
			return queue.Result{
				Status: queue.StatusFailedRetryable,
				Error:  fmt.Sprintf("this Mac's screen is locked; Messages marks a conversation read only while it is on screen, so %d unread message(s) wait until it is unlocked", before),
			}
		}
	}
	if e.readiness != nil {
		if err := e.readiness(); err != nil {
			return queue.Result{Status: queue.StatusFailedRetryable, Error: err.Error()}
		}
	}
	// Leave a window Zach opened himself alone; close only the one we open.
	windowsBefore := -1
	if out, err := e.runner(countWindowsScript()); err == nil {
		fmt.Sscanf(strings.TrimSpace(out), "%d", &windowsBefore)
	}
	opened := e.now()
	if err := e.opener(conversationURL(chat)); err != nil {
		return queue.Result{Status: queue.StatusFailedRetryable, Error: fmt.Sprintf("could not open the conversation in Messages: %v", err)}
	}
	remaining := before
	deadline := opened.Add(markReadWait)
	for {
		e.sleep(verifyPoll)
		count, err := e.store.UnreadCount(chat, opened)
		if err == nil {
			remaining = count
			if remaining == 0 {
				break
			}
		}
		if !e.now().Before(deadline) {
			break
		}
	}
	closed := false
	if windowsBefore == 0 {
		if _, err := e.runner(closeWindowsScript()); err == nil {
			closed = true
		}
	}
	result := map[string]any{
		"chat":          chat,
		"unread_before": before,
		"marked_read":   before - remaining,
		"window_closed": closed,
	}
	switch {
	case remaining == 0:
		result["verified"] = true
		return queue.Result{Status: queue.StatusSucceeded, ResultJSON: result}
	case remaining < before:
		result["verified"] = false
		result["still_unread"] = remaining
		result["note"] = "Messages marked some but not all of the conversation read within 20s"
		return queue.Result{Status: queue.StatusSucceeded, ResultJSON: result}
	default:
		return queue.Result{
			Status:     queue.StatusFailedRetryable,
			Error:      fmt.Sprintf("Messages showed the conversation but chat.db still has %d unread message(s) after %ds", remaining, int(markReadWait.Seconds())),
			ResultJSON: result,
		}
	}
}

// conversationURL is the LaunchServices URL that makes Messages show the
// one-to-one conversation with a handle (phone, email, or short code).
func conversationURL(chat string) string {
	return "sms:" + chat
}

func countWindowsScript() string {
	return applescript.Dedent(fmt.Sprintf(`
		tell application "Messages"
		with timeout of %d seconds
		return count of windows
		end timeout
		end tell
	`, applescript.InScriptTimeoutSeconds))
}

func closeWindowsScript() string {
	return applescript.Dedent(fmt.Sprintf(`
		tell application "Messages"
		with timeout of %d seconds
		close every window
		end timeout
		end tell
		return "ok"
	`, applescript.InScriptTimeoutSeconds))
}

// OpenURL hands a URL to LaunchServices (`open`), which needs no Automation
// grant.
func OpenURL(url string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "/usr/bin/open", url).CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message == "" {
			message = err.Error()
		}
		return fmt.Errorf("open %s: %s", url, message)
	}
	return nil
}

// ScreenLocked reads the console session's lock flag from the IORegistry,
// which any process in the user's session can read without a TCC grant.
func ScreenLocked() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "/usr/sbin/ioreg", "-n", "Root", "-d1", "-a").Output()
	if err != nil {
		return false, err
	}
	return screenLockedFromIORegistry(output), nil
}

func screenLockedFromIORegistry(plist []byte) bool {
	index := bytes.Index(plist, []byte("<key>CGSSessionScreenIsLocked</key>"))
	if index < 0 {
		return false
	}
	rest := bytes.TrimSpace(plist[index+len("<key>CGSSessionScreenIsLocked</key>"):])
	return bytes.HasPrefix(rest, []byte("<true/>"))
}

// UnreadCount implements Store.
func (s *ChatDBStore) UnreadCount(chat string, through time.Time) (int, error) {
	db, err := s.open()
	if err != nil {
		return 0, err
	}
	defer db.Close()
	return unreadCount(db, chat, through)
}

// unreadCount counts incoming, unread, ordinary messages (not tapbacks or
// system rows) in the one-to-one chats with this identifier, up to through.
func unreadCount(db *sql.DB, chat string, through time.Time) (int, error) {
	var count int
	err := db.QueryRow(`
		SELECT count(DISTINCT m.ROWID)
		FROM message AS m
		JOIN chat_message_join AS cmj ON cmj.message_id = m.ROWID
		JOIN chat AS c ON c.ROWID = cmj.chat_id
		WHERE lower(c.chat_identifier) = lower(?)
		  AND c.style = 45
		  AND m.is_from_me = 0
		  AND m.is_read = 0
		  AND COALESCE(m.item_type, 0) = 0
		  AND COALESCE(m.associated_message_type, 0) = 0
		  AND m.date <= ?
	`, chat, toAppleNanos(through)).Scan(&count)
	return count, err
}
