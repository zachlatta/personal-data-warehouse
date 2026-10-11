package mutations

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// apple_messages.mark_read marks one one-to-one Messages conversation read on
// Zach's Mac, the way he would by opening it: Messages has no AppleScript verb
// for read state, so the executor (`pdw mutations apple-messages`) shows the
// conversation in Messages.app, waits for chat.db to record it read, and closes
// the window again. iCloud then carries the read state to his other devices.

// appleMessagesShortCodePattern is a carrier short code or a short numeric
// sender ("72975", "227898"): automated texts (login codes, deliveries) live in
// one-to-one chats keyed by these, and they are the bulk of what goes unread.
var appleMessagesShortCodePattern = regexp.MustCompile(`^[0-9]{3,8}$`)

// appleMessagesGroupPattern is chat.db's identifier for a group conversation.
var appleMessagesGroupPattern = regexp.MustCompile(`^chat[0-9]+$`)

const appleMessagesUnreadPreviewLimit = 10

func isAppleMessagesSendMutation(mutation Mutation) bool {
	return mutation.Operation == AppleMessagesSendMessageOperation ||
		(mutation.Provider == AppleMessagesProvider && mutation.Operation == "")
}

func isAppleMessagesMarkReadMutation(mutation Mutation) bool {
	return mutation.Operation == AppleMessagesMarkReadOperation
}

// normalizeAppleMessagesChat turns the `chat` an agent names into the
// chat.db chat_identifier of a one-to-one conversation: an E.164 phone, a
// lowercased email (iMessage or a business-chat sender), or a short code.
// A group chat is refused: Messages can only be pointed at a group from its
// window by name, which is too easy to get wrong for a write.
func normalizeAppleMessagesChat(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", errors.New("must include chat: the one-to-one conversation's handle (an E.164 phone like +13105550123, an email address, or a short code like 72975)")
	}
	if appleMessagesGroupPattern.MatchString(strings.ToLower(trimmed)) || strings.Contains(trimmed, ";+;") {
		return "", fmt.Errorf("chat %q is a group conversation; apple_messages.mark_read supports one-to-one conversations only", value)
	}
	if appleMessagesShortCodePattern.MatchString(trimmed) {
		return trimmed, nil
	}
	handle, err := normalizeAppleMessagesRecipient(trimmed)
	if err != nil {
		return "", fmt.Errorf("chat %q is not a phone number, email address, or short code Messages keys a conversation by", value)
	}
	return handle, nil
}

func validateAppleMessagesMarkRead(mutation MutationInput) error {
	_, err := normalizeAppleMessagesChat(appleMessagesChatInput(mutation))
	return err
}

// appleMessagesChatInput accepts `chat` and, as a convenience for agents that
// just proposed a send, `recipient`.
func appleMessagesChatInput(mutation MutationInput) string {
	if strings.TrimSpace(mutation.Chat) != "" {
		return mutation.Chat
	}
	return mutation.Recipient
}

func appleMessagesMarkReadPayload(mutation MutationInput) map[string]any {
	chat, _ := normalizeAppleMessagesChat(appleMessagesChatInput(mutation))
	return map[string]any{"chat": chat}
}

func appleMessagesMarkReadTitle(mutation MutationInput) string {
	chat, err := normalizeAppleMessagesChat(appleMessagesChatInput(mutation))
	if err != nil {
		chat = strings.TrimSpace(appleMessagesChatInput(mutation))
	}
	return "Mark texts from " + chat + " read"
}

func appleMessagesMarkReadPreview(payload map[string]any) map[string]any {
	return map[string]any{
		"chat":   stringFromAny(payload["chat"]),
		"effect": "Opens this conversation in Messages on Zach's Mac so every message in it is marked read, then closes the window. The read state syncs to his iPhone, and senders with read receipts on see that he read it.",
	}
}

type appleMessagesChatInfo struct {
	names       []string
	unread      []map[string]any
	unreadCount int64
	ok          bool
}

// hydrateAppleMessagesMarkReadPreviews names the conversation from the
// contact book and lists what is unread in it now, on the read path, because
// new messages can arrive between proposal and review.
func (s *PostgresStore) hydrateAppleMessagesMarkReadPreviews(ctx context.Context, mutations []Mutation) []Mutation {
	if s == nil || s.db == nil {
		return mutations
	}
	infos := map[appleMessagesTarget]*appleMessagesChatInfo{}
	for _, mutation := range mutations {
		if !isAppleMessagesMarkReadMutation(mutation) {
			continue
		}
		target := appleMessagesTarget{account: mutation.Account, recipient: stringFromAny(mutation.Payload["chat"])}
		if target.recipient == "" || infos[target] != nil {
			continue
		}
		info := &appleMessagesChatInfo{}
		infos[target] = info
		nameRows, err := queryContext(ctx, s.db, `
			SELECT DISTINCT display_name
			FROM @clean_contact_points
			WHERE normalized_value = $1 AND COALESCE(display_name, '') <> ''
			ORDER BY display_name
			LIMIT 5
		`, appleMessagesContactKey(target.recipient))
		if err != nil {
			return mutations
		}
		for nameRows.Next() {
			var name string
			if err := nameRows.Scan(&name); err == nil {
				info.names = append(info.names, name)
			}
		}
		nameRows.Close()
		rows, err := queryContext(ctx, s.db, `
			SELECT message.message_at, message.service, LEFT(COALESCE(message.body_text, ''), 300),
			       COUNT(*) OVER () AS unread_count
			FROM @apple_message_chats AS chat
			JOIN @apple_message_chat_messages AS link
			  ON link.account = chat.account AND link.chat_id = chat.chat_id
			JOIN @apple_messages AS message
			  ON message.account = link.account AND message.message_id = link.message_id
			WHERE chat.account = $1
			  AND lower(chat.chat_identifier) = lower($2)
			  AND chat.style = 45
			  AND message.is_from_me = 0
			  AND message.is_read = 0
			  AND message.is_deleted = 0
			  AND message.message_item_type = 0
			  AND COALESCE(message.associated_message_type, 0) = 0
			ORDER BY message.message_at DESC
			LIMIT $3
		`, target.account, target.recipient, appleMessagesUnreadPreviewLimit)
		if err != nil {
			continue
		}
		info.ok = true
		for rows.Next() {
			var at time.Time
			var service, body string
			var count int64
			if err := rows.Scan(&at, &service, &body, &count); err != nil {
				continue
			}
			info.unreadCount = count
			info.unread = append(info.unread, map[string]any{
				"message_at": at.UTC().Format(time.RFC3339),
				"service":    service,
				"text":       body,
			})
		}
		rows.Close()
	}
	return applyAppleMessagesChatInfo(mutations, infos)
}

func applyAppleMessagesChatInfo(mutations []Mutation, infos map[appleMessagesTarget]*appleMessagesChatInfo) []Mutation {
	for index := range mutations {
		mutation := &mutations[index]
		if !isAppleMessagesMarkReadMutation(*mutation) {
			continue
		}
		target := appleMessagesTarget{account: mutation.Account, recipient: stringFromAny(mutation.Payload["chat"])}
		info, ok := infos[target]
		if !ok {
			continue
		}
		preview := cloneMap(mutation.Preview)
		view := cloneMap(mapFromAny(preview["mark_read"]))
		if len(view) == 0 {
			view = appleMessagesMarkReadPreview(mutation.Payload)
		}
		names := append([]string{}, info.names...)
		view["chat_names"] = names
		if len(names) > 0 {
			view["chat_name"] = strings.Join(names, " / ")
		} else {
			delete(view, "chat_name")
		}
		view["unread_messages"] = append([]map[string]any{}, info.unread...)
		view["unread_count"] = info.unreadCount
		warnings := []string{}
		if info.ok && info.unreadCount == 0 {
			warnings = append(warnings, "The warehouse shows nothing unread in this conversation right now; approving changes nothing unless new texts arrive first.")
		}
		view["warnings"] = warnings
		preview["mark_read"] = view
		mutation.Preview = preview
	}
	return mutations
}
