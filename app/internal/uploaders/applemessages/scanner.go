// Package applemessages uploads the Mac's Messages store (chat.db) and its
// attachments through the app's /ingest/apple-messages endpoints.
package applemessages

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultStorePath is the Messages database.
const DefaultStorePath = "~/Library/Messages/chat.db"

// Handle is one row of handle.
type Handle struct {
	HandleID          string
	HandleRowID       int64
	Address           string
	Country           string
	Service           string
	UncanonicalizedID string
	PersonCentricID   string
	Raw               map[string]any
}

// Chat is one row of chat.
type Chat struct {
	ChatID            string
	ChatRowID         int64
	GUID              string
	ChatIdentifier    string
	ServiceName       string
	DisplayName       string
	RoomName          string
	AccountLogin      string
	Style             int64
	State             int64
	IsArchived        bool
	IsFiltered        bool
	IsRecovered       bool
	IsPendingReview   bool
	LastReadMessageAt time.Time
	Raw               map[string]any
}

// ChatHandle links a chat to a handle.
type ChatHandle struct {
	ChatID   string
	HandleID string
	Raw      map[string]any
}

// ChatMessage links a chat to a message.
type ChatMessage struct {
	ChatID        string
	MessageID     string
	MessageDate   time.Time
	MessageDateNS int64
	Raw           map[string]any
}

// Message is one row of message.
type Message struct {
	MessageID              string
	MessageRowID           int64
	HandleID               string
	Service                string
	MessageAccount         string
	Text                   string
	AttributedBody         []byte
	AttributedBodySHA256   string
	Subject                string
	Country                string
	MessageType            int64
	ItemType               int64
	IsFromMe               bool
	IsRead                 bool
	IsSent                 bool
	IsDelivered            bool
	IsFinished             bool
	IsSystemMessage        bool
	IsServiceMessage       bool
	IsForward              bool
	IsEmpty                bool
	IsAudioMessage         bool
	IsPlayed               bool
	CacheHasAttachments    bool
	HasUnseenMention       bool
	IsSpam                 bool
	ReplyToGUID            string
	AssociatedMessageGUID  string
	AssociatedMessageType  int64
	AssociatedMessageEmoji string
	BalloonBundleID        string
	GroupTitle             string
	GroupActionType        int64
	MessageActionType      int64
	MessageSource          int64
	ExpressiveSendStyleID  string
	MessageAt              time.Time
	DateNS                 int64
	DateRead               time.Time
	DateDelivered          time.Time
	DatePlayed             time.Time
	DateEdited             time.Time
	DateRetracted          time.Time
	DateRecovered          time.Time
	Raw                    map[string]any
}

// Attachment is one row of attachment joined to its message.
type Attachment struct {
	AttachmentID    string
	AttachmentRowID int64
	MessageID       string
	GUID            string
	OriginalGUID    string
	Filename        string
	ResolvedPath    string // "" when not locally available
	TransferName    string
	ContentType     string
	UTI             string
	MimeType        string
	TotalBytes      int64
	SizeBytes       int64
	IsMissing       bool
	Error           string
	IsOutgoing      bool
	IsSticker       bool
	HideAttachment  bool
	TransferState   int64
	CreatedAt       time.Time
	StartAt         time.Time
	Raw             map[string]any
}

// DeletedMessage is a tombstone from deleted_messages / sync_deleted_messages.
type DeletedMessage struct {
	MessageID string
	DeletedAt time.Time
	Raw       map[string]any
}

// Snapshot is everything scanned from one chat.db copy.
type Snapshot struct {
	Handles         []Handle
	Chats           []Chat
	ChatHandles     []ChatHandle
	Messages        []Message
	ChatMessages    []ChatMessage
	Attachments     []Attachment
	DeletedMessages []DeletedMessage
}

// SnapshotStore copies the live chat.db into destinationDir/chat.db.
func SnapshotStore(storePath, destinationDir string) (string, error) {
	destination := filepath.Join(destinationDir, "chat.db")
	if err := common.SnapshotSQLite(common.ExpandUser(storePath), destination); err != nil {
		return "", err
	}
	return destination, nil
}

// Scan reads a (snapshotted) store; messagesRoot resolves relative
// attachment paths (defaults to the store's directory).
func Scan(storePath, messagesRoot string) (*Snapshot, error) {
	root := messagesRoot
	if root == "" {
		root = filepath.Dir(common.ExpandUser(storePath))
	} else {
		root = common.ExpandUser(root)
	}
	db, err := common.OpenSQLite(storePath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tables, err := common.TableNames(db)
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, required := range []string{"attachment", "chat", "chat_handle_join", "chat_message_join", "handle", "message"} {
		if !tables[required] {
			missing = append(missing, required)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("Unsupported Apple Messages schema: missing %s", strings.Join(missing, ", "))
	}
	handles, err := scanHandles(db)
	if err != nil {
		return nil, err
	}
	handleIDs := map[int64]string{}
	for _, handle := range handles {
		handleIDs[handle.HandleRowID] = handle.HandleID
	}
	chats, err := scanChats(db)
	if err != nil {
		return nil, err
	}
	chatIDs := map[int64]string{}
	for _, chat := range chats {
		chatIDs[chat.ChatRowID] = chat.ChatID
	}
	messages, err := scanMessages(db, handleIDs)
	if err != nil {
		return nil, err
	}
	messageIDs := map[int64]string{}
	for _, message := range messages {
		messageIDs[message.MessageRowID] = message.MessageID
	}
	chatHandles, err := scanChatHandles(db, chatIDs, handleIDs)
	if err != nil {
		return nil, err
	}
	chatMessages, err := scanChatMessages(db, chatIDs, messageIDs)
	if err != nil {
		return nil, err
	}
	attachments, err := scanAttachments(db, root, messageIDs)
	if err != nil {
		return nil, err
	}
	deleted, err := scanDeletedMessages(db, tables)
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		Handles: handles, Chats: chats, ChatHandles: chatHandles, Messages: messages,
		ChatMessages: chatMessages, Attachments: attachments, DeletedMessages: deleted,
	}, nil
}

func scanHandles(db *sql.DB) ([]Handle, error) {
	rows, err := common.SelectAll(db, "handle", true)
	if err != nil {
		return nil, err
	}
	var handles []Handle
	for _, row := range rows {
		rowID := row.Int("ROWID")
		handles = append(handles, Handle{
			HandleID:          fmt.Sprint(rowID),
			HandleRowID:       rowID,
			Address:           row.String("id"),
			Country:           row.String("country"),
			Service:           row.String("service"),
			UncanonicalizedID: row.String("uncanonicalized_id"),
			PersonCentricID:   row.String("person_centric_id"),
			Raw:               row.Public(),
		})
	}
	return handles, nil
}

func scanChats(db *sql.DB) ([]Chat, error) {
	rows, err := common.SelectAll(db, "chat", true)
	if err != nil {
		return nil, err
	}
	var chats []Chat
	for _, row := range rows {
		rowID := row.Int("ROWID")
		guid := row.String("guid")
		if guid == "" {
			guid = fmt.Sprint(rowID)
		}
		chats = append(chats, Chat{
			ChatID:            guid,
			ChatRowID:         rowID,
			GUID:              guid,
			ChatIdentifier:    row.String("chat_identifier"),
			ServiceName:       row.String("service_name"),
			DisplayName:       row.String("display_name"),
			RoomName:          row.String("room_name"),
			AccountLogin:      row.String("account_login"),
			Style:             row.Int("style"),
			State:             row.Int("state"),
			IsArchived:        row.Bool("is_archived"),
			IsFiltered:        row.Bool("is_filtered"),
			IsRecovered:       row.Bool("is_recovered"),
			IsPendingReview:   row.Bool("is_pending_review"),
			LastReadMessageAt: common.AppleTimestamp(row.Int("last_read_message_timestamp")),
			Raw:               row.Public(),
		})
	}
	return chats, nil
}

func scanChatHandles(db *sql.DB, chatIDs, handleIDs map[int64]string) ([]ChatHandle, error) {
	rows, err := common.SelectAll(db, "chat_handle_join", true)
	if err != nil {
		return nil, err
	}
	var out []ChatHandle
	for _, row := range rows {
		chatID := chatIDs[row.Int("chat_id")]
		handleID := handleIDs[row.Int("handle_id")]
		if chatID != "" && handleID != "" {
			out = append(out, ChatHandle{ChatID: chatID, HandleID: handleID, Raw: row.Public()})
		}
	}
	return out, nil
}

func scanMessages(db *sql.DB, handleIDs map[int64]string) ([]Message, error) {
	rows, err := common.SelectAll(db, "message", true)
	if err != nil {
		return nil, err
	}
	var messages []Message
	for _, row := range rows {
		rowID := row.Int("ROWID")
		guid := row.String("guid")
		if guid == "" {
			guid = fmt.Sprint(rowID)
		}
		attributedBody := row.Bytes("attributedBody")
		dateNS := row.Int("date")
		sha := ""
		if len(attributedBody) > 0 {
			sha = common.BytesSHA256(attributedBody)
		}
		messages = append(messages, Message{
			MessageID:              guid,
			MessageRowID:           rowID,
			HandleID:               handleIDs[row.Int("handle_id")],
			Service:                row.String("service"),
			MessageAccount:         row.String("account"),
			Text:                   row.String("text"),
			AttributedBody:         attributedBody,
			AttributedBodySHA256:   sha,
			Subject:                row.String("subject"),
			Country:                row.String("country"),
			MessageType:            row.Int("type"),
			ItemType:               row.Int("item_type"),
			IsFromMe:               row.Bool("is_from_me"),
			IsRead:                 row.Bool("is_read"),
			IsSent:                 row.Bool("is_sent"),
			IsDelivered:            row.Bool("is_delivered"),
			IsFinished:             row.Bool("is_finished"),
			IsSystemMessage:        row.Bool("is_system_message"),
			IsServiceMessage:       row.Bool("is_service_message"),
			IsForward:              row.Bool("is_forward"),
			IsEmpty:                row.Bool("is_empty"),
			IsAudioMessage:         row.Bool("is_audio_message"),
			IsPlayed:               row.Bool("is_played"),
			CacheHasAttachments:    row.Bool("cache_has_attachments"),
			HasUnseenMention:       row.Bool("has_unseen_mention"),
			IsSpam:                 row.Bool("is_spam"),
			ReplyToGUID:            row.String("reply_to_guid"),
			AssociatedMessageGUID:  row.String("associated_message_guid"),
			AssociatedMessageType:  row.Int("associated_message_type"),
			AssociatedMessageEmoji: row.String("associated_message_emoji"),
			BalloonBundleID:        row.String("balloon_bundle_id"),
			GroupTitle:             row.String("group_title"),
			GroupActionType:        row.Int("group_action_type"),
			MessageActionType:      row.Int("message_action_type"),
			MessageSource:          row.Int("message_source"),
			ExpressiveSendStyleID:  row.String("expressive_send_style_id"),
			MessageAt:              common.AppleTimestamp(dateNS),
			DateNS:                 dateNS,
			DateRead:               common.AppleTimestamp(row.Int("date_read")),
			DateDelivered:          common.AppleTimestamp(row.Int("date_delivered")),
			DatePlayed:             common.AppleTimestamp(row.Int("date_played")),
			DateEdited:             common.AppleTimestamp(row.Int("date_edited")),
			DateRetracted:          common.AppleTimestamp(row.Int("date_retracted")),
			DateRecovered:          common.AppleTimestamp(row.Int("date_recovered")),
			Raw:                    row.PublicWithoutBlobs(),
		})
	}
	return messages, nil
}

func scanChatMessages(db *sql.DB, chatIDs, messageIDs map[int64]string) ([]ChatMessage, error) {
	rows, err := common.SelectAll(db, "chat_message_join", true)
	if err != nil {
		return nil, err
	}
	var out []ChatMessage
	for _, row := range rows {
		chatID := chatIDs[row.Int("chat_id")]
		messageID := messageIDs[row.Int("message_id")]
		dateNS := row.Int("message_date")
		if chatID != "" && messageID != "" {
			out = append(out, ChatMessage{ChatID: chatID, MessageID: messageID, MessageDate: common.AppleTimestamp(dateNS), MessageDateNS: dateNS, Raw: row.Public()})
		}
	}
	return out, nil
}

func scanAttachments(db *sql.DB, root string, messageIDs map[int64]string) ([]Attachment, error) {
	rows, err := common.Query(db, `SELECT attachment.ROWID AS ROWID, attachment.*, message_attachment_join.message_id AS pdw_message_rowid
		FROM attachment
		LEFT JOIN message_attachment_join ON message_attachment_join.attachment_id = attachment.ROWID`)
	if err != nil {
		return nil, err
	}
	var out []Attachment
	for _, row := range rows {
		rowID := row.Int("ROWID")
		guid := row.String("guid")
		if guid == "" {
			guid = fmt.Sprint(rowID)
		}
		filename := row.String("filename")
		resolved := ResolveAttachmentPath(filename, root)
		exists := false
		var size int64
		if resolved != "" {
			if info, err := os.Stat(resolved); err == nil && info.Mode().IsRegular() {
				exists = true
				size = info.Size()
			}
		}
		errText := ""
		if !exists {
			if filename != "" {
				errText = "attachment file is not locally available"
			} else {
				errText = "attachment filename is empty"
			}
		}
		resolvedPath := ""
		if exists {
			resolvedPath = resolved
		}
		out = append(out, Attachment{
			AttachmentID:    guid,
			AttachmentRowID: rowID,
			MessageID:       messageIDs[row.Int("pdw_message_rowid")],
			GUID:            guid,
			OriginalGUID:    row.String("original_guid"),
			Filename:        filename,
			ResolvedPath:    resolvedPath,
			TransferName:    row.String("transfer_name"),
			ContentType:     NormalizedContentType(row.String("mime_type"), row.String("uti"), filename),
			UTI:             row.String("uti"),
			MimeType:        row.String("mime_type"),
			TotalBytes:      row.Int("total_bytes"),
			SizeBytes:       size,
			IsMissing:       !exists,
			Error:           errText,
			IsOutgoing:      row.Bool("is_outgoing"),
			IsSticker:       row.Bool("is_sticker"),
			HideAttachment:  row.Bool("hide_attachment"),
			TransferState:   row.Int("transfer_state"),
			CreatedAt:       common.AppleTimestamp(row.Int("created_date")),
			StartAt:         common.AppleTimestamp(row.Int("start_date")),
			Raw:             row.PublicWithoutBlobs(),
		})
	}
	return out, nil
}

func scanDeletedMessages(db *sql.DB, tables map[string]bool) ([]DeletedMessage, error) {
	var order []string
	byGUID := map[string]DeletedMessage{}
	for _, table := range []string{"deleted_messages", "sync_deleted_messages"} {
		if !tables[table] {
			continue
		}
		rows, err := common.SelectAll(db, table, true)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			guid := row.String("guid")
			if guid == "" {
				continue
			}
			raw := map[string]any{"source_table": table}
			for key, value := range row.Public() {
				raw[key] = value
			}
			if _, seen := byGUID[guid]; !seen {
				order = append(order, guid)
			}
			byGUID[guid] = DeletedMessage{MessageID: guid, DeletedAt: common.UnixEpoch, Raw: raw}
		}
	}
	out := make([]DeletedMessage, 0, len(order))
	for _, guid := range order {
		out = append(out, byGUID[guid])
	}
	return out, nil
}

var utiContentTypes = map[string]string{
	"com.adobe.pdf":                                "application/pdf",
	"com.apple.m4a-audio":                          "audio/x-m4a",
	"com.apple.quicktime-movie":                    "video/quicktime",
	"com.compuserve.gif":                           "image/gif",
	"org.openxmlformats.wordprocessingml.document": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	"public.heic":                                  "image/heic",
	"public.html":                                  "text/html",
	"public.jpeg":                                  "image/jpeg",
	"public.mpeg-4":                                "video/mp4",
	"public.mpeg-4-audio":                          "audio/mp4",
	"public.mp3":                                   "audio/mpeg",
	"public.plain-text":                            "text/plain",
	"public.png":                                   "image/png",
	"public.svg-image":                             "image/svg+xml",
	"public.tiff":                                  "image/tiff",
	"public.vcard":                                 "text/vcard",
	"public.zip-archive":                           "application/zip",
}

// NormalizedContentType prefers the stored MIME type, then the filename's
// extension, then the UTI, then octet-stream.
func NormalizedContentType(value, uti, filename string) string {
	if strings.Contains(value, "/") {
		return value
	}
	if filename != "" {
		if guessed := common.GuessMimeType(filename); guessed != "" {
			return guessed
		}
	}
	if mapped, ok := utiContentTypes[uti]; ok {
		return mapped
	}
	return "application/octet-stream"
}

// ResolveAttachmentPath expands "~/" and joins relative names to the root.
func ResolveAttachmentPath(filename, root string) string {
	if filename == "" {
		return ""
	}
	if strings.HasPrefix(filename, "~/") {
		return common.ExpandUser(filename)
	}
	if filepath.IsAbs(filename) {
		return filename
	}
	return filepath.Join(root, filename)
}
