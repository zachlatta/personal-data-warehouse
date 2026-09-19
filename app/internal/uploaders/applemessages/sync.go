package applemessages

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Summary is the run's counts.
type Summary struct {
	HandlesSeen             int
	ChatsSeen               int
	MessagesSeen            int
	AttachmentsSeen         int
	RecordsSelected         int
	RecordsSkipped          int
	BatchesUploaded         int
	AttachmentsUploaded     int
	AttachmentBytesUploaded int64
	AttachmentsDeferred     int
}

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadAppleMessagesBatch(gzipBytes []byte, exportedAt string) (ingestclient.StoredObject, error)
	UploadAppleMessagesAttachment(content []byte, a ingestclient.AppleMessagesAttachment) (ingestclient.StoredObject, error)
}

// Runner uploads the manifest and a bounded slice of attachment bytes.
type Runner struct {
	Account                string
	StorePath              string
	Client                 Uploader
	Logger                 common.Logger
	State                  *State
	Now                    func() time.Time
	Mode                   string
	Limit                  int
	AttachmentBytesPerRun  int64
	AttachmentCountPerRun  int
	Workers                int
	BeforeUploadCheck      func() string
	snapshotForTest        *Snapshot // set by tests to skip the store snapshot
	attachmentRootForTests string
}

type stateMark struct {
	sourceType    string
	sourceID      string
	fingerprint   string
	contentSHA256 string
	storageKey    string
}

type pendingAttachment struct {
	attachment    Attachment
	contentSHA256 string
	sizeBytes     int64
}

type attachmentResult struct {
	attachment    Attachment
	contentSHA256 string
	stored        ingestclient.StoredObject
	sizeBytes     int64
}

// Sync runs one pass.
func (r *Runner) Sync() (Summary, error) {
	if r.Client == nil {
		return Summary{}, errors.New("ingest client is required")
	}
	if r.Mode == "" {
		r.Mode = "incremental"
	}
	if r.Mode != "incremental" && r.Mode != "full" {
		return Summary{}, errors.New("mode must be 'full' or 'incremental'")
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	if r.Workers < 1 {
		r.Workers = 1
	}
	if r.AttachmentBytesPerRun < 0 {
		r.AttachmentBytesPerRun = 0
	}
	if r.AttachmentCountPerRun < 0 {
		r.AttachmentCountPerRun = 0
	}
	snapshot := r.snapshotForTest
	if snapshot == nil {
		storePath := common.ExpandUser(r.StorePath)
		r.Logger.Infof("Snapshotting Apple Messages store at %s", storePath)
		tempDir, err := os.MkdirTemp("", "pdw-apple-messages-")
		if err != nil {
			return Summary{}, err
		}
		defer os.RemoveAll(tempDir)
		copied, err := SnapshotStore(storePath, tempDir)
		if err != nil {
			return Summary{}, err
		}
		root := r.attachmentRootForTests
		if root == "" {
			root = dirOf(storePath)
		}
		snapshot, err = Scan(copied, root)
		if err != nil {
			return Summary{}, err
		}
	}

	exportedAt := r.Now()
	records, marks, skipped, err := r.selectedManifestRecords(snapshot, exportedAt)
	if err != nil {
		return Summary{}, err
	}
	uploads, attachmentDeferred, err := r.selectedAttachmentUploads(snapshot.Attachments)
	if err != nil {
		return Summary{}, err
	}
	if r.Limit > 0 && len(records) > r.Limit {
		deferredRecords := records[r.Limit:]
		records = records[:r.Limit]
		keep := map[[2]string]bool{}
		for _, mark := range marksForRecords(records) {
			keep[[2]string{mark.sourceType, mark.sourceID}] = true
		}
		var kept []stateMark
		for _, mark := range marks {
			if keep[[2]string{mark.sourceType, mark.sourceID}] {
				kept = append(kept, mark)
			}
		}
		marks = kept
		skipped += len(deferredRecords)
	}

	summary := Summary{
		HandlesSeen: len(snapshot.Handles), ChatsSeen: len(snapshot.Chats), MessagesSeen: len(snapshot.Messages),
		AttachmentsSeen: len(snapshot.Attachments), RecordsSelected: len(records), RecordsSkipped: skipped,
		AttachmentsDeferred: attachmentDeferred,
	}
	if (len(records) > 0 || len(uploads) > 0) && r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping Apple Messages upload: %s", reason)
			summary.AttachmentsDeferred = len(records) + len(uploads) + attachmentDeferred
			return summary, nil
		}
	}

	attachmentRecords, attachmentMarks, uploadedCount, uploadedBytes, err := r.uploadAttachmentBackfill(uploads, exportedAt)
	if err != nil {
		return summary, err
	}
	records = append(records, attachmentRecords...)
	marks = append(marks, attachmentMarks...)
	summary.AttachmentsUploaded = uploadedCount
	summary.AttachmentBytesUploaded = uploadedBytes
	summary.RecordsSelected = len(records)

	if len(records) > 0 {
		encoded, err := common.GzipJSONL(records)
		if err != nil {
			return summary, err
		}
		stored, err := r.Client.UploadAppleMessagesBatch(encoded, common.ISOFormat(exportedAt))
		if err != nil {
			return summary, err
		}
		summary.BatchesUploaded = 1
		for _, mark := range marks {
			if err := r.markSuccess(mark, exportedAt); err != nil {
				return summary, err
			}
		}
		r.Logger.Infof("Uploaded Apple Messages batch %s with %d records", stored.StorageKey, len(records))
	}
	r.Logger.Infof("Apple Messages upload summary: handles=%d chats=%d messages=%d attachments=%d selected=%d skipped=%d batches=%d attachment_uploads=%d",
		summary.HandlesSeen, summary.ChatsSeen, summary.MessagesSeen, summary.AttachmentsSeen, summary.RecordsSelected, summary.RecordsSkipped, summary.BatchesUploaded, summary.AttachmentsUploaded)
	return summary, nil
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}

type snapshotPayload struct {
	sourceType string
	sourceID   string
	payload    map[string]any
}

// SnapshotPayloads yields every non-message record in manifest order.
func SnapshotPayloads(snapshot *Snapshot) []snapshotPayload {
	var out []snapshotPayload
	for _, handle := range snapshot.Handles {
		out = append(out, snapshotPayload{"handle", handle.HandleID, HandlePayload(handle)})
	}
	for _, chat := range snapshot.Chats {
		out = append(out, snapshotPayload{"chat", chat.ChatID, ChatPayload(chat)})
	}
	for _, ch := range snapshot.ChatHandles {
		out = append(out, snapshotPayload{"chat_handle", ch.ChatID + ":" + ch.HandleID, ChatHandlePayload(ch)})
	}
	for _, cm := range snapshot.ChatMessages {
		out = append(out, snapshotPayload{"chat_message", cm.ChatID + ":" + cm.MessageID, ChatMessagePayload(cm)})
	}
	for _, attachment := range snapshot.Attachments {
		out = append(out, snapshotPayload{"attachment", AttachmentSourceID(attachment), AttachmentPayload(attachment, "", nil)})
	}
	for _, deleted := range snapshot.DeletedMessages {
		out = append(out, snapshotPayload{"deleted_message", deleted.MessageID, DeletedMessagePayload(deleted)})
	}
	return out
}

func (r *Runner) selectedManifestRecords(snapshot *Snapshot, exportedAt time.Time) ([]map[string]any, []stateMark, int, error) {
	var records []map[string]any
	var marks []stateMark
	skipped := 0
	for _, item := range SnapshotPayloads(snapshot) {
		fingerprint := common.JSONSHA256(item.payload)
		complete, err := r.isComplete(item.sourceType, item.sourceID, fingerprint)
		if err != nil {
			return nil, nil, 0, err
		}
		if complete {
			skipped++
			continue
		}
		records = append(records, Envelope(r.Account, exportedAt, item.sourceType, item.payload))
		marks = append(marks, stateMark{sourceType: item.sourceType, sourceID: item.sourceID, fingerprint: fingerprint})
	}
	for _, message := range snapshot.Messages {
		fingerprint := common.JSONSHA256(MessageFingerprintPayload(message))
		complete, err := r.isComplete("message", message.MessageID, fingerprint)
		if err != nil {
			return nil, nil, 0, err
		}
		if complete {
			skipped++
			continue
		}
		records = append(records, Envelope(r.Account, exportedAt, "message", MessagePayload(message)))
		marks = append(marks, stateMark{sourceType: "message", sourceID: message.MessageID, fingerprint: fingerprint})
	}
	return records, marks, skipped, nil
}

func (r *Runner) selectedAttachmentUploads(attachments []Attachment) ([]pendingAttachment, int, error) {
	var selected []pendingAttachment
	var selectedBytes int64
	deferred := 0
	seen := map[string]bool{}
	for _, attachment := range attachments {
		if attachment.ResolvedPath == "" || attachment.IsMissing {
			continue
		}
		already, err := r.attachmentBlobAlreadyUploaded(attachment)
		if err != nil {
			return nil, 0, err
		}
		if already {
			continue
		}
		if r.AttachmentCountPerRun > 0 && len(selected) >= r.AttachmentCountPerRun {
			deferred++
			continue
		}
		info, err := os.Stat(attachment.ResolvedPath)
		if err != nil {
			return nil, 0, err
		}
		size := info.Size()
		if r.AttachmentBytesPerRun > 0 && len(selected) > 0 && selectedBytes+size > r.AttachmentBytesPerRun {
			deferred++
			continue
		}
		sha, err := common.FileSHA256(attachment.ResolvedPath)
		if err != nil {
			return nil, 0, err
		}
		if seen[attachment.AttachmentID] {
			continue
		}
		seen[attachment.AttachmentID] = true
		complete, err := r.isComplete("attachment_blob", attachment.AttachmentID, sha)
		if err != nil {
			return nil, 0, err
		}
		if complete {
			continue
		}
		selected = append(selected, pendingAttachment{attachment: attachment, contentSHA256: sha, sizeBytes: size})
		selectedBytes += size
	}
	return selected, deferred, nil
}

func (r *Runner) attachmentBlobAlreadyUploaded(attachment Attachment) (bool, error) {
	if r.Mode != "incremental" || r.State == nil {
		return false, nil
	}
	entry, err := r.State.EntryFor("attachment_blob", attachment.AttachmentID)
	if err != nil || entry == nil {
		return false, err
	}
	return entry.Complete && entry.ContentSHA256 != "", nil
}

func (r *Runner) uploadAttachmentBackfill(uploads []pendingAttachment, exportedAt time.Time) ([]map[string]any, []stateMark, int, int64, error) {
	if len(uploads) == 0 {
		return nil, nil, 0, 0, nil
	}
	r.Logger.Infof("Uploading %d Apple Messages attachment(s) with %d worker(s)", len(uploads), r.Workers)
	results := make([]attachmentResult, len(uploads))
	errs := make([]error, len(uploads))
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, r.Workers)
	for i, upload := range uploads {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(i int, upload pendingAttachment) {
			defer wg.Done()
			defer func() { <-semaphore }()
			results[i], errs[i] = r.uploadAttachment(upload)
		}(i, upload)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, nil, 0, 0, err
		}
	}
	var records []map[string]any
	var marks []stateMark
	var uploadedBytes int64
	for _, result := range results {
		attachment := result.attachment
		payload := AttachmentPayload(attachment, result.contentSHA256, &result.stored)
		storageFingerprint := common.JSONSHA256(map[string]any{
			"attachment_id":  attachment.AttachmentID,
			"message_id":     attachment.MessageID,
			"content_sha256": result.contentSHA256,
			"storage_key":    result.stored.StorageKey,
		})
		records = append(records, Envelope(r.Account, exportedAt, "attachment", payload))
		marks = append(marks,
			stateMark{sourceType: "attachment_blob", sourceID: attachment.AttachmentID, fingerprint: result.contentSHA256, contentSHA256: result.contentSHA256, storageKey: result.stored.StorageKey},
			stateMark{sourceType: "attachment_storage", sourceID: AttachmentSourceID(attachment), fingerprint: storageFingerprint, contentSHA256: result.contentSHA256, storageKey: result.stored.StorageKey},
		)
		uploadedBytes += result.sizeBytes
	}
	return records, marks, len(results), uploadedBytes, nil
}

func (r *Runner) uploadAttachment(upload pendingAttachment) (attachmentResult, error) {
	attachment := upload.attachment
	if attachment.ResolvedPath == "" {
		return attachmentResult{}, fmt.Errorf("Attachment %s has no local file path", attachment.AttachmentID)
	}
	content, err := os.ReadFile(attachment.ResolvedPath)
	if err != nil {
		return attachmentResult{}, err
	}
	stored, err := r.Client.UploadAppleMessagesAttachment(content, ingestclient.AppleMessagesAttachment{
		AttachmentGUID: attachment.AttachmentID,
		MessageGUID:    attachment.MessageID,
		ContentType:    attachment.ContentType,
		CreatedAt:      common.ISOFormat(attachment.CreatedAt),
		Filename:       attachment.Filename,
	})
	if err != nil {
		return attachmentResult{}, err
	}
	return attachmentResult{attachment: attachment, contentSHA256: upload.contentSHA256, stored: stored, sizeBytes: upload.sizeBytes}, nil
}

func (r *Runner) isComplete(sourceType, sourceID, fingerprint string) (bool, error) {
	if r.Mode != "incremental" || r.State == nil {
		return false, nil
	}
	return r.State.IsComplete(sourceType, sourceID, fingerprint)
}

func (r *Runner) markSuccess(mark stateMark, now time.Time) error {
	if r.State == nil {
		return nil
	}
	return r.State.MarkSuccess(mark.sourceType, mark.sourceID, mark.fingerprint, mark.contentSHA256, mark.storageKey, now)
}

func marksForRecords(records []map[string]any) []stateMark {
	var marks []stateMark
	for _, record := range records {
		recordType := common.PyStr(record["record_type"])
		payload, ok := record["record"].(map[string]any)
		if !ok {
			continue
		}
		sourceID := recordSourceID(recordType, payload)
		if sourceID != "" {
			marks = append(marks, stateMark{sourceType: recordType, sourceID: sourceID, fingerprint: common.JSONSHA256(payload)})
		}
	}
	return marks
}

func recordSourceID(recordType string, payload map[string]any) string {
	get := func(key string) string { return common.PyStr(payload[key]) }
	switch recordType {
	case "handle":
		return get("handle_id")
	case "chat":
		return get("chat_id")
	case "chat_handle":
		return get("chat_id") + ":" + get("handle_id")
	case "chat_message":
		return get("chat_id") + ":" + get("message_id")
	case "message":
		return get("message_id")
	case "attachment":
		return get("attachment_id") + ":" + get("message_id")
	case "deleted_message":
		return get("message_id")
	}
	return ""
}

// --- payloads ---------------------------------------------------------------

func HandlePayload(h Handle) map[string]any {
	return map[string]any{
		"handle_id": h.HandleID, "handle_rowid": h.HandleRowID, "address": h.Address, "country": h.Country,
		"service": h.Service, "uncanonicalized_id": h.UncanonicalizedID, "person_centric_id": h.PersonCentricID, "raw": h.Raw,
	}
}

func ChatPayload(c Chat) map[string]any {
	return map[string]any{
		"chat_id": c.ChatID, "chat_rowid": c.ChatRowID, "guid": c.GUID, "chat_identifier": c.ChatIdentifier,
		"service_name": c.ServiceName, "display_name": c.DisplayName, "room_name": c.RoomName, "account_login": c.AccountLogin,
		"style": c.Style, "state": c.State, "is_archived": c.IsArchived, "is_filtered": c.IsFiltered,
		"is_recovered": c.IsRecovered, "is_pending_review": c.IsPendingReview,
		"last_read_message_at": common.ISOFormat(c.LastReadMessageAt), "raw": c.Raw,
	}
}

func ChatHandlePayload(ch ChatHandle) map[string]any {
	return map[string]any{"chat_id": ch.ChatID, "handle_id": ch.HandleID, "raw": ch.Raw}
}

func ChatMessagePayload(cm ChatMessage) map[string]any {
	return map[string]any{
		"chat_id": cm.ChatID, "message_id": cm.MessageID, "message_date": common.ISOFormat(cm.MessageDate),
		"message_date_ns": cm.MessageDateNS, "raw": cm.Raw,
	}
}

// MessageFingerprintPayload is what decides whether a message re-uploads: the
// decoded body is deliberately absent so a decoder change is not a re-upload.
func MessageFingerprintPayload(m Message) map[string]any {
	return map[string]any{
		"message_id": m.MessageID, "message_rowid": m.MessageRowID, "text": m.Text,
		"attributed_body_sha256": m.AttributedBodySHA256, "date_ns": m.DateNS,
		"date_edited": common.ISOFormat(m.DateEdited), "date_retracted": common.ISOFormat(m.DateRetracted), "raw": m.Raw,
	}
}

func MessagePayload(m Message) map[string]any {
	body := DecodeMessageBody(m.Text, m.AttributedBody)
	return map[string]any{
		"message_id": m.MessageID, "message_rowid": m.MessageRowID, "handle_id": m.HandleID, "service": m.Service,
		"message_account": m.MessageAccount, "body_text": body.Text, "body_source": body.Source,
		"body_decode_status": body.Status, "body_decode_error": body.Error, "attributed_body_sha256": body.AttributedBodySHA256,
		"subject": m.Subject, "country": m.Country, "message_type": m.MessageType, "message_item_type": m.ItemType,
		"is_from_me": m.IsFromMe, "is_read": m.IsRead, "is_sent": m.IsSent, "is_delivered": m.IsDelivered,
		"is_finished": m.IsFinished, "is_system_message": m.IsSystemMessage, "is_service_message": m.IsServiceMessage,
		"is_forward": m.IsForward, "is_empty": m.IsEmpty, "is_audio_message": m.IsAudioMessage, "is_played": m.IsPlayed,
		"cache_has_attachments": m.CacheHasAttachments, "has_unseen_mention": m.HasUnseenMention, "is_spam": m.IsSpam,
		"reply_to_guid": m.ReplyToGUID, "associated_message_guid": m.AssociatedMessageGUID,
		"associated_message_type": m.AssociatedMessageType, "associated_message_emoji": m.AssociatedMessageEmoji,
		"balloon_bundle_id": m.BalloonBundleID, "group_title": m.GroupTitle, "group_action_type": m.GroupActionType,
		"message_action_type": m.MessageActionType, "message_source": m.MessageSource,
		"expressive_send_style_id": m.ExpressiveSendStyleID, "message_at": common.ISOFormat(m.MessageAt), "date_ns": m.DateNS,
		"date_read": common.ISOFormat(m.DateRead), "date_delivered": common.ISOFormat(m.DateDelivered),
		"date_played": common.ISOFormat(m.DatePlayed), "date_edited": common.ISOFormat(m.DateEdited),
		"date_retracted": common.ISOFormat(m.DateRetracted), "date_recovered": common.ISOFormat(m.DateRecovered),
		"is_deleted": false, "raw": m.Raw,
	}
}

func AttachmentPayload(a Attachment, contentSHA256 string, stored *ingestclient.StoredObject) map[string]any {
	payload := map[string]any{
		"attachment_id": a.AttachmentID, "attachment_rowid": a.AttachmentRowID, "message_id": a.MessageID, "guid": a.GUID,
		"original_guid": a.OriginalGUID, "filename": a.Filename, "transfer_name": a.TransferName, "content_type": a.ContentType,
		"uti": a.UTI, "mime_type": a.MimeType, "total_bytes": a.TotalBytes, "size_bytes": a.SizeBytes,
		"content_sha256": contentSHA256, "is_missing": a.IsMissing, "error": a.Error, "is_outgoing": a.IsOutgoing,
		"is_sticker": a.IsSticker, "hide_attachment": a.HideAttachment, "transfer_state": a.TransferState,
		"created_at": common.ISOFormat(a.CreatedAt), "start_at": common.ISOFormat(a.StartAt), "raw": a.Raw,
	}
	if stored != nil {
		payload["file"] = stored.Map()
	}
	return payload
}

func DeletedMessagePayload(d DeletedMessage) map[string]any {
	return map[string]any{"message_id": d.MessageID, "deleted_at": common.ISOFormat(d.DeletedAt), "is_deleted": true, "raw": d.Raw}
}

// AttachmentSourceID is the state key for an attachment manifest row.
func AttachmentSourceID(a Attachment) string { return a.AttachmentID + ":" + a.MessageID }

// Envelope wraps one record for the batch.
func Envelope(account string, exportedAt time.Time, recordType string, record map[string]any) map[string]any {
	return map[string]any{
		"schema_version": int64(1),
		"source":         "apple_messages",
		"account":        account,
		"exported_at":    common.ISOFormat(exportedAt),
		"record_type":    recordType,
		"record":         record,
	}
}
