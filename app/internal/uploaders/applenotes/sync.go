package applenotes

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Summary is the run's counts.
type Summary struct {
	NotesSeen           int
	NotesSelected       int
	NotesSkipped        int
	NotesDeleted        int
	RevisionsUploaded   int
	MetadataUploaded    int
	BodyHTMLUploaded    int
	AttachmentsSeen     int
	AttachmentsUploaded int
	AttachmentsMissing  int
	NotesDeferred       int
}

// Revision is one note snapshot to upload; its fingerprint is the revision id.
type Revision struct {
	Note        Note
	RevisionID  string
	Fingerprint string
	IsTombstone bool
}

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadAppleNotesBody(html []byte, noteID, revisionID, modifiedAt string) (ingestclient.StoredObject, error)
	UploadAppleNotesAttachment(content []byte, a ingestclient.AppleNotesAttachment) (ingestclient.StoredObject, error)
	UploadAppleNotesRevision(payload map[string]any, noteID, revisionID, modifiedAt, noteContentSHA256 string) (ingestclient.StoredObject, error)
}

// Runner uploads changed notes and tombstones for vanished ones.
type Runner struct {
	Account           string
	StorePath         string
	Client            Uploader
	Logger            common.Logger
	State             *State
	Now               func() time.Time
	Mode              string
	BeforeUploadCheck func() string
	Limit             int
	Workers           int
	SaveState         func() // called after every state change

	stateMu sync.Mutex
}

type uploadResult struct {
	revisionUploaded    int
	metadataUploaded    int
	htmlUploaded        int
	attachmentsUploaded int
	attachmentsMissing  int
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
	storePath := common.ExpandUser(r.StorePath)
	r.Logger.Infof("Snapshotting Apple Notes store at %s", storePath)
	tempDir, err := os.MkdirTemp("", "pdw-apple-notes-")
	if err != nil {
		return Summary{}, err
	}
	snapshot, err := SnapshotStore(storePath, tempDir)
	if err != nil {
		os.RemoveAll(tempDir)
		return Summary{}, err
	}
	notes, err := Scan(snapshot, filepath.Dir(storePath))
	os.RemoveAll(tempDir)
	if err != nil {
		return Summary{}, err
	}
	return r.syncNotes(notes)
}

func (r *Runner) syncNotes(notes []Note) (Summary, error) {
	if r.Workers < 1 {
		r.Workers = 1
	}
	if r.Mode == "" {
		r.Mode = "incremental"
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	current := map[string]bool{}
	var revisions []Revision
	for _, note := range notes {
		current[note.NoteID] = true
		revisions = append(revisions, RevisionFromNote(note))
	}
	tombstones := r.deletedNoteRevisions(current)
	revisions = append(revisions, tombstones...)

	summary := Summary{NotesSeen: len(notes), NotesDeleted: len(tombstones)}
	for _, note := range notes {
		summary.AttachmentsSeen += len(note.Attachments)
		for _, attachment := range note.Attachments {
			if attachment.IsMissing {
				summary.AttachmentsMissing++
			}
		}
	}
	var selected []Revision
	for _, revision := range revisions {
		if r.Mode == "incremental" && r.State != nil {
			if entry, ok := r.State.EntryFor(revision.Note.NoteID); ok && entry.Complete() && entry.Fingerprint == revision.Fingerprint && entry.IsDeleted == revision.Note.IsDeleted {
				summary.NotesSkipped++
				continue
			}
		}
		selected = append(selected, revision)
	}
	deferred := 0
	if r.Limit > 0 && len(selected) > r.Limit {
		deferred = len(selected) - r.Limit
		selected = selected[:r.Limit]
	}
	summary.NotesSelected = len(selected)
	summary.NotesDeferred = deferred
	if len(selected) > 0 && r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping Apple Notes upload: %s", reason)
			summary.NotesDeferred = len(selected) + deferred
			return summary, nil
		}
	}
	r.Logger.Infof("Uploading with %d worker(s)", r.Workers)
	results := make([]uploadResult, len(selected))
	errs := make([]error, len(selected))
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, r.Workers)
	for i, revision := range selected {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(i int, revision Revision) {
			defer wg.Done()
			defer func() { <-semaphore }()
			results[i], errs[i] = r.syncRevision(i+1, len(selected), revision)
		}(i, revision)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return summary, err
		}
	}
	for _, result := range results {
		summary.RevisionsUploaded += result.revisionUploaded
		summary.MetadataUploaded += result.metadataUploaded
		summary.BodyHTMLUploaded += result.htmlUploaded
		summary.AttachmentsUploaded += result.attachmentsUploaded
	}
	r.Logger.Infof("Apple Notes upload summary: seen=%d selected=%d skipped=%d deferred=%d revisions=%d metadata=%d html=%d attachments=%d missing=%d deleted=%d",
		summary.NotesSeen, summary.NotesSelected, summary.NotesSkipped, summary.NotesDeferred, summary.RevisionsUploaded,
		summary.MetadataUploaded, summary.BodyHTMLUploaded, summary.AttachmentsUploaded, summary.AttachmentsMissing, summary.NotesDeleted)
	return summary, nil
}

func (r *Runner) deletedNoteRevisions(current map[string]bool) []Revision {
	if r.State == nil {
		return nil
	}
	var revisions []Revision
	deletedAt := r.Now()
	for _, noteID := range r.State.SortedNoteIDs() {
		entry := r.State.Entries[noteID]
		if current[noteID] || entry.IsDeleted {
			continue
		}
		note := Note{
			NoteID:     noteID,
			Title:      entry.Title,
			CreatedAt:  common.ParseISO(entry.ModifiedAt),
			ModifiedAt: deletedAt,
			IsDeleted:  true,
			Raw:        map[string]any{"tombstone_from_revision_id": entry.RevisionID},
		}
		revisions = append(revisions, RevisionFromNote(note))
	}
	return revisions
}

func (r *Runner) syncRevision(index, total int, revision Revision) (uploadResult, error) {
	r.Logger.Infof("[%d/%d] upload Apple Note %s revision %s -> app", index, total, revision.Note.NoteID, common.ShortSHA256(revision.RevisionID))
	result, err := r.uploadRevision(revision)
	if err != nil {
		r.markFailure(revision, err.Error())
		return result, err
	}
	r.markSuccess(revision, result)
	return result, nil
}

func (r *Runner) markSuccess(revision Revision, result uploadResult) {
	if r.State == nil {
		return
	}
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	r.State.MarkSuccess(revision.Note.NoteID, revision.Fingerprint, revision.RevisionID, revision.Note.Title, revision.Note.ModifiedAt,
		revision.Note.IsDeleted, true, revision.Note.IsDeleted || result.htmlUploaded > 0, true, r.Now())
	if r.SaveState != nil {
		r.SaveState()
	}
}

func (r *Runner) markFailure(revision Revision, errText string) {
	if r.State == nil {
		return
	}
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	r.State.MarkFailure(revision.Note.NoteID, errText, r.Now())
	if r.SaveState != nil {
		r.SaveState()
	}
}

func (r *Runner) uploadRevision(revision Revision) (uploadResult, error) {
	payload := MetadataPayload(r.Account, revision, r.Now())
	modifiedAt := common.ISOFormat(revision.Note.ModifiedAt)
	noteID := revision.Note.NoteID
	result := uploadResult{}
	for _, attachment := range revision.Note.Attachments {
		if attachment.IsMissing {
			result.attachmentsMissing++
		}
	}
	if !revision.Note.IsDeleted {
		html := revision.Note.BodyHTML
		if html == "" {
			html = HTMLDocumentForNote(revision.Note)
		}
		if html != "" {
			if _, err := r.Client.UploadAppleNotesBody([]byte(html), noteID, revision.RevisionID, modifiedAt); err != nil {
				return result, err
			}
			result.htmlUploaded = 1
		}
		for _, attachment := range revision.Note.Attachments {
			if attachment.IsMissing || attachment.Path == "" || attachment.ContentSHA256 == "" {
				continue
			}
			content, err := os.ReadFile(attachment.Path)
			if err != nil {
				return result, err
			}
			if _, err := r.Client.UploadAppleNotesAttachment(content, ingestclient.AppleNotesAttachment{
				NoteID: noteID, RevisionID: revision.RevisionID, ModifiedAt: modifiedAt,
				AttachmentID: attachment.AttachmentID, Filename: attachment.Filename, ContentType: attachment.ContentType,
			}); err != nil {
				return result, err
			}
			result.attachmentsUploaded++
		}
	}
	if _, err := r.Client.UploadAppleNotesRevision(payload, noteID, revision.RevisionID, modifiedAt, revision.Fingerprint); err != nil {
		return result, err
	}
	result.revisionUploaded = 1
	result.metadataUploaded = 1
	return result, nil
}

// RevisionFromNote fingerprints the note payload; the fingerprint is the
// revision id.
func RevisionFromNote(note Note) Revision {
	fingerprint := common.JSONSHA256(NotePayload(note))
	return Revision{Note: note, RevisionID: fingerprint, Fingerprint: fingerprint, IsTombstone: note.IsDeleted}
}

// NotePayload is the fingerprinted record.
func NotePayload(note Note) map[string]any {
	attachments := make([]any, 0, len(note.Attachments))
	for _, attachment := range note.Attachments {
		attachments = append(attachments, AttachmentPayload(attachment))
	}
	raw := note.Raw
	if raw == nil {
		raw = map[string]any{}
	}
	return map[string]any{
		"note_id":            note.NoteID,
		"title":              note.Title,
		"folder_id":          note.FolderID,
		"folder_path":        note.FolderPath,
		"apple_account_id":   note.AppleAccountID,
		"apple_account_name": note.AppleAccountName,
		"created_at":         common.ISOFormat(note.CreatedAt),
		"modified_at":        common.ISOFormat(note.ModifiedAt),
		"body_text":          note.BodyText,
		"body_html":          note.BodyHTML,
		"body_markdown":      note.BodyMarkdown,
		"attachments":        attachments,
		"is_deleted":         note.IsDeleted,
		"raw":                raw,
	}
}

// AttachmentPayload is one attachment's record.
func AttachmentPayload(attachment Attachment) map[string]any {
	raw := attachment.Raw
	if raw == nil {
		raw = map[string]any{}
	}
	return map[string]any{
		"attachment_id":  attachment.AttachmentID,
		"note_id":        attachment.NoteID,
		"filename":       attachment.Filename,
		"content_type":   attachment.ContentType,
		"size_bytes":     attachment.SizeBytes,
		"content_sha256": attachment.ContentSHA256,
		"is_missing":     attachment.IsMissing,
		"error":          attachment.Error,
		"raw":            raw,
	}
}

// MetadataPayload is the revision sidecar.
func MetadataPayload(account string, revision Revision, exportedAt time.Time) map[string]any {
	note := NotePayload(revision.Note)
	note["revision_id"] = revision.RevisionID
	note["content_sha256"] = revision.Fingerprint
	return map[string]any{
		"schema_version": int64(1),
		"source":         "apple_notes",
		"account":        account,
		"exported_at":    common.ISOFormat(exportedAt),
		"note":           note,
	}
}

// HTMLDocumentForNote wraps a note's body when it has no HTML of its own.
func HTMLDocumentForNote(note Note) string {
	title := note.Title
	if title == "" {
		title = note.NoteID
	}
	body := note.BodyHTML
	if body == "" {
		body = "<pre>" + PyHTMLEscape(note.BodyText) + "</pre>"
	}
	return `<!doctype html><html><head><meta charset="utf-8"><title>` + PyHTMLEscape(title) + `</title></head><body>` + body + `</body></html>`
}
