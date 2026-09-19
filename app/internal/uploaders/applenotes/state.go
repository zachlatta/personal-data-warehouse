package applenotes

import (
	"encoding/json"
	"os"
	"sort"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const stateSchemaVersion = int64(1)

// StateEntry is one note's recorded upload.
type StateEntry struct {
	NoteID              string `json:"note_id"`
	Fingerprint         string `json:"fingerprint"`
	RevisionID          string `json:"revision_id"`
	Title               string `json:"title"`
	ModifiedAt          string `json:"modified_at"`
	IsDeleted           bool   `json:"is_deleted"`
	MetadataUploaded    bool   `json:"metadata_uploaded"`
	HTMLUploaded        bool   `json:"html_uploaded"`
	AttachmentsUploaded bool   `json:"attachments_uploaded"`
	LastSuccessAt       string `json:"last_success_at"`
	LastFailureAt       string `json:"last_failure_at"`
	LastError           string `json:"last_error"`
	LastCheckedAt       string `json:"last_checked_at"`
}

// Complete reports whether the note's metadata and attachments landed.
func (e StateEntry) Complete() bool { return e.MetadataUploaded && e.AttachmentsUploaded }

// State is the JSON state file the Notes uploader keeps
// (apple-notes-upload-state.json), in the same shape the Python one wrote.
type State struct {
	Account   string
	StorePath string
	Entries   map[string]StateEntry
}

// DefaultStateFile is the state path.
func DefaultStateFile() string {
	return common.DefaultStateFile("apple-notes-upload-state.json")
}

// EmptyState is a fresh state.
func EmptyState(account, storePath string) *State {
	return &State{Account: account, StorePath: storePath, Entries: map[string]StateEntry{}}
}

// LoadState reads the file, returning an empty state when it is missing,
// unreadable, or written for a different account/store.
func LoadState(path, account, storePath string) *State {
	data, err := os.ReadFile(path)
	if err != nil {
		return EmptyState(account, storePath)
	}
	var payload struct {
		SchemaVersion int64                      `json:"schema_version"`
		Account       string                     `json:"account"`
		StorePath     string                     `json:"store_path"`
		Entries       map[string]json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(data, &payload); err != nil || payload.SchemaVersion != stateSchemaVersion || payload.Account != account || payload.StorePath != storePath {
		return EmptyState(account, storePath)
	}
	state := EmptyState(account, storePath)
	for key, raw := range payload.Entries {
		var entry StateEntry
		if err := json.Unmarshal(raw, &entry); err == nil {
			state.Entries[key] = entry
		}
	}
	return state
}

// Save writes the state atomically (sort_keys, indent=2, like the Python).
func (s *State) Save(path string) error {
	entries := map[string]any{}
	for key, entry := range s.Entries {
		entries[key] = map[string]any{
			"note_id":              entry.NoteID,
			"fingerprint":          entry.Fingerprint,
			"revision_id":          entry.RevisionID,
			"title":                entry.Title,
			"modified_at":          entry.ModifiedAt,
			"is_deleted":           entry.IsDeleted,
			"metadata_uploaded":    entry.MetadataUploaded,
			"html_uploaded":        entry.HTMLUploaded,
			"attachments_uploaded": entry.AttachmentsUploaded,
			"last_success_at":      entry.LastSuccessAt,
			"last_failure_at":      entry.LastFailureAt,
			"last_error":           entry.LastError,
			"last_checked_at":      entry.LastCheckedAt,
		}
	}
	encoded, err := common.IndentedJSON(map[string]any{
		"schema_version": stateSchemaVersion,
		"account":        s.Account,
		"store_path":     s.StorePath,
		"entries":        entries,
	})
	if err != nil {
		return err
	}
	return common.WriteFileAtomic(path, append(encoded, '\n'), 0o644)
}

// EntryFor returns the note's entry.
func (s *State) EntryFor(noteID string) (StateEntry, bool) {
	entry, ok := s.Entries[noteID]
	return entry, ok
}

// SortedNoteIDs lists the recorded notes.
func (s *State) SortedNoteIDs() []string {
	ids := make([]string, 0, len(s.Entries))
	for id := range s.Entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// MarkSuccess records an uploaded revision.
func (s *State) MarkSuccess(noteID, fingerprint, revisionID, title string, modifiedAt time.Time, isDeleted, metadataUploaded, htmlUploaded, attachmentsUploaded bool, now time.Time) {
	timestamp := common.ISOFormat(now)
	s.Entries[noteID] = StateEntry{
		NoteID: noteID, Fingerprint: fingerprint, RevisionID: revisionID, Title: title,
		ModifiedAt: common.ISOFormat(modifiedAt), IsDeleted: isDeleted, MetadataUploaded: metadataUploaded,
		HTMLUploaded: htmlUploaded, AttachmentsUploaded: attachmentsUploaded, LastSuccessAt: timestamp, LastCheckedAt: timestamp,
	}
}

// MarkFailure records a failed attempt, keeping the last success.
func (s *State) MarkFailure(noteID, errText string, now time.Time) {
	existing := s.Entries[noteID]
	timestamp := common.ISOFormat(now)
	existing.NoteID = noteID
	existing.LastFailureAt = timestamp
	existing.LastError = errText
	existing.LastCheckedAt = timestamp
	s.Entries[noteID] = existing
}
