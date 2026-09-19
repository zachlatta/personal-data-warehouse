package voicememos

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const stateSchemaVersion = int64(1)

// StateEntry is one recording's recorded upload.
type StateEntry struct {
	Path             string `json:"path"`
	SizeBytes        int64  `json:"size_bytes"`
	MtimeNS          int64  `json:"mtime_ns"`
	BirthtimeNS      int64  `json:"birthtime_ns"`
	ContentSHA256    string `json:"content_sha256"`
	AudioUploaded    bool   `json:"audio_uploaded"`
	MetadataUploaded bool   `json:"metadata_uploaded"`
	LastSuccessAt    string `json:"last_success_at"`
	LastFailureAt    string `json:"last_failure_at"`
	LastError        string `json:"last_error"`
	LastCheckedAt    string `json:"last_checked_at"`
}

// Matches reports whether the file is unchanged since the entry was written.
func (e StateEntry) Matches(c Candidate) bool {
	return e.SizeBytes == c.SizeBytes && e.MtimeNS == c.MtimeNS && e.BirthtimeNS == c.BirthtimeNS
}

// Complete reports whether both halves landed.
func (e StateEntry) Complete() bool {
	return e.AudioUploaded && e.MetadataUploaded && e.ContentSHA256 != ""
}

// State is the JSON state file (voice-memos-upload-state.json).
type State struct {
	Account        string
	RecordingsPath string
	Entries        map[string]StateEntry
}

// DefaultStateFile is the state path.
func DefaultStateFile() string {
	return common.DefaultStateFile("voice-memos-upload-state.json")
}

// EmptyState is a fresh state.
func EmptyState(account, recordingsPath string) *State {
	return &State{Account: account, RecordingsPath: recordingsPath, Entries: map[string]StateEntry{}}
}

// LoadState reads the file, empty when missing or for another account/root.
func LoadState(path, account, recordingsPath string) *State {
	data, err := os.ReadFile(path)
	if err != nil {
		return EmptyState(account, recordingsPath)
	}
	var payload struct {
		SchemaVersion  int64                      `json:"schema_version"`
		Account        string                     `json:"account"`
		RecordingsPath string                     `json:"recordings_path"`
		Entries        map[string]json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(data, &payload); err != nil || payload.SchemaVersion != stateSchemaVersion || payload.Account != account || payload.RecordingsPath != recordingsPath {
		return EmptyState(account, recordingsPath)
	}
	state := EmptyState(account, recordingsPath)
	for key, raw := range payload.Entries {
		var entry StateEntry
		if err := json.Unmarshal(raw, &entry); err == nil {
			state.Entries[key] = entry
		}
	}
	return state
}

// Save writes the state atomically in the Python layout.
func (s *State) Save(path string) error {
	entries := map[string]any{}
	for key, entry := range s.Entries {
		entries[key] = map[string]any{
			"path":              entry.Path,
			"size_bytes":        entry.SizeBytes,
			"mtime_ns":          entry.MtimeNS,
			"birthtime_ns":      entry.BirthtimeNS,
			"content_sha256":    entry.ContentSHA256,
			"audio_uploaded":    entry.AudioUploaded,
			"metadata_uploaded": entry.MetadataUploaded,
			"last_success_at":   entry.LastSuccessAt,
			"last_failure_at":   entry.LastFailureAt,
			"last_error":        entry.LastError,
			"last_checked_at":   entry.LastCheckedAt,
		}
	}
	encoded, err := common.IndentedJSON(map[string]any{
		"schema_version":  stateSchemaVersion,
		"account":         s.Account,
		"recordings_path": s.RecordingsPath,
		"entries":         entries,
	})
	if err != nil {
		return err
	}
	return common.WriteFileAtomic(path, append(encoded, '\n'), 0o644)
}

// StateKey is the recording's path relative to the root (pathlib
// relative_to), falling back to the file name.
func (s *State) StateKey(path string) string {
	root := strings.TrimRight(s.RecordingsPath, "/")
	if rel, err := filepath.Rel(root, path); err == nil && !strings.HasPrefix(rel, "..") && rel != "." {
		return rel
	}
	return filepath.Base(path)
}

// EntryFor returns the candidate's entry.
func (s *State) EntryFor(c Candidate) (StateEntry, bool) {
	entry, ok := s.Entries[s.StateKey(c.Path)]
	return entry, ok
}

// MarkSuccess records an upload.
func (s *State) MarkSuccess(c Candidate, contentSHA256 string, audioUploaded, metadataUploaded bool, now time.Time) {
	key := s.StateKey(c.Path)
	timestamp := common.ISOFormat(now)
	s.Entries[key] = StateEntry{
		Path: key, SizeBytes: c.SizeBytes, MtimeNS: c.MtimeNS, BirthtimeNS: c.BirthtimeNS, ContentSHA256: contentSHA256,
		AudioUploaded: audioUploaded, MetadataUploaded: metadataUploaded, LastSuccessAt: timestamp, LastCheckedAt: timestamp,
	}
}

// MarkFailure records a failed attempt.
func (s *State) MarkFailure(c Candidate, contentSHA256, errText string, now time.Time) {
	key := s.StateKey(c.Path)
	existing := s.Entries[key]
	timestamp := common.ISOFormat(now)
	sha := contentSHA256
	if sha == "" {
		sha = existing.ContentSHA256
	}
	s.Entries[key] = StateEntry{
		Path: key, SizeBytes: c.SizeBytes, MtimeNS: c.MtimeNS, BirthtimeNS: c.BirthtimeNS, ContentSHA256: sha,
		AudioUploaded: existing.AudioUploaded, MetadataUploaded: existing.MetadataUploaded, LastSuccessAt: existing.LastSuccessAt,
		LastFailureAt: timestamp, LastError: errText, LastCheckedAt: timestamp,
	}
}

// SHAByFilename maps each recorded filename to its audio sha, the drift-proof
// identity the write-back uses when a filename timestamp has been rebased.
func (s *State) SHAByFilename() map[string]string {
	out := map[string]string{}
	for filename, entry := range s.Entries {
		if entry.ContentSHA256 != "" {
			out[filename] = entry.ContentSHA256
		}
	}
	return out
}
