// Package voicememos uploads the Mac's Voice Memos recordings through the
// app's /ingest/voice-memos endpoints and writes enriched titles back into
// the Voice Memos app.
package voicememos

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultRecordingsPath is the Voice Memos shared container.
const DefaultRecordingsPath = "~/Library/Group Containers/group.com.apple.VoiceMemos.shared/Recordings"

// DefaultExtensions are the recording file types.
var DefaultExtensions = []string{".m4a", ".qta"}

// Candidate is one recording file before its bytes are hashed.
type Candidate struct {
	Path           string
	RecordingID    string
	Filename       string
	Extension      string
	SizeBytes      int64
	FileCreatedAt  time.Time
	FileModifiedAt time.Time
	BirthtimeNS    int64
	MtimeNS        int64
	RecordedAt     time.Time
	Duration       *float64
	LocalDuration  *float64
}

// Recording is a candidate plus its content hash.
type Recording struct {
	Candidate
	Title         string
	ContentType   string
	ContentSHA256 string
}

// cloudMetadata is what CloudRecordings.db knows per file.
type cloudMetadata struct {
	duration      *float64
	localDuration *float64
}

// ScanCandidates lists the recordings under root with one of the extensions.
func ScanCandidates(root string, extensions []string) ([]Candidate, error) {
	root = common.ExpandUser(root)
	allowed := map[string]bool{}
	for _, ext := range extensions {
		allowed[strings.ToLower(ext)] = true
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	metadata := loadCloudRecordingMetadata(root)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	var candidates []Candidate
	for _, name := range names {
		path := filepath.Join(root, name)
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		if !allowed[strings.ToLower(filepath.Ext(name))] {
			continue
		}
		candidate, err := candidateFromPath(path, info, metadata[name])
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

func candidateFromPath(path string, info os.FileInfo, meta cloudMetadata) (Candidate, error) {
	birthSec, birthNsec, mtimeSec, mtimeNsec := fileTimes(info)
	createdAt := common.UnixFromFloat(common.StatSeconds(birthSec, birthNsec))
	modifiedAt := common.UnixFromFloat(common.StatSeconds(mtimeSec, mtimeNsec))
	name := filepath.Base(path)
	stem := strings.TrimSuffix(name, filepath.Ext(name))
	recordedAt, ok := RecordedAtFromFilename(stem)
	if !ok {
		recordedAt = createdAt
	}
	return Candidate{
		Path:           path,
		RecordingID:    stem,
		Filename:       name,
		Extension:      strings.ToLower(filepath.Ext(name)),
		SizeBytes:      info.Size(),
		FileCreatedAt:  createdAt,
		FileModifiedAt: modifiedAt,
		BirthtimeNS:    birthSec*1_000_000_000 + birthNsec,
		MtimeNS:        mtimeSec*1_000_000_000 + mtimeNsec,
		RecordedAt:     recordedAt,
		Duration:       meta.duration,
		LocalDuration:  meta.localDuration,
	}, nil
}

func fileTimes(info os.FileInfo) (birthSec, birthNsec, mtimeSec, mtimeNsec int64) {
	mtime := info.ModTime()
	mtimeSec, mtimeNsec = mtime.Unix(), int64(mtime.Nanosecond())
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		birthSec, birthNsec = birthtime(stat)
		return
	}
	return mtimeSec, mtimeNsec, mtimeSec, mtimeNsec
}

// RecordingFromCandidate hashes the file and resolves its content type.
func RecordingFromCandidate(candidate Candidate) (Recording, error) {
	sha, err := common.FileSHA256(candidate.Path)
	if err != nil {
		return Recording{}, err
	}
	return Recording{
		Candidate:     candidate,
		Title:         candidate.RecordingID,
		ContentType:   ContentTypeForExtension(candidate.Extension),
		ContentSHA256: sha,
	}, nil
}

// ContentTypeForExtension mirrors the Python: .qta and .m4a are pinned,
// everything else asks the Python mimetypes table.
func ContentTypeForExtension(extension string) string {
	switch extension {
	case ".qta":
		return "audio/quicktime"
	case ".m4a":
		return "audio/mp4"
	}
	if mapped := common.GuessMimeType("file" + extension); mapped != "" {
		return mapped
	}
	return "application/octet-stream"
}

// RecordedAtFromFilename parses the "YYYYMMDD HHMMSS" prefix Voice Memos
// gives every recording.
func RecordedAtFromFilename(stem string) (time.Time, bool) {
	if len(stem) < 15 {
		return time.Time{}, false
	}
	t, err := time.Parse("20060102 150405", stem[:15])
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

func loadCloudRecordingMetadata(root string) map[string]cloudMetadata {
	out := map[string]cloudMetadata{}
	databasePath := filepath.Join(root, "CloudRecordings.db")
	if _, err := os.Stat(databasePath); err != nil {
		return out
	}
	db, err := common.OpenSQLiteReadOnly(databasePath)
	if err != nil {
		return out
	}
	defer db.Close()
	rows, err := common.Query(db, "SELECT ZPATH, ZDURATION, ZLOCALDURATION FROM ZCLOUDRECORDING WHERE ZPATH IS NOT NULL")
	if err != nil {
		return out
	}
	for _, row := range rows {
		filename := row.String("ZPATH")
		if filename == "" {
			continue
		}
		out[filename] = cloudMetadata{duration: optionalFloat(row.Get("ZDURATION")), localDuration: optionalFloat(row.Get("ZLOCALDURATION"))}
	}
	return out
}

func optionalFloat(value any) *float64 {
	if value == nil {
		return nil
	}
	f := common.ToFloat(value)
	return &f
}
