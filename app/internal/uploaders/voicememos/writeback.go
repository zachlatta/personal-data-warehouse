package voicememos

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// After the warehouse transcribes and enriches a memo, the write-back renames
// the recording inside the Voice Memos app so the library shows the AI title
// instead of "New Recording 12" or a street address. Only auto-named memos
// are ever touched: a title the user typed is never overwritten. The rename
// is a real Core Data save (see storewriter.go) so it syncs to every device.

// AutoNamedFlag is the ZFLAGS bit Voice Memos keeps set while a recording
// still carries the name the app assigned, and clears when the user types a
// title.
const AutoNamedFlag = 0x1000

// MaxTitleLength caps a written title.
const MaxTitleLength = 200

// TransactionAuthor is recorded with every write-back save, so warehouse
// renames are attributable in the store's own persistent history.
const TransactionAuthor = "com.zachlatta.pdw.voice-memo-writeback"

// defaultTitlePattern matches the app's non-location default names, which
// pre-flag-era memos carry without the bit.
var defaultTitlePattern = regexp.MustCompile(`^New Recording \p{Nd}+$`)

// LocalRecordingTitle is one CloudRecordings.db row's naming state.
type LocalRecordingTitle struct {
	UniqueID    string
	RecordingID string
	Title       string
	Flags       int64
	Filename    string
}

// EnrichedTitles is the warehouse's newest completed title per recording,
// keyed both ways.
type EnrichedTitles struct {
	ByRecordingID   map[string]string
	ByContentSHA256 map[string]string
}

// RenamePlanItem is one rename to apply.
type RenamePlanItem struct {
	UniqueID    string `json:"unique_id"`
	RecordingID string `json:"recording_id"`
	OldTitle    string `json:"old_title"`
	NewTitle    string `json:"new_title"`
}

// WritebackSummary is the write-back run's counts.
type WritebackSummary struct {
	LocalRecordings int
	AutoNamed       int
	EnrichedTitles  int
	Planned         int
	Renamed         int
	Skipped         int
	DryRun          bool
}

// IsAutoNamed reports whether the app, not the user, named the recording.
func IsAutoNamed(title string, flags int64) bool {
	if flags&AutoNamedFlag != 0 {
		return true
	}
	return defaultTitlePattern.MatchString(title)
}

// SanitizeTitle drops control characters, collapses whitespace and caps the
// length; ok is false when nothing is left.
func SanitizeTitle(title string) (string, bool) {
	var cleaned strings.Builder
	for _, r := range title {
		if r >= ' ' || r == '\t' || r == '\n' {
			cleaned.WriteRune(r)
		}
	}
	collapsed := strings.Join(strings.Fields(cleaned.String()), " ")
	if collapsed == "" {
		return "", false
	}
	runes := []rune(collapsed)
	if len(runes) > MaxTitleLength {
		runes = runes[:MaxTitleLength]
	}
	return string(runes), true
}

// LoadLocalRecordingTitles reads every named recording from the store (read
// only); a missing store is an empty list.
func LoadLocalRecordingTitles(recordingsPath string) ([]LocalRecordingTitle, error) {
	databasePath := filepath.Join(common.ExpandUser(recordingsPath), "CloudRecordings.db")
	if _, err := os.Stat(databasePath); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	db, err := common.OpenSQLiteReadOnly(databasePath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := common.Query(db, "SELECT ZUNIQUEID, ZPATH, ZENCRYPTEDTITLE, ZFLAGS FROM ZCLOUDRECORDING WHERE ZUNIQUEID IS NOT NULL AND ZPATH IS NOT NULL")
	if err != nil {
		return nil, err
	}
	var titles []LocalRecordingTitle
	for _, row := range rows {
		filename := row.String("ZPATH")
		if filename == "" {
			continue
		}
		titles = append(titles, LocalRecordingTitle{
			UniqueID:    row.String("ZUNIQUEID"),
			RecordingID: stemOf(filename),
			Title:       row.String("ZENCRYPTEDTITLE"),
			Flags:       row.Int("ZFLAGS"),
			Filename:    filename,
		})
	}
	return titles, nil
}

// stemOf mirrors filename.rsplit(".", 1)[0].
func stemOf(filename string) string {
	if idx := strings.LastIndex(filename, "."); idx >= 0 {
		return filename[:idx]
	}
	return filename
}

// ResolveEffectiveTitles maps each local recording's stem to its enriched
// title. Stems normally match enrichments.recording_id directly, but Voice
// Memos rebases filename timestamps when the timezone changes, leaving the
// warehouse knowing a memo only under an older stem; the audio content sha
// (cached per filename in the upload state) is the fallback join key.
func ResolveEffectiveTitles(local []LocalRecordingTitle, titles EnrichedTitles, shaByFilename map[string]string) map[string]string {
	effective := map[string]string{}
	for _, recording := range local {
		title, ok := titles.ByRecordingID[recording.RecordingID]
		if !ok && len(shaByFilename) > 0 {
			if sha := shaByFilename[recording.Filename]; sha != "" {
				title, ok = titles.ByContentSHA256[sha]
			}
		}
		if ok {
			effective[recording.RecordingID] = title
		}
	}
	return effective
}

// BuildRenamePlan lists the auto-named recordings whose enriched title
// differs, newest first (recording ids start with the timestamp), stopping
// at limit when it is positive.
func BuildRenamePlan(local []LocalRecordingTitle, enrichedTitles map[string]string, limit int) []RenamePlanItem {
	sorted := append([]LocalRecordingTitle(nil), local...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].RecordingID > sorted[j].RecordingID })
	var plan []RenamePlanItem
	for _, recording := range sorted {
		if !IsAutoNamed(recording.Title, recording.Flags) {
			continue
		}
		raw, ok := enrichedTitles[recording.RecordingID]
		if !ok {
			continue
		}
		newTitle, ok := SanitizeTitle(raw)
		if !ok || newTitle == recording.Title {
			continue
		}
		plan = append(plan, RenamePlanItem{UniqueID: recording.UniqueID, RecordingID: recording.RecordingID, OldTitle: recording.Title, NewTitle: newTitle})
		if limit > 0 && len(plan) >= limit {
			break
		}
	}
	return plan
}

// enrichmentsRelation is the catalog id of the enrichment attempts table.
const enrichmentsRelation = "apple_voice_memos_enrichments"

// EnrichedTitlesSQL is the statement the write-back runs through the app's
// sql tool: the newest completed enrichment title per recording for the
// account. The relation is named through the catalog, because this SQL
// crosses the HTTP tool API and is not expanded by the warehouse itself --
// and it is rendered UNQUOTED (schema.name), exactly as the Python
// fetch_enriched_titles did, so the wire statement is the same text.
func EnrichedTitlesSQL(account string) string {
	// SchemaOf panics on an id the catalog does not know, which is the
	// fail-loud check DisplayRelation alone would skip.
	_ = warehouse.SchemaOf(enrichmentsRelation)
	return "SELECT DISTINCT ON (recording_id) recording_id, content_sha256, title " +
		"FROM " + warehouse.DisplayRelation(enrichmentsRelation) + " " +
		"WHERE status = 'completed' AND title IS NOT NULL " +
		"AND account = " + common.SQLLiteral(account) + " " +
		"ORDER BY recording_id, created_at DESC"
}

// SQLQuerier runs a statement through the app's sql tool and returns rows;
// common.SQLToolQuery in production, a fake in tests.
type SQLQuerier func(question, statement string) ([]map[string]any, error)

// SQLToolQuerier binds common.SQLToolQuery to the app.
func SQLToolQuerier(baseURL, clientName, token string, timeout time.Duration) SQLQuerier {
	return func(question, statement string) ([]map[string]any, error) {
		return common.SQLToolQuery(baseURL, clientName, token, question, statement, timeout)
	}
}

// FetchEnrichedTitles asks the app for the titles, keyed by recording_id and
// by audio content sha (first title wins per sha).
func FetchEnrichedTitles(query SQLQuerier, account string) (EnrichedTitles, error) {
	rows, err := query("Voice memo enriched titles for app write-back", EnrichedTitlesSQL(account))
	if err != nil {
		return EnrichedTitles{}, fmt.Errorf("enriched title query failed: %w", err)
	}
	titles := EnrichedTitles{ByRecordingID: map[string]string{}, ByContentSHA256: map[string]string{}}
	for _, row := range rows {
		recordingID := pyStrOrEmpty(row["recording_id"])
		title := pyStrOrEmpty(row["title"])
		if recordingID == "" || title == "" {
			continue
		}
		titles.ByRecordingID[recordingID] = title
		if sha := pyStrOrEmpty(row["content_sha256"]); sha != "" {
			if _, exists := titles.ByContentSHA256[sha]; !exists {
				titles.ByContentSHA256[sha] = title
			}
		}
	}
	return titles, nil
}

func pyStrOrEmpty(value any) string {
	if value == nil {
		return ""
	}
	return common.PyStr(value)
}

// WriteResult is one plan item's outcome from the store writer.
type WriteResult struct {
	UniqueID string `json:"unique_id"`
	Status   string `json:"status"`
}

// StoreWriter applies a rename plan to CloudRecordings.db (a real Core Data
// save through the Swift helper); tests inject a fake.
type StoreWriter func(storePath string, items []RenamePlanItem, author string, dryRun bool) ([]WriteResult, error)

// WritebackRunner renames auto-named memos to their enriched titles.
type WritebackRunner struct {
	RecordingsPath string
	Account        string
	Query          SQLQuerier
	Logger         common.Logger
	Writer         StoreWriter // nil means DefaultStoreWriter
	Limit          int         // 0 means no limit
	DryRun         bool
	SHAByFilename  map[string]string
}

// Run performs one write-back pass.
func (r *WritebackRunner) Run() (WritebackSummary, error) {
	root := common.ExpandUser(r.RecordingsPath)
	local, err := LoadLocalRecordingTitles(root)
	if err != nil {
		return WritebackSummary{}, err
	}
	autoNamed := 0
	for _, item := range local {
		if IsAutoNamed(item.Title, item.Flags) {
			autoNamed++
		}
	}
	r.Logger.Infof("Voice Memos write-back: %d local recordings, %d still auto-named", len(local), autoNamed)
	summary := WritebackSummary{LocalRecordings: len(local), AutoNamed: autoNamed, DryRun: r.DryRun}
	if autoNamed == 0 {
		return summary, nil
	}
	titles, err := FetchEnrichedTitles(r.Query, r.Account)
	if err != nil {
		return summary, err
	}
	enriched := ResolveEffectiveTitles(local, titles, r.SHAByFilename)
	plan := BuildRenamePlan(local, enriched, r.Limit)
	summary.EnrichedTitles = len(enriched)
	summary.Planned = len(plan)
	r.Logger.Infof("Voice Memos write-back: %d enriched titles available (%d matched locally), %d renames planned", len(titles.ByRecordingID), len(enriched), len(plan))
	for _, item := range plan {
		verb := "will"
		if r.DryRun {
			verb = "[dry-run] would"
		}
		r.Logger.Infof("%s rename %s: %s -> %s", verb, item.RecordingID, pyRepr(item.OldTitle), pyRepr(item.NewTitle))
	}
	if r.DryRun || len(plan) == 0 {
		return summary, nil
	}
	writer := r.Writer
	if writer == nil {
		writer = DefaultStoreWriter
	}
	results, err := writer(filepath.Join(root, "CloudRecordings.db"), plan, TransactionAuthor, false)
	if err != nil {
		return summary, err
	}
	for _, result := range results {
		if result.Status == StatusRenamed {
			summary.Renamed++
		} else {
			r.Logger.Warningf("Voice Memos write-back skipped %s: %s", result.UniqueID, result.Status)
		}
	}
	summary.Skipped = len(results) - summary.Renamed
	return summary, nil
}

// pyRepr renders a string as Python's repr() does, for the log lines the
// Python write-back printed with %r.
func pyRepr(s string) string {
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, "\"") {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case r == rune(quote) || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\r':
			b.WriteString(`\r`)
		case r < ' ' || r == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte(quote)
	return b.String()
}
