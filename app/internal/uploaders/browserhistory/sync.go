package browserhistory

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultBatchSize is the visits per uploaded batch. A first backfill of a
// heavily used browser is a few hundred thousand visits; 5,000 keeps each
// gzipped batch around a megabyte and each Dagster promotion small.
const DefaultBatchSize = 5000

// State is the per-profile upload high-water mark: the largest visit id
// shipped from each (browser, profile) history database. Visit ids are
// AUTOINCREMENT in both Chromium and Safari, so everything above the mark is
// new -- including visits a browser's sync delivered from another device.
type State struct {
	db *common.StateDB
}

// DefaultStateFile is the state path.
func DefaultStateFile() string {
	return common.DefaultStateFile("browser-history-upload-state.sqlite")
}

// OpenState opens the state for (account, device).
func OpenState(path, account, device string) (*State, error) {
	db, err := common.OpenStateDB(path, []string{
		`CREATE TABLE IF NOT EXISTS upload_state (
			browser TEXT NOT NULL,
			profile TEXT NOT NULL,
			last_visit_id INTEGER NOT NULL DEFAULT 0,
			last_success_at TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (browser, profile)
		)`,
	}, map[string]string{"schema_version": "1", "account": account, "device": device}, []string{"upload_state"})
	if err != nil {
		return nil, err
	}
	return &State{db: db}, nil
}

// Close releases the database.
func (s *State) Close() error { return s.db.Close() }

// LastVisitID is the profile's high-water mark (0 when never uploaded).
func (s *State) LastVisitID(browser, profile string) (int64, error) {
	rows, err := common.Query(s.db.DB, "SELECT last_visit_id FROM upload_state WHERE browser = ? AND profile = ?", browser, profile)
	if err != nil || len(rows) == 0 {
		return 0, err
	}
	return rows[0].Int("last_visit_id"), nil
}

// SetLastVisitID records an uploaded batch's last visit id.
func (s *State) SetLastVisitID(browser, profile string, visitID int64, now time.Time) error {
	_, err := s.db.DB.Exec(`INSERT INTO upload_state (browser, profile, last_visit_id, last_success_at) VALUES (?, ?, ?, ?)
		ON CONFLICT(browser, profile) DO UPDATE SET last_visit_id = excluded.last_visit_id, last_success_at = excluded.last_success_at`,
		browser, profile, visitID, common.ISOFormat(now))
	return err
}

// Summary is the run's counts.
type Summary struct {
	Profiles        int
	VisitsUploaded  int
	Searches        int
	BatchesUploaded int
	Deferred        bool
	Failed          []string
}

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadBrowserHistoryBatch(gzipBytes []byte, exportedAt string) (ingestclient.StoredObject, error)
}

// Runner uploads new visits from every discovered profile.
type Runner struct {
	Account           string
	Device            string
	Home              string
	Browsers          []Browser
	Client            Uploader
	Logger            common.Logger
	State             *State
	Now               func() time.Time
	Mode              string
	Limit             int
	BatchSize         int
	BeforeUploadCheck func() string
}

// ErrProfilesFailed is returned (wrapped) when some profile could not be
// read; the readable profiles were still uploaded.
var ErrProfilesFailed = errors.New("some browser profiles could not be read")

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
	if r.BatchSize <= 0 {
		r.BatchSize = DefaultBatchSize
	}
	browsers := r.Browsers
	if browsers == nil {
		browsers = DefaultBrowsers
	}
	profiles, err := DiscoverProfiles(r.Home, browsers)
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{Profiles: len(profiles)}
	tempDir, err := os.MkdirTemp("", "pdw-browser-history-")
	if err != nil {
		return summary, err
	}
	defer os.RemoveAll(tempDir)

	run := &profileRun{runner: r, remaining: r.Limit}
	for index, profile := range profiles {
		if r.Limit > 0 && run.remaining <= 0 {
			break
		}
		err := run.sync(profile, filepath.Join(tempDir, fmt.Sprint(index)))
		if run.deferred {
			break
		}
		if err != nil {
			r.Logger.Warningf("Browser history %s/%s failed: %v", profile.Browser, profile.Key, err)
			summary.Failed = append(summary.Failed, fmt.Sprintf("%s/%s: %v", profile.Browser, profile.Key, err))
		}
	}
	summary.VisitsUploaded = run.uploaded
	summary.Searches = run.searches
	summary.BatchesUploaded = run.batches
	summary.Deferred = run.deferred
	if len(summary.Failed) > 0 {
		return summary, fmt.Errorf("%w: %s", ErrProfilesFailed, strings.Join(summary.Failed, "; "))
	}
	return summary, nil
}

// profileRun carries one Sync's running totals across profiles.
type profileRun struct {
	runner    *Runner
	remaining int
	guarded   bool
	deferred  bool
	uploaded  int
	searches  int
	batches   int
}

func (p *profileRun) sync(profile Profile, dir string) error {
	r := p.runner
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	snapshot, err := Snapshot(profile.HistoryPath, dir)
	if err != nil {
		return err
	}
	db, err := common.OpenSQLite(snapshot)
	if err != nil {
		return err
	}
	defer db.Close()
	maxID, err := MaxVisitID(db, profile.Engine)
	if err != nil {
		return fmt.Errorf("read %s: %w", profile.HistoryPath, err)
	}
	after := int64(0)
	if r.Mode == "incremental" && r.State != nil {
		if after, err = r.State.LastVisitID(profile.Browser, profile.Key); err != nil {
			return err
		}
		// A history database whose ids fell below the mark was recreated
		// (profile reset, history wiped and rebuilt): start it over. The
		// warehouse upserts by visit id, so a re-send never duplicates.
		if maxID < after {
			r.Logger.Warningf("Browser history %s/%s restarted below the upload mark (%d < %d); re-reading it", profile.Browser, profile.Key, maxID, after)
			after = 0
		}
	}
	for {
		size := r.BatchSize
		if r.Limit > 0 && p.remaining < size {
			size = p.remaining
		}
		if size <= 0 {
			return nil
		}
		visits, err := ScanVisits(db, profile.Engine, after, size)
		if err != nil {
			return fmt.Errorf("read %s: %w", profile.HistoryPath, err)
		}
		if len(visits) == 0 {
			return nil
		}
		if !p.guarded && r.BeforeUploadCheck != nil {
			if reason := r.BeforeUploadCheck(); reason != "" {
				r.Logger.Warningf("Skipping browser history upload: %s", reason)
				p.deferred = true
				return nil
			}
		}
		p.guarded = true
		exportedAt := r.Now()
		records := make([]map[string]any, 0, len(visits))
		for _, visit := range visits {
			if visit.SearchTerm != "" {
				p.searches++
			}
			records = append(records, Envelope(r.Account, exportedAt, VisitPayload(r.Device, profile, visit)))
		}
		body, err := common.GzipJSONL(records)
		if err != nil {
			return err
		}
		stored, err := r.Client.UploadBrowserHistoryBatch(body, common.ISOFormat(exportedAt))
		if err != nil {
			return err
		}
		p.batches++
		p.uploaded += len(visits)
		p.remaining -= len(visits)
		after = visits[len(visits)-1].VisitID
		r.Logger.Infof("Uploaded browser history batch %s with %d %s/%s visits", stored.StorageKey, len(visits), profile.Browser, profile.Key)
		if r.State != nil {
			if err := r.State.SetLastVisitID(profile.Browser, profile.Key, after, exportedAt); err != nil {
				return err
			}
		}
		if len(visits) < size {
			return nil
		}
	}
}

// VisitPayload is the record shipped for one visit.
func VisitPayload(device string, profile Profile, visit Visit) map[string]any {
	raw := visit.Raw
	if raw == nil {
		raw = map[string]any{}
	}
	return map[string]any{
		"device":            device,
		"browser":           profile.Browser,
		"profile":           profile.Key,
		"profile_name":      profile.Name,
		"visit_id":          fmt.Sprint(visit.VisitID),
		"url_id":            fmt.Sprint(visit.URLID),
		"url":               visit.URL,
		"host":              visit.Host,
		"title":             visit.Title,
		"visited_at":        common.ISOFormat(visit.VisitedAt),
		"transition":        visit.Transition,
		"transition_raw":    visit.RawTransit,
		"is_redirect":       visit.IsRedirect,
		"is_synced":         visit.IsSynced,
		"from_visit_id":     fmt.Sprint(visit.FromVisitID),
		"visit_duration_ms": visit.DurationMS,
		"search_term":       visit.SearchTerm,
		"raw":               raw,
	}
}

// Envelope wraps one record for the batch.
func Envelope(account string, exportedAt time.Time, record map[string]any) map[string]any {
	return map[string]any{
		"schema_version": int64(1),
		"source":         "browser_history",
		"account":        account,
		"exported_at":    common.ISOFormat(exportedAt),
		"record_type":    "visit",
		"record":         record,
	}
}
