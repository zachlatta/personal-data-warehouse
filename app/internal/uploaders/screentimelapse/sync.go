// Package screentimelapse is `pdw ingest screen-timelapse`: it ships the
// screen timelapse -- the periodic whole-screen images and accessibility trees
// the capture loops write -- to the app one file at a time, exactly as written.
//
// A capture file is immutable once its loop has renamed it into place, and
// its name carries everything the warehouse records about it (see
// internal/timelapsefiles), so the uploader's whole job is: find each settled
// capture file, send it once, and remember that it was acknowledged. The app
// stores the bytes and writes the row in the same request.
package screentimelapse

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/timelapsefiles"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultSettleAge is how long a file must sit unmodified before it is sent.
// The capture loops rename a finished image into place but gzip the Mac's
// tree in place, so a young file may still be growing.
const DefaultSettleAge = 2 * time.Minute

// DefaultLimit is the files sent per run. Newest go first, so a backlog (the
// first run on a Mac holds months of frames) drains behind the live minute
// over successive runs instead of holding the lock for hours in front of it.
const DefaultLimit = 300

// Source is one folder of capture files and the device whose screen they are.
// The device is configured, never the hostname: the Mac that images the phone
// uploads the phone's frames too.
type Source struct {
	Device string
	Dir    string
}

var devicePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// ParseSources reads "device=dir,device=dir".
func ParseSources(spec string) ([]Source, error) {
	var out []Source
	seen := map[string]bool{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		device, dir, ok := strings.Cut(part, "=")
		device, dir = strings.TrimSpace(device), strings.TrimSpace(dir)
		if !ok || dir == "" || !devicePattern.MatchString(device) {
			return nil, fmt.Errorf("source %q must be device=folder with a lowercase device name", part)
		}
		if seen[device] {
			return nil, fmt.Errorf("device %q is configured twice", device)
		}
		seen[device] = true
		out = append(out, Source{Device: device, Dir: common.ExpandUser(dir)})
	}
	if len(out) == 0 {
		return nil, errors.New("no sources configured")
	}
	return out, nil
}

// State is the ledger of acknowledged files. A file is skipped only while its
// path, size and mtime all still match what the app acknowledged; the app
// dedups by sha too, so a lost ledger costs a resend, never a duplicate.
type State struct {
	db *common.StateDB
}

// DefaultStateFile is the ledger path.
func DefaultStateFile() string {
	return common.DefaultStateFile("screen-timelapse-upload-state.sqlite")
}

// OpenState opens the ledger.
func OpenState(path string) (*State, error) {
	db, err := common.OpenStateDB(path, []string{
		`CREATE TABLE IF NOT EXISTS acknowledged (
			path TEXT PRIMARY KEY,
			size_bytes INTEGER NOT NULL,
			mtime_ns INTEGER NOT NULL,
			content_sha256 TEXT NOT NULL DEFAULT '',
			acknowledged_at TEXT NOT NULL DEFAULT ''
		)`,
	}, map[string]string{"schema_version": "1"}, []string{"acknowledged"})
	if err != nil {
		return nil, err
	}
	return &State{db: db}, nil
}

// Close releases the database.
func (s *State) Close() error { return s.db.Close() }

func (s *State) acknowledged() (map[string][2]int64, error) {
	rows, err := common.Query(s.db.DB, "SELECT path, size_bytes, mtime_ns FROM acknowledged")
	if err != nil {
		return nil, err
	}
	out := make(map[string][2]int64, len(rows))
	for _, row := range rows {
		out[row.String("path")] = [2]int64{row.Int("size_bytes"), row.Int("mtime_ns")}
	}
	return out, nil
}

func (s *State) acknowledge(c candidate, sha string, now time.Time) error {
	_, err := s.db.DB.Exec(`INSERT INTO acknowledged (path, size_bytes, mtime_ns, content_sha256, acknowledged_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET size_bytes = excluded.size_bytes, mtime_ns = excluded.mtime_ns,
			content_sha256 = excluded.content_sha256, acknowledged_at = excluded.acknowledged_at`,
		c.path, c.size, c.modified.UnixNano(), sha, common.ISOFormat(now))
	return err
}

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadScreenTimelapseFile(content []byte, f ingestclient.ScreenTimelapseFile) (ingestclient.ScreenTimelapseStored, error)
}

// Summary is one run's counts.
type Summary struct {
	Uploaded  int
	Skipped   int // the app already held the file
	Unsettled int // recognised, but still too young to send
	Remaining int // waiting for a later run (the limit)
	Deferred  bool
}

// Runner sends every unacknowledged settled capture file, newest first.
type Runner struct {
	Sources           []Source
	Client            Uploader
	State             *State
	Logger            common.Logger
	Now               func() time.Time
	Location          *time.Location // the capture host's zone
	SettleAge         time.Duration
	Limit             int
	BeforeUploadCheck func() string
}

type candidate struct {
	source   Source
	path     string
	file     timelapsefiles.File
	size     int64
	modified time.Time
}

// Sync runs one pass. It stops at the first failed upload -- the app or its
// database being down fails every file alike -- and returns the error, so the
// run's heartbeat reads failing; that file is retried next run.
func (r *Runner) Sync() (Summary, error) {
	if r.Client == nil || r.State == nil {
		return Summary{}, errors.New("ingest client and state are required")
	}
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now()
	}
	loc := r.Location
	if loc == nil {
		loc = time.Local
	}
	settle := r.SettleAge
	if settle <= 0 {
		settle = DefaultSettleAge
	}
	limit := r.Limit
	if limit <= 0 {
		limit = DefaultLimit
	}
	acknowledged, err := r.State.acknowledged()
	if err != nil {
		return Summary{}, err
	}
	var summary Summary
	var pending []candidate
	for _, source := range r.Sources {
		entries, err := os.ReadDir(source.Dir)
		if err != nil {
			return summary, fmt.Errorf("read %s source %s: %w", source.Device, source.Dir, err)
		}
		for _, entry := range entries {
			file, ok := timelapsefiles.Parse(entry.Name())
			if !ok || !entry.Type().IsRegular() {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				continue // renamed away between the listing and the stat
			}
			c := candidate{source: source, path: filepath.Join(source.Dir, entry.Name()), file: file, size: info.Size(), modified: info.ModTime()}
			if now.Sub(c.modified) < settle {
				summary.Unsettled++
				continue
			}
			if ack, ok := acknowledged[c.path]; ok && ack == [2]int64{c.size, c.modified.UnixNano()} {
				continue
			}
			pending = append(pending, c)
		}
	}
	sort.Slice(pending, func(i, j int) bool {
		if !pending[i].modified.Equal(pending[j].modified) {
			return pending[i].modified.After(pending[j].modified)
		}
		return pending[i].path > pending[j].path
	})
	if len(pending) > limit {
		summary.Remaining = len(pending) - limit
		pending = pending[:limit]
	}
	if len(pending) > 0 && r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping screen timelapse upload: %s", reason)
			summary.Deferred = true
			summary.Remaining += len(pending)
			return summary, nil
		}
	}
	for index, c := range pending {
		content, err := os.ReadFile(c.path)
		if err != nil {
			summary.Remaining += len(pending) - index
			return summary, fmt.Errorf("read %s: %w", c.path, err)
		}
		stored, err := r.Client.UploadScreenTimelapseFile(content, ingestclient.ScreenTimelapseFile{
			Device:         c.source.Device,
			SourceFilename: filepath.Base(c.path),
			CapturedAt:     c.file.CapturedAt(loc, c.modified).Format(time.RFC3339),
			ContentType:    c.file.MimeType,
		})
		if err != nil {
			summary.Remaining += len(pending) - index
			return summary, fmt.Errorf("upload %s: %w", c.path, err)
		}
		if err := r.State.acknowledge(c, sha256Hex(content), now); err != nil {
			return summary, err
		}
		if stored.Skipped {
			summary.Skipped++
		} else {
			summary.Uploaded++
		}
	}
	return summary, nil
}

func sha256Hex(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}
