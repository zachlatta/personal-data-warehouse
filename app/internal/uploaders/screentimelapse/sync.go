// Package screentimelapse is `pdw ingest screen-timelapse`: it ships the
// screen timelapse -- the periodic whole-screen images and accessibility trees
// the capture loops write -- to the app one file at a time.
//
// A capture file is immutable once its loop has renamed it into place, and
// its name carries everything the warehouse records about it (see
// internal/timelapsefiles), so the uploader's whole job is: find each settled
// capture file, send it once, and remember that it was acknowledged. The app
// stores the bytes and writes the row in the same request. The one exception
// to "exactly as written" is the card guard (redact.go): a moment whose tree
// holds a card number is sent redacted or withheld.
package screentimelapse

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
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

// ackRecord is what the ledger holds for one path. contentSHA256 is the sha
// of the bytes the app acknowledged, or withheldMarker for a file that was
// deliberately never sent.
type ackRecord struct {
	size, mtimeNS int64
	contentSHA256 string
}

func (a ackRecord) matches(c candidate) bool {
	return a.size == c.size && a.mtimeNS == c.modified.UnixNano()
}

// withheldMarker stands in for the sha of a screenshot the card guard
// withheld: recorded at the file's size and mtime, so it is not considered
// again until it changes.
const withheldMarker = "withheld: " + cardReason

func (s *State) acknowledged() (map[string]ackRecord, error) {
	rows, err := common.Query(s.db.DB, "SELECT path, size_bytes, mtime_ns, content_sha256 FROM acknowledged")
	if err != nil {
		return nil, err
	}
	out := make(map[string]ackRecord, len(rows))
	for _, row := range rows {
		out[row.String("path")] = ackRecord{row.Int("size_bytes"), row.Int("mtime_ns"), row.String("content_sha256")}
	}
	return out, nil
}

func (s *State) acknowledge(c *candidate, sha string, now time.Time) error {
	_, err := s.db.DB.Exec(`INSERT INTO acknowledged (path, size_bytes, mtime_ns, content_sha256, acknowledged_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET size_bytes = excluded.size_bytes, mtime_ns = excluded.mtime_ns,
			content_sha256 = excluded.content_sha256, acknowledged_at = excluded.acknowledged_at`,
		c.path, c.size, c.modified.UnixNano(), sha, common.ISOFormat(now))
	return err
}

func (s *State) forget(path string) error {
	_, err := s.db.DB.Exec(`DELETE FROM acknowledged WHERE path = ?`, path)
	return err
}

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadScreenTimelapseFile(content []byte, f ingestclient.ScreenTimelapseFile) (ingestclient.ScreenTimelapseStored, error)
}

// Summary is one run's counts.
type Summary struct {
	Uploaded   int
	Skipped    int // the app already held the file
	Compressed int // PNG screenshots re-encoded in memory before upload
	Deleted    int // local files unlinked after the server acknowledged them
	Unsettled  int // recognised, but still too young to send
	Held       int // settled, waiting for its moment's tree to settle
	Redacted   int // sent with its card number replaced or boxed out
	Withheld   int // a screenshot of a card moment the box could not cover
	Remaining  int // waiting for a later run (the limit)
	Deferred   bool
}

// DefaultNoTreeWait is how long past the settle age a moment's screenshots
// wait for a tree that has not appeared. The Mac gzips its tree in place after
// the image is renamed into place, so a screenshot can settle before its
// tree exists under the name the uploader reads.
const DefaultNoTreeWait = 10 * time.Minute

// Runner sends every unacknowledged settled capture file, newest first, one
// moment (a device's screenshots and tree of one instant) at a time.
type Runner struct {
	Sources           []Source
	Client            Uploader
	State             *State
	Logger            common.Logger
	Now               func() time.Time
	Location          *time.Location // the capture host's zone
	SettleAge         time.Duration
	NoTreeWait        time.Duration
	Limit             int
	Codec             ImageCodec // default SystemCodec
	CompressPNG       bool
	CompressQuality   int
	CompressMaxEdge   int
	Compressor        PNGCompressor
	DeleteAfterUpload bool
	BeforeUploadCheck func() string
}

type candidate struct {
	source     Source
	path       string
	file       timelapsefiles.File
	size       int64
	modified   time.Time
	capturedAt time.Time
	settled    bool
	// editedWithheld: the guard withheld this file, and it has changed
	// since. The edit (a manual redaction) is the human review, so it is
	// sent as it now is, without a scan.
	editedWithheld bool
}

// moment is every recognised file of one device's instant on disk: trees
// have DisplayIndex 0, screenshots 1..n.
type moment struct {
	tree        *candidate
	screenshots []*candidate
	pending     []*candidate // settled and not acknowledged as they are
}

func (m *moment) newest() time.Time {
	var newest time.Time
	for _, c := range m.pending {
		if c.modified.After(newest) {
			newest = c.modified
		}
	}
	return newest
}

// ready is whether the moment can be judged: all of its screenshots have
// settled, and its tree has settled (or it has none and its screenshots are
// past the wait for one).
func (m *moment) ready(now time.Time, settle, noTreeWait time.Duration) bool {
	for _, c := range m.screenshots {
		if !c.settled {
			return false
		}
	}
	if m.tree != nil {
		return m.tree.settled
	}
	for _, c := range m.screenshots {
		if now.Sub(c.modified) < settle+noTreeWait {
			return false
		}
	}
	return true
}

func (m *moment) hasPreviouslyWithheld(acknowledged map[string]ackRecord) bool {
	for _, c := range m.screenshots {
		if indexOf(m.pending, c) >= 0 {
			continue
		}
		if ack, ok := acknowledged[c.path]; ok && ack.contentSHA256 == withheldMarker {
			return true
		}
	}
	return false
}

// outgoing is one file to send and the bytes to send for it.
type outgoing struct {
	c          *candidate
	content    []byte
	filename   string
	mimeType   string
	redacted   bool
	compressed bool
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
	noTreeWait := r.NoTreeWait
	if noTreeWait <= 0 {
		noTreeWait = DefaultNoTreeWait
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
	moments := map[string]*moment{}
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
			c := &candidate{source: source, path: filepath.Join(source.Dir, entry.Name()), file: file, size: info.Size(), modified: info.ModTime()}
			c.capturedAt = file.CapturedAt(loc, c.modified)
			c.settled = now.Sub(c.modified) >= settle
			key := source.Device + "\x00" + c.capturedAt.Format(time.RFC3339Nano)
			m := moments[key]
			if m == nil {
				m = &moment{}
				moments[key] = m
			}
			if file.Kind == timelapsefiles.AccessibilityTree {
				m.tree = c
			} else {
				m.screenshots = append(m.screenshots, c)
			}
			if !c.settled {
				summary.Unsettled++
				continue
			}
			ack, ok := acknowledged[c.path]
			if ok && ack.matches(*c) {
				continue
			}
			c.editedWithheld = ok && ack.contentSHA256 == withheldMarker
			m.pending = append(m.pending, c)
		}
	}
	var ready []*moment
	for _, m := range moments {
		if len(m.pending) == 0 {
			continue
		}
		if !m.ready(now, settle, noTreeWait) {
			summary.Held += len(m.pending)
			continue
		}
		sortNewestFirst(m.pending)
		ready = append(ready, m)
	}
	sort.Slice(ready, func(i, j int) bool {
		a, b := ready[i].newest(), ready[j].newest()
		if !a.Equal(b) {
			return a.After(b)
		}
		return ready[i].pending[0].path > ready[j].pending[0].path
	})
	// The limit cuts between moments, never through one.
	taken, files := 0, 0
	for taken < len(ready) && (taken == 0 || files+len(ready[taken].pending) <= limit) {
		files += len(ready[taken].pending)
		taken++
	}
	for _, m := range ready[taken:] {
		summary.Remaining += len(m.pending)
	}
	ready = ready[:taken]
	if len(ready) > 0 && r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping screen timelapse upload: %s", reason)
			summary.Deferred = true
			summary.Remaining += files
			return summary, nil
		}
	}
	left := files
	for _, m := range ready {
		sends, withheld, err := r.judgeMoment(m)
		if err != nil {
			summary.Remaining += left
			return summary, err
		}
		for _, c := range withheld {
			if err := r.State.acknowledge(c, withheldMarker, now); err != nil {
				return summary, err
			}
			summary.Withheld++
			left--
		}
		for _, out := range sends {
			stored, err := r.Client.UploadScreenTimelapseFile(out.content, ingestclient.ScreenTimelapseFile{
				Device:         out.c.source.Device,
				SourceFilename: out.filename,
				CapturedAt:     out.c.capturedAt.Format(time.RFC3339),
				ContentType:    out.mimeType,
			})
			if err != nil {
				summary.Remaining += left
				return summary, fmt.Errorf("upload %s: %w", out.c.path, err)
			}
			if err := r.State.acknowledge(out.c, sha256Hex(out.content), now); err != nil {
				return summary, err
			}
			left--
			if out.redacted {
				summary.Redacted++
			}
			if out.compressed {
				summary.Compressed++
			}
			if stored.Skipped {
				summary.Skipped++
			} else {
				summary.Uploaded++
			}
		}
		// Only unlink a moment's files once every send in the moment has
		// succeeded and no screenshot is withheld, unlinking screenshots
		// before the accessibility tree so a failed screenshot upload never
		// loses the tree needed to box out card numbers on retry.
		if r.DeleteAfterUpload && len(withheld) == 0 && !m.hasPreviouslyWithheld(acknowledged) {
			for _, c := range m.screenshots {
				if err := os.Remove(c.path); err != nil && !os.IsNotExist(err) {
					return summary, fmt.Errorf("remove %s after upload: %w", c.path, err)
				}
				if err := r.State.forget(c.path); err != nil {
					return summary, err
				}
				summary.Deleted++
			}
			if m.tree != nil {
				if err := os.Remove(m.tree.path); err != nil && !os.IsNotExist(err) {
					return summary, fmt.Errorf("remove %s after upload: %w", m.tree.path, err)
				}
				if err := r.State.forget(m.tree.path); err != nil {
					return summary, err
				}
				summary.Deleted++
			}
		}
	}
	return summary, nil
}

// judgeMoment scans the moment's tree before anything of it is sent, and
// returns the bytes to send for each pending file and the screenshots to
// withhold. Any failure (a file unreadable, a codec error, a compressor error)
// fails the run before a byte of the moment leaves, so the whole moment is
// judged again.
func (r *Runner) judgeMoment(m *moment) ([]outgoing, []*candidate, error) {
	var sends []outgoing
	var judged []*candidate
	for _, c := range m.pending {
		if c.editedWithheld {
			content, err := os.ReadFile(c.path)
			if err != nil {
				return nil, nil, fmt.Errorf("read %s: %w", c.path, err)
			}
			r.Logger.Infof("Sending edited withheld file %s", c.path)
			sends = append(sends, outgoing{c: c, content: content})
			continue
		}
		judged = append(judged, c)
	}
	asWritten := func() ([]outgoing, []*candidate, error) {
		for _, c := range judged {
			content, err := os.ReadFile(c.path)
			if err != nil {
				return nil, nil, fmt.Errorf("read %s: %w", c.path, err)
			}
			sends = append(sends, outgoing{c: c, content: content})
		}
		finalized, err := r.finalizeOutgoing(sends)
		if err != nil {
			return nil, nil, err
		}
		return finalized, nil, nil
	}
	if len(judged) == 0 || m.tree == nil {
		return asWritten()
	}
	treeContent, err := os.ReadFile(m.tree.path)
	if err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", m.tree.path, err)
	}
	scan, err := scanTree(treeContent)
	if err != nil {
		return nil, nil, fmt.Errorf("redact %s: %w", m.tree.path, err)
	}
	if scan.unreadable {
		// A tree that cannot be read cannot be checked: nothing of the
		// moment goes up until the file is fixed.
		for _, c := range judged {
			r.Logger.Warningf("Withholding %s: its accessibility tree %s is unreadable", c.path, filepath.Base(m.tree.path))
		}
		finalized, err := r.finalizeOutgoing(sends)
		if err != nil {
			return nil, nil, err
		}
		return finalized, judged, nil
	}
	if scan.hits == 0 {
		return asWritten()
	}
	codec := r.Codec
	if codec == nil {
		codec = SystemCodec{}
	}
	images := make([]image.Image, len(m.screenshots))
	sizes := make([]image.Point, len(m.screenshots))
	for i, c := range m.screenshots {
		content, err := os.ReadFile(c.path)
		if err != nil {
			return nil, nil, fmt.Errorf("read %s: %w", c.path, err)
		}
		img, err := codec.Decode(content, c.file.MimeType)
		if err != nil {
			return nil, nil, fmt.Errorf("decode %s: %w", c.path, err)
		}
		images[i], sizes[i] = img, img.Bounds().Size()
	}
	boxes, placeErr := imageBoxes(scan, sizes)
	var withheld []*candidate
	for _, c := range judged {
		if c == m.tree {
			r.Logger.Infof("Redacting %s: %s", c.path, cardReason)
			sends = append(sends, outgoing{c: c, content: scan.redacted, redacted: true})
			continue
		}
		index := indexOf(m.screenshots, c)
		switch {
		case placeErr != nil:
			r.Logger.Warningf("Withholding %s: %s (no sure box: %v)", c.path, cardReason, placeErr)
			withheld = append(withheld, c)
		case len(boxes[index]) == 0:
			r.Logger.Warningf("Withholding %s: %s (the box does not reach this screen)", c.path, cardReason)
			withheld = append(withheld, c)
		default:
			content, err := codec.Encode(fillBoxes(images[index], boxes[index]), c.file.MimeType)
			if err != nil {
				return nil, nil, fmt.Errorf("encode %s: %w", c.path, err)
			}
			r.Logger.Infof("Redacting %s: %s", c.path, cardReason)
			sends = append(sends, outgoing{c: c, content: content, redacted: true})
		}
	}
	finalized, err := r.finalizeOutgoing(sends)
	if err != nil {
		return nil, nil, err
	}
	return finalized, withheld, nil
}

// finalizeOutgoing populates filename and mimeType for each outgoing file, and
// when CompressPNG is enabled, re-encodes PNG screenshots (after any redaction
// boxes have been drawn) into WebP (or the fallback format).
func (r *Runner) finalizeOutgoing(sends []outgoing) ([]outgoing, error) {
	compressor := r.Compressor
	if compressor == nil && r.CompressPNG {
		compressor = &DefaultPNGCompressor{Quality: r.CompressQuality, MaxEdge: r.CompressMaxEdge}
	}
	for i := range sends {
		sends[i].filename = filepath.Base(sends[i].c.path)
		sends[i].mimeType = sends[i].c.file.MimeType
		if compressor != nil && sends[i].c.file.Kind == timelapsefiles.Screenshot && sends[i].c.file.MimeType == "image/png" {
			compressed, err := compressor.CompressPNG(sends[i].content)
			if err != nil {
				return nil, fmt.Errorf("compress %s: %w", sends[i].c.path, err)
			}
			base := sends[i].filename
			sends[i].filename = strings.TrimSuffix(base, filepath.Ext(base)) + compressed.Extension
			sends[i].mimeType = compressed.MimeType
			sends[i].content = compressed.Content
			sends[i].compressed = true
		}
	}
	return sends, nil
}

func indexOf(list []*candidate, c *candidate) int {
	for i, item := range list {
		if item == c {
			return i
		}
	}
	return -1
}

func sortNewestFirst(list []*candidate) {
	sort.Slice(list, func(i, j int) bool {
		if !list[i].modified.Equal(list[j].modified) {
			return list[i].modified.After(list[j].modified)
		}
		return list[i].path > list[j].path
	})
}

func sha256Hex(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}
