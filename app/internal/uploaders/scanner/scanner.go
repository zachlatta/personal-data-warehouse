// Package scanner turns a network scanner's own Scan button into documents in
// the warehouse.
//
// It registers one panel name per profile (color, black and white, and a
// two-pass "duplex" of each) with the scanner's Scan to PC button (skey.go),
// so the panel lists them under Scan > to PC. Pressing Start on one sends a
// datagram here; the uploader starts a WS-Scan job (wsd.go), pulls every
// page, wraps them in a PDF and sends the PDF plus one record per scan to
// /ingest/scans.
//
// Duplex is faked for scanners whose feeder reads one side: scan the stack
// on a duplex profile, flip it, and scan it again on the same profile within
// the duplex window. The second pass is the backs in reverse order (the last
// sheet's back comes out first), so the pages are interleaved front 1, back
// 1, front 2, ... A first pass with no second one is filed on its own when
// the window closes.
//
// Every finished scan is written to a spool directory before it is sent and
// removed only once the warehouse has it, so a network outage or a restart
// delays a scan rather than losing it. A pending duplex first pass is
// spooled the same way.
package scanner

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Profile is one panel name and the scan it makes.
type Profile struct {
	// Context is the profile name stored with each scan.
	Context string
	// PanelName is the name the panel lists under Scan > to PC (at most 15
	// characters, no spaces: what brscan-skey allows).
	PanelName string
	Mode      NativeMode
	Duplex    bool
}

// ColorMode is the warehouse's name for a profile's color processing.
func (p Profile) ColorMode() string {
	if p.Mode == ModeGray {
		return "grayscale"
	}
	return "color"
}

// Profiles are the names the panel lists. The "B&W" ones scan 8-bit
// grayscale JPEG: a document reads as black and white, and the scanner's
// 1-bit mode streams an undocumented encoding.
var Profiles = []Profile{
	{Context: "pdw-color", PanelName: "PDW-Color", Mode: ModeColor},
	{Context: "pdw-bw", PanelName: "PDW-BW", Mode: ModeGray},
	{Context: "pdw-duplex-color", PanelName: "PDW-DuplexColor", Mode: ModeColor, Duplex: true},
	{Context: "pdw-duplex-bw", PanelName: "PDW-DuplexBW", Mode: ModeGray, Duplex: true},
}

// ProfileByContext finds a profile by its context.
func ProfileByContext(context string) (Profile, bool) {
	for _, p := range Profiles {
		if p.Context == context {
			return p, true
		}
	}
	return Profile{}, false
}

// ProfileByPanelName finds the profile a button press names.
func ProfileByPanelName(name string) (Profile, bool) {
	for _, p := range Profiles {
		if p.PanelName == name {
			return p, true
		}
	}
	return Profile{}, false
}

// PanelNames are the names registered on the panel.
func PanelNames() []string {
	out := make([]string, len(Profiles))
	for i, p := range Profiles {
		out[i] = p.PanelName
	}
	return out
}

// PageHeights are the scan heights, in inches, by page size.
var PageHeights = map[string]float64{
	"letter": 11,
	"legal":  14,
	"a4":     11.69,
}

// Duplex statuses stored with each scan.
const (
	DuplexNone       = ""
	DuplexPaired     = "paired"
	DuplexMismatched = "mismatched"
	DuplexFrontsOnly = "fronts_only"
)

// PageScanner scans everything loaded, one JPEG per page (NativeClient).
type PageScanner interface {
	Scan(ctx context.Context, mode NativeMode, dpi int, heightInches float64) ([][]byte, error)
}

// Uploader is what the scanner needs from the ingest client.
type Uploader interface {
	UploadScanDocument(path, scannedAt, extension, contentType, contentSHA256 string) (ingestclient.StoredObject, error)
	UploadScansBatch(gzipBytes []byte, exportedAt string) (ingestclient.StoredObject, error)
}

// Pass is one run of paper through the scanner.
type Pass struct {
	StartedAt   time.Time
	InputSource string
	Pages       []Page
}

// Scanner drives one scanner for one account.
type Scanner struct {
	Client      PageScanner
	Uploader    Uploader
	Logger      common.Logger
	Account     string
	Device      string
	ScannerName string
	SpoolDir    string
	Resolution  int
	PageSize    string
	// DuplexWindow is how long a duplex first pass waits for its second.
	DuplexWindow time.Duration
	Now          func() time.Time
	// OnFiled is told about each scan once it is spooled (tests, logs).
	OnFiled func(record map[string]any)

	mu sync.Mutex
}

func (s *Scanner) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Scanner) outboxDir() string { return filepath.Join(s.SpoolDir, "outbox") }
func (s *Scanner) duplexDir(context string) string {
	return filepath.Join(s.SpoolDir, "duplex", context)
}

// HandleScan runs the scan a button press (or a local request) asks for,
// then files it: a single-sided scan at once, a duplex first pass after its
// second or when its window closes.
func (s *Scanner) HandleScan(ctx context.Context, profile Profile) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A stale first pass is filed on its own before this scan starts, so a
	// late second pass becomes a new first pass instead of pairing with it.
	if err := s.flushExpiredLocked(); err != nil {
		s.Logger.Warningf("Could not file an expired duplex first pass: %v", err)
	}
	pass, err := s.scanPass(ctx, profile)
	if err != nil {
		return err
	}
	if !profile.Duplex {
		return s.file(profile, []Pass{pass}, DuplexNone)
	}
	pending, err := s.loadPending(profile.Context)
	if err != nil {
		return err
	}
	if pending == nil {
		if err := s.savePending(profile.Context, pass); err != nil {
			return err
		}
		s.Logger.Infof("Duplex %s: first pass of %d pages held; flip the stack and scan %q again within %s",
			profile.Context, len(pass.Pages), profile.PanelName, s.DuplexWindow)
		return nil
	}
	fronts := *pending
	status := DuplexPaired
	if len(fronts.Pages) != len(pass.Pages) {
		status = DuplexMismatched
	}
	if err := s.file(profile, []Pass{fronts, pass}, status); err != nil {
		return err
	}
	return os.RemoveAll(s.duplexDir(profile.Context))
}

// FlushExpired files every duplex first pass whose window has closed.
func (s *Scanner) FlushExpired() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushExpiredLocked()
}

func (s *Scanner) flushExpiredLocked() error {
	var errs []error
	for _, profile := range Profiles {
		if !profile.Duplex {
			continue
		}
		pending, err := s.loadPending(profile.Context)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if pending == nil || s.now().Sub(pending.StartedAt) < s.DuplexWindow {
			continue
		}
		if err := s.file(profile, []Pass{*pending}, DuplexFrontsOnly); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := os.RemoveAll(s.duplexDir(profile.Context)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// PendingDuplex reports the duplex profiles holding a first pass.
func (s *Scanner) PendingDuplex() []string {
	var out []string
	for _, profile := range Profiles {
		if pending, err := s.loadPending(profile.Context); err == nil && pending != nil {
			out = append(out, profile.Context)
		}
	}
	return out
}

func (s *Scanner) scanPass(ctx context.Context, profile Profile) (Pass, error) {
	height, ok := PageHeights[s.PageSize]
	if !ok {
		height = PageHeights["letter"]
	}
	started := s.now()
	jpegs, err := s.Client.Scan(ctx, profile.Mode, s.Resolution, height)
	if err != nil {
		return Pass{}, fmt.Errorf("scan: %w", err)
	}
	if len(jpegs) > maxPagesPerPass {
		return Pass{}, fmt.Errorf("the scanner sent %d pages", len(jpegs))
	}
	// The scanner takes the feeder when it has paper, else the glass, and
	// does not say which.
	pass := Pass{StartedAt: started, InputSource: "auto"}
	for _, data := range jpegs {
		pass.Pages = append(pass.Pages, Page{Format: "exif", Data: data, DPI: s.Resolution})
	}
	s.Logger.Infof("Scanned %d pages (%s)", len(pass.Pages), profile.Context)
	return pass, nil
}

// maxPagesPerPass stops a job that never ends; a 50-sheet feeder is far below.
const maxPagesPerPass = 500

// Interleave orders a duplex scan's pages: front 1, back 1, front 2, ...
// The backs arrive last sheet first, so they are reversed. When the passes
// differ in length the pairs that exist are interleaved and the rest follow.
func Interleave(fronts, backs []Page) []Page {
	out := make([]Page, 0, len(fronts)+len(backs))
	for i := 0; i < max(len(fronts), len(backs)); i++ {
		if i < len(fronts) {
			out = append(out, fronts[i])
		}
		if i < len(backs) {
			out = append(out, backs[len(backs)-1-i])
		}
	}
	return out
}

// file writes the scan's PDF and record into the outbox.
func (s *Scanner) file(profile Profile, passes []Pass, duplexStatus string) error {
	pages := passes[0].Pages
	frontCount, backCount := len(passes[0].Pages), 0
	if len(passes) == 2 {
		backCount = len(passes[1].Pages)
		pages = Interleave(passes[0].Pages, passes[1].Pages)
	}
	scannedAt := passes[0].StartedAt
	title := fmt.Sprintf("Scan %s", scannedAt.Local().Format("2006-01-02 15:04"))
	pdf, err := BuildPDF(pages, title, scannedAt)
	if err != nil {
		return err
	}
	scanID := newScanID()
	dir := filepath.Join(s.outboxDir(), scanID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	inputSources := []string{}
	for _, p := range passes {
		inputSources = append(inputSources, p.InputSource)
	}
	record := map[string]any{
		"record_type":      "scan",
		"scan_id":          scanID,
		"scanned_at":       scannedAt.Format(time.RFC3339Nano),
		"completed_at":     s.now().Format(time.RFC3339Nano),
		"profile":          profile.Context,
		"profile_name":     profile.PanelName,
		"color_mode":       profile.ColorMode(),
		"resolution_dpi":   int64(s.Resolution),
		"page_size":        s.PageSize,
		"input_source":     strings.Join(inputSources, ","),
		"is_duplex":        profile.Duplex,
		"duplex_status":    duplexStatus,
		"page_count":       int64(len(pages)),
		"front_page_count": int64(frontCount),
		"back_page_count":  int64(backCount),
		"scanner_name":     s.ScannerName,
		"filename":         strings.ReplaceAll(title, ":", "-") + ".pdf",
		"content_type":     "application/pdf",
		"content_sha256":   common.BytesSHA256(pdf),
		"size_bytes":       int64(len(pdf)),
		"time_zone":        scannedAt.Location().String(),
	}
	if err := common.WriteFileAtomic(filepath.Join(dir, "document.pdf"), pdf, 0o600); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := common.WriteFileAtomic(filepath.Join(dir, "record.json"), raw, 0o600); err != nil {
		return err
	}
	s.Logger.Infof("Filed scan %s: %d pages, %s, %s", scanID, len(pages), profile.ColorMode(), common.FormatBytes(int64(len(pdf))))
	if s.OnFiled != nil {
		s.OnFiled(record)
	}
	return nil
}

type pendingManifest struct {
	StartedAt   time.Time `json:"started_at"`
	InputSource string    `json:"input_source"`
	Format      string    `json:"format"`
	DPI         int       `json:"dpi"`
	PageFiles   []string  `json:"page_files"`
}

func (s *Scanner) savePending(context string, pass Pass) error {
	dir := s.duplexDir(context)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	manifest := pendingManifest{StartedAt: pass.StartedAt, InputSource: pass.InputSource}
	for i, page := range pass.Pages {
		name := fmt.Sprintf("page-%03d.bin", i+1)
		if err := common.WriteFileAtomic(filepath.Join(dir, name), page.Data, 0o600); err != nil {
			return err
		}
		manifest.PageFiles = append(manifest.PageFiles, name)
		manifest.Format, manifest.DPI = page.Format, page.DPI
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	// The manifest is written last: a pass without one is not pending.
	return common.WriteFileAtomic(filepath.Join(dir, "manifest.json"), raw, 0o600)
}

func (s *Scanner) loadPending(context string) (*Pass, error) {
	dir := s.duplexDir(context)
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest pendingManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("duplex manifest %s: %w", dir, err)
	}
	pass := Pass{StartedAt: manifest.StartedAt, InputSource: manifest.InputSource}
	for _, name := range manifest.PageFiles {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		pass.Pages = append(pass.Pages, Page{Format: manifest.Format, Data: data, DPI: manifest.DPI})
	}
	return &pass, nil
}

// SendOutbox sends every spooled scan, oldest first, and removes each once
// the warehouse has it. It stops at the first failure; the rest wait for the
// next attempt.
func (s *Scanner) SendOutbox() (sent int, err error) {
	entries, err := os.ReadDir(s.outboxDir())
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	type item struct {
		dir    string
		record map[string]any
	}
	var items []item
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(s.outboxDir(), e.Name())
		raw, err := os.ReadFile(filepath.Join(dir, "record.json"))
		if err != nil {
			// The record is written after the PDF: no record means the scan
			// was interrupted while filing, and nothing describes the PDF.
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			return sent, fmt.Errorf("spooled record %s: %w", dir, err)
		}
		items = append(items, item{dir: dir, record: record})
	}
	sort.Slice(items, func(i, j int) bool {
		return fmt.Sprint(items[i].record["scanned_at"]) < fmt.Sprint(items[j].record["scanned_at"])
	})
	for _, it := range items {
		if err := s.send(it.dir, it.record); err != nil {
			return sent, err
		}
		sent++
	}
	return sent, nil
}

func (s *Scanner) send(dir string, record map[string]any) error {
	pdfPath := filepath.Join(dir, "document.pdf")
	sha, err := common.FileSHA256(pdfPath)
	if err != nil {
		return err
	}
	if sha != record["content_sha256"] {
		return fmt.Errorf("spooled scan %s: PDF does not match its record", dir)
	}
	scannedAt, _ := record["scanned_at"].(string)
	stored, err := s.Uploader.UploadScanDocument(pdfPath, scannedAt, ".pdf", "application/pdf", sha)
	if err != nil {
		return fmt.Errorf("upload scan %s: %w", record["scan_id"], err)
	}
	record["storage_backend"] = stored.StorageBackend
	record["storage_key"] = stored.StorageKey
	record["storage_file_id"] = stored.StorageFileID
	record["storage_url"] = stored.StorageURL
	exportedAt := s.now().UTC()
	body, err := common.GzipJSONL([]map[string]any{Envelope(s.Account, s.Device, exportedAt, record)})
	if err != nil {
		return err
	}
	if _, err := s.Uploader.UploadScansBatch(body, common.ISOFormat(exportedAt)); err != nil {
		return fmt.Errorf("upload scan record %s: %w", record["scan_id"], err)
	}
	s.Logger.Infof("Sent scan %s (%v pages) to the warehouse as %s", record["scan_id"], record["page_count"], stored.StorageKey)
	return os.RemoveAll(dir)
}

func newScanID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// Envelope wraps one scan record for a batch.
func Envelope(account, device string, exportedAt time.Time, record map[string]any) map[string]any {
	return map[string]any{
		"schema_version": int64(1),
		"source":         "scans",
		"account":        account,
		"device":         device,
		"exported_at":    common.ISOFormat(exportedAt),
		"record_type":    "scan",
		"record":         record,
	}
}
