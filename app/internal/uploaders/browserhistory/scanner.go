// Package browserhistory uploads this Mac's Chrome, Brave and Safari history
// (every profile) through the app's /ingest/browser-history/batch endpoint.
//
// Each history database is copied (with its -wal / -journal sidecars) to a
// temporary directory before it is read: Chromium holds its History file
// under an exclusive lock while the browser runs, and nothing here may ever
// write to a live browser store.
package browserhistory

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Engine is the on-disk history format a browser uses.
type Engine string

const (
	EngineChromium Engine = "chromium"
	EngineSafari   Engine = "safari"
)

// Browser is one browser the uploader knows how to read.
type Browser struct {
	// Name is the stable slug stored in base_browser_history.visits.browser.
	Name   string
	Engine Engine
	// Root is the browser's user-data directory, relative to the home
	// directory: Chromium profiles are its subdirectories, Safari's default
	// profile is its History.db.
	Root string
}

// DefaultBrowsers are the browsers read when --browser is not given.
var DefaultBrowsers = []Browser{
	{Name: "chrome", Engine: EngineChromium, Root: "Library/Application Support/Google/Chrome"},
	{Name: "brave", Engine: EngineChromium, Root: "Library/Application Support/BraveSoftware/Brave-Browser"},
	{Name: "safari", Engine: EngineSafari, Root: "Library/Safari"},
}

// safariProfileRoots are where Safari 17+ keeps non-default profiles; each
// holds <uuid>/History.db. Both locations have shipped.
var safariProfileRoots = []string{
	"Library/Safari/Profiles",
	"Library/Containers/com.apple.Safari/Data/Library/Safari/Profiles",
}

// Profile is one history database to read.
type Profile struct {
	Browser string
	Engine  Engine
	// Key is the stable profile id (Chromium's directory name, "Default" for
	// Safari's main profile, otherwise its profile directory's uuid).
	Key string
	// Name is the human profile name where the browser records one.
	Name        string
	HistoryPath string
}

// Visit is one history visit as the uploader ships it.
type Visit struct {
	VisitID     int64
	URLID       int64
	URL         string
	Host        string
	Title       string
	VisitedAt   time.Time
	Transition  string
	RawTransit  int64
	IsRedirect  bool
	IsSynced    bool
	FromVisitID int64
	DurationMS  int64
	SearchTerm  string
	Raw         map[string]any
}

// MaxURLChars bounds a stored URL: data: URLs and tracking-laden redirects can
// run to megabytes, and nothing reads past this.
const MaxURLChars = 4096

// chromiumUnixOffsetMicros is Chromium's time zero relative to Unix time:
// visit times are microseconds since 1601-01-01 UTC (the Windows FILETIME
// epoch). 425 years overflow a time.Duration, so convert through Unix time.
const chromiumUnixOffsetMicros = 11_644_473_600 * 1_000_000

// safariEpoch is Core Foundation's absolute time zero: Safari visit times are
// seconds since 2001-01-01 UTC.
var safariEpoch = time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)

// ChromiumTime converts a Chromium timestamp; ok=false for zero/unset.
func ChromiumTime(micros int64) (time.Time, bool) {
	if micros <= 0 {
		return time.Time{}, false
	}
	return time.UnixMicro(micros - chromiumUnixOffsetMicros).UTC(), true
}

// SafariTime converts a Safari timestamp; ok=false for zero/unset.
func SafariTime(seconds float64) (time.Time, bool) {
	if seconds <= 0 {
		return time.Time{}, false
	}
	return safariEpoch.Add(time.Duration(seconds * float64(time.Second))).UTC(), true
}

// Chromium page transition core types (ui/base/page_transition_types.h).
var chromiumCoreTransitions = []string{
	"link", "typed", "auto_bookmark", "auto_subframe", "manual_subframe",
	"generated", "auto_toplevel", "form_submit", "reload", "keyword", "keyword_generated",
}

const (
	chromiumChainEnd       = 0x20000000
	chromiumQualifierMask  = 0xFFFFFF00
	chromiumCoreMask       = 0xFF
	chromiumClientRedirect = 0x40000000
	chromiumServerRedirect = 0x80000000
)

// ChromiumTransition names a raw transition's core type and reports whether
// the visit was a redirect hop (a visit that did not end its redirect chain).
func ChromiumTransition(raw int64) (name string, redirectHop bool) {
	core := raw & chromiumCoreMask
	name = "unknown"
	if core >= 0 && int(core) < len(chromiumCoreTransitions) {
		name = chromiumCoreTransitions[core]
	}
	return name, raw&chromiumChainEnd == 0
}

// DiscoverProfiles lists every history database of the given browsers under
// home. A browser that is not installed contributes nothing.
func DiscoverProfiles(home string, browsers []Browser) ([]Profile, error) {
	var profiles []Profile
	for _, browser := range browsers {
		root := filepath.Join(home, browser.Root)
		switch browser.Engine {
		case EngineChromium:
			found, err := chromiumProfiles(browser, root)
			if err != nil {
				return nil, err
			}
			profiles = append(profiles, found...)
		case EngineSafari:
			profiles = append(profiles, safariProfiles(browser, home)...)
		default:
			return nil, fmt.Errorf("unknown browser engine %q", browser.Engine)
		}
	}
	return profiles, nil
}

func chromiumProfiles(browser Browser, root string) ([]Profile, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	names := chromiumProfileNames(filepath.Join(root, "Local State"))
	var profiles []Profile
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		key := entry.Name()
		// The system and guest profiles are browser machinery, never a
		// person's browsing history worth keeping.
		if key == "System Profile" || key == "Guest Profile" {
			continue
		}
		history := filepath.Join(root, key, "History")
		if info, err := os.Stat(history); err != nil || !info.Mode().IsRegular() {
			continue
		}
		name := names[key]
		if name == "" {
			name = key
		}
		profiles = append(profiles, Profile{Browser: browser.Name, Engine: EngineChromium, Key: key, Name: name, HistoryPath: history})
	}
	sort.Slice(profiles, func(i, j int) bool { return profiles[i].Key < profiles[j].Key })
	return profiles, nil
}

// chromiumProfileNames reads profile.info_cache from Local State: directory
// name -> the name the person gave the profile.
func chromiumProfileNames(localState string) map[string]string {
	out := map[string]string{}
	data, err := os.ReadFile(localState)
	if err != nil {
		return out
	}
	var parsed struct {
		Profile struct {
			InfoCache map[string]struct {
				Name string `json:"name"`
			} `json:"info_cache"`
		} `json:"profile"`
	}
	if json.Unmarshal(data, &parsed) != nil {
		return out
	}
	for key, info := range parsed.Profile.InfoCache {
		out[key] = info.Name
	}
	return out
}

func safariProfiles(browser Browser, home string) []Profile {
	var profiles []Profile
	// Safari's History.db is unreadable without Full Disk Access, but its
	// existence is not: list it whenever it is there, so a missing grant is a
	// loud error at read time rather than a silently skipped browser.
	main := filepath.Join(home, browser.Root, "History.db")
	if _, err := os.Lstat(main); err == nil {
		profiles = append(profiles, Profile{Browser: browser.Name, Engine: EngineSafari, Key: "Default", Name: "Default", HistoryPath: main})
	}
	for _, root := range safariProfileRoots {
		matches, _ := filepath.Glob(filepath.Join(home, root, "*", "History.db"))
		sort.Strings(matches)
		for _, match := range matches {
			key := filepath.Base(filepath.Dir(match))
			profiles = append(profiles, Profile{Browser: browser.Name, Engine: EngineSafari, Key: key, Name: key, HistoryPath: match})
		}
	}
	return profiles
}

// Snapshot copies a history database and its -wal / -journal sidecars into
// dir and returns the copy's path. Copying the sidecars with the main file is
// what makes the copy consistent: SQLite replays the WAL (or rolls back a hot
// journal) when the copy is opened.
func Snapshot(source, dir string) (string, error) {
	destination := filepath.Join(dir, filepath.Base(source))
	if err := copyFile(source, destination); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		return "", &common.PermissionError{Path: source, Err: err}
	}
	for _, suffix := range []string{"-wal", "-journal"} {
		if err := copyFile(source+suffix, destination+suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", &common.PermissionError{Path: source + suffix, Err: err}
		}
	}
	return destination, nil
}

func copyFile(source, destination string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(destination)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// MaxVisitID is the highest visit id in a snapshot (0 when empty).
func MaxVisitID(db *sql.DB, engine Engine) (int64, error) {
	table := "visits"
	if engine == EngineSafari {
		table = "history_visits"
	}
	var max sql.NullInt64
	if err := db.QueryRow("SELECT max(id) FROM " + table).Scan(&max); err != nil {
		return 0, err
	}
	return max.Int64, nil
}

// ScanVisits reads up to limit visits with id > afterID, in id order.
func ScanVisits(db *sql.DB, engine Engine, afterID int64, limit int) ([]Visit, error) {
	if engine == EngineSafari {
		return scanSafari(db, afterID, limit)
	}
	return scanChromium(db, afterID, limit)
}

func columnsOf(db *sql.DB, table string) (map[string]bool, error) {
	rows, err := common.Query(db, "SELECT name FROM pragma_table_info(?)", table)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, row := range rows {
		out[row.String("name")] = true
	}
	return out, nil
}

func scanChromium(db *sql.DB, afterID int64, limit int) ([]Visit, error) {
	columns, err := columnsOf(db, "visits")
	if err != nil {
		return nil, err
	}
	tables, err := common.TableNames(db)
	if err != nil {
		return nil, err
	}
	optional := func(column, fallback string) string {
		if columns[column] {
			return "v." + column + " AS " + column
		}
		return fallback + " AS " + column
	}
	searchTerm := "'' AS search_term, '' AS normalized_search_term"
	if tables["keyword_search_terms"] {
		searchTerm = "COALESCE((SELECT k.term FROM keyword_search_terms k WHERE k.url_id = v.url ORDER BY k.keyword_id LIMIT 1), '') AS search_term, " +
			"COALESCE((SELECT k.normalized_term FROM keyword_search_terms k WHERE k.url_id = v.url ORDER BY k.keyword_id LIMIT 1), '') AS normalized_search_term"
	}
	visitSource := "NULL AS visit_source"
	if tables["visit_source"] {
		visitSource = "(SELECT s.source FROM visit_source s WHERE s.id = v.id) AS visit_source"
	}
	statement := `SELECT v.id AS id, v.url AS url_id, COALESCE(u.url, '') AS url, COALESCE(u.title, '') AS title,
		v.visit_time AS visit_time, COALESCE(v.transition, 0) AS transition, ` +
		optional("from_visit", "0") + ", " +
		optional("visit_duration", "0") + ", " +
		optional("opener_visit", "0") + ", " +
		optional("originator_cache_guid", "''") + ", " +
		optional("external_referrer_url", "''") + ", " +
		`COALESCE(u.visit_count, 0) AS url_visit_count, COALESCE(u.typed_count, 0) AS url_typed_count, ` +
		searchTerm + ", " + visitSource + `
		FROM visits v LEFT JOIN urls u ON u.id = v.url
		WHERE v.id > ? ORDER BY v.id LIMIT ?`
	rows, err := common.Query(db, statement, afterID, sqlLimit(limit))
	if err != nil {
		return nil, err
	}
	visits := make([]Visit, 0, len(rows))
	for _, row := range rows {
		visitedAt, ok := ChromiumTime(row.Int("visit_time"))
		if !ok {
			continue
		}
		raw := row.Int("transition")
		name, redirect := ChromiumTransition(raw)
		pageURL := row.String("url")
		term := row.String("search_term")
		if term == "" {
			term = SearchTermFromURL(pageURL)
		}
		// visit_source 0 is SOURCE_SYNCED; a non-empty originator cache guid
		// is a visit another device recorded and sync delivered here.
		synced := row.String("originator_cache_guid") != "" || (row.Get("visit_source") != nil && row.Int("visit_source") == 0)
		visits = append(visits, Visit{
			VisitID:     row.Int("id"),
			URLID:       row.Int("url_id"),
			URL:         truncate(pageURL, MaxURLChars),
			Host:        HostOf(pageURL),
			Title:       row.String("title"),
			VisitedAt:   visitedAt,
			Transition:  name,
			RawTransit:  raw,
			IsRedirect:  redirect,
			IsSynced:    synced,
			FromVisitID: row.Int("from_visit"),
			DurationMS:  row.Int("visit_duration") / 1000,
			SearchTerm:  term,
			Raw: map[string]any{
				"transition":             raw,
				"from_visit":             row.Int("from_visit"),
				"opener_visit":           row.Int("opener_visit"),
				"visit_duration_us":      row.Int("visit_duration"),
				"visit_source":           row.Get("visit_source"),
				"originator_cache_guid":  row.String("originator_cache_guid"),
				"external_referrer_url":  truncate(row.String("external_referrer_url"), MaxURLChars),
				"url_visit_count":        row.Int("url_visit_count"),
				"url_typed_count":        row.Int("url_typed_count"),
				"normalized_search_term": row.String("normalized_search_term"),
				"client_redirect":        raw&chromiumClientRedirect != 0,
				"server_redirect":        raw&chromiumServerRedirect != 0,
				"qualifiers":             raw & chromiumQualifierMask,
			},
		})
	}
	return visits, nil
}

func scanSafari(db *sql.DB, afterID int64, limit int) ([]Visit, error) {
	columns, err := columnsOf(db, "history_visits")
	if err != nil {
		return nil, err
	}
	optional := func(column, fallback string) string {
		if columns[column] {
			return "v." + column + " AS " + column
		}
		return fallback + " AS " + column
	}
	statement := `SELECT v.id AS id, v.history_item AS url_id, COALESCE(i.url, '') AS url, COALESCE(v.title, '') AS title,
		v.visit_time AS visit_time, ` +
		optional("load_successful", "1") + ", " +
		optional("http_non_get", "0") + ", " +
		optional("synthesized", "0") + ", " +
		optional("redirect_source", "NULL") + ", " +
		optional("redirect_destination", "NULL") + ", " +
		optional("origin", "0") + ", " +
		optional("generation", "0") + ", " +
		optional("attributes", "0") + ", " +
		optional("score", "0") + `,
		COALESCE(i.visit_count, 0) AS url_visit_count
		FROM history_visits v LEFT JOIN history_items i ON i.id = v.history_item
		WHERE v.id > ? ORDER BY v.id LIMIT ?`
	rows, err := common.Query(db, statement, afterID, sqlLimit(limit))
	if err != nil {
		return nil, err
	}
	visits := make([]Visit, 0, len(rows))
	for _, row := range rows {
		visitedAt, ok := SafariTime(common.ToFloat(row.Get("visit_time")))
		if !ok {
			continue
		}
		pageURL := row.String("url")
		visits = append(visits, Visit{
			VisitID:   row.Int("id"),
			URLID:     row.Int("url_id"),
			URL:       truncate(pageURL, MaxURLChars),
			Host:      HostOf(pageURL),
			Title:     row.String("title"),
			VisitedAt: visitedAt,
			// Safari records no transition type.
			Transition: "",
			// A visit with a redirect destination is a hop that redirected
			// onward; the destination is its own visit.
			IsRedirect:  row.Get("redirect_destination") != nil,
			IsSynced:    row.Int("origin") != 0,
			FromVisitID: row.Int("redirect_source"),
			SearchTerm:  SearchTermFromURL(pageURL),
			Raw: map[string]any{
				"load_successful":      row.Int("load_successful"),
				"http_non_get":         row.Int("http_non_get"),
				"synthesized":          row.Int("synthesized"),
				"redirect_source":      row.Get("redirect_source"),
				"redirect_destination": row.Get("redirect_destination"),
				"origin":               row.Int("origin"),
				"generation":           row.Int("generation"),
				"attributes":           row.Int("attributes"),
				"score":                row.Int("score"),
				"url_visit_count":      row.Int("url_visit_count"),
			},
		})
	}
	return visits, nil
}

func sqlLimit(limit int) int64 {
	if limit <= 0 {
		return -1
	}
	return int64(limit)
}

func truncate(value string, max int) string {
	if len(value) <= max {
		return value
	}
	cut := max
	// Never split a UTF-8 sequence.
	for cut > 0 && value[cut]&0xC0 == 0x80 {
		cut--
	}
	return value[:cut]
}

// HostOf is the lowercased host of an http(s) URL, else "".
func HostOf(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return ""
	}
	return strings.ToLower(parsed.Hostname())
}

// searchEngines maps a search engine's host (or host suffix) to the result
// path and query parameter that carry the query.
var searchEngines = []struct {
	host  string
	path  string
	param string
}{
	{host: "google.", path: "/search", param: "q"},
	{host: "bing.com", path: "/search", param: "q"},
	{host: "duckduckgo.com", path: "/", param: "q"},
	{host: "search.brave.com", path: "/search", param: "q"},
	{host: "kagi.com", path: "/search", param: "q"},
	{host: "search.yahoo.com", path: "/search", param: "p"},
	{host: "ecosia.org", path: "/search", param: "q"},
}

// SearchTermFromURL recovers the query from a web search results URL. Chromium
// records omnibox searches in keyword_search_terms, which wins when present;
// this covers Safari (which records none) and searches typed on the engine's
// own page.
func SearchTermFromURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return ""
	}
	host := strings.TrimPrefix(strings.ToLower(parsed.Hostname()), "www.")
	for _, engine := range searchEngines {
		matched := host == engine.host || strings.HasSuffix(host, "."+engine.host)
		if strings.HasSuffix(engine.host, ".") {
			matched = strings.HasPrefix(host, engine.host)
		}
		if !matched || parsed.Path != engine.path {
			continue
		}
		return strings.TrimSpace(parsed.Query().Get(engine.param))
	}
	return ""
}
