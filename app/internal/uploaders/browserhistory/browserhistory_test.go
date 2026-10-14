package browserhistory

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/testfixtures"
)

const chromiumSchema = `CREATE TABLE urls(id INTEGER PRIMARY KEY AUTOINCREMENT,url LONGVARCHAR,title LONGVARCHAR,visit_count INTEGER DEFAULT 0 NOT NULL,typed_count INTEGER DEFAULT 0 NOT NULL,last_visit_time INTEGER NOT NULL,hidden INTEGER DEFAULT 0 NOT NULL);
CREATE TABLE visits(id INTEGER PRIMARY KEY AUTOINCREMENT,url INTEGER NOT NULL,visit_time INTEGER NOT NULL,from_visit INTEGER,external_referrer_url TEXT,transition INTEGER DEFAULT 0 NOT NULL,segment_id INTEGER,visit_duration INTEGER DEFAULT 0 NOT NULL,incremented_omnibox_typed_score BOOLEAN DEFAULT FALSE NOT NULL,opener_visit INTEGER,originator_cache_guid TEXT,originator_visit_id INTEGER,originator_from_visit INTEGER,originator_opener_visit INTEGER,is_known_to_sync BOOLEAN DEFAULT FALSE NOT NULL,consider_for_ntp_most_visited BOOLEAN DEFAULT FALSE NOT NULL,visited_link_id INTEGER DEFAULT 0 NOT NULL,app_id TEXT);
CREATE TABLE keyword_search_terms (keyword_id INTEGER NOT NULL,url_id INTEGER NOT NULL,term LONGVARCHAR NOT NULL,normalized_term LONGVARCHAR NOT NULL);
CREATE TABLE visit_source(id INTEGER PRIMARY KEY,source INTEGER NOT NULL);`

const safariSchema = `CREATE TABLE history_items (id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT NOT NULL UNIQUE, domain_expansion TEXT NULL, visit_count INTEGER NOT NULL, daily_visit_counts BLOB NOT NULL DEFAULT x'', weekly_visit_counts BLOB NULL, autocomplete_triggers BLOB NULL, should_recompute_derived_visit_counts INTEGER NOT NULL DEFAULT 0, visit_count_score INTEGER NOT NULL DEFAULT 0, status_code INTEGER NOT NULL DEFAULT 0);
CREATE TABLE history_visits (id INTEGER PRIMARY KEY AUTOINCREMENT, history_item INTEGER NOT NULL REFERENCES history_items(id) ON DELETE CASCADE, visit_time REAL NOT NULL, title TEXT NULL, load_successful BOOLEAN NOT NULL DEFAULT 1, http_non_get BOOLEAN NOT NULL DEFAULT 0, synthesized BOOLEAN NOT NULL DEFAULT 0, redirect_source INTEGER NULL UNIQUE REFERENCES history_visits(id) ON DELETE CASCADE, redirect_destination INTEGER NULL UNIQUE REFERENCES history_visits(id) ON DELETE CASCADE, origin INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0, attributes INTEGER NOT NULL DEFAULT 0, score INTEGER NOT NULL DEFAULT 0);`

var fixtureTime = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func chromiumMicros(t time.Time) int64  { return (t.Unix() + 11644473600) * 1_000_000 }
func safariSeconds(t time.Time) float64 { return float64(t.Unix() - 978307200) }

const (
	linkChainBoth  = 0x30000000     // LINK, chain start + end: an ordinary click
	typedChainBoth = 0x30000000 | 1 // TYPED in the omnibox
	redirectHop    = 0x10000000     // LINK, chain start without end: redirected onward
	serverRedirect = 0xA0000000 | 0 // LINK, chain end via server redirect
	autoSubframe   = 0x30000000 | 3
)

func writeChromium(t *testing.T, path string, statements ...string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	testfixtures.ExecScript(t, path, append([]string{chromiumSchema}, statements...)...)
}

func chromiumVisit(id, urlID int64, at time.Time, transition int64) string {
	return fmt.Sprintf("INSERT INTO visits (id, url, visit_time, from_visit, transition, visit_duration, originator_cache_guid) VALUES (%d, %d, %d, 0, %d, 2500000, '')", id, urlID, chromiumMicros(at), transition)
}

// fakeHome builds Chrome (two profiles + a system profile), Brave and Safari.
func fakeHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	chrome := filepath.Join(home, "Library/Application Support/Google/Chrome")
	writeChromium(t, filepath.Join(chrome, "Default", "History"),
		"INSERT INTO urls (id, url, title, last_visit_time) VALUES (1, 'https://example.com/a', 'Example A', 0)",
		"INSERT INTO urls (id, url, title, last_visit_time) VALUES (2, 'https://www.google.com/search?q=synthetic+query&oq=x', 'synthetic query - Google Search', 0)",
		"INSERT INTO urls (id, url, title, last_visit_time) VALUES (3, 'https://short.example/r', '', 0)",
		"INSERT INTO keyword_search_terms VALUES (2, 2, 'Synthetic Query', 'synthetic query')",
		chromiumVisit(1, 1, fixtureTime, linkChainBoth),
		chromiumVisit(2, 2, fixtureTime.Add(time.Minute), typedChainBoth),
		chromiumVisit(3, 3, fixtureTime.Add(2*time.Minute), redirectHop),
		chromiumVisit(4, 1, fixtureTime.Add(2*time.Minute), serverRedirect),
		chromiumVisit(5, 1, fixtureTime.Add(3*time.Minute), autoSubframe),
		// A visit another device recorded and Chrome sync delivered.
		fmt.Sprintf("INSERT INTO visits (id, url, visit_time, transition, originator_cache_guid) VALUES (6, 1, %d, %d, 'other-device')", chromiumMicros(fixtureTime.Add(4*time.Minute)), linkChainBoth),
		// No time: unusable, skipped, but still advances the mark.
		"INSERT INTO visits (id, url, visit_time, transition) VALUES (7, 1, 0, 0)",
	)
	writeChromium(t, filepath.Join(chrome, "Profile 1", "History"),
		"INSERT INTO urls (id, url, title, last_visit_time) VALUES (1, 'https://work.example/', 'Work', 0)",
		chromiumVisit(1, 1, fixtureTime, linkChainBoth),
	)
	writeChromium(t, filepath.Join(chrome, "System Profile", "History"),
		"INSERT INTO urls (id, url, title, last_visit_time) VALUES (1, 'https://system.example/', 'System', 0)",
		chromiumVisit(1, 1, fixtureTime, linkChainBoth),
	)
	if err := os.WriteFile(filepath.Join(chrome, "Local State"), []byte(`{"profile":{"info_cache":{"Default":{"name":"Personal"},"Profile 1":{"name":"Work"}}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	writeChromium(t, filepath.Join(home, "Library/Application Support/BraveSoftware/Brave-Browser/Default/History"),
		"INSERT INTO urls (id, url, title, last_visit_time) VALUES (1, 'https://brave.example/', 'Brave page', 0)",
		chromiumVisit(1, 1, fixtureTime, linkChainBoth),
	)
	safari := filepath.Join(home, "Library/Safari")
	if err := os.MkdirAll(safari, 0o755); err != nil {
		t.Fatal(err)
	}
	testfixtures.ExecScript(t, filepath.Join(safari, "History.db"), safariSchema,
		"INSERT INTO history_items (id, url, visit_count) VALUES (1, 'http://redirect.example/', 1)",
		"INSERT INTO history_items (id, url, visit_count) VALUES (2, 'https://duckduckgo.com/?q=safari+term', 1)",
		"INSERT INTO history_items (id, url, visit_count) VALUES (3, 'https://phone.example/', 1)",
		fmt.Sprintf("INSERT INTO history_visits (id, history_item, visit_time, title, redirect_destination) VALUES (1, 1, %f, '', 2)", safariSeconds(fixtureTime)),
		fmt.Sprintf("INSERT INTO history_visits (id, history_item, visit_time, title, redirect_source) VALUES (2, 2, %f, 'safari term at DuckDuckGo', 1)", safariSeconds(fixtureTime)),
		fmt.Sprintf("INSERT INTO history_visits (id, history_item, visit_time, title, origin) VALUES (3, 3, %f, 'From the phone', 1)", safariSeconds(fixtureTime.Add(time.Hour))),
	)
	return home
}

func TestTimeConversions(t *testing.T) {
	if got, ok := ChromiumTime(chromiumMicros(fixtureTime)); !ok || !got.Equal(fixtureTime) {
		t.Fatalf("chromium = %v %v", got, ok)
	}
	if got, ok := SafariTime(safariSeconds(fixtureTime)); !ok || !got.Equal(fixtureTime) {
		t.Fatalf("safari = %v %v", got, ok)
	}
	if _, ok := ChromiumTime(0); ok {
		t.Fatal("zero chromium time must be unset")
	}
	if _, ok := SafariTime(0); ok {
		t.Fatal("zero safari time must be unset")
	}
}

func TestChromiumTransition(t *testing.T) {
	for _, tc := range []struct {
		raw      int64
		name     string
		redirect bool
	}{
		{linkChainBoth, "link", false},
		{typedChainBoth, "typed", false},
		{redirectHop, "link", true},
		{serverRedirect, "link", false},
		{autoSubframe, "auto_subframe", false},
		{0x30000000 | 8, "reload", false},
		{0x30000000 | 99, "unknown", false},
	} {
		name, redirect := ChromiumTransition(tc.raw)
		if name != tc.name || redirect != tc.redirect {
			t.Fatalf("%#x = %s %v, want %s %v", tc.raw, name, redirect, tc.name, tc.redirect)
		}
	}
}

func TestSearchTermFromURL(t *testing.T) {
	for raw, want := range map[string]string{
		"https://www.google.com/search?q=hello+world": "hello world",
		"https://www.google.co.uk/search?q=tea":       "tea",
		"https://duckduckgo.com/?q=ducks&ia=web":      "ducks",
		"https://www.bing.com/search?q=bing+it":       "bing it",
		"https://search.brave.com/search?q=brave":     "brave",
		"https://kagi.com/search?q=kagi":              "kagi",
		"https://www.google.com/maps?q=not+a+search":  "",
		"https://example.com/search?q=not+an+engine":  "",
		"https://notgoogle.example/search?q=nope":     "",
		"chrome://history/?q=internal":                "",
	} {
		if got := SearchTermFromURL(raw); got != want {
			t.Fatalf("%s = %q, want %q", raw, got, want)
		}
	}
}

func TestDiscoverProfilesNamesProfilesAndSkipsMachinery(t *testing.T) {
	home := fakeHome(t)
	profiles, err := DiscoverProfiles(home, DefaultBrowsers)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, p := range profiles {
		got = append(got, p.Browser+"/"+p.Key+"="+p.Name)
	}
	want := "chrome/Default=Personal chrome/Profile 1=Work brave/Default=Default safari/Default=Default"
	if strings.Join(got, " ") != want {
		t.Fatalf("profiles = %v", got)
	}
	// A Mac with none of the browsers is not an error.
	if profiles, err := DiscoverProfiles(t.TempDir(), DefaultBrowsers); err != nil || len(profiles) != 0 {
		t.Fatalf("empty home = %v %v", profiles, err)
	}
}

func TestSnapshotReadsVisitsStillInTheWAL(t *testing.T) {
	dir := t.TempDir()
	live := filepath.Join(dir, "History")
	writeChromium(t, live, "PRAGMA journal_mode=WAL")
	db, err := sql.Open("sqlite", live)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"PRAGMA wal_autocheckpoint=0",
		"INSERT INTO urls (id, url, title, last_visit_time) VALUES (1, 'https://wal.example/', 'In the WAL', 0)",
		chromiumVisit(1, 1, fixtureTime, linkChainBoth),
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if info, err := os.Stat(live + "-wal"); err != nil || info.Size() == 0 {
		t.Fatalf("expected a non-empty WAL: %v", err)
	}
	copyDir := t.TempDir()
	snapshot, err := Snapshot(live, copyDir)
	if err != nil {
		t.Fatal(err)
	}
	copyDB, err := common.OpenSQLite(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer copyDB.Close()
	visits, err := ScanVisits(copyDB, EngineChromium, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(visits) != 1 || visits[0].Title != "In the WAL" {
		t.Fatalf("visits = %+v", visits)
	}
}

func runCLI(t *testing.T, args []string, env map[string]string, cfg ingestclient.Config) (int, string, string) {
	t.Helper()
	common.OverrideBeforeUploadCheck = func() string { return "" }
	t.Cleanup(func() { common.OverrideBeforeUploadCheck = nil })
	var out, errOut bytes.Buffer
	code := Run(args, strings.NewReader(""), &out, &errOut, func(k string) string { return env[k] }, cfg)
	return code, out.String(), errOut.String()
}

func cliSetup(t *testing.T) (map[string]string, []string, string) {
	home := fakeHome(t)
	root := t.TempDir()
	env := map[string]string{
		"BROWSER_HISTORY_ACCOUNT": "owner@example.com",
		"BROWSER_HISTORY_DEVICE":  "testmac",
		"BROWSER_HISTORY_HOME":    home,
		"PDW_INGEST_PROJECT_DIR":  root,
	}
	return env, []string{"--state-file", filepath.Join(root, "state.sqlite"), "--lock-file", filepath.Join(root, "state.lock")}, home
}

func uploadedRecords(app *testfixtures.FakeApp) []map[string]any {
	var records []map[string]any
	for _, call := range app.Calls() {
		if call.Path != "/ingest/browser-history/batch" {
			continue
		}
		for _, envelope := range testfixtures.DecodeGzipJSONL(call.Body) {
			records = append(records, envelope)
		}
	}
	return records
}

func byKey(records []map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, envelope := range records {
		record := envelope["record"].(map[string]any)
		out[fmt.Sprintf("%s/%s/%s", record["browser"], record["profile"], record["visit_id"])] = record
	}
	return out
}

func TestRunUploadsEveryBrowserThenOnlyNewVisits(t *testing.T) {
	env, args, home := cliSetup(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	code, out, errOut := runCLI(t, args, env, cfg)
	if code != 0 {
		t.Fatalf("code=%d out=%q stderr=%q", code, out, errOut)
	}
	if !strings.Contains(out, "profiles=4 visits=11 searches=2 batches=4") {
		t.Fatalf("summary: %q", out)
	}
	records := uploadedRecords(app)
	if records[0]["source"] != "browser_history" || records[0]["account"] != "owner@example.com" || records[0]["record_type"] != "visit" {
		t.Fatalf("envelope = %v", records[0])
	}
	visits := byKey(records)
	if len(visits) != 11 {
		t.Fatalf("visits = %d: %v", len(visits), visits)
	}
	search := visits["chrome/Default/2"]
	if search["search_term"] != "Synthetic Query" || search["transition"] != "typed" || search["device"] != "testmac" ||
		search["profile_name"] != "Personal" || search["host"] != "www.google.com" || search["visited_at"] != "2026-10-01T12:01:00+00:00" {
		t.Fatalf("search visit = %v", search)
	}
	if visits["chrome/Default/3"]["is_redirect"] != true || visits["chrome/Default/1"]["is_redirect"] != false {
		t.Fatal("redirect hop not flagged")
	}
	if visits["chrome/Default/6"]["is_synced"] != true || visits["chrome/Default/1"]["is_synced"] != false {
		t.Fatal("synced visit not flagged")
	}
	if visits["chrome/Default/1"]["visit_duration_ms"] != float64(2500) {
		t.Fatalf("duration = %v", visits["chrome/Default/1"]["visit_duration_ms"])
	}
	if _, ok := visits["chrome/Default/7"]; ok {
		t.Fatal("a visit with no time must not ship")
	}
	if _, ok := visits["chrome/System Profile/1"]; ok {
		t.Fatal("the system profile must not ship")
	}
	safariSearch := visits["safari/Default/2"]
	if safariSearch["search_term"] != "safari term" || safariSearch["visited_at"] != "2026-10-01T12:00:00+00:00" || safariSearch["transition"] != "" {
		t.Fatalf("safari visit = %v", safariSearch)
	}
	if visits["safari/Default/1"]["is_redirect"] != true || visits["safari/Default/3"]["is_synced"] != true {
		t.Fatal("safari redirect/synced flags wrong")
	}

	// Nothing new: nothing uploaded.
	before := len(app.Calls())
	if code, out, _ := runCLI(t, args, env, cfg); code != 0 || !strings.Contains(out, "visits=0") || len(app.Calls()) != before {
		t.Fatalf("second run: code=%d out=%q calls=%d", code, out, len(app.Calls())-before)
	}

	// One new visit: only it ships.
	db, err := sql.Open("sqlite", filepath.Join(home, "Library/Application Support/Google/Chrome/Default/History"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(chromiumVisit(8, 1, fixtureTime.Add(time.Hour), linkChainBoth)); err != nil {
		t.Fatal(err)
	}
	db.Close()
	code, out, _ = runCLI(t, args, env, cfg)
	newRecords := uploadedRecords(app)[len(records):]
	if code != 0 || len(newRecords) != 1 || newRecords[0]["record"].(map[string]any)["visit_id"] != "8" {
		t.Fatalf("third run: code=%d out=%q new=%v", code, out, newRecords)
	}
}

func TestRunRespectsLimitAndBrowserFilter(t *testing.T) {
	env, args, _ := cliSetup(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	if code, out, errOut := runCLI(t, append(args, "--browser", "safari", "--limit", "2"), env, cfg); code != 0 || !strings.Contains(out, "visits=2") {
		t.Fatalf("code=%d out=%q stderr=%q", code, out, errOut)
	}
	for key := range byKey(uploadedRecords(app)) {
		if !strings.HasPrefix(key, "safari/") {
			t.Fatalf("unexpected %s", key)
		}
	}
	if code, _, errOut := runCLI(t, append(args, "--browser", "firefox"), env, cfg); code != 2 || !strings.Contains(errOut, "unknown browser") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}

func TestUnreadableSafariFailsAfterTheOtherBrowsersUpload(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode-000 file")
	}
	env, args, home := cliSetup(t)
	safari := filepath.Join(home, "Library/Safari/History.db")
	if err := os.Chmod(safari, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(safari, 0o644) })
	app := testfixtures.NewFakeApp(t)
	code, out, errOut := runCLI(t, args, env, ingestclient.Config{BaseURL: app.URL(), Token: "secret"})
	if code != 1 || !strings.Contains(errOut, "Full Disk Access") || !strings.Contains(out, "failed=1") {
		t.Fatalf("code=%d out=%q stderr=%q", code, out, errOut)
	}
	if got := len(byKey(uploadedRecords(app))); got != 8 {
		t.Fatalf("chromium visits uploaded = %d, want 8", got)
	}
}

func TestRecreatedHistoryIsReadFromTheStart(t *testing.T) {
	env, args, home := cliSetup(t)
	app := testfixtures.NewFakeApp(t)
	cfg := ingestclient.Config{BaseURL: app.URL(), Token: "secret"}
	if code, _, errOut := runCLI(t, append(args, "--browser", "brave"), env, cfg); code != 0 {
		t.Fatal(errOut)
	}
	brave := filepath.Join(home, "Library/Application Support/BraveSoftware/Brave-Browser/Default/History")
	os.Remove(brave)
	writeChromium(t, brave,
		"INSERT INTO urls (id, url, title, last_visit_time) VALUES (1, 'https://fresh.example/', 'Fresh', 0)",
		chromiumVisit(1, 1, fixtureTime, linkChainBoth),
	)
	// Same max id as the mark: nothing new yet.
	if code, out, _ := runCLI(t, append(args, "--browser", "brave"), env, cfg); code != 0 || !strings.Contains(out, "visits=0") {
		t.Fatalf("out=%q", out)
	}
	os.Remove(brave)
	writeChromium(t, brave, "INSERT INTO urls (id, url, title, last_visit_time) VALUES (1, 'https://x.example/', 'x', 0)")
	if code, out, _ := runCLI(t, append(args, "--browser", "brave"), env, cfg); code != 0 || !strings.Contains(out, "visits=0") {
		t.Fatalf("out=%q", out)
	}
}

func TestRunHelpAndAccount(t *testing.T) {
	code, out, _ := runCLI(t, []string{"--help"}, nil, ingestclient.Config{})
	if code != 0 || !strings.Contains(out, "pdw ingest browser-history") {
		t.Fatalf("help: %d %q", code, out)
	}
	env, args, _ := cliSetup(t)
	delete(env, "BROWSER_HISTORY_ACCOUNT")
	if code, _, errOut := runCLI(t, args, env, ingestclient.Config{BaseURL: "http://x", Token: "t"}); code != 1 || !strings.Contains(errOut, "BROWSER_HISTORY_ACCOUNT") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}

func TestDryRunSendsNothingAndKeepsNoState(t *testing.T) {
	env, args, _ := cliSetup(t)
	code, out, errOut := runCLI(t, append(args, "--dry-run"), env, ingestclient.Config{})
	if code != 0 || !strings.Contains(out, "visits=11") || !strings.Contains(out, "dry-run-1") {
		t.Fatalf("code=%d out=%q stderr=%q", code, out, errOut)
	}
	if _, err := os.Stat(args[1]); !os.IsNotExist(err) {
		t.Fatalf("dry run wrote state: %v", err)
	}
}
