package x_test

import (
	"database/sql"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	x "github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/x"
	_ "modernc.org/sqlite"
)

// createLegacyStateDB builds a pre-migration SQLite state database without the
// operation column on backfill or catchup, matching porygon's existing DB.
func createLegacyStateDB(t *testing.T, postsCursor string, postsDone int, legacyCatchup string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "legacy-state.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmts := []string{
		`CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE seen (stream TEXT NOT NULL, post_id TEXT NOT NULL, discovered_at TEXT NOT NULL, PRIMARY KEY (stream, post_id))`,
		`CREATE TABLE backfill (stream TEXT PRIMARY KEY, cursor TEXT NOT NULL DEFAULT '', done INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL DEFAULT '')`,
		`CREATE TABLE catchup (stream TEXT NOT NULL, cursor TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY (stream, cursor))`,
		`CREATE TABLE cooldown (scope TEXT PRIMARY KEY, until TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '')`,
		`INSERT INTO meta (key, value) VALUES ('schema_version', '1'), ('account', '` + myHandle + `')`,
		`INSERT INTO seen (stream, post_id, discovered_at) VALUES ('posts', '100', '2026-11-27T12:00:00Z')`,
		`INSERT INTO backfill (stream, cursor, done, updated_at) VALUES ('likes', '', 1, '2026-11-27T12:00:00Z')`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO backfill (stream, cursor, done, updated_at) VALUES ('posts', ?, ?, '2026-11-27T12:00:00Z')`, postsCursor, postsDone); err != nil {
		t.Fatal(err)
	}
	if legacyCatchup != "" {
		if _, err := db.Exec(`INSERT INTO catchup (stream, cursor, created_at) VALUES ('posts', ?, '2026-11-27T12:00:00Z')`, legacyCatchup); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

func TestLegacyBackfillStateMigratesAndResetsForUserTweetsAndReplies(t *testing.T) {
	for _, tc := range []struct {
		name      string
		postsDone int
		cursor    string
		catchup   string
	}{
		{name: "in-progress legacy UserTweets backfill", postsDone: 0, cursor: "old-ut-cursor", catchup: "old-ut-gap"},
		{name: "completed legacy UserTweets backfill", postsDone: 1, cursor: "", catchup: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbPath := createLegacyStateDB(t, tc.cursor, tc.postsDone, tc.catchup)
			state, err := x.OpenState(dbPath, myHandle)
			if err != nil {
				t.Fatal(err)
			}
			defer state.Close()

			// Likes (no fallback) keeps its completed backfill untouched.
			likesBF, err := state.Backfill("likes")
			if err != nil || !likesBF.Started || !likesBF.Done {
				t.Fatalf("likes backfill = %+v err = %v", likesBF, err)
			}

			postsStream, _ := x.StreamByName("posts")
			now := time.Date(2026, 11, 28, 8, 0, 0, 0, time.UTC)
			replyNew := tweet("201", myID, myHandle, "@someone recent reply", map[string]any{"legacy.in_reply_to_status_id_str": "200"})
			replyOld := tweet("150", myID, myHandle, "@someone older reply", map[string]any{"legacy.in_reply_to_status_id_str": "149"})
			api := &fakeAPI{remaining: -1, pages: map[string]map[string]any{
				"UserTweetsAndReplies|":       roundTrip(t, profileTimeline(itemEntry("201", replyNew), cursorEntry("Bottom", "utr-c1"))),
				"UserTweetsAndReplies|utr-c1": roundTrip(t, profileTimeline(itemEntry("150", replyOld), cursorEntry("Bottom", "utr-c2"))),
			}}
			uploader := &recordingUploader{}
			if _, err := (&x.Runner{
				Account: myHandle, UserID: myID, API: api, Client: uploader, State: state,
				Streams: []x.Stream{postsStream}, Logger: common.NewWriterLogger(io.Discard),
				Now: func() time.Time { return now }, Pages: 1, BackfillPages: 1,
			}).Sync(); err != nil {
				t.Fatal(err)
			}

			// Must never pass the legacy UserTweets cursor or gap to UserTweetsAndReplies.
			for _, call := range api.calls {
				if strings.Contains(call, "old-ut-") {
					t.Fatalf("passed legacy UserTweets cursor to UserTweetsAndReplies: %v", api.calls)
				}
			}
			if strings.Join(api.calls, ",") != "UserTweetsAndReplies|,UserTweetsAndReplies|utr-c1" {
				t.Fatalf("calls = %v", api.calls)
			}
			rels := uploader.relations()
			if _, ok := rels["authored|150"]; !ok {
				t.Fatalf("older reply was not backfilled: %v", rels)
			}
			bf, err := state.Backfill("posts")
			if err != nil {
				t.Fatal(err)
			}
			if !bf.Started || bf.Done || bf.Cursor != "utr-c2" || bf.Operation != "UserTweetsAndReplies" {
				t.Fatalf("posts backfill = %+v", bf)
			}
		})
	}
}

func TestExplicitUserTweetsBackfillResetsWhenUserTweetsAndRepliesSucceeds(t *testing.T) {
	state := openState(t)
	now := time.Date(2026, 11, 28, 8, 0, 0, 0, time.UTC)
	if err := state.SetBackfill("posts", "ut-c9", "UserTweets", false, now); err != nil {
		t.Fatal(err)
	}
	if err := state.ReplaceCatchup("posts", "", "ut-gap", "UserTweets", now); err != nil {
		t.Fatal(err)
	}
	postsStream, _ := x.StreamByName("posts")
	reply := tweet("301", myID, myHandle, "@someone reply", map[string]any{"legacy.in_reply_to_status_id_str": "300"})
	olderReply := tweet("250", myID, myHandle, "@someone older", map[string]any{"legacy.in_reply_to_status_id_str": "249"})
	api := &fakeAPI{remaining: -1, pages: map[string]map[string]any{
		"UserTweetsAndReplies|":       roundTrip(t, profileTimeline(itemEntry("301", reply), cursorEntry("Bottom", "utr-c1"))),
		"UserTweetsAndReplies|utr-c1": roundTrip(t, profileTimeline(itemEntry("250", olderReply))),
	}}
	uploader := &recordingUploader{}
	if _, err := (&x.Runner{
		Account: myHandle, UserID: myID, API: api, Client: uploader, State: state,
		Streams: []x.Stream{postsStream}, Logger: common.NewWriterLogger(io.Discard),
		Now: func() time.Time { return now }, Pages: 1, BackfillPages: 2,
	}).Sync(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(api.calls, ",") != "UserTweetsAndReplies|,UserTweetsAndReplies|utr-c1" {
		t.Fatalf("calls = %v", api.calls)
	}
	bf, err := state.Backfill("posts")
	if err != nil {
		t.Fatal(err)
	}
	if !bf.Done || bf.Operation != "UserTweetsAndReplies" {
		t.Fatalf("backfill = %+v", bf)
	}
	gaps, err := state.Catchups("posts")
	if err != nil || len(gaps) != 0 {
		t.Fatalf("stale UserTweets catchup gap was not cleared: %+v err=%v", gaps, err)
	}
}

func TestFallbackToUserTweetsPreservesUserTweetsAndRepliesBackfillAndCatchups(t *testing.T) {
	state := openState(t)
	now := time.Date(2026, 11, 28, 8, 0, 0, 0, time.UTC)
	if err := state.SetBackfill("posts", "utr-c5", "UserTweetsAndReplies", false, now); err != nil {
		t.Fatal(err)
	}
	if err := state.ReplaceCatchup("posts", "", "utr-gap", "UserTweetsAndReplies", now); err != nil {
		t.Fatal(err)
	}
	if err := state.MarkDiscovered("posts", "400", now); err != nil {
		t.Fatal(err)
	}

	postsStream, _ := x.StreamByName("posts")
	// Transient 404 on UserTweetsAndReplies triggers fallback to UserTweets.
	api := &fakeAPI{
		remaining: -1,
		errs:      map[string]error{"UserTweetsAndReplies|": errors.New("UserTweetsAndReplies: HTTP 404")},
		pages: map[string]map[string]any{
			"UserTweets|": roundTrip(t, profileTimeline(itemEntry("400", tweet("400", myID, myHandle, "seen", nil)))),
		},
	}
	uploader := &recordingUploader{}
	if _, err := (&x.Runner{
		Account: myHandle, UserID: myID, API: api, Client: uploader, State: state,
		Streams: []x.Stream{postsStream}, Logger: common.NewWriterLogger(io.Discard),
		Now: func() time.Time { return now }, Pages: 1, BackfillPages: 3,
	}).Sync(); err != nil {
		t.Fatal(err)
	}
	// Fallback must only read the top page of UserTweets, never passing utr-gap or utr-c5.
	if strings.Join(api.calls, ",") != "UserTweetsAndReplies|,UserTweets|" {
		t.Fatalf("calls during fallback = %v", api.calls)
	}
	bf, err := state.Backfill("posts")
	if err != nil {
		t.Fatal(err)
	}
	if !bf.Started || bf.Done || bf.Cursor != "utr-c5" || bf.Operation != "UserTweetsAndReplies" {
		t.Fatalf("fallback clobbered UserTweetsAndReplies backfill: %+v", bf)
	}
	gaps, err := state.Catchups("posts")
	if err != nil || len(gaps) != 1 || gaps[0].Cursor != "utr-gap" || gaps[0].Operation != "UserTweetsAndReplies" {
		t.Fatalf("fallback clobbered UserTweetsAndReplies catchup gap: %+v err=%v", gaps, err)
	}

	// Next run: UserTweetsAndReplies recovers and resumes both utr-gap and utr-c5 without resetting.
	delete(api.errs, "UserTweetsAndReplies|")
	api.calls = nil
	api.pages["UserTweetsAndReplies|"] = roundTrip(t, profileTimeline(itemEntry("400", tweet("400", myID, myHandle, "seen", nil)), cursorEntry("Bottom", "utr-c1")))
	api.pages["UserTweetsAndReplies|utr-gap"] = roundTrip(t, profileTimeline(itemEntry("400", tweet("400", myID, myHandle, "seen", nil))))
	api.pages["UserTweetsAndReplies|utr-c5"] = roundTrip(t, profileTimeline(itemEntry("350", tweet("350", myID, myHandle, "older", nil)), cursorEntry("Bottom", "utr-c6")))
	if _, err := (&x.Runner{
		Account: myHandle, UserID: myID, API: api, Client: uploader, State: state,
		Streams: []x.Stream{postsStream}, Logger: common.NewWriterLogger(io.Discard),
		Now: func() time.Time { return now.Add(30 * time.Minute) }, Pages: 1, BackfillPages: 1,
	}).Sync(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(api.calls, ",") != "UserTweetsAndReplies|,UserTweetsAndReplies|utr-gap,UserTweetsAndReplies|utr-c5" {
		t.Fatalf("calls after recovery = %v", api.calls)
	}
	bf, err = state.Backfill("posts")
	if err != nil {
		t.Fatal(err)
	}
	if bf.Cursor != "utr-c6" || bf.Done || bf.Operation != "UserTweetsAndReplies" {
		t.Fatalf("backfill after recovery = %+v", bf)
	}
}

func TestNormalUserTweetsAndRepliesRunsPreserveCursorWithoutResetting(t *testing.T) {
	state := openState(t)
	now := time.Date(2026, 11, 28, 8, 0, 0, 0, time.UTC)
	postsStream, _ := x.StreamByName("posts")
	api := &fakeAPI{remaining: -1, pages: map[string]map[string]any{
		"UserTweetsAndReplies|":   roundTrip(t, profileTimeline(itemEntry("10", tweet("10", myID, myHandle, "p10", nil)), cursorEntry("Bottom", "c1"))),
		"UserTweetsAndReplies|c1": roundTrip(t, profileTimeline(itemEntry("9", tweet("9", myID, myHandle, "p9", nil)), cursorEntry("Bottom", "c2"))),
		"UserTweetsAndReplies|c2": roundTrip(t, profileTimeline(itemEntry("8", tweet("8", myID, myHandle, "p8", nil)), cursorEntry("Bottom", "c3"))),
	}}
	uploader := &recordingUploader{}
	run := func(at time.Time) {
		t.Helper()
		if _, err := (&x.Runner{
			Account: myHandle, UserID: myID, API: api, Client: uploader, State: state,
			Streams: []x.Stream{postsStream}, Logger: common.NewWriterLogger(io.Discard),
			Now: func() time.Time { return at }, Pages: 1, BackfillPages: 1,
		}).Sync(); err != nil {
			t.Fatal(err)
		}
	}
	run(now)
	if strings.Join(api.calls, ",") != "UserTweetsAndReplies|,UserTweetsAndReplies|c1" {
		t.Fatalf("run 1 calls = %v", api.calls)
	}
	api.calls = nil
	run(now.Add(30 * time.Minute))
	if strings.Join(api.calls, ",") != "UserTweetsAndReplies|,UserTweetsAndReplies|c2" {
		t.Fatalf("run 2 must advance from c2 without resetting: %v", api.calls)
	}
	bf, err := state.Backfill("posts")
	if err != nil || bf.Cursor != "c3" || bf.Operation != "UserTweetsAndReplies" {
		t.Fatalf("backfill after run 2 = %+v err=%v", bf, err)
	}
}

func TestFallbackMidwayThroughAFreshBackfillKeepsTheUserTweetsAndRepliesCursor(t *testing.T) {
	state := openState(t)
	now := time.Date(2026, 11, 28, 8, 0, 0, 0, time.UTC)
	postsStream, _ := x.StreamByName("posts")
	// A first run: the walk starts via UserTweetsAndReplies, then X answers 404
	// on the second history page and the run falls back to UserTweets.
	api := &fakeAPI{
		remaining: -1,
		errs:      map[string]error{"UserTweetsAndReplies|utr-c2": errors.New("UserTweetsAndReplies: HTTP 404")},
		pages: map[string]map[string]any{
			"UserTweetsAndReplies|":       roundTrip(t, profileTimeline(itemEntry("503", tweet("503", myID, myHandle, "newest", nil)), cursorEntry("Bottom", "utr-c1"))),
			"UserTweetsAndReplies|utr-c1": roundTrip(t, profileTimeline(itemEntry("502", tweet("502", myID, myHandle, "older", nil)), cursorEntry("Bottom", "utr-c2"))),
			"UserTweets|utr-c2":           roundTrip(t, profileTimeline(itemEntry("501", tweet("501", myID, myHandle, "oldest", nil)), cursorEntry("Bottom", "ut-c3"))),
		},
	}
	if _, err := (&x.Runner{
		Account: myHandle, UserID: myID, API: api, Client: &recordingUploader{}, State: state,
		Streams: []x.Stream{postsStream}, Logger: common.NewWriterLogger(io.Discard),
		Now: func() time.Time { return now }, Pages: 1, BackfillPages: 3,
	}).Sync(); err != nil {
		t.Fatal(err)
	}
	bf, err := state.Backfill("posts")
	if err != nil {
		t.Fatal(err)
	}
	if !bf.Started || bf.Done || bf.Cursor != "utr-c2" || bf.Operation != "UserTweetsAndReplies" {
		t.Fatalf("fallback overwrote the fresh UserTweetsAndReplies backfill: %+v", bf)
	}
}
