package externalfeeds

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestPostgresProjectionCursor(t *testing.T) {
	url := os.Getenv("PDW_EXTERNAL_FEED_TEST_URL")
	if url == "" {
		t.Skip("disposable warehouse required")
	}
	store, err := NewPostgresStore(url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var relations map[string]string
	if err = json.Unmarshal([]byte(os.Getenv("PDW_EXTERNAL_FEED_TEST_RELATIONS")), &relations); err != nil {
		t.Fatal(err)
	}
	store.expand = func(q string) string {
		for id, physical := range relations {
			q = strings.ReplaceAll(q, "@"+id, physical)
		}
		return q
	}
	store.db.SetMaxOpenConns(1)
	ctx := context.Background()
	exec := func(q string) {
		t.Helper()
		if _, err := store.db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	exec(`CREATE TEMP TABLE feed_source(item_key text PRIMARY KEY,recorded_at timestamptz DEFAULT now(),payload_json text,ready boolean DEFAULT true)`)
	exec(`INSERT INTO feed_source(item_key,payload_json) VALUES ('b','{"id":"b"}'),('c','{"id":"c"}')`)
	feed := All()[0]
	feed.SourceSQL = `SELECT item_key,recorded_at,payload_json FROM pg_temp.feed_source WHERE ready AND recorded_at>=now()-interval '48 hours'`
	rows, cursor, more, err := store.Page(ctx, feed, 0, 1)
	if err != nil || len(rows) != 1 || !more {
		t.Fatalf("first page %v %v", rows, err)
	}
	seen := map[string]bool{rows[0]["id"].(string): true}
	// A late/backdated item sorting BEFORE the already-delivered item must
	// still arrive after the watermark, rather than disappear behind it.
	exec(`INSERT INTO feed_source(item_key,recorded_at,payload_json) VALUES ('a',now()-interval '2 hours','{"id":"a"}')`)
	for {
		rows, cursor, more, err = store.Page(ctx, feed, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			seen[row["id"].(string)] = true
		}
		if !more {
			break
		}
	}
	if len(seen) != 3 {
		t.Fatalf("missed an arrival: %v", seen)
	}
	// An unchanged poll must not create a new delivery or write row versions.
	var before string
	if err := store.db.QueryRowContext(ctx, store.expand(`SELECT xmin::text FROM @external_feed_items WHERE item_key='b'`)).Scan(&before); err != nil {
		t.Fatal(err)
	}
	rows, next, _, err := store.Page(ctx, feed, cursor, 10)
	if err != nil || len(rows) != 0 || next != cursor {
		t.Fatalf("unchanged poll %v %v", rows, err)
	}
	var after string
	if err := store.db.QueryRowContext(ctx, store.expand(`SELECT xmin::text FROM @external_feed_items WHERE item_key='b'`)).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("unchanged projection wrote a row version")
	}
	// Re-transcription hides the stored enrichment until it is ready again.
	exec(`UPDATE feed_source SET ready=false WHERE item_key='b'`)
	rows, _, _, err = store.Page(ctx, feed, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row["id"] == "b" {
			t.Fatal("stale projection served")
		}
	}
	exec(`UPDATE feed_source SET ready=true,payload_json='{"id":"b","transcript":"new"}' WHERE item_key='b'`)
	rows, _, _, err = store.Page(ctx, feed, cursor, 10)
	if err != nil || len(rows) != 1 || rows[0]["transcript"] != "new" {
		t.Fatalf("re-enriched memo %v %v", rows, err)
	}
}
