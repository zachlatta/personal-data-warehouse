package externalfeeds

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
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
	var sequenceBefore, sequenceAfter int64
	if err := store.db.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT last_value FROM @external_feed_seq`)).Scan(&sequenceBefore); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT xmin::text || '/' || xmax::text FROM @external_feed_items WHERE item_key='b'`)).Scan(&before); err != nil {
		t.Fatal(err)
	}
	rows, next, _, err := store.Page(ctx, feed, cursor, 10)
	if err != nil || len(rows) != 0 || next != cursor {
		t.Fatalf("unchanged poll %v %v", rows, err)
	}
	var after string
	if err := store.db.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT xmin::text || '/' || xmax::text FROM @external_feed_items WHERE item_key='b'`)).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("unchanged projection wrote or locked a tuple")
	}
	if err := store.db.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT last_value FROM @external_feed_seq`)).Scan(&sequenceAfter); err != nil {
		t.Fatal(err)
	}
	if sequenceBefore != sequenceAfter {
		t.Fatal("idle poll allocated sequence values")
	}
	// EXPLAIN's WAL accounting isolates this statement from other test workers.
	var plan []byte
	if err := store.db.QueryRowContext(ctx, "EXPLAIN (ANALYZE,WAL,FORMAT JSON) "+refreshSQL(feed.SourceSQL), feed.Scope, warehouse.SQLRelation("external_feed_seq")).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	var nodes any
	if err := json.Unmarshal(plan, &nodes); err != nil {
		t.Fatal(err)
	}
	var checkWAL func(any)
	checkWAL = func(v any) {
		switch n := v.(type) {
		case map[string]any:
			for key, value := range n {
				if (key == "WAL Records" || key == "WAL Bytes") && value.(float64) != 0 {
					t.Fatalf("idle refresh wrote WAL: %s", plan)
				}
				checkWAL(value)
			}
		case []any:
			for _, child := range n {
				checkWAL(child)
			}
		}
	}
	checkWAL(nodes)
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
	t.Run("overlapping refresh waits before allocating arrivals", func(t *testing.T) {
		probe := feed
		probe.SourceSQL = `SELECT 'probe'::text AS item_key, now() AS recorded_at, '{"id":"probe"}'::text AS payload_json`
		// A separate pool models an earlier poll on another app instance. The
		// existing store has only one connection, so sharing it would test pool
		// contention rather than the database advisory lock.
		earlier, err := NewPostgresStore(url)
		if err != nil {
			t.Fatal(err)
		}
		defer earlier.Close()
		tx, err := earlier.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, warehouse.SQLRelation("external_feed_items")); err != nil {
			t.Fatal(err)
		}
		if err := store.db.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT last_value FROM @external_feed_seq`)).Scan(&sequenceBefore); err != nil {
			t.Fatal(err)
		}
		blocked, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		_, _, _, err = store.Page(blocked, probe, cursor, 10)
		if err == nil || blocked.Err() != context.DeadlineExceeded {
			t.Fatalf("overlapping refresh did not wait for the earlier transaction: %v", err)
		}
		if err := store.db.QueryRowContext(ctx, warehouse.ExpandRelations(`SELECT last_value FROM @external_feed_seq`)).Scan(&sequenceAfter); err != nil {
			t.Fatal(err)
		}
		if sequenceAfter != sequenceBefore {
			t.Fatal("blocked refresh allocated arrivals before obtaining the lock")
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		rows, _, _, err := store.Page(ctx, probe, cursor, 10)
		if err != nil || len(rows) != 1 || rows[0]["id"] != "probe" {
			t.Fatalf("refresh did not recover after lock release: %v %v", rows, err)
		}
	})
}
