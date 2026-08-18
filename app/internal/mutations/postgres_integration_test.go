package mutations

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// testStore opens a real Postgres when PDW_TEST_POSTGRES_URL points at a
// throwaway database, and skips otherwise. Every other test in this package
// runs against a fake store, so without this the store's SQL — the half of the
// mutation system that actually touches the warehouse — has no coverage at all.
func testStore(t *testing.T) *PostgresStore {
	t.Helper()
	url := strings.TrimSpace(os.Getenv("PDW_TEST_POSTGRES_URL"))
	if url == "" {
		t.Skip("PDW_TEST_POSTGRES_URL is not set")
	}
	store, err := NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.EnsureTables(context.Background()); err != nil {
		t.Fatalf("ensure tables: %v", err)
	}
	return store
}

// seedRequest makes every run's request unique. CreateRequest deduplicates by
// content hash, so a fixed title would silently reuse the previous run's row
// and make these tests pass or fail depending on how often they had been run.
func seedRequest(t *testing.T, store *PostgresStore, label string) Request {
	t.Helper()
	title := fmt.Sprintf("%s %s %d", t.Name(), label, time.Now().UnixNano())
	request, err := store.CreateRequest(context.Background(), CreateRequestInput{
		Title:       title,
		Reason:      "integration test",
		RequestedBy: "test",
		Mutations: []MutationInput{{
			Type:    GooglePeopleContactsOperation,
			Account: "zach@example.test",
			Operations: []map[string]any{{
				"op":            "delete_contact",
				"resource_name": "people/" + strings.ReplaceAll(title, " ", "-"),
				"etag":          "etag-" + title,
			}},
		}},
	})
	if err != nil {
		t.Fatalf("create request %q: %v", title, err)
	}
	return request
}

func setRequestStatus(t *testing.T, store *PostgresStore, id string, status string) {
	t.Helper()
	if _, err := execContext(context.Background(), store.db, `UPDATE @upstream_mutation_requests SET status = $2 WHERE id = $1`, id, status); err != nil {
		t.Fatalf("set status: %v", err)
	}
}

func TestSupersedeRequestPersistsTheLinkAndEvent(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	old := seedRequest(t, store, "old request")
	replacement := seedRequest(t, store, "replacement request")
	setRequestStatus(t, store, old.ID, "failed_terminal")

	updated, err := store.SupersedeRequest(ctx, old.ID, replacement.ID, "web-ui")
	if err != nil {
		t.Fatalf("SupersedeRequest: %v", err)
	}
	if updated.SupersededBy != replacement.ID {
		t.Fatalf("SupersededBy = %q", updated.SupersededBy)
	}
	// The failure is history, not something to paper over.
	if updated.Status != "failed_terminal" {
		t.Fatalf("status = %q; superseding must not rewrite the outcome", updated.Status)
	}

	// It has to survive a fresh read, since the review UI re-reads on every page.
	reread, err := store.GetRequest(ctx, old.ID)
	if err != nil {
		t.Fatalf("GetRequest: %v", err)
	}
	if reread.SupersededBy != replacement.ID {
		t.Fatalf("reread SupersededBy = %q", reread.SupersededBy)
	}

	var eventCount int
	if err := queryRowContext(ctx, store.db, `
		SELECT count(*) FROM @upstream_mutation_request_events
		WHERE request_id = $1 AND event_type = 'superseded' AND event_json ->> 'superseded_by' = $2
	`, old.ID, replacement.ID).Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("superseded event count = %d", eventCount)
	}
}

func TestSupersedeRequestRefusesALiveRequest(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	pending := seedRequest(t, store, "pending request")
	replacement := seedRequest(t, store, "another replacement")

	if _, err := store.SupersedeRequest(ctx, pending.ID, replacement.ID, "web-ui"); err == nil {
		t.Fatal("superseded a pending request")
	} else if !strings.Contains(err.Error(), "pending_review") {
		t.Fatalf("error = %v", err)
	}
}

func TestSupersedeRequestRefusesAMissingReplacement(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	old := seedRequest(t, store, "orphan request")
	setRequestStatus(t, store, old.ID, "failed_terminal")

	if _, err := store.SupersedeRequest(ctx, old.ID, "req_does_not_exist", "web-ui"); err == nil {
		t.Fatal("superseded by a request that does not exist")
	} else if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("error = %v", err)
	}
}

// The contacts preview enrichment is the reason the failed request rendered a
// blank diff. It only works if the SQL against the synced contact card runs.
func TestCreateRequestFillsContactPreviewFromTheSyncedCard(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	if _, err := store.db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+warehouse.QuoteIdent(warehouse.SchemaOf("contact_cards"))); err != nil {
		t.Skipf("cannot create the contacts schema in this database: %v", err)
	}
	if _, err := execContext(ctx, store.db, `
		CREATE TABLE IF NOT EXISTS @contact_cards (
			source text, account text, source_kind text, address_book_id text,
			card_id text, etag text, is_deleted int, raw_json jsonb
		)
	`); err != nil {
		t.Skipf("cannot create contact_cards in this database: %v", err)
	}
	if _, err := execContext(ctx, store.db, `DELETE FROM @contact_cards WHERE card_id = 'people/preview'`); err != nil {
		t.Fatalf("clear card: %v", err)
	}
	if _, err := execContext(ctx, store.db, `
		INSERT INTO @contact_cards (source, account, source_kind, address_book_id, card_id, etag, is_deleted, raw_json)
		VALUES ('google_people', 'zach@example.test', 'google_contacts', 'people/me', 'people/preview', 'etag-live', 0,
		        '{"names":[{"displayName":"Before Name"}]}'::jsonb)
	`); err != nil {
		t.Fatalf("insert card: %v", err)
	}

	request, err := store.CreateRequest(ctx, CreateRequestInput{
		Title:       "preview enrichment",
		Reason:      "integration test",
		RequestedBy: "test",
		Mutations: []MutationInput{{
			Type:    GooglePeopleContactsOperation,
			Account: "zach@example.test",
			Operations: []map[string]any{{
				"op":            "update_contact",
				"resource_name": "people/preview",
				"etag":          "etag-live",
				"person":        map[string]any{"names": []any{map[string]any{"displayName": "After Name"}}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("CreateRequest: %v", err)
	}
	if len(request.Mutations) != 1 {
		t.Fatalf("mutations = %d", len(request.Mutations))
	}
	previews := mapSliceFromAny(request.Mutations[0].Preview["operations"])
	if len(previews) != 1 {
		t.Fatalf("preview operations = %#v", request.Mutations[0].Preview["operations"])
	}
	before := mapFromAny(previews[0]["before"])
	if len(before) == 0 {
		t.Fatalf("preview has no before state: %#v", previews[0])
	}
	if previews[0]["etag_is_current"] != true {
		t.Fatalf("etag_is_current = %#v", previews[0]["etag_is_current"])
	}
}
