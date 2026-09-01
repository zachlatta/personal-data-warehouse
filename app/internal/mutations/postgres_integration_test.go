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

// seedSlackPreviewSchema creates just enough of the Slack source layer for the
// preview queries to run: the columns they read, under the catalog's own
// schema names.
func seedSlackPreviewSchema(t *testing.T, store *PostgresStore) {
	t.Helper()
	ctx := context.Background()
	for _, logical := range []string{"slack_messages", "slack_conversations", "slack_account_identities", "slack_users", "slack_teams"} {
		if _, err := store.db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+warehouse.QuoteIdent(warehouse.SchemaOf(logical))); err != nil {
			t.Skipf("cannot create Slack schema in this database: %v", err)
		}
	}
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS @slack_conversations (
			account text, team_id text, conversation_id text, conversation_type text,
			name text, raw_json text, PRIMARY KEY (account, team_id, conversation_id)
		)`,
		`CREATE TABLE IF NOT EXISTS @slack_account_identities (
			account text, team_id text, user_id text, PRIMARY KEY (account, team_id)
		)`,
		`CREATE TABLE IF NOT EXISTS @slack_users (
			account text, team_id text, user_id text, display_name text, real_name text, name text,
			raw_json text,
			PRIMARY KEY (account, team_id, user_id)
		)`,
		`CREATE TABLE IF NOT EXISTS @slack_teams (
			account text, team_id text, team_name text, domain text,
			PRIMARY KEY (account, team_id)
		)`,
		`CREATE TABLE IF NOT EXISTS @slack_messages (
			account text, team_id text, conversation_id text, message_ts text,
			message_datetime timestamptz, thread_ts text, parent_message_ts text,
			user_id text, bot_id text, username text, text text,
			is_thread_parent bigint, is_thread_reply bigint, reply_count bigint, is_deleted bigint,
			PRIMARY KEY (account, team_id, conversation_id, message_ts)
		)`,
	} {
		if _, err := execContext(ctx, store.db, statement); err != nil {
			t.Skipf("cannot create Slack source table in this database: %v", err)
		}
	}
}

func TestCreateRequestFillsSlackMarkReadPreviewWithConversationContext(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	seedSlackPreviewSchema(t, store)

	account := fmt.Sprintf("slack-preview-%d", time.Now().UnixNano())
	for _, statement := range []string{
		`INSERT INTO @slack_account_identities (account, team_id, user_id) VALUES ($1, 'T1', 'U-ME')`,
		`INSERT INTO @slack_users (account, team_id, user_id, display_name, real_name, name, raw_json)
		 VALUES ($1, 'T1', 'U-ME', 'Zach', 'Zach Lata', 'zach', '{}'),
		        ($1, 'T1', 'U-MARCUS', 'Marcus', 'Marcus', 'marcus', '{"profile":{"image_192":"https://avatars.example.test/marcus_192.png"}}')`,
		`INSERT INTO @slack_teams (account, team_id, team_name, domain) VALUES ($1, 'T1', 'Example', 'example')`,
		`INSERT INTO @slack_conversations (account, team_id, conversation_id, conversation_type, name, raw_json)
		 VALUES ($1, 'T1', 'D1', 'im', '', '{"user":"U-MARCUS","last_read":"1593473500.000100","unread_count_display":2}')`,
		`INSERT INTO @slack_messages (
			account, team_id, conversation_id, message_ts, message_datetime, thread_ts,
			parent_message_ts, user_id, bot_id, username, text,
			is_thread_parent, is_thread_reply, reply_count, is_deleted
		 ) VALUES
			($1, 'T1', 'D1', '1593473500.000100', '2026-08-29 14:00:00+00', '1593473500.000100', '', 'U-ME', '', '', 'Did you see this?', 0, 0, 0, 0),
			($1, 'T1', 'D1', '1593473566.000200', '2026-08-29 14:01:00+00', '1593473566.000200', '', 'U-MARCUS', '', '', 'Yep — all handled.', 0, 0, 0, 0),
			($1, 'T1', 'D1', '1593473600.000300', '2026-08-29 14:02:00+00', '1593473600.000300', '', 'U-MARCUS', '', '', 'One more thing.', 0, 0, 0, 0)`,
	} {
		if _, err := execContext(ctx, store.db, statement, account); err != nil {
			t.Fatalf("seed Slack context: %v", err)
		}
	}

	request, err := store.CreateRequest(ctx, CreateRequestInput{
		Title: "Mark Marcus read", Reason: "integration test", RequestedBy: "test",
		Mutations: []MutationInput{{
			Type: SlackMarkConversationReadOperation, Account: account,
			ConversationID: "D1", MessageTS: "1593473566.000200",
		}},
	})
	if err != nil {
		t.Fatalf("CreateRequest: %v", err)
	}
	preview := mapFromAny(request.Mutations[0].Preview["slack_read"])
	if preview["conversation_name"] != "Marcus" || preview["current_unread_count"] != float64(2) {
		t.Fatalf("Slack conversation preview = %#v", preview)
	}
	messages := mapSliceFromAny(preview["messages"])
	if len(messages) != 3 || messages[1]["is_target"] != true || messages[1]["actor_name"] != "Marcus" {
		t.Fatalf("Slack context messages = %#v", messages)
	}
	// Every message carries the face and the link that let a reviewer answer
	// it in Slack instead of approving it unread.
	if messages[1]["avatar_url"] != "https://avatars.example.test/marcus_192.png" {
		t.Fatalf("target avatar = %#v", messages[1]["avatar_url"])
	}
	open := mapFromAny(messages[1]["open"])
	if open["url"] != "https://example.slack.com/archives/D1/p1593473566000200" {
		t.Fatalf("target permalink = %#v", open)
	}
	if mapFromAny(preview["open"])["url"] != open["url"] {
		t.Fatalf("conversation link = %#v", preview["open"])
	}
	if preview["avatar_url"] != "https://avatars.example.test/marcus_192.png" {
		t.Fatalf("conversation avatar = %#v", preview["avatar_url"])
	}
}

// The threaded branch is a different query with its own column list, and a
// reply's permalink is the one that has to name its thread — without it Slack
// opens the channel at the parent and the reply is nowhere on screen.
func TestSlackMarkReadPreviewLinksAReplyToItsThread(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	seedSlackPreviewSchema(t, store)

	account := fmt.Sprintf("slack-thread-%d", time.Now().UnixNano())
	for _, statement := range []string{
		`INSERT INTO @slack_account_identities (account, team_id, user_id) VALUES ($1, 'T1', 'U-ME')`,
		`INSERT INTO @slack_users (account, team_id, user_id, display_name, real_name, name, raw_json)
		 VALUES ($1, 'T1', 'U-MARCUS', 'Marcus', 'Marcus', 'marcus', '{"profile":{"image_72":"https://avatars.example.test/marcus_72.png"}}')`,
		`INSERT INTO @slack_teams (account, team_id, team_name, domain) VALUES ($1, 'T1', 'Example', 'example')`,
		`INSERT INTO @slack_conversations (account, team_id, conversation_id, conversation_type, name, raw_json)
		 VALUES ($1, 'T1', 'C1', 'public_channel', 'general', '{"last_read":"1593473500.000100","unread_count_display":1}')`,
		`INSERT INTO @slack_messages (
			account, team_id, conversation_id, message_ts, message_datetime, thread_ts,
			parent_message_ts, user_id, bot_id, username, text,
			is_thread_parent, is_thread_reply, reply_count, is_deleted
		 ) VALUES
			($1, 'T1', 'C1', '1593473500.000100', '2026-08-29 14:00:00+00', '1593473500.000100', '', 'U-ME', '', '', 'Kicking this off.', 1, 0, 1, 0),
			($1, 'T1', 'C1', '1593473566.000200', '2026-08-29 14:01:00+00', '1593473500.000100', '1593473500.000100', 'U-MARCUS', '', '', 'On it.', 0, 1, 0, 0)`,
	} {
		if _, err := execContext(ctx, store.db, statement, account); err != nil {
			t.Fatalf("seed Slack thread: %v", err)
		}
	}

	request, err := store.CreateRequest(ctx, CreateRequestInput{
		Title: "Mark the reply read", Reason: "integration test", RequestedBy: "test",
		Mutations: []MutationInput{{
			Type: SlackMarkConversationReadOperation, Account: account,
			ConversationID: "C1", MessageTS: "1593473566.000200",
		}},
	})
	if err != nil {
		t.Fatalf("CreateRequest: %v", err)
	}
	preview := mapFromAny(request.Mutations[0].Preview["slack_read"])
	if preview["context_kind"] != "thread" {
		t.Fatalf("context_kind = %#v", preview["context_kind"])
	}
	messages := mapSliceFromAny(preview["messages"])
	if len(messages) != 2 {
		t.Fatalf("thread messages = %#v", messages)
	}
	reply := messages[1]
	if reply["is_target"] != true || reply["avatar_url"] != "https://avatars.example.test/marcus_72.png" {
		t.Fatalf("reply = %#v", reply)
	}
	want := "https://example.slack.com/archives/C1/p1593473566000200?thread_ts=1593473500.000100&cid=C1"
	if got := mapFromAny(reply["open"])["url"]; got != want {
		t.Fatalf("reply permalink = %#v, want %s", got, want)
	}
}
