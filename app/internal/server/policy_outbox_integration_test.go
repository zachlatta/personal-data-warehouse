package server

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/app/internal/notifications"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

type scriptedSender struct {
	results []notifications.Result
	sent    []notifications.Delivery
}

func (s *scriptedSender) Send(_ context.Context, d notifications.Delivery) notifications.Result {
	s.sent = append(s.sent, d)
	result := s.results[0]
	if len(s.results) > 1 {
		s.results = s.results[1:]
	}
	return result
}

func (s *scriptedSender) Receipt(context.Context, notifications.Delivery) notifications.Result {
	return notifications.Result{Status: "provider_accepted"}
}

// A policy outcome survives the process that queued it and a failing push
// transport: it is a row in the notification outbox, and the outbox worker
// (a fresh one, as after a restart) delivers it, retries a transport failure,
// and re-sends a delivery whose worker died mid-send.
// Run through tests/test_mutation_policies.py, which provisions the tables.
func TestPolicyOutcomeAlertsSurviveRestartAndTransportFailure(t *testing.T) {
	url := os.Getenv("PDW_POLICY_OUTBOX_TEST_URL")
	if url == "" {
		t.Skip("run through tests/test_mutation_policies.py")
	}
	ctx := context.Background()
	store, err := mutations.NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	sender := &scriptedSender{results: []notifications.Result{{Status: "retry", Error: "push service unavailable"}, {Status: "accepted", TicketID: "t1"}}}
	render := func(row map[string]any) notifications.Alert {
		return renderTimelineNotification(row, "", timelineLinkEnv{})
	}
	worker, err := notifications.New(url, []byte("test-secret"), sender, render)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := worker.DB.ExecContext(ctx, warehouse.ExpandRelations(q), args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'`)
	exec(`INSERT INTO @push_devices (expo_push_token, client_name, platform, status) VALUES ('ExponentPushToken[outbox-test]', 'pixel', 'ios', 'active') ON CONFLICT DO NOTHING`)
	exec(`INSERT INTO @upstream_mutation_requests (id, title, reason, status, approved_by, updated_at)
	      VALUES ('req-outbox', 'Set the thermostat: heat to 55°', 'You left home.', 'succeeded', 'policy:heating-winter@1', now())`)

	// The app queues the outcome and "dies": nothing was sent in passing.
	if _, err := store.ClaimPolicyOutcomes(ctx, 50); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 0 {
		t.Fatal("the outcome was sent before the outbox worker ran")
	}
	status := func() string {
		var s string
		if err := worker.DB.QueryRowContext(ctx, warehouse.ExpandRelations(`
			SELECT d.status FROM @notification_deliveries d JOIN @notification_events e ON e.id = d.notification_id
			WHERE e.payload->>'request_id' = 'req-outbox'`)).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	// A fresh outbox worker (after the restart) fans it out; the transport fails.
	if err := worker.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 1 || status() != "retry" {
		t.Fatalf("after a transport failure: %d sends, status %s", len(sender.sent), status())
	}
	if got := sender.sent[0].Alert; got.Category != "mutation_applied" || got.RequestID != "req-outbox" || !got.Quiet {
		t.Fatalf("alert %+v", got)
	}
	// The worker dies mid-send on the retry: its lease runs out and the next
	// worker sends again.
	exec(`UPDATE @notification_deliveries SET status = 'sending', lease_id = 'dead', lease_until = now() - interval '1 minute', next_attempt_at = now() - interval '1 minute'`)
	if err := worker.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 2 || status() != "accepted" {
		t.Fatalf("after recovery: %d sends, status %s", len(sender.sent), status())
	}
}
