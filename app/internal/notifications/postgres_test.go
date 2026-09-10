package notifications

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

type fakeSender struct {
	sent    []Delivery
	result  Result
	receipt Result
}

func (f *fakeSender) Send(_ context.Context, d Delivery) Result {
	f.sent = append(f.sent, d)
	return f.result
}
func (f *fakeSender) Receipt(_ context.Context, d Delivery) Result { return f.receipt }

// This is invoked by the canonical pytest run with a fresh isolated schema.
func TestPostgresNotifications(t *testing.T) {
	url := os.Getenv("PDW_NOTIFICATION_TEST_URL")
	if url == "" {
		t.Skip("run through tests/test_timeline_notifications.py for managed Postgres")
	}
	ctx := context.Background()
	sender := &fakeSender{result: Result{Status: "accepted", TicketID: "test-ticket"}, receipt: Result{Status: "provider_accepted"}}
	s, err := New(url, []byte("test-secret"), sender, func(row map[string]any) Alert { return Alert{Title: "Hello", Route: "/timeline/test/one"} })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var relations map[string]string
	if err = json.Unmarshal([]byte(os.Getenv("PDW_NOTIFICATION_TEST_RELATIONS")), &relations); err != nil {
		t.Fatal(err)
	}
	s.Expand = func(q string) string {
		for k, v := range relations {
			q = strings.ReplaceAll(q, "@"+k, v)
		}
		return q
	}
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := s.DB.ExecContext(ctx, s.q(q), args...); err != nil {
			t.Fatal(err)
		}
	}
	count := func(q string) int {
		t.Helper()
		var n int
		if err := s.DB.QueryRowContext(ctx, s.q(q)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	exec(`UPDATE @notification_state SET enabled=1 WHERE id='timeline'`)
	exec(`INSERT INTO @push_devices (expo_push_token) VALUES ('ExponentPushToken[one]'),('ExponentPushToken[two]')`)
	exec(`INSERT INTO @timeline_events (adapter,event_id,source,priority) VALUES ('test','one','test','direct')`)
	if err = s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 2 || count(`SELECT count(*) FROM @notification_deliveries WHERE status='accepted'`) != 2 {
		t.Fatal("fanout did not persist per device")
	}
	if err = s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 2 {
		t.Fatal("duplicate delivery")
	}
	d := sender.sent[0]
	if d.Alert.DeliveryID != d.ID || !s.validProof(d.ID, d.Alert.OpenProof) {
		t.Fatal("missing open proof")
	}
	mux := http.NewServeMux()
	s.Register(mux, func(h http.Handler) http.Handler { return h })
	req := func(path, body string) int {
		t.Helper()
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
		return w.Code
	}
	proof, _ := json.Marshal(map[string]string{"delivery_id": d.ID, "open_proof": d.Alert.OpenProof})
	if req(APIPath+"/opened", string(proof)) != 200 || req(APIPath+"/opened", string(proof)) != 200 {
		t.Fatal("open failed")
	}
	if count(`SELECT count(*) FROM @notification_deliveries WHERE opened_at>'epoch'`) != 1 {
		t.Fatal("open is not idempotent")
	}
	if req(APIPath+"/opened", `{"delivery_id":"other","open_proof":"bad"}`) != 403 {
		t.Fatal("unscoped open write")
	}
	exec(`UPDATE @notification_deliveries SET next_attempt_at=now()-interval '1 second'`)
	if err = s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if count(`SELECT count(*) FROM @notification_deliveries WHERE status='provider_accepted' AND receipt_at>'epoch'`) != 2 {
		t.Fatal("receipt missing")
	}
	// A failed transport is retried durably, with a stable id and bounded attempts.
	sender.result = Result{Status: "retry", Error: "temporary failure"}
	exec(`INSERT INTO @timeline_events (adapter,event_id,source,priority) VALUES ('test','retry','test','cc')`)
	if err = s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if count(`SELECT count(*) FROM @notification_deliveries WHERE status='retry' AND attempt_count=1`) != 2 {
		t.Fatal("no retry ledger")
	}
	exec(`UPDATE @notification_deliveries SET attempt_count=5,next_attempt_at=now()-interval '1 second' WHERE status='retry'`)
	if err = s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if count(`SELECT count(*) FROM @notification_deliveries WHERE status='failed' AND attempt_count=6`) != 2 {
		t.Fatal("retry not bounded")
	}
	// Multiple workers must not claim the same outstanding lease.
	exec(`INSERT INTO @timeline_events (adapter,event_id,source,priority) VALUES ('test','lease','test','direct')`)
	if _, err = s.fanout(ctx); err != nil {
		t.Fatal(err)
	}
	a, err := s.claim(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.claim(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID == b.ID {
		t.Fatal("same live lease claimed twice")
	}
	exec(`UPDATE @notification_deliveries SET lease_until=now()-interval '1 second' WHERE id=$1`, a.ID)
	reclaimed, err := s.claim(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.ID != a.ID || reclaimed.LeaseID == a.LeaseID {
		t.Fatal("expired lease not recovered")
	}
	if s.finish(ctx, a, Result{Status: "accepted"}, false) == nil {
		t.Fatal("stale worker overwrote newer claim")
	}
	// Pause cancels unclaimed work, but preserves evidence and existing receipts.
	exec(`INSERT INTO @timeline_events (adapter,event_id,source,priority) VALUES ('test','paused','test','direct')`)
	if req(APIPath+"/settings", `{"enabled":false}`) != 200 {
		t.Fatal("pause failed")
	}
	if count(`SELECT count(*) FROM @notification_events WHERE event_id='paused' AND status='cancelled'`) != 1 {
		t.Fatal("paused backlog will replay")
	}
	before := len(sender.sent)
	if err = s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != before {
		t.Fatal("paused worker sent")
	}
	// An in-flight rejection after pause must not resurrect a retry on resume.
	if err = s.finish(ctx, reclaimed, Result{Status: "retry", Error: "rate limited"}, true); err != nil {
		t.Fatal(err)
	}
	if count(`SELECT count(*) FROM @notification_deliveries WHERE status='retry'`) != 0 {
		t.Fatal("late receipt resurrected paused work")
	}
	// Retired endpoints must not keep receiving deliveries already queued.
	if req(APIPath+"/settings", `{"enabled":true}`) != 200 {
		t.Fatal("resume failed")
	}
	sender.result = Result{Status: "failed", Error: "DeviceNotRegistered", Disable: true}
	exec(`INSERT INTO @timeline_events (adapter,event_id,source,priority) VALUES ('test','retire-one','test','direct'),('test','retire-two','test','cc')`)
	before = len(sender.sent)
	if err = s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent)-before != 2 {
		t.Fatal("disabled device received additional queued alerts")
	}
	if count(`SELECT count(*) FROM @push_devices WHERE status='active'`) != 0 {
		t.Fatal("dead endpoints not retired")
	}
	exec(`INSERT INTO @timeline_events (adapter,event_id,source,priority) VALUES ('test','no-device','test','cc')`)
	if err = s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if count(`SELECT count(*) FROM @notification_events WHERE event_id='no-device' AND status='no_devices'`) != 1 {
		t.Fatal("missing devices silently dropped")
	}
	if count(`SELECT count(*) FROM @marts_notification_health WHERE status='attention'`) != 1 {
		t.Fatal("failure not visible in health")
	}

	// Status API must execute against the production view and bounded history.
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, APIPath, nil))
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
}
