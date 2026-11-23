package notifications

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// A source mute is a vacation switch for one source's alerts: while it holds,
// that source's events are recorded as `muted` and never sent, other sources
// keep paging, and the mute ends by itself at its expiry without anything
// being replayed.
func TestPostgresNotificationsSourceMute(t *testing.T) {
	url := os.Getenv("PDW_NOTIFICATION_TEST_URL")
	if url == "" {
		t.Skip("run through tests/test_timeline_notifications.py for managed Postgres")
	}
	ctx := context.Background()
	sender := &fakeSender{result: Result{Status: "accepted", TicketID: "t"}, receipt: Result{Status: "provider_accepted"}}
	s, err := newWithExpansion(url, []byte("test-secret"), sender, func(row map[string]any) Alert { return Alert{Title: "Hello", Route: "/timeline"} }, notificationTestExpansion(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := s.DB.ExecContext(ctx, s.q(q), args...); err != nil {
			t.Fatal(err)
		}
	}
	count := func(q string, args ...any) int {
		t.Helper()
		var n int
		if err := s.DB.QueryRowContext(ctx, s.q(q), args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	tick := func() {
		t.Helper()
		if err := s.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	sentFor := func(eventID string) int {
		t.Helper()
		return count(`SELECT count(*) FROM @notification_deliveries d JOIN @notification_events n ON n.id=d.notification_id WHERE n.event_id=$1 AND d.status='accepted'`, eventID)
	}
	statusOf := func(eventID string) string {
		t.Helper()
		var status string
		if err := s.DB.QueryRowContext(ctx, s.q(`SELECT status FROM @notification_events WHERE event_id=$1`), eventID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		return status
	}
	mux := http.NewServeMux()
	s.Register(mux, func(h http.Handler) http.Handler { return h })
	do := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		return w
	}
	exec(`UPDATE @notification_state SET enabled=1 WHERE id='timeline'`)
	exec(`INSERT INTO @push_devices (expo_push_token) VALUES ('ExponentPushToken[mute]') ON CONFLICT DO NOTHING`)
	exec(`UPDATE @push_devices SET status='active'`)

	// Only a source that has paged before can be muted: a typo would
	// otherwise mute nothing and look like it worked.
	until := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	if w := do(http.MethodPost, APIPath+"/mutes", `{"source":"mutechat","until":"`+until+`"}`); w.Code != 400 || !strings.Contains(w.Body.String(), "has never") {
		t.Fatalf("a never-seen source was muted: %d %s", w.Code, w.Body.String())
	}
	exec(`INSERT INTO @timeline_events (adapter,event_id,source,priority) VALUES ('test','mute-before','mutechat','direct'),('test','other-before','mutemail','direct')`)
	tick()
	if sentFor("mute-before") == 0 || sentFor("other-before") == 0 {
		t.Fatal("an unmuted source did not page")
	}

	// A mute needs a future end, bounded so a forgotten one cannot last.
	for _, body := range []string{
		`{"source":"mutechat"}`,
		`{"source":"mutechat","until":"` + time.Now().Add(-time.Minute).UTC().Format(time.RFC3339) + `"}`,
		`{"source":"mutechat","until":"` + time.Now().Add(MaxSourceMute+time.Hour).UTC().Format(time.RFC3339) + `"}`,
		`{"source":"mutechat","until":"next week"}`,
		`{"source":"pdw_mutations","until":"` + until + `"}`,
	} {
		if w := do(http.MethodPost, APIPath+"/mutes", body); w.Code != 400 {
			t.Fatalf("POST mutes %s = %d, want 400", body, w.Code)
		}
	}
	w := do(http.MethodPost, APIPath+"/mutes", `{"source":"mutechat","until":"`+until+`","reason":"vacation"}`)
	if w.Code != 200 {
		t.Fatalf("mute: %d %s", w.Code, w.Body.String())
	}
	var set SourceMute
	if err = json.Unmarshal(w.Body.Bytes(), &set); err != nil || set.Source != "mutechat" || !set.Active || set.Reason != "vacation" || set.MutedUntil.Format(time.RFC3339) != until {
		t.Fatalf("mute response: %s", w.Body.String())
	}

	// While muted: the muted source is recorded, not sent; the other pages.
	exec(`INSERT INTO @timeline_events (adapter,event_id,source,priority) VALUES ('test','mute-during','mutechat','direct'),('test','other-during','mutemail','direct')`)
	tick()
	if got := statusOf("mute-during"); got != "muted" {
		t.Fatalf("muted event status = %q, want muted", got)
	}
	if n := count(`SELECT count(*) FROM @notification_deliveries d JOIN @notification_events n ON n.id=d.notification_id WHERE n.event_id='mute-during'`); n != 0 {
		t.Fatalf("a muted event got %d deliveries", n)
	}
	if sentFor("other-during") == 0 {
		t.Fatal("muting one source silenced another")
	}

	// Work queued before the mute (an Android hold, a retry) is skipped too.
	exec(`UPDATE @notification_mutes SET muted_until=now()-interval '1 second' WHERE source='mutechat'`)
	exec(`INSERT INTO @timeline_events (adapter,event_id,source,priority) VALUES ('test','mute-queued','mutechat','direct')`)
	if ok, err := s.fanout(ctx); err != nil || !ok {
		t.Fatalf("fanout: %v %v", ok, err)
	}
	exec(`UPDATE @notification_mutes SET muted_until=$1 WHERE source='mutechat'`, until)
	tick()
	if sentFor("mute-queued") != 0 || count(`SELECT count(*) FROM @notification_deliveries d JOIN @notification_events n ON n.id=d.notification_id WHERE n.event_id='mute-queued' AND d.status='suppressed' AND d.error='source_muted'`) == 0 {
		t.Fatal("a delivery queued before the mute was sent")
	}
	if got := statusOf("mute-queued"); got != "muted" {
		t.Fatalf("an event whose queued deliveries were muted reads %q, want muted", got)
	}

	// The list says what is muted and until when.
	w = do(http.MethodGet, APIPath+"/mutes", "")
	var listed struct {
		Mutes []SourceMute `json:"mutes"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &listed) != nil || len(listed.Mutes) != 1 || !listed.Mutes[0].Active || listed.Mutes[0].Source != "mutechat" {
		t.Fatalf("list: %d %s", w.Code, w.Body.String())
	}
	// The ledger tells a muted event from a failure.
	if w = do(http.MethodGet, APIPath, ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"muted"`) {
		t.Fatalf("ledger does not show the muted event: %s", w.Body.String())
	}

	// At its expiry the mute ends by itself, and what it dropped stays dropped.
	exec(`UPDATE @notification_mutes SET muted_until=now()-interval '1 second' WHERE source='mutechat'`)
	exec(`INSERT INTO @timeline_events (adapter,event_id,source,priority) VALUES ('test','mute-after','mutechat','direct')`)
	tick()
	if sentFor("mute-after") == 0 {
		t.Fatal("an expired mute still silences its source")
	}
	if statusOf("mute-during") != "muted" || sentFor("mute-during") != 0 {
		t.Fatal("an event dropped by the mute was replayed after it ended")
	}
	if w = do(http.MethodGet, APIPath+"/mutes", ""); json.Unmarshal(w.Body.Bytes(), &listed) != nil || len(listed.Mutes) != 1 || listed.Mutes[0].Active {
		t.Fatalf("an expired mute is listed as active: %s", w.Body.String())
	}

	// Unmuting ends a mute now; ending one that is not active is a 404.
	if w = do(http.MethodPost, APIPath+"/mutes", `{"source":"mutechat","until":"`+until+`"}`); w.Code != 200 {
		t.Fatalf("re-mute: %d", w.Code)
	}
	if w = do(http.MethodDelete, APIPath+"/mutes/mutechat", ""); w.Code != 200 {
		t.Fatalf("unmute: %d %s", w.Code, w.Body.String())
	}
	if count(`SELECT count(*) FROM @notification_mutes WHERE source='mutechat' AND muted_until>now()`) != 0 {
		t.Fatal("unmute left the mute active")
	}
	if w = do(http.MethodDelete, APIPath+"/mutes/mutechat", ""); w.Code != 404 {
		t.Fatalf("unmuting an inactive mute = %d, want 404", w.Code)
	}
	exec(`INSERT INTO @timeline_events (adapter,event_id,source,priority) VALUES ('test','mute-unmuted','mutechat','direct')`)
	tick()
	if sentFor("mute-unmuted") == 0 {
		t.Fatal("an unmuted source does not page")
	}
}

// Mutation alerts are not timeline notifications: no source mute holds them.
func TestPostgresNotificationsSourceMuteLeavesMutationAlerts(t *testing.T) {
	url := os.Getenv("PDW_NOTIFICATION_TEST_URL")
	if url == "" {
		t.Skip("run through tests/test_timeline_notifications.py for managed Postgres")
	}
	ctx := context.Background()
	sender := &fakeSender{result: Result{Status: "accepted", TicketID: "t"}, receipt: Result{Status: "provider_accepted"}}
	s, err := newWithExpansion(url, []byte("test-secret"), sender, func(row map[string]any) Alert { return Alert{Title: "Applied"} }, notificationTestExpansion(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, q := range []string{
		`INSERT INTO @push_devices (expo_push_token) VALUES ('ExponentPushToken[mute-mutation]') ON CONFLICT DO NOTHING`,
		`INSERT INTO @notification_mutes (source, muted_until) VALUES ('pdw_mutations', now()+interval '1 hour') ON CONFLICT (source) DO UPDATE SET muted_until=EXCLUDED.muted_until`,
		`INSERT INTO @notification_events (adapter, event_id, source, priority, event_ts, landed_at, payload)
		 VALUES ('mutation_policy', 'req-mute:applied:0:0', 'pdw_mutations', 'self', now(), now(), '{"adapter":"mutation_policy"}')`,
	} {
		if _, err = s.DB.ExecContext(ctx, s.q(q)); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var status string
	if err = s.DB.QueryRowContext(ctx, s.q(`SELECT status FROM @notification_events WHERE event_id='req-mute:applied:0:0'`)).Scan(&status); err != nil || status != "fanned_out" {
		t.Fatalf("a mutation alert was held by a source mute: status %q, %v", status, err)
	}
	if _, err = s.DB.ExecContext(ctx, s.q(`DELETE FROM @notification_mutes WHERE source='pdw_mutations'`)); err != nil {
		t.Fatal(err)
	}
}
