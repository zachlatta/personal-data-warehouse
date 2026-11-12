package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// An Android alert is skipped when another app on the phone already posted a
// notification for the same event; an iOS device still gets it. Run through
// tests/test_timeline_notifications.py for the managed Postgres.
func TestPostgresNotificationsPhoneWitness(t *testing.T) {
	url := os.Getenv("PDW_NOTIFICATION_TEST_URL")
	if url == "" {
		t.Skip("run through tests/test_timeline_notifications.py for managed Postgres")
	}
	ctx := context.Background()
	sender := &fakeSender{result: Result{Status: "accepted", TicketID: "witness-ticket"}, receipt: Result{Status: "provider_accepted"}}
	s, err := New(url, []byte("test-secret"), sender, func(map[string]any) Alert { return Alert{Title: "Synthetic"} })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var names map[string]string
	if err = json.Unmarshal([]byte(os.Getenv("PDW_NOTIFICATION_TEST_RELATIONS")), &names); err != nil {
		t.Fatal(err)
	}
	s.Expand = func(q string) string {
		for k, v := range names {
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
	clean := func() {
		exec("TRUNCATE @notification_deliveries, @notification_events, @push_devices, @android_notification_events")
	}
	clean()
	defer clean()
	exec("UPDATE @notification_state SET enabled=1 WHERE id='timeline'")
	// Two Android phones, an Android build too old to stamp its alerts (so no
	// phone is known for it), and an iPhone.
	const pixel, tablet, oldBuild, iphone = "ExponentPushToken[pixel]", "ExponentPushToken[tablet]", "ExponentPushToken[old-build]", "ExponentPushToken[iphone]"
	exec(`INSERT INTO @push_devices (expo_push_token, platform) VALUES ($1,'android'), ($2,'android'), ($3,'android'), ($4,'ios')`, pixel, tablet, oldBuild, iphone)

	// listen records a notification on a phone's listener, as uploaded.
	n := 0
	listen := func(device, pkg, at, title, text, bigText string, messages any, summary int, extras map[string]string) {
		t.Helper()
		n++
		raw, _ := json.Marshal(messages)
		rawJSON, _ := json.Marshal(map[string]any{"extras": extras})
		exec(`INSERT INTO @android_notification_events (account,device,event_id,event_type,event_at,posted_at,synced_at,package_name,title,text,big_text,messages,is_group_summary,raw_json)
 VALUES ('me',$8,$1,'posted',`+at+`,`+at+`,now(),$2,$3,$4,$5,$6::jsonb,$7,$9::jsonb)`, fmt.Sprintf("e%d", n), pkg, title, text, bigText, string(raw), summary, device, string(rawJSON))
	}
	// phone is the pixel's listener ("pichu").
	phone := func(pkg, at, title, text, bigText string, messages any, summary int) {
		t.Helper()
		listen("pichu", pkg, at, title, text, bigText, messages, summary, nil)
	}
	// Each phone's listener has recorded an earlier PDW alert stamped with a
	// delivery to that phone's push device: that is how its phone is known.
	raw, _ := json.Marshal(map[string]any{"source": "test", "source_pk": "{}", "actor": "", "title": "", "snippet": ""})
	exec(`INSERT INTO @notification_events(adapter,event_id,source,priority,event_ts,landed_at,payload) VALUES ('test','earlier','test','direct',now()-interval '1 day',now()-interval '1 day',$1)`, raw)
	if ok, e := s.fanout(ctx); e != nil || !ok {
		t.Fatalf("fanout: %v %v", ok, e)
	}
	exec(`UPDATE @notification_deliveries SET status='provider_accepted'`)
	for token, device := range map[string]string{pixel: "pichu", tablet: "tablet"} {
		var delivery string
		if err := s.DB.QueryRowContext(ctx, s.q(`SELECT id FROM @notification_deliveries WHERE endpoint->>'token'=$1`), token).Scan(&delivery); err != nil {
			t.Fatal(err)
		}
		listen(device, "com.zachlatta.pdw", "now() - interval '1 day'", "Earlier", "Earlier alert", "", nil, 0, map[string]string{"pdw.delivery_id": delivery})
	}
	// An alert stamped for a delivery to another device says nothing about
	// this one: the old build's alerts were never stamped.

	// verdicts fans the event out, makes the hold elapse, ticks, and returns
	// each device's verdict by token.
	verdicts := func(name, source, eventTS, actor, title, snippet string) map[string]string {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"source": source, "source_pk": "{}", "actor": actor, "title": title, "snippet": snippet})
		exec(`INSERT INTO @notification_events(adapter,event_id,source,priority,event_ts,landed_at,payload) VALUES ('test',$1,$2,'direct',`+eventTS+`,now(),$3)`, name, source, raw)
		if ok, e := s.fanout(ctx); e != nil || !ok {
			t.Fatalf("fanout: %v %v", ok, e)
		}
		exec(`UPDATE @notification_deliveries SET next_attempt_at=now() WHERE notification_id=(SELECT id FROM @notification_events WHERE event_id=$1)`, name)
		if err := s.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		rows, err := s.DB.QueryContext(ctx, s.q(`SELECT d.endpoint->>'token', d.status||':'||d.error FROM @notification_deliveries d JOIN @notification_events e ON e.id=d.notification_id WHERE e.event_id=$1`), name)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := map[string]string{}
		for rows.Next() {
			var token, verdict string
			if err := rows.Scan(&token, &verdict); err != nil {
				t.Fatal(err)
			}
			got[token] = verdict
		}
		return got
	}
	// outcome is the pixel's verdict and the iPhone's; every other device
	// must have been sent, since nothing was shown on its phone.
	outcome := func(name, source, eventTS, actor, title, snippet string) (android, ios string) {
		t.Helper()
		got := verdicts(name, source, eventTS, actor, title, snippet)
		if got[tablet] != "accepted:" || got[oldBuild] != "accepted:" {
			t.Fatalf("%s: another phone's notification decided this device: tablet %q, old build %q", name, got[tablet], got[oldBuild])
		}
		return got[pixel], got[iphone]
	}
	const skipped, sent = "suppressed:already_notified", "accepted:"
	expectFrom := func(name, source, eventTS, actor, title, snippet, wantAndroid string) {
		t.Helper()
		android, ios := outcome(name, source, eventTS, actor, title, snippet)
		if android != wantAndroid || ios != sent {
			t.Fatalf("%s: android %q (want %q), ios %q (want %q)", name, android, wantAndroid, ios, sent)
		}
	}
	expect := func(name, source, eventTS, title, snippet, wantAndroid string) {
		t.Helper()
		expectFrom(name, source, eventTS, "", title, snippet, wantAndroid)
	}

	// A chat app's MessagingStyle notification carries the message's own time
	// to the millisecond. The same millisecond alone is not the same message
	// (two apps' messages do collide): the sender or the text must agree too.
	msgAt := "timestamptz '2026-11-01 12:00:00.123456+00'"
	phone("com.example.chat", msgAt+" + interval '1 second'", "Someone", "", "", []map[string]any{{"text": "lunch?", "sender": "Someone", "time_ms": 1793534400123.0}}, 0)
	expect("chat-shown", "slack", msgAt, "", "lunch?", skipped)
	expectFrom("chat-same-sender", "slack", msgAt, "someone", "", "[attachment: image/jpeg]", skipped)
	expect("chat-other-ms", "slack", msgAt+" + interval '5 milliseconds'", "", "lunch?", sent)
	expectFrom("chat-unrelated-same-ms", "android_messages", msgAt, "Another Person", "", "running late, sorry", sent)
	expectFrom("chat-no-corroboration", "android_messages", msgAt, "", "", "", sent)

	// An email app shows the subject and the body's opening.
	mailAt := "timestamptz '2026-11-01 13:00:00+00'"
	phone("com.example.mail", mailAt+" + interval '20 seconds'", "sender@example.com", "Re: Budget", "Re: Budget\nLooks  good to\nme, ship it. Thanks!", nil, 0)
	expect("mail-shown", "gmail", mailAt, "Re: Budget", "Looks good to me, ship it. Thanks!", skipped)
	expect("mail-other-body", "gmail", mailAt, "Re: Budget", "Not this one", sent)
	expect("mail-other-subject", "gmail", mailAt, "Re: Something else", "Looks good to me, ship it", sent)
	// A subject alone, or a subject with a body too short to tell messages
	// apart, is not proof it is the same email.
	expect("mail-title-only", "gmail", mailAt, "Re: Budget", "", sent)
	expect("mail-short-body", "gmail", mailAt, "Re: Budget", "Looks", sent)

	// Only a notification posted near the event counts: a later one repeating
	// the subject and body (a resend, an automated mail) is another event,
	// even when this alert is retried or recovered after it appeared.
	lateAt := "timestamptz '2026-11-01 16:00:00+00'"
	phone("com.example.mail", lateAt+" + interval '30 minutes'", "Weekly digest", "Weekly digest", "Weekly digest\nHere is what happened this week", nil, 0)
	expect("later-repeat", "gmail", lateAt, "Weekly digest", "Here is what happened this week", sent)
	retryAt := "timestamptz '2026-11-01 17:00:00+00'"
	phone("com.example.mail", retryAt+" + interval '40 seconds'", "Invoice ready", "Invoice ready", "Invoice ready\nYour invoice for November is attached", nil, 0)
	phone("com.example.mail", retryAt+" + interval '2 hours'", "Report ready", "Report ready", "Report ready\nYour monthly report is attached here", nil, 0)
	retried := func(name, title, snippet string) string {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"source": "gmail", "source_pk": "{}", "actor": "", "title": title, "snippet": snippet})
		exec(`INSERT INTO @notification_events(adapter,event_id,source,priority,event_ts,landed_at,payload) VALUES ('test',$1,'gmail','direct',`+retryAt+`,`+retryAt+`,$2)`, name, raw)
		if ok, e := s.fanout(ctx); e != nil || !ok {
			t.Fatalf("fanout: %v %v", ok, e)
		}
		// The first attempt failed in transport and is due again now.
		exec(`UPDATE @notification_deliveries SET status='retry',attempt_count=1,next_attempt_at=now() WHERE notification_id=(SELECT id FROM @notification_events WHERE event_id=$1)`, name)
		if err := s.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		var verdict string
		if err := s.DB.QueryRowContext(ctx, s.q(`SELECT d.status||':'||d.error FROM @notification_deliveries d JOIN @notification_events e ON e.id=d.notification_id WHERE e.event_id=$1 AND d.endpoint->>'token'=$2`), name, pixel).Scan(&verdict); err != nil {
			t.Fatal(err)
		}
		return verdict
	}
	if v := retried("retry-witnessed", "Invoice ready", "Your invoice for November is attached"); v != skipped {
		t.Fatalf("a retry whose event the phone showed is skipped: %q", v)
	}
	if v := retried("retry-later-repeat", "Report ready", "Your monthly report is attached here"); v != sent {
		t.Fatalf("a retry is not skipped by a notification posted hours after its event: %q", v)
	}

	// PDW's own alerts and group summaries are not another app's notification.
	ownAt := "timestamptz '2026-11-01 14:00:00+00'"
	phone("com.zachlatta.pdw", ownAt+" + interval '3 minutes'", "Subject A", "Subject A", "Subject A\nThe first body text", nil, 0)
	phone("com.example.mail", ownAt+" + interval '3 minutes'", "Subject B", "Subject B", "Subject B\nThe second body text", nil, 1)
	expect("own-alert", "gmail", ownAt, "Subject A", "The first body text", sent)
	expect("summary", "gmail", ownAt, "Subject B", "The second body text", sent)

	// A notification from before the event is about something else.
	oldAt := "timestamptz '2026-11-01 15:00:00+00'"
	phone("com.example.mail", oldAt+" - interval '10 minutes'", "Subject C", "Subject C", "Subject C\nThe third body text", nil, 0)
	expect("before-event", "gmail", oldAt, "Subject C", "The third body text", sent)

	// Only the recipient's own phone counts: a notification on the tablet
	// skips the tablet's alert, and the pixel, which showed nothing, gets it.
	tabletAt := "timestamptz '2026-11-01 18:00:00+00'"
	listen("tablet", "com.example.mail", tabletAt+" + interval '20 seconds'", "Lease renewal", "Lease renewal", "Lease renewal\nPlease sign the attached renewal", nil, 0, nil)
	got := verdicts("other-phone", "gmail", tabletAt, "", "Lease renewal", "Please sign the attached renewal")
	if got[tablet] != skipped || got[pixel] != sent || got[oldBuild] != sent || got[iphone] != sent {
		t.Fatalf("a notification on one phone decided another's alert: %v", got)
	}

	// Nothing on the phone: Android still gets the alert, after its hold.
	expect("unwitnessed", "whatsapp", "now() - interval '1 hour'", "", "anyone there?", sent)

	// The hold: a fresh Android delivery waits for the phone's own app to
	// post and upload; a calendar event dated in the future holds from when
	// it landed, not until the meeting; iOS goes at once.
	// hold fans an event out and returns how long each device's delivery
	// waits, by token.
	hold := func(name, eventTS string) map[string]time.Duration {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"source": "gmail", "source_pk": "{}", "actor": "", "title": "", "snippet": ""})
		exec(`INSERT INTO @notification_events(adapter,event_id,source,priority,event_ts,landed_at,payload) VALUES ('test',$1,'gmail','direct',`+eventTS+`,now(),$2)`, name, raw)
		if ok, e := s.fanout(ctx); e != nil || !ok {
			t.Fatalf("fanout: %v %v", ok, e)
		}
		rows, err := s.DB.QueryContext(ctx, s.q(`SELECT d.endpoint->>'token', extract(epoch FROM d.next_attempt_at-now()) FROM @notification_deliveries d JOIN @notification_events e ON e.id=d.notification_id WHERE e.event_id=$1`), name)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		waits := map[string]time.Duration{}
		for rows.Next() {
			var token string
			var seconds float64
			if err := rows.Scan(&token, &seconds); err != nil {
				t.Fatal(err)
			}
			waits[token] = time.Duration(seconds * float64(time.Second))
		}
		return waits
	}
	near := func(got, want time.Duration) bool { return got > want-5*time.Second && got < want+5*time.Second }
	// Only a device whose phone is known waits: there is nothing to wait
	// for on an iPhone, or on an Android build whose phone PDW has not seen.
	expectHold := func(name, eventTS string, want time.Duration) {
		t.Helper()
		w := hold(name, eventTS)
		if !near(w[pixel], want) || !near(w[tablet], want) || !near(w[oldBuild], 0) || !near(w[iphone], 0) {
			t.Fatalf("%s: waits %v, want %s on the known phones and none elsewhere", name, w, want)
		}
	}
	expectHold("hold-fresh", "now() - interval '30 seconds'", PhoneWitnessHold-30*time.Second)
	// A calendar invite carries the meeting's start: it holds from when it landed.
	expectHold("hold-future", "now() + interval '3 days'", PhoneWitnessHold)
	expectHold("hold-old", "now() - interval '1 hour'", 0)
	expectHold("hold-unknown-time", "'epoch'", PhoneWitnessHold)
	// Held deliveries are not sent early.
	before := len(sender.sent)
	if err := s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	for _, d := range sender.sent[before:] {
		var name, token string
		_ = s.DB.QueryRowContext(ctx, s.q(`SELECT e.event_id, d.endpoint->>'token' FROM @notification_deliveries d JOIN @notification_events e ON e.id=d.notification_id WHERE d.id=$1`), d.ID).Scan(&name, &token)
		if (token == pixel || token == tablet) && name != "hold-old" {
			t.Fatalf("held delivery %s to %s was sent early", name, token)
		}
	}

	// A slow phone lookup must not take the worker pass with it: each lookup
	// has its own short deadline, after which the alert is sent unchecked
	// (and not held at fan-out), the delivery is settled, and the pass ends
	// normally well inside its own deadline.
	{
		original := s.Expand
		s.WitnessTimeout = 300 * time.Millisecond
		s.Expand = func(q string) string {
			return original(strings.ReplaceAll(q, "@android_notification_events", "(SELECT e0.* FROM @android_notification_events e0, pg_sleep(30))"))
		}
		raw, _ := json.Marshal(map[string]any{"source": "gmail", "source_pk": "{}", "actor": "", "title": "Subject E", "snippet": "The fifth body text"})
		exec(`INSERT INTO @notification_events(adapter,event_id,source,priority,event_ts,landed_at,payload) VALUES ('test','witness-timeout','gmail','direct',now(),now(),$1)`, raw)
		pass, cancel := context.WithTimeout(ctx, 20*time.Second)
		started := time.Now()
		err := s.Tick(pass)
		cancel()
		s.Expand, s.WitnessTimeout = original, 0
		if err != nil || time.Since(started) > 15*time.Second {
			t.Fatalf("a slow phone lookup held up the worker pass: %v after %s", err, time.Since(started))
		}
		var unchecked, other int
		if err := s.DB.QueryRowContext(ctx, s.q(`SELECT count(*) FILTER (WHERE d.status='accepted' AND d.error='phone_witness_unchecked'), count(*) FILTER (WHERE NOT (d.status='accepted' AND d.error IN ('phone_witness_unchecked',''))) FROM @notification_deliveries d JOIN @notification_events e ON e.id=d.notification_id WHERE e.event_id='witness-timeout'`)).Scan(&unchecked, &other); err != nil || unchecked != 3 || other != 0 {
			t.Fatalf("a timed-out phone lookup must send and settle unchecked: %d unchecked, %d other, %v", unchecked, other, err)
		}
	}

	// The phone's evidence failing to load is not a reason to hold the alert:
	// the witness check fails open (sends, and logs), unlike the read/reply
	// checks, which stop an unchecked send.
	original := s.Expand
	defer func() { s.Expand = original }()
	s.Expand = func(q string) string {
		return original(strings.ReplaceAll(q, "@android_notification_events", "missing_phone_witness_source"))
	}
	// The Android deliveries say they went out unchecked; iOS was never checked.
	if got := verdicts("witness-query-error", "gmail", "now() - interval '1 hour'", "", "Subject D", "The fourth body text"); got[pixel] != "accepted:phone_witness_unchecked" ||
		got[tablet] != "accepted:phone_witness_unchecked" || got[iphone] != sent {
		t.Fatalf("a failed witness check must send, and say so: %v", got)
	}
	// Expo's receipt confirming the push does not erase that it went unchecked.
	exec(`UPDATE @notification_deliveries SET next_attempt_at=now() WHERE status='accepted' AND notification_id=(SELECT id FROM @notification_events WHERE event_id='witness-query-error')`)
	if err := s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var marked int
	if err := s.DB.QueryRowContext(ctx, s.q(`SELECT count(*) FROM @notification_deliveries d JOIN @notification_events e ON e.id=d.notification_id WHERE e.event_id='witness-query-error' AND d.status='provider_accepted' AND d.error='phone_witness_unchecked'`)).Scan(&marked); err != nil || marked != 3 {
		t.Fatalf("receipt erased the unchecked marker: %d %v", marked, err)
	}
	s.Expand = original

	w := httptest.NewRecorder()
	s.status(w, httptest.NewRequest("GET", "/api/notifications", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"suppressed_notified":1`) {
		t.Fatalf("already-notified skips missing from the status API: %d %s", w.Code, w.Body.String())
	}
}
