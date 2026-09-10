package notifications

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestPostgresNotificationsSuppression(t *testing.T) {
	url := os.Getenv("PDW_NOTIFICATION_TEST_URL")
	if url == "" {
		t.Skip("run through tests/test_timeline_notifications.py for managed Postgres")
	}
	ctx := context.Background()
	sender := &fakeSender{result: Result{Status: "accepted", TicketID: "suppression-ticket"}}
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
	exec := func(q string) {
		t.Helper()
		if _, err := s.DB.ExecContext(ctx, s.q(q)); err != nil {
			t.Fatal(err)
		}
	}
	exec("TRUNCATE @notification_deliveries, @notification_events, @timeline_events, @push_devices")
	exec("UPDATE @notification_state SET enabled=1 WHERE id='timeline'")
	exec("INSERT INTO @push_devices (expo_push_token) VALUES ('ExponentPushToken[suppression]')")
	exec(`INSERT INTO @gmail_messages(account,message_id,thread_id,internal_date,label_ids) VALUES
 ('a','mail','thread',now()-interval '1 hour',ARRAY['UNREAD'])`)
	exec(`INSERT INTO @slack_account_identities(account,team_id,user_id) VALUES ('a','t','me')`)
	exec(`INSERT INTO @slack_conversations(account,team_id,conversation_id,is_im,raw_json) VALUES
 ('a','t','dm',1,'{"last_read":"100.100000"}'),('a','t','channel',0,'{"last_read":"999.000000"}')`)
	exec(`INSERT INTO @slack_messages(account,team_id,conversation_id,message_ts,message_datetime,thread_ts,user_id,raw_json) VALUES
 ('a','t','dm','100.100000',now()-interval '1 hour','','other','{}'),
 ('a','t','channel','100.000000',now()-interval '1 hour','','other','{"last_read":"101.000000"}'),
 ('a','t','channel','101.000000',now()-interval '30 minutes','100.000000','other','{}'),
 ('a','t','channel','102.000000',now()-interval '20 minutes','100.000000','other','{}')`)
	exec(`INSERT INTO @apple_messages(account,message_id,message_at,is_read) VALUES ('a','im',now()-interval '1 hour',1)`)
	exec(`INSERT INTO @apple_message_chats(account,chat_id,style) VALUES ('a','chat',43)`)
	exec(`INSERT INTO @apple_message_chat_messages(account,chat_id,message_id,message_date) VALUES ('a','chat','im',now()-interval '1 hour')`)
	exec(`INSERT INTO @whatsapp_messages(account,chat_id,message_id,message_at) VALUES ('a','group@g.us','wa',now()-interval '1 hour')`)

	// Check source state AFTER fanout, including a retry; no cached capture verdict.
	check := func(name, source, pk, beforeSend, want string) {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"source": source, "source_pk": pk, "actor": "", "title": "", "snippet": ""})
		_, err = s.DB.ExecContext(ctx, s.q(`INSERT INTO @notification_events(adapter,event_id,source,priority,event_ts,landed_at,payload) VALUES ('test',$1,$2,'direct',now(),now(),$3)`), name, source, raw)
		if err != nil {
			t.Fatal(err)
		}
		if ok, e := s.fanout(ctx); e != nil || !ok {
			t.Fatalf("fanout: %v %v", ok, e)
		}
		if beforeSend != "" {
			exec(beforeSend)
		}
		sent := len(sender.sent)
		if err = s.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		var status, reason string
		err = s.DB.QueryRowContext(ctx, s.q(`SELECT d.status,d.error FROM @notification_deliveries d JOIN @notification_events n ON n.id=d.notification_id WHERE n.event_id=$1`), name).Scan(&status, &reason)
		if err != nil {
			t.Fatal(err)
		}
		if want == "" {
			if status != "accepted" || len(sender.sent) != sent+1 {
				t.Fatalf("%s: expected send, got %s %s", name, status, reason)
			}
		} else {
			if status != "suppressed" || reason != want || len(sender.sent) != sent {
				t.Fatalf("%s: expected suppression %s, got %s %s", name, want, status, reason)
			}
		}
	}
	mail := `{"account":"a","message_id":"mail"}`
	check("mail-read", "gmail", mail, `UPDATE @gmail_messages SET label_ids=ARRAY[]::text[] WHERE message_id='mail'`, "already_read")
	check("mail-reply", "gmail", mail, `UPDATE @gmail_messages SET label_ids=ARRAY['UNREAD']; INSERT INTO @gmail_messages(account,message_id,thread_id,internal_date,label_ids) VALUES ('a','sent','thread',now(),ARRAY['SENT'])`, "already_replied")
	check("mail-other-account", "gmail", `{"account":"b","message_id":"mail"}`, "", "")
	check("slack-read", "slack", `{"account":"a","team_id":"t","conversation_id":"dm","message_ts":"100.100000"}`, "", "already_read")
	check("slack-thread-read", "slack", `{"account":"a","team_id":"t","conversation_id":"channel","message_ts":"101.000000"}`, "", "already_read")
	check("slack-thread-unread", "slack", `{"account":"a","team_id":"t","conversation_id":"channel","message_ts":"102.000000"}`, "", "")
	check("slack-thread-replied", "slack", `{"account":"a","team_id":"t","conversation_id":"channel","message_ts":"102.000000"}`,
		`INSERT INTO @slack_messages(account,team_id,conversation_id,message_ts,message_datetime,thread_ts,user_id) VALUES ('a','t','channel','103.000000',now(),'100.000000','me')`, "already_replied")
	check("slack-malformed-watermark", "slack", `{"account":"a","team_id":"t","conversation_id":"dm","message_ts":"100.100000"}`,
		`UPDATE @slack_conversations SET raw_json='{"last_read":"unknown"}' WHERE conversation_id='dm'`, "")
	check("slack-dm-replied", "slack", `{"account":"a","team_id":"t","conversation_id":"dm","message_ts":"100.100000"}`,
		`INSERT INTO @slack_messages(account,team_id,conversation_id,message_ts,message_datetime,user_id) VALUES ('a','t','dm','104.000000',now(),'me')`, "already_replied")
	check("im-read", "apple_messages", `{"account":"a","message_id":"im"}`, "", "already_read")
	check("im-group-unrelated", "apple_messages", `{"account":"a","message_id":"im"}`,
		`UPDATE @apple_messages SET is_read=0; INSERT INTO @apple_messages(account,message_id,message_at,is_from_me,is_sent) VALUES ('a','imreply',now(),1,1); INSERT INTO @apple_message_chat_messages(account,chat_id,message_id,message_date) VALUES ('a','chat','imreply',now())`, "")
	check("im-group-replied", "apple_messages", `{"account":"a","message_id":"im"}`, `UPDATE @apple_messages SET reply_to_guid='im' WHERE message_id='imreply'`, "already_replied")
	check("wa-group-unrelated", "whatsapp", `{"account":"a","chat_id":"group@g.us","message_id":"wa"}`,
		`INSERT INTO @whatsapp_messages(account,chat_id,message_id,message_at,is_from_me,message_kind) VALUES ('a','group@g.us','wareply',now(),1,'text')`, "")
	check("wa-group-replied", "whatsapp", `{"account":"a","chat_id":"group@g.us","message_id":"wa"}`, `UPDATE @whatsapp_messages SET quoted_message_id='wa' WHERE message_id='wareply'`, "already_replied")
	check("unsupported", "calendar", `{"id":123}`, "", "")
	check("missing-key", "gmail", `{}`, "", "")

	check("im-direct-response", "apple_messages", `{"account":"a","message_id":"im"}`,
		`UPDATE @apple_message_chats SET style=45; UPDATE @apple_messages SET reply_to_guid='' WHERE message_id='imreply'`, "already_replied")
	check("im-unsent", "apple_messages", `{"account":"a","message_id":"im"}`,
		`UPDATE @apple_messages SET is_sent=0 WHERE message_id='imreply'`, "")
	check("im-reaction", "apple_messages", `{"account":"a","message_id":"im"}`,
		`UPDATE @apple_messages SET is_sent=1,associated_message_type=2000 WHERE message_id='imreply'`, "")
	check("slack-unrelated-channel", "slack", `{"account":"a","team_id":"t","conversation_id":"channel","message_ts":"102.000000"}`,
		`UPDATE @slack_messages SET thread_ts='' WHERE message_ts='103.000000'`, "")
	check("slack-newer-than-reply", "slack", `{"account":"a","team_id":"t","conversation_id":"channel","message_ts":"102.000000"}`,
		`UPDATE @slack_messages SET thread_ts='100.000000',message_datetime=now()-interval '2 hours' WHERE message_ts='103.000000'`, "")
	exec(`INSERT INTO @whatsapp_messages(account,chat_id,message_id,message_at) VALUES ('a','direct@lid','wa',now()-interval '1 hour');
 INSERT INTO @whatsapp_messages(account,chat_id,message_id,message_at,is_from_me,message_kind) VALUES ('a','direct@lid','out',now(),1,'text')`)
	check("wa-direct-response", "whatsapp", `{"account":"a","chat_id":"direct@lid","message_id":"wa"}`, "", "already_replied")
	check("wa-reaction-not-response", "whatsapp", `{"account":"a","chat_id":"direct@lid","message_id":"wa"}`,
		`UPDATE @whatsapp_messages SET message_kind='encReactionMessage' WHERE message_id='out'`, "")
	// A retry must evaluate fresh read state.
	exec("DELETE FROM @gmail_messages WHERE message_id='sent'")
	check("retry-read", "gmail", mail, `UPDATE @notification_deliveries SET status='retry',next_attempt_at=now() WHERE notification_id=(SELECT id FROM @notification_events WHERE event_id='retry-read'); UPDATE @gmail_messages SET label_ids=ARRAY[]::text[]`, "already_read")
	w := httptest.NewRecorder()
	s.status(w, httptest.NewRequest("GET", "/api/notifications", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"suppressed_read":1`) || !strings.Contains(w.Body.String(), `"suppressed_replied":1`) {
		t.Fatalf("suppression counts missing from status API: %d", w.Code)
	}
	// Database errors are not permission to send unchecked.
	raw, _ := json.Marshal(map[string]any{"source": "gmail", "source_pk": mail})
	if _, err = s.DB.ExecContext(ctx, s.q(`INSERT INTO @notification_events(adapter,event_id,source,priority,event_ts,landed_at,payload) VALUES ('test','query-error','gmail','direct',now(),now(),$1)`), raw); err != nil {
		t.Fatal(err)
	}
	sent := len(sender.sent)
	original := s.Expand
	s.Expand = func(q string) string {
		return original(strings.ReplaceAll(q, "@gmail_messages", "missing_suppression_source"))
	}
	if err = s.Tick(ctx); err == nil || len(sender.sent) != sent {
		t.Fatalf("source check failure must not send: %v", err)
	}

}
