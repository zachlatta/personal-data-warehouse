package androidnotifications

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Lines in the Pixel's own format (`logcat -b events -v epoch,uid`), with
// made-up packages.
const sampleSystemLog = `--------- beginning of events
         1792866656.539  1000  1408  1408 I notification_canceled: [0|com.example.mail|-1989189029|summary-a,b@example.test|10338,2,194816,194816,0,0,11,NULL]
         1792866656.573  1000  1408  5340 I notification_visibility: [0|com.example.park|1|null|10383,1,347179,223,0,2]
         1792866657.001  1000  1408  1408 I notification_clicked: [0|com.example.chat|7|null|10123,5000,4000,3000,1,4]
         1792866657.101  1000  1408  1408 I notification_action_clicked: [0|com.example.chat|7|null|10123,123abc,PendingIntent{9f: PendingIntentRecord{1 com.example.chat broadcastIntent}},1,5000,4000,3000,1,4]
         1792866657.201  1000  1408  1408 I notification_expansion: [0|com.example.chat|7|null|10123,1,1,5100,4100,3100]
         1792866658.000  1000  1408  1408 I notification_alert: [0|com.example.park|1|null|10383,0,0,0,0,32]
         1792866659.000  1000  1408  1408 I notification_alert: [0|com.example.park|1|null|10383,0,0,0,0,32]
         1792866659.500  1000  1408  1408 I notification_alert: [0|com.example.chat|8|null|10123,1,1,0,0,0]
         1792866660.000  1000  1408  1408 I notification_enqueue: [10383,26192,com.example.park,1,NULL,0,Notification(channel=x),1,1]
         1792866661.000  1000  1408  1408 I screen_toggled: 1
         1792866662.000  1000  1408  1408 I screen_toggled: 1
         1792866663.000 10292  2036  2036 I sysui_status_bar_state: [1,1,0,0,1,0]
         1792866664.000 10292  2036  2036 I sysui_heads_up_status: [0|com.example.chat|8|null|10123,1]
         1792866665.000  1000  1408  1408 I notification_panel_revealed: 3
         1792866666.000  1000  1408  1408 I notification_panel_hidden: 
`

func TestTailSpoolsEachNotificationStateOnceWithItsFields(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system-log-spool")
	cursor := filepath.Join(t.TempDir(), "system-log.cursor")
	tail := &Tail{SpoolDir: dir, CursorFile: cursor}
	written, err := tail.Consume(strings.NewReader(sampleSystemLog))
	if err != nil {
		t.Fatal(err)
	}
	// enqueue is not recorded, the silent re-alert and the repeated screen
	// state are dropped.
	if written != 12 {
		t.Fatalf("written = %d", written)
	}
	names, err := spoolFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	events := map[string]map[string]any{}
	for _, name := range names {
		event, err := readEvent(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if event["event_source"] != "system_log" || !strings.HasPrefix(event["event_id"].(string), "syslog-") {
			t.Fatalf("event = %v", event)
		}
		events[event["event_type"].(string)] = event
	}
	canceled := events["canceled"]
	if canceled["notification_key"] != "0|com.example.mail|-1989189029|summary-a,b@example.test|10338" ||
		canceled["package_name"] != "com.example.mail" || canceled["tag"] != "summary-a,b@example.test" ||
		canceled["removal_reason"] != float64(2) || canceled["removal_reason_name"] != "cancel" ||
		canceled["lifespan_ms"] != float64(194816) || canceled["canceled_by_listener"] != "" ||
		canceled["event_at_ms"] != float64(1792866656539) {
		t.Fatalf("canceled = %v", canceled)
	}
	if v := events["visibility"]; v["visible"] != float64(1) || v["exposure_ms"] != float64(0) || v["rank"] != float64(2) {
		t.Fatalf("visibility = %v", v)
	}
	if a := events["action_clicked"]; a["action_index"] != float64(1) || a["count"] != float64(4) ||
		!strings.HasPrefix(a["pending_intent"].(string), "PendingIntent{") {
		t.Fatalf("action_clicked = %v", a)
	}
	if e := events["expansion"]; e["user_action"] != float64(1) || e["expanded"] != float64(1) {
		t.Fatalf("expansion = %v", e)
	}
	if a := events["alert"]; a["notification_key"] != "0|com.example.chat|8|null|10123" || a["buzz"] != float64(1) {
		t.Fatalf("the last alert should be the real one: %v", a)
	}
	if s := events["shade_opened"]; s["items"] != float64(3) || s["notification_key"] != nil {
		t.Fatalf("shade_opened = %v", s)
	}
	if _, ok := events["shade_closed"]; !ok {
		t.Fatal("shade_closed missing")
	}
	if s := events["status_bar"]; s["keyguard_showing"] != float64(1) {
		t.Fatalf("status_bar = %v", s)
	}
	if h := events["heads_up"]; h["visible"] != float64(1) || h["package_name"] != "com.example.chat" {
		t.Fatalf("heads_up = %v", h)
	}
	raw, _ := os.ReadFile(cursor)
	if strings.TrimSpace(string(raw)) != "1792866666.000" {
		t.Fatalf("cursor = %q", raw)
	}

	// A restart re-reads from the cursor: the same line is the same id.
	again := &Tail{SpoolDir: dir}
	if _, err := again.Consume(strings.NewReader(sampleSystemLog)); err != nil {
		t.Fatal(err)
	}
	after, _ := spoolFiles(dir)
	if len(after) != len(names) {
		t.Fatalf("a re-read wrote new files: %d -> %d", len(names), len(after))
	}
}

func TestLogcatArgsResumeFromTheCursor(t *testing.T) {
	args := strings.Join(logcatArgs("1792866666.000"), " ")
	if !strings.HasPrefix(args, "-b events -v epoch,uid -T 1792866666.000 -s ") || !strings.Contains(args, " notification_clicked ") {
		t.Fatalf("args = %s", args)
	}
	if strings.Contains(strings.Join(logcatArgs(""), " "), "-T") {
		t.Fatal("no cursor reads the whole buffer")
	}
}

func TestSyncSendsTheSystemLogSpoolWithTheListenersMergedByTime(t *testing.T) {
	listener := spool(t, event("b", 1792863236900, "removed"))
	system := spool(t, event("a", 1792863236787, "visibility"), event("c", 1792863237000, "clicked"))
	client := &fakeUploader{}
	r := runner(listener, client)
	r.ExtraSpoolDirs = []string{system, ""}
	summary, err := r.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Uploaded != 3 {
		t.Fatalf("summary = %+v", summary)
	}
	var order []string
	for _, envelope := range client.batches[0] {
		order = append(order, envelope["record"].(map[string]any)["event_id"].(string))
	}
	if strings.Join(order, ",") != "a,b,c" {
		t.Fatalf("order = %v", order)
	}
	if len(remaining(t, listener))+len(remaining(t, system)) != 0 {
		t.Fatal("sent files left behind")
	}
}
