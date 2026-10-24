package androidnotifications

// The system-log tail: `pdw ingest android-notifications tail-system-log`.
//
// A notification listener hears a notification posted and removed, and
// nothing in between. Android's system server and System UI log the rest to
// the `events` log buffer (frameworks/base EventLogTags): when a notification
// came on screen and left it, was expanded, tapped, had an action button
// pressed, buzzed or rang (or was muted), and was cancelled with how long it
// lived and was seen; when the shade opened and closed, a heads-up peeked,
// the screen turned on or off, and the lock screen showed. The tail runs
// `logcat` on that buffer for as long as it lives (the buffer is 256 KiB, a
// few hours at most, so reading it now and then would lose events), turns each
// line into one event in the listener's shape, and writes it into its own
// spool for the uploader. Event ids are a hash of the log line, so a restart
// that re-reads from the saved cursor writes the same ids and the warehouse
// upserts them once.

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// SystemLogVersion is stamped on every system-log event as listener_version's
// counterpart, so a change in what the tail records can be told apart later.
const SystemLogVersion = 1

// systemLogTag is one event-log tag the tail records: its event_type and the
// names of the fields after the notification key (nil when the payload has
// no key; then fields are the whole payload).
type systemLogTag struct {
	eventType string
	keyed     bool
	fields    []string
}

// systemLogTags are frameworks/base EventLogTags, checked against the Pixel's
// /system/etc/event-log-tags on Android 17 (2026-10-24).
var systemLogTags = map[string]systemLogTag{
	"notification_visibility":        {"visibility", true, []string{"visible", "lifespan_ms", "freshness_ms", "exposure_ms", "rank"}},
	"notification_expansion":         {"expansion", true, []string{"user_action", "expanded", "lifespan_ms", "freshness_ms", "exposure_ms"}},
	"notification_clicked":           {"clicked", true, []string{"lifespan_ms", "freshness_ms", "exposure_ms", "rank", "count"}},
	"notification_action_clicked":    {"action_clicked", true, []string{"pending_intent_id", "pending_intent", "action_index", "lifespan_ms", "freshness_ms", "exposure_ms", "rank", "count"}},
	"notification_alert":             {"alert", true, []string{"buzz", "beep", "blink", "politeness", "mute_reason"}},
	"notification_canceled":          {"canceled", true, []string{"removal_reason", "lifespan_ms", "freshness_ms", "exposure_ms", "rank", "count", "canceled_by_listener"}},
	"notification_autogrouped":       {"autogrouped", true, nil},
	"notification_unautogrouped":     {"unautogrouped", true, nil},
	"notification_adjusted":          {"adjusted", true, []string{"adjustment_type", "new_value"}},
	"notification_cancel_prevented":  {"cancel_prevented", true, nil},
	"notification_summary_converted": {"summary_converted", true, nil},
	"sysui_heads_up_status":          {"heads_up", true, []string{"visible"}},
	"notification_panel_revealed":    {"shade_opened", false, []string{"items"}},
	"notification_panel_hidden":      {"shade_closed", false, nil},
	"screen_toggled":                 {"screen", false, []string{"screen_state"}},
	"sysui_status_bar_state":         {"status_bar", false, []string{"status_bar_state", "keyguard_showing", "keyguard_occluded", "bouncer_showing", "secure", "currently_insecure"}},
}

// systemLogTextFields stay strings; every other field is an integer.
var systemLogTextFields = map[string]bool{
	"pending_intent_id": true, "pending_intent": true, "canceled_by_listener": true,
	"adjustment_type": true, "new_value": true,
}

// `logcat -v epoch,uid`: seconds.millis, uid, pid, tid, priority, tag: payload.
var systemLogLine = regexp.MustCompile(`^\s*(\d+)\.(\d{3})\d*\s+(\S+)\s+(\d+)\s+(\d+)\s+[VDIWEF]\s+([A-Za-z0-9_]+): ?(.*)$`)

// parseSystemLogLine turns one logcat line into an event, or nil for a line
// the tail does not record.
func parseSystemLogLine(line string) map[string]any {
	m := systemLogLine.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	tag, ok := systemLogTags[m[6]]
	if !ok {
		return nil
	}
	secs, _ := strconv.ParseInt(m[1], 10, 64)
	millis, _ := strconv.ParseInt(m[2], 10, 64)
	sum := sha256.Sum256([]byte(strings.TrimSpace(line)))
	event := map[string]any{
		"event_id":         "syslog-" + hex.EncodeToString(sum[:16]),
		"event_type":       tag.eventType,
		"event_source":     "system_log",
		"event_at_ms":      secs*1000 + millis,
		"listener_version": SystemLogVersion,
		"log_tag":          m[6],
		"log_line":         strings.TrimSpace(line),
	}
	payload := strings.TrimSpace(m[7])
	payload = strings.TrimSuffix(strings.TrimPrefix(payload, "["), "]")
	parts := []string{}
	if payload != "" {
		parts = strings.Split(payload, ",")
	}
	if tag.keyed {
		// The key (user|package|id|tag|uid) comes first and its tag may hold
		// commas: it is everything before the fixed trailing fields.
		if len(parts) < len(tag.fields)+1 {
			return event
		}
		cut := len(parts) - len(tag.fields)
		key := strings.Join(parts[:cut], ",")
		parts = parts[cut:]
		event["notification_key"] = key
		if pieces := strings.Split(key, "|"); len(pieces) >= 5 {
			event["user_id"], _ = strconv.Atoi(pieces[0])
			event["package_name"] = pieces[1]
			event["notification_id"], _ = strconv.Atoi(pieces[2])
			if t := strings.Join(pieces[3:len(pieces)-1], "|"); t != "null" {
				event["tag"] = t
			}
		}
	}
	for i, name := range tag.fields {
		if i >= len(parts) {
			break
		}
		value := strings.TrimSpace(parts[i])
		if systemLogTextFields[name] {
			if value == "NULL" {
				value = ""
			}
			event[name] = value
			continue
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			event[name] = value
			continue
		}
		event[name] = n
	}
	if tag.eventType == "canceled" {
		if reason, ok := event["removal_reason"].(int64); ok {
			event["removal_reason_name"] = removalReasonName(reason)
		}
	}
	return event
}

// systemLogFilter drops repeats that carry nothing new. A notification that
// re-posts every second (a parking timer) logs an "alert" each time even when
// it makes no sound; the status bar and screen log the same state again when
// nothing changed. A real alert (buzz, beep or blink) is always kept.
type systemLogFilter struct {
	last map[string]string
}

func (f *systemLogFilter) keep(event map[string]any) bool {
	if f.last == nil {
		f.last = map[string]string{}
	}
	var key, state string
	switch event["event_type"] {
	case "alert":
		if event["buzz"] != int64(0) || event["beep"] != int64(0) || event["blink"] != int64(0) {
			return true
		}
		key = "alert|" + fmt.Sprint(event["notification_key"])
		state = fmt.Sprint(event["politeness"], ",", event["mute_reason"])
	case "status_bar":
		key = "status_bar"
		state = fmt.Sprint(event["status_bar_state"], event["keyguard_showing"], event["keyguard_occluded"], event["bouncer_showing"])
	case "screen":
		key = "screen"
		state = fmt.Sprint(event["screen_state"])
	default:
		return true
	}
	if f.last[key] == state {
		return false
	}
	if len(f.last) > 10000 {
		f.last = map[string]string{}
	}
	f.last[key] = state
	return true
}

// writeSpoolEvent writes one event as the listener does: into .tmp/, then
// renamed into place, named by its epoch milliseconds and id.
func writeSpoolEvent(dir string, event map[string]any) error {
	tmp := filepath.Join(dir, ".tmp")
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	name := fmt.Sprintf("%013d-%s.json", event["event_at_ms"], event["event_id"])
	partial := filepath.Join(tmp, name)
	if err := os.WriteFile(partial, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(partial, filepath.Join(dir, name))
}

// Tail reads logcat output and spools what it records. It returns when the
// reader ends.
type Tail struct {
	SpoolDir   string
	CursorFile string
	Logger     common.Logger
	filter     systemLogFilter
}

// Consume spools every recorded line of r and saves the cursor (the last
// line's timestamp) after each write.
func (t *Tail) Consume(r io.Reader) (int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	written := 0
	for scanner.Scan() {
		event := parseSystemLogLine(scanner.Text())
		if event == nil || !t.filter.keep(event) {
			continue
		}
		if err := writeSpoolEvent(t.SpoolDir, event); err != nil {
			return written, err
		}
		written++
		if t.CursorFile != "" {
			ms := event["event_at_ms"].(int64)
			cursor := fmt.Sprintf("%d.%03d", ms/1000, ms%1000)
			if err := writeFileAtomic(t.CursorFile, []byte(cursor+"\n")); err != nil {
				return written, err
			}
		}
	}
	return written, scanner.Err()
}

func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// logcatArgs is the logcat command line: the events buffer, epoch times, only
// the recorded tags, from the cursor when there is one (a line at the cursor
// is read again and gets the same id) or else the whole buffer.
func logcatArgs(cursor string) []string {
	args := []string{"-b", "events", "-v", "epoch,uid"}
	if cursor != "" {
		args = append(args, "-T", cursor)
	}
	args = append(args, "-s")
	return append(args, sortedSystemLogTags()...)
}

func sortedSystemLogTags() []string {
	tags := make([]string, 0, len(systemLogTags))
	for tag := range systemLogTags {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	return tags
}

// TailUsage is `pdw ingest android-notifications tail-system-log --help`.
const TailUsage = `pdw ingest android-notifications tail-system-log - record what happens to each notification on screen.

USAGE
  pdw ingest android-notifications tail-system-log --spool-dir DIR [--cursor-file PATH]
                                                   [--logcat PATH]

Runs ON the phone, as root, for as long as the phone is up (the
pdw-android-notifications Magisk service supervises it). Follows Android's
events log and writes one JSON event per notification state change into
DIR, which ` + "`pdw ingest android-notifications --system-log-spool-dir DIR`" + ` sends:

  visibility      came on screen (visible=1) or left it (0), with how long it
                  had lived (lifespan_ms) and been seen (exposure_ms)
  expansion       expanded (expanded=1) or collapsed, by Zach (user_action=1)
  clicked         tapped
  action_clicked  an action button pressed (action_index into its actions)
  alert           buzzed, beeped or blinked, or was muted (mute_reason)
  canceled        removed, with Android's reason and its lifespan and exposure
  heads_up        a heads-up peek shown (visible=1) or gone
  autogrouped, unautogrouped, adjusted, cancel_prevented, summary_converted
  shade_opened, shade_closed, screen, status_bar   (the phone around them)

Restarts resume from the cursor file (default: next to DIR); a re-read line
gets the same event id, so nothing is stored twice.

FLAGS
  --spool-dir DIR      Where events are written (required).
  --cursor-file PATH   The last recorded line's time (default DIR/../system-log.cursor).
  --logcat PATH        The logcat binary (default logcat).
`

// RunTail is the tail-system-log entry point.
func RunTail(args []string, stdout, stderr io.Writer) int {
	fs := common.NewFlagSet("pdw ingest android-notifications tail-system-log")
	spoolDir := fs.String("spool-dir", "", "")
	cursorFile := fs.String("cursor-file", "", "")
	logcat := fs.String("logcat", "logcat", "")
	if _, code, done := common.ParseArgs(fs, args, TailUsage, stdout, stderr); done {
		return code
	}
	if *spoolDir == "" {
		return common.UsageError(fs, stderr, "--spool-dir is required")
	}
	if *cursorFile == "" {
		*cursorFile = filepath.Join(filepath.Dir(filepath.Clean(*spoolDir)), "system-log.cursor")
	}
	logger := common.NewWriterLogger(stdout)
	cursor := ""
	if raw, err := os.ReadFile(*cursorFile); err == nil {
		cursor = strings.TrimSpace(string(raw))
	} else if !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(stderr, "pdw ingest android-notifications tail-system-log: %v\n", err)
		return common.ExitFailure
	}
	cmd := exec.Command(*logcat, logcatArgs(cursor)...)
	cmd.Stderr = stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-notifications tail-system-log: %v\n", err)
		return common.ExitFailure
	}
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-notifications tail-system-log: %v\n", err)
		return common.ExitFailure
	}
	logger.Infof("Following the events log from %s", common.FirstNonEmpty(cursor, "the start of the buffer"))
	started := time.Now()
	tail := &Tail{SpoolDir: *spoolDir, CursorFile: *cursorFile, Logger: logger}
	written, err := tail.Consume(out)
	waitErr := cmd.Wait()
	logger.Infof("logcat ended after %s; %d events written", time.Since(started).Round(time.Second), written)
	if err != nil {
		fmt.Fprintf(stderr, "pdw ingest android-notifications tail-system-log: %v\n", err)
		return common.ExitFailure
	}
	if waitErr != nil {
		fmt.Fprintf(stderr, "pdw ingest android-notifications tail-system-log: logcat: %v\n", waitErr)
		return common.ExitFailure
	}
	return 0
}

// removalReasonName is NotificationListenerService's REASON_* name, as the
// listener records it.
func removalReasonName(reason int64) string {
	names := map[int64]string{
		1: "click", 2: "cancel", 3: "cancel_all", 4: "error", 5: "package_changed",
		6: "user_stopped", 7: "package_banned", 8: "app_cancel", 9: "app_cancel_all",
		10: "listener_cancel", 11: "listener_cancel_all", 12: "group_summary_canceled",
		13: "group_optimization", 14: "package_suspended", 15: "profile_turned_off",
		16: "unautobundled", 17: "channel_banned", 18: "snoozed", 19: "timeout",
		20: "channel_removed", 21: "clear_data", 22: "assistant_cancel", 23: "lockdown",
	}
	if name, ok := names[reason]; ok {
		return name
	}
	return fmt.Sprintf("reason_%d", reason)
}
