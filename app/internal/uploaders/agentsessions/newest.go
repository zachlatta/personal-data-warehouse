package agentsessions

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Contract C11: each pass posts what the device's own transcripts say is
// each tool's newest event, read from the files themselves rather than from
// the lines the tail happened to ship, so a tail that stopped advancing cannot
// hide the lines it never read. marts_ops.source_upstream_health compares it
// with what landed in base_<tool>.events for the same session.
const (
	// observeInterval paces observations: the resident watcher runs a pass
	// every two seconds, and the evidence only needs the collector's cadence.
	observeInterval = 5 * time.Minute
	// observeTailBytes is how much of each transcript's end is read.
	observeTailBytes = 256 << 10
	// observeRecentSpan: every transcript modified within this span of the
	// tool's most recently modified one is read, because the newest-modified
	// file does not always hold the newest event (two Codex rollouts written
	// in the same minute, measured 2026-11-11).
	observeRecentSpan = 24 * time.Hour
)

type newestEvent struct {
	At        time.Time
	SessionID string
}

// eventTime mirrors agent_sessions_drive_ingest's occurred_at for each tool:
// `timestamp` for Claude Code, Codex, OpenClaw and Pi; `created_at` (ISO, or
// epoch seconds or milliseconds) for Muse and Argon; `recorded_at` (epoch
// microseconds, a frame taking its first child's) for Muse Code.
func eventTime(tool string, line map[string]any) (time.Time, bool) {
	switch tool {
	case MuseTool, ArgonTool:
		return museTime(line["created_at"])
	case MuseCodeTool:
		value := line["recorded_at"]
		if _, framed := line["retained_frame"]; framed {
			value = nil
			if children, ok := line["children"].([]any); ok && len(children) > 0 {
				if first, ok := children[0].(map[string]any); ok {
					var child map[string]any
					if json.Unmarshal([]byte(stringOf(first["record_json"])), &child) == nil {
						value = child["recorded_at"]
					}
				}
			}
		}
		micros, ok := numberOf(value)
		if !ok || micros <= 0 {
			return time.Time{}, false
		}
		return time.UnixMicro(int64(micros)).UTC(), true
	default:
		return parseISO(stringOf(line["timestamp"]))
	}
}

func museTime(value any) (time.Time, bool) {
	seconds, ok := numberOf(value)
	if !ok {
		return parseISO(stringOf(value))
	}
	if seconds > 1e11 { // milliseconds
		seconds /= 1000
	}
	if seconds <= 0 {
		return time.Time{}, false
	}
	return time.UnixMicro(int64(seconds * 1e6)).UTC(), true
}

func numberOf(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return f, err == nil
	}
	return 0, false
}

func stringOf(value any) string {
	text, _ := value.(string)
	return text
}

func parseISO(text string) (time.Time, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999Z07:00"} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed.UTC(), true
		}
	}
	return time.Time{}, false
}

// newestDeviceEvents reads the end of every recently modified transcript and
// returns each tool's newest event and its session.
func newestDeviceEvents(files []SessionFile) map[string]newestEvent {
	latestMod := map[string]time.Time{}
	mods := make([]time.Time, len(files))
	for i, file := range files {
		info, err := os.Stat(file.Path)
		if err != nil {
			continue
		}
		mods[i] = info.ModTime()
		if mods[i].After(latestMod[file.Tool]) {
			latestMod[file.Tool] = mods[i]
		}
	}
	newest := map[string]newestEvent{}
	for i, file := range files {
		if mods[i].IsZero() || latestMod[file.Tool].Sub(mods[i]) > observeRecentSpan {
			continue
		}
		for _, line := range tailLines(file.Path) {
			at, ok := eventTime(file.Tool, line)
			if ok && at.After(newest[file.Tool].At) {
				newest[file.Tool] = newestEvent{At: at, SessionID: file.SessionID}
			}
		}
	}
	return newest
}

// tailLines parses the complete JSON lines in the last observeTailBytes of path.
func tailLines(path string) []map[string]any {
	handle, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer handle.Close()
	info, err := handle.Stat()
	if err != nil {
		return nil
	}
	start := info.Size() - observeTailBytes
	if start < 0 {
		start = 0
	}
	if _, err := handle.Seek(start, io.SeekStart); err != nil {
		return nil
	}
	data, _ := io.ReadAll(io.LimitReader(handle, observeTailBytes))
	if start > 0 {
		// The first line may be cut mid-way; drop it.
		if cut := bytes.IndexByte(data, '\n'); cut >= 0 {
			data = data[cut+1:]
		}
	}
	var lines []map[string]any
	for _, raw := range completeLines(data) {
		var parsed map[string]any
		if json.Unmarshal(bytes.TrimSpace(raw), &parsed) == nil && parsed != nil {
			lines = append(lines, parsed)
		}
	}
	return lines
}

// observeNewest posts each tool's newest event, at most once per
// observeInterval, before this pass uploads anything. Never fails the pass.
func (r *Runner) observeNewest(files []SessionFile) {
	if r.Observe == nil {
		return
	}
	now := r.Now()
	if !r.lastObserved.IsZero() && now.Sub(r.lastObserved) < observeInterval {
		return
	}
	r.lastObserved = now
	newest := newestDeviceEvents(files)
	if store, ok := r.openClawStoreNewest(); ok && store.At.After(newest[OpenClawTool].At) {
		newest[OpenClawTool] = store
	}
	for tool, event := range newest {
		ingestclient.Observe(r.Observe, r.Logger, tool, r.Account+"/"+r.Device, event.At, event.SessionID,
			"the device's own transcripts: the newest event time", now)
	}
}

// openClawStoreNewest reads the newest events in the OpenClaw SQLite store.
func (r *Runner) openClawStoreNewest() (newestEvent, bool) {
	path := r.Dirs.OpenClawStore
	if strings.TrimSpace(path) == "" {
		return newestEvent{}, false
	}
	if _, err := os.Stat(path); err != nil {
		return newestEvent{}, false
	}
	db, err := common.OpenSQLiteReadOnly(path)
	if err != nil {
		return newestEvent{}, false
	}
	defer db.Close()
	rows, err := common.Query(db, `SELECT session_id, event_json FROM transcript_events ORDER BY rowid DESC LIMIT 200`)
	if err != nil {
		return newestEvent{}, false
	}
	var best newestEvent
	for _, row := range rows {
		var parsed map[string]any
		if json.Unmarshal([]byte(row.String("event_json")), &parsed) != nil {
			continue
		}
		if at, ok := eventTime(OpenClawTool, parsed); ok && at.After(best.At) {
			best = newestEvent{At: at, SessionID: row.String("session_id")}
		}
	}
	return best, !best.At.IsZero()
}
