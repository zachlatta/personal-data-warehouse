package agentsessions

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Muse (Meta's hosted personal agent) keeps one transcript per agent at
// ~/agents/agent-<id>/sessions/<id>.jsonl, beside a sessions.json index. The
// transcript lines are append-only (monotonic `seq`), so the byte-offset
// cursor every other tool uses applies unchanged.
//
// What a line cannot say is what kind of session it belongs to. Muse writes
// its background loops (self-improvement, the hourly feed, cron workers) on
// the same user channel as the words Zach types, and a subagent's brief from
// its parent is a user message too. The opening item says which it is, so
// the uploader reads it once per transcript and ships the answer on every
// line; the warehouse cannot recover it from a later batch on its own.

// museOpeningLineLimit bounds how far into a transcript the opener is sought.
// The opener is the first message item; a turn can begin with a thinking or
// tool item before it.
const museOpeningLineLimit = 32

var museRequesterRe = regexp.MustCompile(`Requester agent id: ([0-9a-fA-F-]{36})`)

// Item sources whose sessions are conversations with Zach rather than
// Muse's own background loops.
var museChatSources = map[string]bool{
	"runtime":            true,
	"runtime.onboarding": true,
	"runtime.chats":      true,
}

func discoverMuse(root string) []SessionFile {
	var files []SessionFile
	walkJSONL(root, func(path, name string) {
		if filepath.Base(filepath.Dir(path)) != "sessions" {
			return
		}
		id := stem(name)
		files = append(files, SessionFile{
			Tool:      MuseTool,
			SessionID: id,
			Path:      path,
			Session:   museSessionMeta(path, id),
		})
	})
	return files
}

// museSessionMeta reads the transcript's opening item and its sessions.json
// entry. Every field is always present so the warehouse never has to guess
// between "unknown" and "absent".
func museSessionMeta(path, sessionID string) map[string]any {
	meta := map[string]any{"kind": "", "opening_source": "", "requester_agent_id": "", "model": museSessionModel(filepath.Join(filepath.Dir(path), "sessions.json"), sessionID)}
	handle, err := os.Open(path)
	if err != nil {
		return meta
	}
	defer handle.Close()
	reader := bufio.NewReaderSize(handle, 64*1024)
	for i := 0; i < museOpeningLineLimit; i++ {
		raw, err := reader.ReadBytes('\n')
		var line struct {
			Type   string `json:"type"`
			Source string `json:"source"`
			Item   struct {
				Type  string `json:"type"`
				Text  string `json:"text"`
				Parts []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"item"`
		}
		if json.Unmarshal(raw, &line) == nil && line.Type == "item" && (line.Item.Type == "message" || line.Item.Type == "message_parts") {
			text := line.Item.Text
			for _, part := range line.Item.Parts {
				if part.Type == "text" {
					text += part.Text
				}
			}
			meta["opening_source"] = line.Source
			switch {
			case strings.HasPrefix(strings.TrimSpace(text), "[Subagent Context]"):
				meta["kind"] = "subagent"
				if match := museRequesterRe.FindStringSubmatch(text); match != nil {
					meta["requester_agent_id"] = strings.ToLower(match[1])
				}
			case museChatSources[line.Source]:
				meta["kind"] = "chat"
			default:
				meta["kind"] = "background"
			}
			return meta
		}
		if err != nil {
			break
		}
	}
	return meta
}

func museSessionModel(indexPath, sessionID string) string {
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return ""
	}
	var index struct {
		Sessions []struct {
			SessionID          string `json:"session_id"`
			ContextWindowUsage struct {
				ModelID string `json:"model_id"`
			} `json:"context_window_usage"`
		} `json:"sessions"`
	}
	if json.Unmarshal(data, &index) != nil {
		return ""
	}
	for _, session := range index.Sessions {
		if session.SessionID == sessionID {
			return session.ContextWindowUsage.ModelID
		}
	}
	return ""
}

// MuseManifestEntry is one hosted-Muse transcript as the warehouse counts it.
// It carries no transcript text: only the ids, the count, the newest line's
// own timestamp and the file's stat.
type MuseManifestEntry struct {
	SessionID     string `json:"session_id"`
	AgentID       string `json:"agent_id"`
	EventCount    int64  `json:"event_count"`
	LastTimestamp string `json:"last_timestamp"`
	SizeBytes     int64  `json:"size_bytes"`
	ModifiedAt    string `json:"modified_at"`
}

// MuseManifest lists every transcript the Muse uploader ships from
// agentsRoot (the VM's ~/agents), sorted by session id. EventCount is the
// number of rows marts_ai_conversations.sessions will hold for the session
// once every complete line has landed: lines are read with parseLine, the
// uploader's own rule, and keyed the way muse_event_row keys them
// (`#<seq>` for an item, `#compaction-<id>`, `#header`, else `#line-<n>`),
// because base_muse.events is unique on that key and a repeated key is one
// row. A trailing partial line is left out, as the uploader leaves it.
func MuseManifest(agentsRoot string) []MuseManifestEntry {
	byID := map[string]*MuseManifestEntry{}
	keys := map[string]map[string]bool{}
	walkJSONL(agentsRoot, func(path, name string) {
		if filepath.Base(filepath.Dir(path)) != "sessions" {
			return
		}
		info, err := os.Stat(path)
		if err != nil {
			return
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return
		}
		id := stem(name)
		entry, ok := byID[id]
		if !ok {
			entry = &MuseManifestEntry{
				SessionID: id,
				AgentID:   strings.TrimPrefix(filepath.Base(filepath.Dir(filepath.Dir(path))), "agent-"),
			}
			byID[id] = entry
			keys[id] = map[string]bool{}
		}
		entry.SizeBytes += info.Size()
		if modified := info.ModTime().UTC().Format(time.RFC3339Nano); modified > entry.ModifiedAt {
			entry.ModifiedAt = modified
		}
		for lineNo, raw := range completeLines(data) {
			line, _ := parseLine(raw)
			if line == nil {
				continue
			}
			keys[id][museEventKey(line, int64(lineNo))] = true
			if stamp := museTimestampText(line["created_at"]); stamp != "" {
				entry.LastTimestamp = stamp
			}
		}
		entry.EventCount = int64(len(keys[id]))
	})
	out := make([]MuseManifestEntry, 0, len(byID))
	for _, entry := range byID {
		out = append(out, *entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })
	return out
}

// museEventKey mirrors the event_uuid suffix muse_event_row builds
// (agent_sessions_drive_ingest.py). lineNo is the line's 0-based position in
// the file, which is the seq the uploader ships with it.
func museEventKey(line map[string]any, lineNo int64) string {
	switch line["type"] {
	case "item":
		return "#" + strconv.FormatInt(pythonInt(line["seq"]), 10)
	case "compaction_checkpoint":
		return "#compaction-" + strconv.FormatInt(pythonInt(line["compaction_id"]), 10)
	case "session_header":
		return "#header"
	default:
		return "#line-" + strconv.FormatInt(lineNo, 10)
	}
}

// pythonInt is the warehouse's `_int`: int(value or 0), and 0 when that
// raises. JSON decodes numbers to float64, which int() truncates toward zero.
func pythonInt(value any) int64 {
	switch v := value.(type) {
	case bool:
		if v {
			return 1
		}
		return 0
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0
		}
		return int64(v)
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}

// museTimestampText is a line's created_at as written (ISO text on most
// lines, epoch seconds on some), or "" when it has none.
func museTimestampText(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return ""
	}
}
