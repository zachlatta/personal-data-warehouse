package agentsessions

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
