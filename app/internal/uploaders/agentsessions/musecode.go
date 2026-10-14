package agentsessions

import (
	"path/filepath"
	"strings"
)

// Muse Code (the Muse models' coding CLI, `muse`) keeps one append-only
// event log per session at
// <data>/muse/sessions/YYYY/MM/DD/<session-id>/session.jsonl, where <data> is
// $XDG_DATA_HOME or ~/.local/share. A subagent's log is nested under its
// parent at <parent-id>/subagent/<child-id>/session.jsonl. Siblings such as
// session.peer-history.jsonl and the .msp-view-v1 index are derived views,
// not the transcript.
//
// The session id is the directory name. A subagent's own lines never name
// its parent, so discovery reads the parent from the path and ships it on
// every line, as Muse (Meta's agent) ships its session kind.
const museCodeTranscriptName = "session.jsonl"

func defaultMuseCodeSessions(getenv func(string) string) string {
	if data := strings.TrimSpace(getenv("XDG_DATA_HOME")); data != "" {
		return filepath.Join(data, "muse", "sessions")
	}
	return "~/.local/share/muse/sessions"
}

func discoverMuseCode(root string) []SessionFile {
	var files []SessionFile
	walkJSONL(root, func(path, name string) {
		if name != museCodeTranscriptName {
			return
		}
		dir := filepath.Dir(path)
		file := SessionFile{Tool: MuseCodeTool, SessionID: filepath.Base(dir), Path: path}
		if parent := filepath.Dir(dir); filepath.Base(parent) == "subagent" {
			file.Session = map[string]any{"parent_session_id": filepath.Base(filepath.Dir(parent))}
		}
		files = append(files, file)
	})
	return files
}
