// Package agentsessions uploads local AI agent CLI session transcripts
// (Claude Code, Codex, OpenClaw, pi) through the app's ingest API. The
// transcripts are append-only JSONL files, one per session, so the uploader
// remembers a byte offset per file and ships only what is new.
package agentsessions

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

const (
	ClaudeCodeTool = "claude_code"
	CodexTool      = "codex"
	OpenClawTool   = "openclaw"
	PiTool         = "pi"
)

// OpenClaw writes several sidecar files next to each "<sessionId>.jsonl"
// transcript: a lower-level "<sessionId>.trajectory.jsonl" runtime trace plus
// "<sessionId>.*.json" metadata. Only the bare "<sessionId>.jsonl" is the
// conversational transcript.
const openClawSidecarSuffix = ".trajectory.jsonl"

var uuidRe = regexp.MustCompile(`([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})`)

// SessionFile is one transcript.
type SessionFile struct {
	Tool      string
	SessionID string
	Path      string
}

// Dirs names each tool's transcript root; an empty root disables that tool.
type Dirs struct {
	ClaudeProjects   string
	CodexSessions    string
	OpenClawSessions string
	PiSessions       string
}

// DirsFromEnv reads AGENT_SESSIONS_*_DIR with the documented defaults; a
// variable set to empty disables that tool on this host.
func DirsFromEnv(getenv common.Getenv) Dirs {
	resolve := func(name, fallback string) string {
		value, set := lookup(getenv, name)
		if set && strings.TrimSpace(value) == "" {
			return ""
		}
		if strings.TrimSpace(value) == "" {
			value = fallback
		}
		return common.ExpandUser(value)
	}
	return Dirs{
		ClaudeProjects:   resolve("AGENT_SESSIONS_CLAUDE_PROJECTS_DIR", "~/.claude/projects"),
		CodexSessions:    resolve("AGENT_SESSIONS_CODEX_SESSIONS_DIR", "~/.codex/sessions"),
		OpenClawSessions: resolve("AGENT_SESSIONS_OPENCLAW_SESSIONS_DIR", "~/.openclaw/agents/main/sessions"),
		PiSessions:       resolve("AGENT_SESSIONS_PI_SESSIONS_DIR", "~/.pi/agent/sessions"),
	}
}

// lookup distinguishes "unset" from "set to empty" for a plain getenv by
// consulting os.LookupEnv when the getenv is the process environment; a test
// getenv that returns "" is treated as unset.
func lookup(getenv common.Getenv, name string) (string, bool) {
	value := getenv(name)
	if value != "" {
		return value, true
	}
	if _, set := os.LookupEnv(name); set && os.Getenv(name) == "" && getenv(name) == "" {
		return "", true
	}
	return "", false
}

// Discover lists every transcript under the configured roots, in a stable
// order so batching is deterministic across runs.
func Discover(dirs Dirs) []SessionFile {
	var files []SessionFile
	files = append(files, discoverClaudeCode(dirs.ClaudeProjects)...)
	files = append(files, discoverCodex(dirs.CodexSessions)...)
	files = append(files, discoverOpenClaw(dirs.OpenClawSessions)...)
	files = append(files, discoverPi(dirs.PiSessions)...)
	sort.Slice(files, func(i, j int) bool {
		if files[i].Tool != files[j].Tool {
			return files[i].Tool < files[j].Tool
		}
		return files[i].Path < files[j].Path
	})
	return files
}

func walkJSONL(root string, visit func(path, name string)) {
	if strings.TrimSpace(root) == "" {
		return
	}
	root = common.ExpandUser(root)
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return
	}
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".jsonl") {
			return nil
		}
		if info, err := d.Info(); err != nil || !info.Mode().IsRegular() {
			return nil
		}
		visit(path, d.Name())
		return nil
	})
}

func discoverClaudeCode(root string) []SessionFile {
	var files []SessionFile
	walkJSONL(root, func(path, name string) {
		// journal.jsonl is Claude Code's journal feature, not a session
		// transcript; shipping it created a fake session_id='journal' session.
		if name == "journal.jsonl" {
			return
		}
		files = append(files, SessionFile{Tool: ClaudeCodeTool, SessionID: ClaudeSessionID(name), Path: path})
	})
	return files
}

func discoverCodex(root string) []SessionFile {
	var files []SessionFile
	walkJSONL(root, func(path, name string) {
		if !strings.HasPrefix(name, "rollout-") {
			return
		}
		files = append(files, SessionFile{Tool: CodexTool, SessionID: CodexSessionID(name), Path: path})
	})
	return files
}

func discoverOpenClaw(root string) []SessionFile {
	var files []SessionFile
	walkJSONL(root, func(path, name string) {
		if strings.HasSuffix(name, openClawSidecarSuffix) {
			return
		}
		files = append(files, SessionFile{Tool: OpenClawTool, SessionID: OpenClawSessionID(name), Path: path})
	})
	return files
}

func discoverPi(root string) []SessionFile {
	var files []SessionFile
	walkJSONL(root, func(path, name string) {
		files = append(files, SessionFile{Tool: PiTool, SessionID: PiSessionID(name), Path: path})
	})
	return files
}

func stem(name string) string {
	if strings.HasSuffix(name, ".jsonl") {
		return strings.TrimSuffix(name, ".jsonl")
	}
	return strings.TrimSuffix(name, filepath.Ext(name))
}

// ClaudeSessionID: Claude Code names each transcript "<sessionId>.jsonl".
func ClaudeSessionID(name string) string { return stem(name) }

// CodexSessionID: Codex names each rollout "rollout-<ts>-<uuid>.jsonl"; the
// trailing UUID is the session id.
func CodexSessionID(name string) string {
	matches := uuidRe.FindAllString(name, -1)
	if len(matches) > 0 {
		return strings.ToLower(matches[len(matches)-1])
	}
	s := stem(name)
	return strings.TrimPrefix(s, "rollout-")
}

// OpenClawSessionID: OpenClaw names each transcript "<sessionId>.jsonl".
func OpenClawSessionID(name string) string { return stem(name) }

// PiSessionID: pi names transcripts "<timestamp>_<sessionId>.jsonl".
func PiSessionID(name string) string {
	matches := uuidRe.FindAllString(name, -1)
	if len(matches) > 0 {
		return strings.ToLower(matches[len(matches)-1])
	}
	s := stem(name)
	if idx := strings.LastIndex(s, "_"); idx >= 0 {
		return s[idx+1:]
	}
	return s
}
