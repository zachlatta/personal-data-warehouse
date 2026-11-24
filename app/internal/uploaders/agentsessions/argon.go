package agentsessions

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// Argon (Google's Antigravity CLI, `agy`, running Gemini 4 Argon) keeps one
// untruncated append-only transcript per session at
// ~/.gemini/antigravity-cli/brain/<session-id>/.system_generated/logs/transcript_full.jsonl.
// Sibling transcript.jsonl truncates content/thinking and double-encodes tool
// arguments, and chunks/ holds per-step fragments, so only transcript_full.jsonl
// is the authoritative stream.
//
// Individual JSONL lines carry no cwd, session title, or parent session id.
// Those live in ~/.gemini/antigravity-cli/conversation_summaries.db and
// annotations/<session-id>.pbtxt, so discovery reads them once per pass and
// ships non-empty values on SessionFile.Session.
const argonTranscriptName = "transcript_full.jsonl"

var argonAnnotationTitleRe = regexp.MustCompile(`(?m)^\s*title:\s*"((?:\\.|[^"\\])*)"`)

type argonSummary struct {
	title           string
	cwd             string
	parentSessionID string
}

type argonSummaryCacheEntry struct {
	sig       fileSignature
	hasSig    bool
	summaries map[string]argonSummary
}

type argonMetaCache struct {
	byDB map[string]argonSummaryCacheEntry
}

func (c *argonMetaCache) summariesFor(agyRoot string) map[string]argonSummary {
	dbPath := filepath.Join(agyRoot, "conversation_summaries.db")
	sig, found := signatureOf(dbPath, dbPath+"-wal")
	if c.byDB == nil {
		c.byDB = map[string]argonSummaryCacheEntry{}
	}
	if cached, ok := c.byDB[dbPath]; ok && cached.hasSig == found && (!found || cached.sig == sig) {
		return cached.summaries
	}
	var summaries map[string]argonSummary
	if found {
		summaries = loadArgonSummaries(dbPath)
	} else {
		summaries = map[string]argonSummary{}
	}
	c.byDB[dbPath] = argonSummaryCacheEntry{sig: sig, hasSig: found, summaries: summaries}
	return summaries
}

func (c *argonMetaCache) sessionMeta(path, sessionID string) map[string]any {
	sessionDir := filepath.Dir(filepath.Dir(filepath.Dir(path)))
	agyRoot := argonRootDir(sessionDir)
	summary := c.summariesFor(agyRoot)[sessionID]
	if summary.title == "" {
		summary.title = readArgonAnnotationTitle(filepath.Join(agyRoot, "annotations", sessionID+".pbtxt"))
	}
	meta := map[string]any{}
	if summary.cwd != "" {
		meta["cwd"] = summary.cwd
	}
	if summary.title != "" {
		meta["title"] = summary.title
	}
	if summary.parentSessionID != "" {
		meta["parent_session_id"] = summary.parentSessionID
	}
	if len(meta) == 0 {
		return nil
	}
	return meta
}

func discoverArgon(root string, loadMeta bool) []SessionFile {
	if strings.TrimSpace(root) == "" {
		return nil
	}
	root = common.ExpandUser(root)
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	var files []SessionFile
	var cache argonMetaCache
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		sessionID := entry.Name()
		path := filepath.Join(root, sessionID, ".system_generated", "logs", argonTranscriptName)
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		file := sessionFileWithInfo(ArgonTool, sessionID, path, info)
		if loadMeta {
			file.Session = cache.sessionMeta(path, sessionID)
		}
		files = append(files, file)
	}
	return files
}

func argonRootDir(sessionDir string) string {
	parent := filepath.Dir(sessionDir)
	if filepath.Base(parent) == "brain" {
		return filepath.Dir(parent)
	}
	return parent
}

func loadArgonSummaries(dbPath string) map[string]argonSummary {
	out := map[string]argonSummary{}
	if _, err := os.Stat(dbPath); err != nil {
		return out
	}
	db, err := common.OpenSQLiteReadOnly(dbPath)
	if err != nil {
		return out
	}
	defer db.Close()
	rows, err := common.Query(db, `SELECT conversation_id, COALESCE(title, '') AS title,
		COALESCE(workspace_uris, '') AS workspace_uris,
		COALESCE(parent_conversation_id, '') AS parent_conversation_id
		FROM conversation_summaries`)
	if err != nil {
		return out
	}
	for _, row := range rows {
		id := strings.TrimSpace(row.String("conversation_id"))
		if id == "" {
			continue
		}
		out[id] = argonSummary{
			title:           strings.TrimSpace(row.String("title")),
			cwd:             firstWorkspacePath(row.String("workspace_uris")),
			parentSessionID: strings.TrimSpace(row.String("parent_conversation_id")),
		}
	}
	return out
}

func firstWorkspacePath(rawJSON string) string {
	var uris []string
	if err := json.Unmarshal([]byte(rawJSON), &uris); err != nil {
		return ""
	}
	for _, raw := range uris {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if strings.HasPrefix(raw, "file://") {
			if parsed, err := url.Parse(raw); err == nil && parsed.Path != "" {
				return parsed.Path
			}
			return strings.TrimPrefix(raw, "file://")
		}
		return raw
	}
	return ""
}

func readArgonAnnotationTitle(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	match := argonAnnotationTitleRe.FindSubmatch(data)
	if match == nil {
		return ""
	}
	var decoded string
	if err := json.Unmarshal([]byte(`"`+string(match[1])+`"`), &decoded); err == nil {
		return strings.TrimSpace(decoded)
	}
	return strings.TrimSpace(string(match[1]))
}
