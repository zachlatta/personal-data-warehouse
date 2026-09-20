package agentsessions

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// openClawStoreName is the agent's SQLite store. Since OpenClaw 2026.9 the
// transcript lives in its transcript_events table, one row per event whose
// event_json is exactly the line the "<sessionId>.jsonl" file used to hold;
// the sessions directory keeps only archives of deleted and reset sessions.
// A host still on the JSONL layout has no such file and nothing changes.
const openClawStoreName = "openclaw-agent.sqlite"

// defaultOpenClawStore is <agent root>/agent/openclaw-agent.sqlite, beside
// <agent root>/sessions, so disabling the sessions dir disables the store too.
func defaultOpenClawStore(sessionsDir string) string {
	if strings.TrimSpace(sessionsDir) == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(filepath.Clean(sessionsDir)), "agent", openClawStoreName)
}

// openClawStoreSession is one session's unshipped cursor. The rewrite
// generation is part of the key because a rewrite (compaction, rewind) reuses
// seq numbers for different events; a new generation re-ships the session and
// ingest dedupes by event id.
type openClawStoreSession struct {
	sessionID string
	stateKey  string
}

func (r *Runner) openClawStoreSessions(rows []common.Row) []openClawStoreSession {
	sessions := make([]openClawStoreSession, 0, len(rows))
	for _, row := range rows {
		id := row.String("session_id")
		sessions = append(sessions, openClawStoreSession{
			sessionID: id,
			stateKey:  "openclaw-store:" + r.Dirs.OpenClawStore + "#" + id + "@" + row.String("generation"),
		})
	}
	return sessions
}

// readOpenClawStore ships transcript_events rows past each session's cursor.
// It reports how many sessions the store holds.
func (r *Runner) readOpenClawStore(batch *pendingBatch, remaining *int, summary *Summary) error {
	path := r.Dirs.OpenClawStore
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	db, err := common.OpenSQLiteReadOnly(path)
	if err != nil {
		return &common.PermissionError{Path: path, Err: err}
	}
	defer db.Close()
	rows, err := common.Query(db, `SELECT e.session_id AS session_id, COALESCE(w.generation, '') AS generation
		FROM (SELECT DISTINCT session_id FROM transcript_events) e
		LEFT JOIN transcript_rewrite_watermarks w ON w.session_id = e.session_id
		ORDER BY e.session_id`)
	if err != nil {
		return err
	}
	sessions := r.openClawStoreSessions(rows)
	summary.FilesSeen += len(sessions)
	r.Logger.Infof("Discovered %d OpenClaw session(s) in %s", len(sessions), path)
	for _, session := range sessions {
		if r.Limit > 0 && *remaining <= 0 {
			summary.LimitReached = true
			return nil
		}
		_, nextSeq, err := r.startPosition(session.stateKey, 0)
		if err != nil {
			return err
		}
		events, err := common.Query(db, `SELECT seq, event_json FROM transcript_events
			WHERE session_id = ? AND seq >= ? ORDER BY seq`, session.sessionID, nextSeq)
		if err != nil {
			return err
		}
		file := SessionFile{Tool: OpenClawTool, SessionID: session.sessionID, Path: session.stateKey}
		selected := 0
		for _, event := range events {
			if r.Limit > 0 && *remaining <= 0 {
				summary.LimitReached = true
				break
			}
			seq := event.Int("seq")
			var parsed map[string]any
			if err := json.Unmarshal([]byte(event.String("event_json")), &parsed); err != nil || parsed == nil {
				r.Logger.Warningf("Skipping unparseable OpenClaw event %d in session %s", seq, session.sessionID)
				summary.LinesSkipped++
				continue
			}
			batch.add(r.envelope(file, seq, parsed), session.stateKey, 0, seq+1)
			selected++
			if r.Limit > 0 {
				*remaining--
			}
			if batch.full() {
				if err := r.flush(batch); err != nil {
					return err
				}
			}
		}
		if selected > 0 {
			summary.FilesWithNewLines++
			summary.LinesSelected += selected
		}
		if summary.LimitReached {
			return nil
		}
	}
	return nil
}
