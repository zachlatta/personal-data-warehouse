package x

import (
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// State is the uploader's memory between runs: which (stream, post) relations
// it has already shipped, and when it first saw each (X stamps no time on a
// like or a bookmark, so that first sighting is the time the warehouse keeps),
// and how far each stream's history backfill has walked.
type State struct {
	db *common.StateDB
}

// DefaultStateFile is the state path.
func DefaultStateFile() string {
	return common.DefaultStateFile("x-upload-state.sqlite")
}

// OpenState opens the state for the account.
func OpenState(path, account string) (*State, error) {
	db, err := common.OpenStateDB(path, []string{
		`CREATE TABLE IF NOT EXISTS seen (
			stream TEXT NOT NULL,
			post_id TEXT NOT NULL,
			discovered_at TEXT NOT NULL,
			PRIMARY KEY (stream, post_id)
		)`,
		`CREATE TABLE IF NOT EXISTS backfill (
			stream TEXT PRIMARY KEY,
			cursor TEXT NOT NULL DEFAULT '',
			done INTEGER NOT NULL DEFAULT 0,
			updated_at TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS catchup (
			stream TEXT NOT NULL,
			cursor TEXT NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY (stream, cursor)
		)`,
		`CREATE TABLE IF NOT EXISTS cooldown (
			scope TEXT PRIMARY KEY,
			until TEXT NOT NULL,
			reason TEXT NOT NULL DEFAULT ''
		)`,
	}, map[string]string{"schema_version": "1", "account": account}, []string{"seen", "backfill", "catchup", "cooldown"})
	if err != nil {
		return nil, err
	}
	return &State{db: db}, nil
}

// Close releases the database.
func (s *State) Close() error { return s.db.Close() }

// Discovered is when the uploader first shipped (stream, post), if it has.
func (s *State) Discovered(stream, postID string) (time.Time, bool, error) {
	rows, err := common.Query(s.db.DB, "SELECT discovered_at FROM seen WHERE stream = ? AND post_id = ?", stream, postID)
	if err != nil || len(rows) == 0 {
		return time.Time{}, false, err
	}
	at, err := time.Parse(time.RFC3339Nano, rows[0].String("discovered_at"))
	if err != nil {
		return time.Time{}, false, nil
	}
	return at.UTC(), true, nil
}

// MarkDiscovered records a shipped relation (first sighting wins).
func (s *State) MarkDiscovered(stream, postID string, at time.Time) error {
	_, err := s.db.DB.Exec(`INSERT INTO seen (stream, post_id, discovered_at) VALUES (?, ?, ?)
		ON CONFLICT(stream, post_id) DO NOTHING`, stream, postID, at.UTC().Format(time.RFC3339Nano))
	return err
}

// BackfillState is one stream's walk into its history.
type BackfillState struct {
	Started bool
	Cursor  string
	Done    bool
}

// Backfill reads one stream's backfill position.
func (s *State) Backfill(stream string) (BackfillState, error) {
	rows, err := common.Query(s.db.DB, "SELECT cursor, done FROM backfill WHERE stream = ?", stream)
	if err != nil || len(rows) == 0 {
		return BackfillState{}, err
	}
	return BackfillState{Started: true, Cursor: rows[0].String("cursor"), Done: rows[0].Int("done") != 0}, nil
}

// SetBackfill records where a stream's backfill stands.
func (s *State) SetBackfill(stream, cursor string, done bool, now time.Time) error {
	doneFlag := 0
	if done {
		doneFlag = 1
	}
	_, err := s.db.DB.Exec(`INSERT INTO backfill (stream, cursor, done, updated_at) VALUES (?, ?, ?, ?)
		ON CONFLICT(stream) DO UPDATE SET cursor = excluded.cursor, done = excluded.done, updated_at = excluded.updated_at`,
		stream, cursor, doneFlag, common.ISOFormat(now))
	return err
}

// Catchups are a stream's open gaps, oldest first: cursors a run stopped at
// while its pages still held items it had never shipped.
func (s *State) Catchups(stream string) ([]string, error) {
	rows, err := common.Query(s.db.DB, "SELECT cursor FROM catchup WHERE stream = ? ORDER BY created_at, cursor", stream)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.String("cursor"))
	}
	return out, nil
}

// ReplaceCatchup moves a gap's position (old "" opens a new gap), keeping
// when it opened so the oldest gap is resumed first.
func (s *State) ReplaceCatchup(stream, old, next string, now time.Time) error {
	created := common.ISOFormat(now)
	if old != "" {
		rows, err := common.Query(s.db.DB, "SELECT created_at FROM catchup WHERE stream = ? AND cursor = ?", stream, old)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			created = rows[0].String("created_at")
		}
		if _, err := s.db.DB.Exec("DELETE FROM catchup WHERE stream = ? AND cursor = ?", stream, old); err != nil {
			return err
		}
	}
	_, err := s.db.DB.Exec(`INSERT INTO catchup (stream, cursor, created_at) VALUES (?, ?, ?)
		ON CONFLICT(stream, cursor) DO NOTHING`, stream, next, created)
	return err
}

// RemoveCatchup closes a gap: its walk reached items already shipped.
func (s *State) RemoveCatchup(stream, cursor string) error {
	_, err := s.db.DB.Exec("DELETE FROM catchup WHERE stream = ? AND cursor = ?", stream, cursor)
	return err
}

// RunScope is the cooldown scope that holds every request, not one stream's.
const RunScope = "run"

// Cooldown is when a scope (a stream, or RunScope) may next call x.com, and why.
func (s *State) Cooldown(scope string) (time.Time, string, error) {
	rows, err := common.Query(s.db.DB, "SELECT until, reason FROM cooldown WHERE scope = ?", scope)
	if err != nil || len(rows) == 0 {
		return time.Time{}, "", err
	}
	until, err := time.Parse(time.RFC3339Nano, rows[0].String("until"))
	if err != nil {
		return time.Time{}, "", nil
	}
	return until.UTC(), rows[0].String("reason"), nil
}

// SetCooldown holds a scope off x.com until a time.
func (s *State) SetCooldown(scope string, until time.Time, reason string) error {
	_, err := s.db.DB.Exec(`INSERT INTO cooldown (scope, until, reason) VALUES (?, ?, ?)
		ON CONFLICT(scope) DO UPDATE SET until = excluded.until, reason = excluded.reason`,
		scope, until.UTC().Format(time.RFC3339Nano), reason)
	return err
}

// ClearCooldown lifts a scope's cooldown (after signing back in).
func (s *State) ClearCooldown(scope string) error {
	_, err := s.db.DB.Exec("DELETE FROM cooldown WHERE scope = ?", scope)
	return err
}

// ResetBackfill starts a stream's history walk over (`--mode full`).
func (s *State) ResetBackfill(stream string) error {
	_, err := s.db.DB.Exec("DELETE FROM backfill WHERE stream = ?", stream)
	return err
}
