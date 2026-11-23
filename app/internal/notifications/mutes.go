package notifications

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// A source mute silences one timeline source's alerts (`slack`, `gmail`, ...:
// timeline.events.source) until a stated time, and then ends by itself: a
// vacation needs no one to remember to turn it back on. While it holds, the
// source's captured events are fanned out to no device and recorded with
// status `muted`; a delivery queued before the mute is skipped as
// `suppressed` / `source_muted`. Nothing is replayed when it ends: a week of
// Slack is not news on the Monday back, and the timeline keeps every event.
//
// Mutation alerts (MutationAdapter) are not timeline notifications and no
// mute holds them, the same as the global switch.

// MaxSourceMute bounds a mute's end, so a forgotten one cannot become a
// permanent off switch.
const MaxSourceMute = 90 * 24 * time.Hour

// The Go app deploys before the Python ensure path runs, and the worker reads
// this table every tick, so the notification service creates it at startup
// too. src/personal_data_warehouse/notifications.py holds the same statement
// (MUTES_TABLE_SQL), pinned by tests/test_timeline_notifications.py.
const notificationMutesDDL = `CREATE TABLE IF NOT EXISTS @notification_mutes (
        source text PRIMARY KEY,
        muted_until timestamptz NOT NULL,
        reason text NOT NULL DEFAULT '',
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now()
    )`

// sourceMuted is the condition an outbox event (aliased n) is muted on.
const sourceMuted = `(n.adapter <> '` + MutationAdapter + `' AND EXISTS (SELECT 1 FROM @notification_mutes m WHERE m.source = n.source AND m.muted_until > now()))`

var sourceName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

type SourceMute struct {
	Source     string    `json:"source"`
	MutedUntil time.Time `json:"muted_until"`
	Reason     string    `json:"reason"`
	Active     bool      `json:"active"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func ensureMutes(ctx context.Context, db *sql.DB, expand func(string) string) error {
	if expand == nil {
		if _, err := db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+warehouse.QuoteIdent(warehouse.SchemaOf("notification_mutes"))); err != nil {
			return err
		}
		expand = warehouse.ExpandRelations
	}
	_, err := db.ExecContext(ctx, expand(notificationMutesDDL))
	return err
}

// deliveryMuted reports whether a queued delivery's source is muted now.
func (s *Service) deliveryMuted(ctx context.Context, d Delivery) (bool, error) {
	var muted bool
	err := s.DB.QueryRowContext(ctx, s.q(`SELECT EXISTS (SELECT 1 FROM @notification_events n WHERE n.id=$1 AND `+sourceMuted+`)`), d.NotificationID).Scan(&muted)
	return muted, err
}

func (s *Service) listMutes(w http.ResponseWriter, r *http.Request) {
	rows, err := s.DB.QueryContext(r.Context(), s.q(`SELECT source,muted_until,reason,muted_until>now(),created_at,updated_at FROM @notification_mutes ORDER BY muted_until DESC, source`))
	if err != nil {
		unavailable(w)
		return
	}
	defer rows.Close()
	mutes := []SourceMute{}
	for rows.Next() {
		var m SourceMute
		if rows.Scan(&m.Source, &m.MutedUntil, &m.Reason, &m.Active, &m.CreatedAt, &m.UpdatedAt) != nil {
			unavailable(w)
			return
		}
		mutes = append(mutes, utcMute(m))
	}
	if rows.Err() != nil {
		unavailable(w)
		return
	}
	writeJSON(w, map[string]any{"mutes": mutes})
}

func (s *Service) setMute(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source string `json:"source"`
		Until  string `json:"until"`
		Reason string `json:"reason"`
	}
	if !decode(w, r, &input) {
		return
	}
	source := strings.TrimSpace(input.Source)
	if !sourceName.MatchString(source) {
		http.Error(w, "source must be a timeline source such as slack or gmail", 400)
		return
	}
	until, err := time.Parse(time.RFC3339, strings.TrimSpace(input.Until))
	if err != nil {
		http.Error(w, "until is required: the RFC 3339 time the mute ends, e.g. 2027-01-04T14:00:00Z", 400)
		return
	}
	now := time.Now()
	if !until.After(now) || until.After(now.Add(MaxSourceMute)) {
		http.Error(w, "until must be in the future and at most 90 days away", 400)
		return
	}
	var mutation bool
	var known []string
	err = s.DB.QueryRowContext(r.Context(), s.q(`SELECT EXISTS (SELECT 1 FROM @notification_events WHERE source=$1 AND adapter='`+MutationAdapter+`')`), source).Scan(&mutation)
	if err != nil {
		unavailable(w)
		return
	}
	if mutation {
		http.Error(w, "mutation alerts are not timeline notifications and cannot be muted", 400)
		return
	}
	// A typo would mute nothing and look like it worked, so only a source
	// that has paged before can be muted.
	rows, err := s.DB.QueryContext(r.Context(), s.q(`SELECT DISTINCT source FROM @notification_events WHERE adapter <> '`+MutationAdapter+`'`))
	if err != nil {
		unavailable(w)
		return
	}
	for rows.Next() {
		var name string
		if rows.Scan(&name) != nil {
			rows.Close()
			unavailable(w)
			return
		}
		known = append(known, name)
	}
	rows.Close()
	if rows.Err() != nil {
		unavailable(w)
		return
	}
	found := false
	for _, name := range known {
		found = found || name == source
	}
	if !found {
		sort.Strings(known)
		http.Error(w, "source "+source+" has never sent a notification; known sources: "+strings.Join(known, ", "), 400)
		return
	}
	reason := []rune(strings.TrimSpace(input.Reason))
	var m SourceMute
	err = s.DB.QueryRowContext(r.Context(), s.q(`INSERT INTO @notification_mutes (source,muted_until,reason) VALUES ($1,$2,$3)
 ON CONFLICT (source) DO UPDATE SET muted_until=EXCLUDED.muted_until,reason=EXCLUDED.reason,updated_at=now()
 RETURNING source,muted_until,reason,muted_until>now(),created_at,updated_at`), source, until.UTC(), string(reason[:min(200, len(reason))])).
		Scan(&m.Source, &m.MutedUntil, &m.Reason, &m.Active, &m.CreatedAt, &m.UpdatedAt)
	if err != nil {
		unavailable(w)
		return
	}
	writeJSON(w, utcMute(m))
}

// endMute ends an active mute now. The row stays, as the record of the mute.
func (s *Service) endMute(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	var ended string
	err := s.DB.QueryRowContext(r.Context(), s.q(`UPDATE @notification_mutes SET muted_until=now(),updated_at=now() WHERE source=$1 AND muted_until>now() RETURNING source`), source).Scan(&ended)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "no active mute for source "+source, 404)
		return
	}
	if err != nil {
		unavailable(w)
		return
	}
	writeJSON(w, map[string]any{"source": ended, "ended": true})
}

func utcMute(m SourceMute) SourceMute {
	m.MutedUntil, m.CreatedAt, m.UpdatedAt = m.MutedUntil.UTC(), m.CreatedAt.UTC(), m.UpdatedAt.UTC()
	return m
}
