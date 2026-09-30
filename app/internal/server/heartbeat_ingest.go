package server

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/push"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// uploaderFailureAlertRuns is how many failed runs IN A ROW make a push alert.
// A single failed run is a blip (a deploy, a Wi-Fi drop) and the row already
// reads failing; six is half an hour of a five-minute uploader. The alert
// fires once, on the run that reaches it, and a success resets the streak.
// Until 2026-09-30 nothing did this: a 625 MiB voice memo failed 37 runs in a
// row, the pipeline row read failing beside data_status=ok and last_error
// NULL, and nobody was told.
const uploaderFailureAlertRuns = 6

// uploaderHeartbeatEndpoint receives one row per uploader RUN from the machines
// that push data through /ingest/* (the Mac LaunchAgents, the openclaw VM
// timer). Those devices have no presence in the warehouse when a run has
// nothing to upload, so until this endpoint existed a laptop uploader that
// fired every five minutes and failed every time -- macOS revoking Full Disk
// Access on a uv python bump, for one -- was indistinguishable on /pipelines
// from a source that was merely quiet: `apple_voice_memos` read `late` for
// fifteen days with no way to say whether the uploader was healthy.
//
// The row is the run's own verdict (exit code, duration, error text), keyed by
// (pipeline, device), and marts_ops.pipeline_health reads it as the pipeline's
// run heartbeat exactly the way it reads a Dagster sync-state table.
const uploaderHeartbeatEndpoint = "/ingest/heartbeat"

type uploaderHeartbeat struct {
	Pipeline        string `json:"pipeline"`
	Device          string `json:"device"`
	RanAt           string `json:"ran_at"`
	ExitCode        int64  `json:"exit_code"`
	DurationSeconds int64  `json:"duration_seconds"`
	Error           string `json:"error"`
}

// uploaderHeartbeatStreak is the (pipeline, device) row's failure streak after
// the upsert: consecutive non-zero exits and when the streak began.
type uploaderHeartbeatStreak struct {
	ConsecutiveFailures int64
	FailingSince        time.Time
}

type uploaderHeartbeatWriter interface {
	upsert(context.Context, uploaderHeartbeat) (uploaderHeartbeatStreak, error)
}

type uploaderHeartbeatStore struct {
	db      *sql.DB
	timeout time.Duration
	// schema/relation default to the catalog's ops.uploader_heartbeats; the
	// integration test points them at an isolated pdw_test_* schema.
	schema   string
	relation string

	ensureMu sync.Mutex
	ensured  bool
}

func newUploaderHeartbeatStore(databaseURL string, timeout time.Duration) (*uploaderHeartbeatStore, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &uploaderHeartbeatStore{
		db:       db,
		timeout:  timeout,
		schema:   warehouse.SchemaOf("uploader_heartbeats"),
		relation: warehouse.SQLRelation("uploader_heartbeats"),
	}, nil
}

func (s *uploaderHeartbeatStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Identical to the Python TableSpec for `uploader_heartbeats` (text ” / bigint
// 0 / epoch-sentinel timestamptz defaults), so whichever side provisions first
// leaves the same table and the other side's CREATE IF NOT EXISTS is a no-op.
// The ALTERs bring a table created before the streak columns up to date.
func uploaderHeartbeatDDL(schema, relation string) []string {
	return []string{
		"CREATE SCHEMA IF NOT EXISTS " + warehouse.QuoteIdent(schema),
		`CREATE TABLE IF NOT EXISTS ` + relation + ` (
	pipeline text NOT NULL DEFAULT '',
	device text NOT NULL DEFAULT '',
	ran_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
	status text NOT NULL DEFAULT '',
	error text NOT NULL DEFAULT '',
	exit_code bigint NOT NULL DEFAULT 0,
	duration_seconds bigint NOT NULL DEFAULT 0,
	consecutive_failures bigint NOT NULL DEFAULT 0,
	failing_since timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
	updated_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
	sync_version bigint NOT NULL DEFAULT 0,
	PRIMARY KEY (pipeline, device)
)`,
		`ALTER TABLE ` + relation + ` ADD COLUMN IF NOT EXISTS consecutive_failures bigint NOT NULL DEFAULT 0`,
		`ALTER TABLE ` + relation + ` ADD COLUMN IF NOT EXISTS failing_since timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz`,
	}
}

// ensure runs the DDL once per process, not once per heartbeat: an ALTER
// takes an ACCESS EXCLUSIVE lock even when the column exists, and every
// uploader on every device posts every five minutes.
func (s *uploaderHeartbeatStore) ensure(ctx context.Context) error {
	s.ensureMu.Lock()
	defer s.ensureMu.Unlock()
	if s.ensured {
		return nil
	}
	for _, statement := range uploaderHeartbeatDDL(s.schema, s.relation) {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	s.ensured = true
	return nil
}

func (s *uploaderHeartbeatStore) upsert(ctx context.Context, hb uploaderHeartbeat) (uploaderHeartbeatStreak, error) {
	if err := s.ensure(ctx); err != nil {
		return uploaderHeartbeatStreak{}, err
	}
	ranAt, err := parseTimestampUTC(hb.RanAt)
	if err != nil {
		ranAt = time.Now().UTC()
	}
	status := "ok"
	if hb.ExitCode != 0 {
		status = "error"
	}
	// The streak is computed in the upsert itself, against the stored row, so
	// two devices or a replayed post cannot race it. A post older than the
	// stored one (sync_version) changes nothing and reads the stored streak.
	var streak uploaderHeartbeatStreak
	err = s.db.QueryRowContext(ctx, `
		INSERT INTO `+s.relation+` AS hb
			(pipeline, device, ran_at, status, error, exit_code, duration_seconds,
			 consecutive_failures, failing_since, updated_at, sync_version)
		VALUES ($1, $2, $3, $4, $5, $6, $7,
			CASE WHEN $6::bigint <> 0 THEN 1 ELSE 0 END,
			CASE WHEN $6::bigint <> 0 THEN $3::timestamptz ELSE '1970-01-01 00:00:00+00'::timestamptz END,
			now(), $8)
		ON CONFLICT (pipeline, device) DO UPDATE SET
			ran_at = EXCLUDED.ran_at,
			status = EXCLUDED.status,
			error = EXCLUDED.error,
			exit_code = EXCLUDED.exit_code,
			duration_seconds = EXCLUDED.duration_seconds,
			consecutive_failures = CASE WHEN EXCLUDED.exit_code <> 0 THEN hb.consecutive_failures + 1 ELSE 0 END,
			failing_since = CASE
				WHEN EXCLUDED.exit_code = 0 THEN '1970-01-01 00:00:00+00'::timestamptz
				WHEN hb.consecutive_failures = 0 THEN EXCLUDED.ran_at
				ELSE hb.failing_since
			END,
			updated_at = now(),
			sync_version = EXCLUDED.sync_version
		WHERE hb.sync_version <= EXCLUDED.sync_version
		RETURNING consecutive_failures, failing_since
	`, hb.Pipeline, hb.Device, ranAt, status, hb.Error, hb.ExitCode, hb.DurationSeconds, ranAt.UnixMicro()).Scan(&streak.ConsecutiveFailures, &streak.FailingSince)
	if err == sql.ErrNoRows {
		// A stale replay: the stored row is newer and was left alone.
		return uploaderHeartbeatStreak{}, nil
	}
	return streak, err
}

// uploaderFailingNotification is the one push a failure streak earns.
func uploaderFailingNotification(hb uploaderHeartbeat, streak uploaderHeartbeatStreak) push.Notification {
	body := hb.Error
	if body == "" {
		body = fmt.Sprintf("exit code %d, no error text reported", hb.ExitCode)
	}
	const maxBody = 200
	if len(body) > maxBody {
		body = body[:maxBody-1] + "…"
	}
	subtitle := fmt.Sprintf("%d runs in a row on %s", streak.ConsecutiveFailures, hb.Device)
	if !streak.FailingSince.IsZero() && streak.FailingSince.Unix() > 0 {
		subtitle += " since " + streak.FailingSince.UTC().Format("Jan 2 15:04 UTC")
	}
	return push.Notification{
		Title:      "Uploader failing: " + hb.Pipeline,
		Subtitle:   subtitle,
		Body:       body,
		Route:      "/pipelines",
		ThreadID:   "uploader-failing",
		CollapseID: "uploader-failing:" + hb.Pipeline + ":" + hb.Device,
		Data: map[string]any{
			"kind":                 "uploader_failing",
			"pipeline":             hb.Pipeline,
			"device":               hb.Device,
			"consecutive_failures": streak.ConsecutiveFailures,
		},
	}
}

// heartbeatIngestService is the credential endpoint's shape over a different
// row: the same HMAC upload signature (endpoint + body sha + expiry), so every
// uploader already holds what it needs to post one.
type heartbeatIngestService struct {
	store    uploaderHeartbeatWriter
	signer   *pdwauth.Service
	maxBytes int64
	timeout  time.Duration
	logger   *slog.Logger
	// notify delivers the failure-streak alert; nil disables it.
	notify func(push.Notification)
}

func (svc *heartbeatIngestService) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		endpoint := path.Clean(r.URL.Path)
		q := r.URL.Query()
		declaredSHA := q.Get("content_sha256")
		if err := svc.signer.VerifyObjectUpload(endpoint, declaredSHA, q.Get("exp"), q.Get("sig")); err != nil {
			svc.logger.WarnContext(r.Context(), "heartbeat upload link rejected", "endpoint", endpoint, "error", err)
			http.Error(w, "invalid or expired upload link", http.StatusForbidden)
			return
		}
		body, err := readLimited(r.Body, svc.maxBytes)
		if err == errTooLarge {
			http.Error(w, "object too large", http.StatusRequestEntityTooLarge)
			return
		}
		if err != nil {
			http.Error(w, "could not read body", http.StatusBadRequest)
			return
		}
		if hex.EncodeToString(sha256Sum(body)) != declaredSHA {
			http.Error(w, "content_sha256 does not match body", http.StatusBadRequest)
			return
		}
		var hb uploaderHeartbeat
		if err := json.Unmarshal(body, &hb); err != nil {
			http.Error(w, "invalid heartbeat json", http.StatusBadRequest)
			return
		}
		hb.Pipeline = strings.TrimSpace(hb.Pipeline)
		hb.Device = strings.TrimSpace(hb.Device)
		hb.Error = strings.TrimSpace(hb.Error)
		if len(hb.Error) > 500 {
			hb.Error = hb.Error[:500]
		}
		if hb.Pipeline == "" || hb.Device == "" {
			http.Error(w, "pipeline and device are required", http.StatusBadRequest)
			return
		}
		if hb.ExitCode < 0 {
			http.Error(w, "exit_code must be non-negative", http.StatusBadRequest)
			return
		}
		timeout := svc.timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		streak, err := svc.store.upsert(ctx, hb)
		if err != nil {
			svc.logger.ErrorContext(r.Context(), "heartbeat store write failed", "endpoint", endpoint, "error", err)
			http.Error(w, "heartbeat store error", http.StatusBadGateway)
			return
		}
		svc.logger.InfoContext(r.Context(), "uploader heartbeat stored", "pipeline", hb.Pipeline, "device", hb.Device, "exit_code", hb.ExitCode, "consecutive_failures", streak.ConsecutiveFailures)
		if streak.ConsecutiveFailures == uploaderFailureAlertRuns && svc.notify != nil {
			svc.notify(uploaderFailingNotification(hb, streak))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"ok":true}`)
	})
}
