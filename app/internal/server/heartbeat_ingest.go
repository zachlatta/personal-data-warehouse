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
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

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

type uploaderHeartbeatWriter interface {
	upsert(context.Context, uploaderHeartbeat) error
}

type uploaderHeartbeatStore struct {
	db      *sql.DB
	timeout time.Duration
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
	return &uploaderHeartbeatStore{db: db, timeout: timeout}, nil
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
var uploaderHeartbeatSchemaDDL = "CREATE SCHEMA IF NOT EXISTS " +
	warehouse.QuoteIdent(warehouse.SchemaOf("uploader_heartbeats"))

var uploaderHeartbeatDDL = `CREATE TABLE IF NOT EXISTS ` + warehouse.SQLRelation("uploader_heartbeats") + ` (
	pipeline text NOT NULL DEFAULT '',
	device text NOT NULL DEFAULT '',
	ran_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
	status text NOT NULL DEFAULT '',
	error text NOT NULL DEFAULT '',
	exit_code bigint NOT NULL DEFAULT 0,
	duration_seconds bigint NOT NULL DEFAULT 0,
	updated_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
	sync_version bigint NOT NULL DEFAULT 0,
	PRIMARY KEY (pipeline, device)
)`

func (s *uploaderHeartbeatStore) ensure(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, uploaderHeartbeatSchemaDDL); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, uploaderHeartbeatDDL)
	return err
}

func (s *uploaderHeartbeatStore) upsert(ctx context.Context, hb uploaderHeartbeat) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	ranAt, err := parseTimestampUTC(hb.RanAt)
	if err != nil {
		ranAt = time.Now().UTC()
	}
	status := "ok"
	if hb.ExitCode != 0 {
		status = "error"
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO `+warehouse.SQLRelation("uploader_heartbeats")+`
			(pipeline, device, ran_at, status, error, exit_code, duration_seconds, updated_at, sync_version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now(), $8)
		ON CONFLICT (pipeline, device) DO UPDATE SET
			ran_at = EXCLUDED.ran_at,
			status = EXCLUDED.status,
			error = EXCLUDED.error,
			exit_code = EXCLUDED.exit_code,
			duration_seconds = EXCLUDED.duration_seconds,
			updated_at = now(),
			sync_version = EXCLUDED.sync_version
		WHERE `+warehouse.SQLRelation("uploader_heartbeats")+`.sync_version <= EXCLUDED.sync_version
	`, hb.Pipeline, hb.Device, ranAt, status, hb.Error, hb.ExitCode, hb.DurationSeconds, ranAt.UnixMicro())
	return err
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
		if err := svc.store.upsert(ctx, hb); err != nil {
			svc.logger.ErrorContext(r.Context(), "heartbeat store write failed", "endpoint", endpoint, "error", err)
			http.Error(w, "heartbeat store error", http.StatusBadGateway)
			return
		}
		svc.logger.InfoContext(r.Context(), "uploader heartbeat stored", "pipeline", hb.Pipeline, "device", hb.Device, "exit_code", hb.ExitCode)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"ok":true}`)
	})
}
