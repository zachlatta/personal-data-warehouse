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

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// sourceObservationEndpoint receives what a device's OWN store says is its
// newest item, from the uploader that just read it (contract C11). A Mac
// uploader is the only thing that can see chat.db; without this, PDW could
// say only that the uploader ran, never that what the Mac holds has landed.
// The row lands in ops.source_upstream_observations, and
// marts_ops.source_upstream_health compares it with base_*.
const sourceObservationEndpoint = "/ingest/source-observation"

type sourceObservation struct {
	Pipeline         string `json:"pipeline"`
	Scope            string `json:"scope"`
	UpstreamNewestAt string `json:"upstream_newest_at"`
	UpstreamNewestID string `json:"upstream_newest_id"`
	Detail           string `json:"detail"`
	ObservedAt       string `json:"observed_at"`
}

type sourceObservationWriter interface {
	upsert(context.Context, sourceObservation) error
}

type sourceObservationStore struct {
	db       *sql.DB
	schema   string
	relation string

	ensureMu sync.Mutex
	ensured  bool
}

func newSourceObservationStore(databaseURL string, timeout time.Duration) (*sourceObservationStore, error) {
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
	return &sourceObservationStore{
		db:       db,
		schema:   warehouse.SchemaOf("source_upstream_observations"),
		relation: warehouse.SQLRelation("source_upstream_observations"),
	}, nil
}

func (s *sourceObservationStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Identical to the Python TableSpec for `source_upstream_observations`, so
// whichever side provisions first leaves the same table: the app deploys
// before the Python ensure path runs.
func sourceObservationDDL(schema, relation string) []string {
	return []string{
		"CREATE SCHEMA IF NOT EXISTS " + warehouse.QuoteIdent(schema),
		`CREATE TABLE IF NOT EXISTS ` + relation + ` (
	pipeline text NOT NULL DEFAULT '',
	scope text NOT NULL DEFAULT '',
	upstream_newest_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
	upstream_newest_id text NOT NULL DEFAULT '',
	detail text NOT NULL DEFAULT '',
	observed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
	updated_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
	sync_version bigint NOT NULL DEFAULT 0,
	PRIMARY KEY (pipeline, scope)
)`,
	}
}

func (s *sourceObservationStore) ensure(ctx context.Context) error {
	s.ensureMu.Lock()
	defer s.ensureMu.Unlock()
	if s.ensured {
		return nil
	}
	for _, statement := range sourceObservationDDL(s.schema, s.relation) {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	s.ensured = true
	return nil
}

func (s *sourceObservationStore) upsert(ctx context.Context, obs sourceObservation) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	observedAt, err := parseTimestampUTC(obs.ObservedAt)
	if err != nil {
		observedAt = time.Now().UTC()
	}
	// An empty newest means the store holds nothing: the epoch, as everywhere.
	newestAt := time.Unix(0, 0).UTC()
	if strings.TrimSpace(obs.UpstreamNewestAt) != "" {
		if parsed, err := parseTimestampUTC(obs.UpstreamNewestAt); err == nil {
			newestAt = parsed
		}
	}
	// A replayed older observation never overwrites a newer one.
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO `+s.relation+` AS o
			(pipeline, scope, upstream_newest_at, upstream_newest_id, detail, observed_at, updated_at, sync_version)
		VALUES ($1, $2, $3, $4, $5, $6, now(), $7)
		ON CONFLICT (pipeline, scope) DO UPDATE SET
			upstream_newest_at = EXCLUDED.upstream_newest_at,
			upstream_newest_id = EXCLUDED.upstream_newest_id,
			detail = EXCLUDED.detail,
			observed_at = EXCLUDED.observed_at,
			updated_at = now(),
			sync_version = EXCLUDED.sync_version
		WHERE o.sync_version <= EXCLUDED.sync_version
	`, obs.Pipeline, obs.Scope, newestAt, obs.UpstreamNewestID, obs.Detail, observedAt, observedAt.UnixMicro())
	return err
}

// sourceObservationIngestService takes the same HMAC upload signature as the
// heartbeat endpoint, so every uploader already holds what it needs to post.
type sourceObservationIngestService struct {
	store    sourceObservationWriter
	signer   *pdwauth.Service
	maxBytes int64
	timeout  time.Duration
	logger   *slog.Logger
}

func (svc *sourceObservationIngestService) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		endpoint := path.Clean(r.URL.Path)
		q := r.URL.Query()
		declaredSHA := q.Get("content_sha256")
		if err := svc.signer.VerifyObjectUpload(endpoint, declaredSHA, q.Get("exp"), q.Get("sig")); err != nil {
			svc.logger.WarnContext(r.Context(), "source observation link rejected", "endpoint", endpoint, "error", err)
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
		var obs sourceObservation
		if err := json.Unmarshal(body, &obs); err != nil {
			http.Error(w, "invalid observation json", http.StatusBadRequest)
			return
		}
		obs.Pipeline = strings.TrimSpace(obs.Pipeline)
		obs.Scope = strings.TrimSpace(obs.Scope)
		obs.UpstreamNewestID = strings.TrimSpace(obs.UpstreamNewestID)
		obs.Detail = strings.TrimSpace(obs.Detail)
		if len(obs.Detail) > 500 {
			obs.Detail = obs.Detail[:500]
		}
		if obs.Pipeline == "" || obs.Scope == "" {
			http.Error(w, "pipeline and scope are required", http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(obs.UpstreamNewestAt) != "" {
			if _, err := parseTimestampUTC(obs.UpstreamNewestAt); err != nil {
				http.Error(w, "upstream_newest_at must be RFC 3339", http.StatusBadRequest)
				return
			}
		}
		timeout := svc.timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := svc.store.upsert(ctx, obs); err != nil {
			svc.logger.ErrorContext(r.Context(), "source observation write failed", "endpoint", endpoint, "error", err)
			http.Error(w, "source observation store error", http.StatusBadGateway)
			return
		}
		svc.logger.InfoContext(r.Context(), "source observation stored", "pipeline", obs.Pipeline, "scope", obs.Scope)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"ok":true}`)
	})
}
