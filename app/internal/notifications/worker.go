// Package notifications drains a transactional, insert-only timeline outbox.
// The Python warehouse owns DDL. Transport acceptance is NOT device display.
package notifications

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/zachlatta/personal-data-warehouse/app/internal/deeplink"
	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

type Alert struct {
	Title          string         `json:"title"`
	Subtitle       string         `json:"subtitle"`
	Body           string         `json:"body"`
	Icon           string         `json:"icon"`
	Route          string         `json:"route"`
	Open           *deeplink.Link `json:"open,omitempty"`
	ThreadID       string         `json:"thread_id"`
	NotificationID string         `json:"notification_id"`
	DeliveryID     string         `json:"delivery_id"`
	OpenProof      string         `json:"open_proof"`
}

type Delivery struct {
	ID, NotificationID, DeviceID, Transport, Status, LeaseID, TicketID string
	Endpoint                                                           json.RawMessage
	Alert                                                              Alert
	Attempts                                                           int
	AcceptedAt                                                         time.Time
}

type Result struct {
	Status, TicketID, Error string
	Disable                 bool
}
type Sender interface {
	Send(context.Context, Delivery) Result
	Receipt(context.Context, Delivery) Result
}

type Service struct {
	DB        *sql.DB
	Secret    []byte
	PublicKey string
	Sender    Sender
	Render    func(map[string]any) Alert
	// Expand is overridden only by integration tests with isolated schemas.
	Expand func(string) string
}

func New(databaseURL string, secret []byte, sender Sender, render func(map[string]any) Alert) (*Service, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(3)
	return &Service{DB: db, Secret: secret, Sender: sender, Render: render}, nil
}
func (s *Service) Close() error { return s.DB.Close() }
func (s *Service) q(query string) string {
	if s.Expand != nil {
		return s.Expand(query)
	}
	return warehouse.ExpandRelations(query)
}
func deviceID(value string) string {
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])
}
func (s *Service) openProof(id string) string {
	h := hmac.New(sha256.New, s.Secret)
	h.Write([]byte("notification-open-v1:" + id))
	return hex.EncodeToString(h.Sum(nil))
}
func (s *Service) validProof(id, proof string) bool {
	return len(s.Secret) > 0 && hmac.Equal([]byte(s.openProof(id)), []byte(proof))
}
func retryDelay(attempt int) time.Duration {
	return time.Duration(10*(1<<min(max(attempt-1, 0), 5))) * time.Second
}

// Run stops with the HTTP process, and does not silently activate the experiment.
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		runCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		err := s.Tick(runCtx)
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Error("timeline notification worker failed", "error", err)
			c, done := context.WithTimeout(ctx, 5*time.Second)
			// Do not persist raw driver/provider errors, which can contain endpoints.
			_, _ = s.DB.ExecContext(c, s.q(`UPDATE @notification_state SET status='error', error='worker failed; inspect server logs', last_run_at=now() WHERE id='timeline'`))
			done()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) Tick(ctx context.Context) error {
	var enabled bool
	if err := s.DB.QueryRowContext(ctx, s.q(`SELECT enabled=1 FROM @notification_state WHERE id='timeline'`)).Scan(&enabled); err != nil {
		return err
	}
	if enabled {
		// A process can die after its final send but before storing the verdict.
		// Bound crash recovery too; do not create an infinite retrying lease.
		_, err := s.DB.ExecContext(ctx, s.q(`UPDATE @notification_deliveries SET status='unknown',error='send attempts exhausted after interrupted worker',updated_at=now() WHERE status='sending' AND lease_until<now() AND attempt_count>=6`))
		if err != nil {
			return err
		}
		for range 100 {
			ok, err := s.fanout(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
		}
		for range 100 {
			d, err := s.claim(ctx, false)
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			if err != nil {
				return err
			}
			active, err := s.deliveryActive(ctx, d)
			if err != nil {
				return err
			}
			result := Result{Status: "cancelled", Error: "device disabled or experiment paused"}
			if active {
				reason, err := s.suppressionReason(ctx, d)
				if err != nil {
					return err
				}
				if reason != "" {
					result = Result{Status: "suppressed", Error: reason}
				} else {
					result = s.Sender.Send(ctx, d)
				}
			}
			if err = s.settle(ctx, d, result, false); err != nil {
				return err
			}
		}
	}
	// Receipt checks remain useful while paused, and never send another alert.
	for range 50 {
		d, err := s.claim(ctx, true)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		result := s.Sender.Receipt(ctx, d)
		if result.Status == "waiting" && time.Since(d.AcceptedAt) > 23*time.Hour {
			result = Result{Status: "unknown", Error: "Expo receipt unavailable after 23 hours"}
		}
		if err = s.settle(ctx, d, result, true); err != nil {
			return err
		}
	}
	_, err := s.DB.ExecContext(ctx, s.q(`UPDATE @notification_state SET last_run_at=now(), status=CASE WHEN enabled=1 THEN 'ok' ELSE 'paused' END, error='' WHERE id='timeline'`))
	return err
}

// fanout commits the fixed recipient set atomically with the outbox verdict.
func (s *Service) fanout(ctx context.Context) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var enabled bool
	if err = tx.QueryRowContext(ctx, s.q(`SELECT enabled=1 FROM @notification_state WHERE id='timeline' FOR SHARE`)).Scan(&enabled); err != nil {
		return false, err
	}
	if !enabled {
		return false, nil
	}
	var id string
	var payload []byte
	err = tx.QueryRowContext(ctx, s.q(`SELECT id,payload FROM @notification_events WHERE status='pending' AND EXISTS (SELECT 1 FROM @notification_state WHERE id='timeline' AND enabled=1) ORDER BY created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`)).Scan(&id, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var row map[string]any
	if err = json.Unmarshal(payload, &row); err != nil {
		return false, err
	}
	alert := s.Render(row)
	alert.NotificationID = id
	rows, err := tx.QueryContext(ctx, s.q(`SELECT 'expo', expo_push_token, jsonb_build_object('token',expo_push_token) FROM @push_devices WHERE status='active'
 UNION ALL SELECT 'web', id, subscription FROM @web_push_devices WHERE status='active'`))
	if err != nil {
		return false, err
	}
	type target struct {
		transport, id string
		endpoint      []byte
	}
	var targets []target
	for rows.Next() {
		var d target
		if err = rows.Scan(&d.transport, &d.id, &d.endpoint); err != nil {
			rows.Close()
			return false, err
		}
		targets = append(targets, d)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	for _, d := range targets {
		var deliveryID string
		if err = tx.QueryRowContext(ctx, `SELECT gen_random_uuid()::text`).Scan(&deliveryID); err != nil {
			return false, err
		}
		alert.DeliveryID = deliveryID
		alert.OpenProof = s.openProof(deliveryID)
		raw, err := json.Marshal(alert)
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, s.q(`INSERT INTO @notification_deliveries (id,notification_id,device_id,transport,endpoint,payload) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (notification_id,device_id) DO NOTHING`), deliveryID, id, deviceID(d.transport+":"+d.id), d.transport, d.endpoint, raw)
		if err != nil {
			return false, err
		}
	}
	status := "fanned_out"
	if len(targets) == 0 {
		status = "no_devices"
	}
	preview, err := json.Marshal(map[string]string{"title": alert.Title, "subtitle": alert.Subtitle, "body": alert.Body, "icon": alert.Icon, "route": alert.Route})
	if err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, s.q(`UPDATE @notification_events SET status=$2,payload=jsonb_set(payload,'{presentation}',$3::jsonb) WHERE id=$1`), id, status, preview)
	if err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func (s *Service) claim(ctx context.Context, receipt bool) (Delivery, error) {
	predicate := `status IN ('pending','retry','sending') AND next_attempt_at<=now() AND lease_until<now() AND EXISTS (SELECT 1 FROM @notification_state WHERE id='timeline' AND enabled=1)`
	status := "sending"
	if receipt {
		predicate = `status='accepted' AND transport='expo' AND ticket_id<>'' AND next_attempt_at<=now() AND lease_until<now()`
		status = "accepted"
	}
	query := `WITH candidate AS (SELECT id FROM @notification_deliveries WHERE ` + predicate + ` ORDER BY next_attempt_at,id LIMIT 1 FOR UPDATE SKIP LOCKED)
 UPDATE @notification_deliveries d SET status=$1,lease_id=gen_random_uuid()::text,lease_until=now()+interval '2 minutes',updated_at=now(),
 attempt_count=attempt_count+CASE WHEN $2 THEN 0 ELSE 1 END FROM candidate c WHERE d.id=c.id
 RETURNING d.id,d.notification_id,d.device_id,d.transport,d.endpoint,d.payload,d.attempt_count,d.lease_id,d.ticket_id,d.accepted_at`
	var d Delivery
	var payload []byte
	err := s.DB.QueryRowContext(ctx, s.q(query), status, receipt).Scan(&d.ID, &d.NotificationID, &d.DeviceID, &d.Transport, &d.Endpoint, &payload, &d.Attempts, &d.LeaseID, &d.TicketID, &d.AcceptedAt)
	if err != nil {
		return d, err
	}
	err = json.Unmarshal(payload, &d.Alert)
	return d, err
}

func (s *Service) finish(ctx context.Context, d Delivery, r Result, receipt bool) error {
	delay := 15 * time.Minute
	if r.Status == "retry" {
		delay = retryDelay(d.Attempts)
		if d.Attempts >= 6 {
			r.Status = "failed"
		}
	}
	if r.Status == "waiting" {
		r.Status = "accepted"
	}
	switch r.Status {
	case "accepted", "provider_accepted", "failed", "retry", "unknown", "cancelled", "suppressed":
	default:
		return fmt.Errorf("invalid notification transport verdict %q", r.Status)
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if r.Status == "retry" {
		// A receipt can reject an earlier accepted push after the user paused.
		// Do not resurrect that alert on resume; serialize with the pause switch.
		var enabled bool
		if err = tx.QueryRowContext(ctx, s.q(`SELECT enabled=1 FROM @notification_state WHERE id='timeline' FOR SHARE`)).Scan(&enabled); err != nil {
			return err
		}
		if !enabled {
			r.Status = "cancelled"
		}
	}
	res, err := tx.ExecContext(ctx, s.q(`UPDATE @notification_deliveries SET status=$3,error=$4,
 ticket_id=CASE WHEN $5<>'' THEN $5 ELSE ticket_id END,
 accepted_at=CASE WHEN $3 IN ('accepted','provider_accepted') AND accepted_at='epoch'::timestamptz THEN now() ELSE accepted_at END,
 receipt_at=CASE WHEN $6 AND $3 NOT IN ('accepted','unknown') THEN now() ELSE receipt_at END,
 updated_at=now(),next_attempt_at=now()+$7::interval,lease_until='epoch',lease_id=''
 WHERE id=$1 AND lease_id=$2`), d.ID, d.LeaseID, r.Status, r.Error, r.TicketID, receipt, fmt.Sprintf("%f seconds", delay.Seconds()))
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("notification delivery lease lost")
	}
	if r.Disable {
		if d.Transport == "expo" {
			var e struct{ Token string }
			_ = json.Unmarshal(d.Endpoint, &e)
			_, err = tx.ExecContext(ctx, s.q(`UPDATE @push_devices SET status='disabled',error=$2,updated_at=now() WHERE expo_push_token=$1`), e.Token, r.Error)
		} else {
			_, err = tx.ExecContext(ctx, s.q(`UPDATE @web_push_devices SET status='disabled',error=$2,updated_at=now() WHERE subscription->>'endpoint'=$1`), endpointURL(d.Endpoint), r.Error)
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func endpointURL(raw []byte) string {
	var e struct{ Endpoint string }
	_ = json.Unmarshal(raw, &e)
	return e.Endpoint
}

// Honor a device retirement for work that was queued before it was disabled.
func (s *Service) deliveryActive(ctx context.Context, d Delivery) (bool, error) {
	var active bool
	var query string
	var key string
	if d.Transport == "expo" {
		var endpoint struct{ Token string }
		_ = json.Unmarshal(d.Endpoint, &endpoint)
		key = endpoint.Token
		query = `SELECT EXISTS(SELECT 1 FROM @push_devices WHERE expo_push_token=$1 AND status='active') AND EXISTS(SELECT 1 FROM @notification_state WHERE id='timeline' AND enabled=1)`
	} else {
		key = endpointURL(d.Endpoint)
		query = `SELECT EXISTS(SELECT 1 FROM @web_push_devices WHERE subscription->>'endpoint'=$1 AND status='active') AND EXISTS(SELECT 1 FROM @notification_state WHERE id='timeline' AND enabled=1)`
	}
	err := s.DB.QueryRowContext(ctx, s.q(query), key).Scan(&active)
	return active, err
}

// Store the result even if the parent tick expired during a provider response;
// abandoning an already accepted ticket needlessly causes a duplicate retry.
func (s *Service) settle(parent context.Context, d Delivery, r Result, receipt bool) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	return s.finish(ctx, d, r, receipt)
}
