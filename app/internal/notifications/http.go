package notifications

import (
	"context"
	"crypto/ecdh"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const APIPath = "/api/notifications"

// The ledger is read newest-first in pages: the phone's Notifications tab and
// the web alerts view both walk it with ?before=<created_at of the last row>.
const (
	defaultLedgerLimit = 50
	maxLedgerLimit     = 500
)

type ledgerPage struct {
	limit    int
	before   time.Time // zero means "from the newest"
	beforeID string
}

// A cursor is "<created_at RFC3339Nano>|<id>": the keyset the page is ordered
// by, so rows created in the same instant are neither skipped nor repeated.
func encodeCursor(created time.Time, id string) string {
	return created.UTC().Format(time.RFC3339Nano) + "|" + id
}

func decodeCursor(raw string) (time.Time, string, bool) {
	stamp, id, found := strings.Cut(raw, "|")
	if !found || id == "" {
		return time.Time{}, "", false
	}
	t, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return time.Time{}, "", false
	}
	return t, id, true
}

func ledgerPageParams(r *http.Request) (ledgerPage, bool) {
	page := ledgerPage{limit: defaultLedgerLimit}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return ledgerPage{}, false
		}
		page.limit = min(n, maxLedgerLimit)
	}
	if raw := r.URL.Query().Get("before"); raw != "" {
		t, id, ok := decodeCursor(raw)
		if !ok {
			return ledgerPage{}, false
		}
		page.before, page.beforeID = t, id
	}
	return page, true
}

func (s *Service) Register(mux *http.ServeMux, auth func(http.Handler) http.Handler) {
	mux.Handle("GET "+APIPath, auth(bounded(s.status)))
	mux.Handle("POST "+APIPath+"/settings", auth(bounded(s.settings)))
	mux.Handle("POST "+APIPath+"/web/register", auth(bounded(s.registerWeb)))
	mux.Handle("POST "+APIPath+"/web/disable", auth(bounded(s.disableWeb)))
	// This capability authorizes exactly one idempotent timestamp write, no
	// reads or redirects. The service worker never needs the warehouse bearer.
	mux.Handle("POST "+APIPath+"/opened", bounded(s.opened))
}
func bounded(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		w.Header().Set("Cache-Control", "no-store")
		next(w, r.WithContext(ctx))
	})
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 16384)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if d.Decode(v) != nil || d.Decode(new(any)) != io.EOF {
		http.Error(w, "invalid JSON request", 400)
		return false
	}
	return true
}
func unavailable(w http.ResponseWriter) { http.Error(w, "notification storage unavailable", 503) }
func (s *Service) status(w http.ResponseWriter, r *http.Request) {
	page, ok := ledgerPageParams(r)
	if !ok {
		http.Error(w, "limit must be a positive integer and before a next_cursor from an earlier page", 400)
		return
	}
	var enabled bool
	var status, lastRun, reason string
	err := s.DB.QueryRowContext(r.Context(), s.q(`SELECT enabled=1,status,COALESCE(last_run_at::text,''),error FROM @marts_notification_health`)).Scan(&enabled, &status, &lastRun, &reason)
	if err != nil {
		unavailable(w)
		return
	}
	// One row past the page says whether there is more without a second count.
	rows, err := s.DB.QueryContext(r.Context(), s.q(`SELECT n.id,n.source,n.priority,n.created_at,n.payload->>'actor',n.payload->>'title',n.payload->>'snippet',n.status,n.payload,
 (SELECT count(*) FROM @notification_deliveries d WHERE d.notification_id=n.id),
 (SELECT count(*) FROM @notification_deliveries d WHERE d.notification_id=n.id AND d.accepted_at>'epoch'),
 (SELECT count(*) FROM @notification_deliveries d WHERE d.notification_id=n.id AND d.opened_at>'epoch'),
 (SELECT count(*) FROM @notification_deliveries d WHERE d.notification_id=n.id AND d.status IN ('failed','unknown')),
 (SELECT count(*) FROM @notification_deliveries d WHERE d.notification_id=n.id AND d.status='suppressed' AND d.error='already_read'),
 (SELECT count(*) FROM @notification_deliveries d WHERE d.notification_id=n.id AND d.status='suppressed' AND d.error='already_replied')
 FROM @notification_events n WHERE ($1::timestamptz IS NULL OR (n.created_at, n.id) < ($1, $2)) ORDER BY n.created_at DESC, n.id DESC LIMIT $3`), nullableTime(page.before), page.beforeID, page.limit+1)
	if err != nil {
		unavailable(w)
		return
	}
	defer rows.Close()
	events := []map[string]any{}
	var lastCreated time.Time
	var lastID string
	hasMore := false
	for rows.Next() {
		var id, source, priority, actor, title, body, state string
		var created time.Time
		var devices, accepted, opened, failed, suppressedRead, suppressedReplied int
		var payload []byte
		if rows.Scan(&id, &source, &priority, &created, &actor, &title, &body, &state, &payload, &devices, &accepted, &opened, &failed, &suppressedRead, &suppressedReplied) != nil {
			unavailable(w)
			return
		}
		if len(events) == page.limit {
			hasMore = true
			break
		}
		lastCreated, lastID = created, id

		var snapshot map[string]any
		_ = json.Unmarshal(payload, &snapshot)
		preview, _ := snapshot["presentation"].(map[string]any)

		events = append(events, map[string]any{"preview": preview, "id": id, "source": source, "priority": priority, "created_at": created.UTC().Format(time.RFC3339Nano), "actor": actor, "title": title, "body": body, "status": state, "devices": devices, "accepted": accepted, "opened": opened, "failed": failed, "suppressed_read": suppressedRead, "suppressed_replied": suppressedReplied})
	}
	if rows.Err() != nil {
		unavailable(w)
		return
	}
	out := map[string]any{"enabled": enabled, "status": status, "last_run_at": lastRun, "error": reason, "web_public_key": s.PublicKey, "events": events, "has_more": hasMore}
	if hasMore {
		out["next_cursor"] = encodeCursor(lastCreated, lastID)
	}
	writeJSON(w, out)
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}
func (s *Service) settings(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Enabled *bool `json:"enabled"`
	}
	if !decode(w, r, &input) {
		return
	}
	if input.Enabled == nil {
		http.Error(w, "enabled is required", 400)
		return
	}
	tx, err := s.DB.BeginTx(r.Context(), nil)
	if err != nil {
		unavailable(w)
		return
	}
	defer tx.Rollback()
	value := 0
	if *input.Enabled {
		value = 1
	}
	res, err := tx.ExecContext(r.Context(), s.q(`UPDATE @notification_state SET enabled=$1,updated_at=now() WHERE id='timeline'`), value)
	if err == nil {
		if n, _ := res.RowsAffected(); n != 1 {
			unavailable(w)
			return
		}
	}
	if err == nil && value == 0 {
		_, err = tx.ExecContext(r.Context(), s.q(`UPDATE @notification_events SET status='cancelled' WHERE status='pending'`))
		if err == nil {
			_, err = tx.ExecContext(r.Context(), s.q(`UPDATE @notification_deliveries SET status='cancelled',updated_at=now() WHERE status IN ('pending','retry','sending')`))
		}
	}
	if err != nil || tx.Commit() != nil {
		unavailable(w)
		return
	}
	writeJSON(w, map[string]bool{"enabled": *input.Enabled})
}

type subscription struct {
	Endpoint       string `json:"endpoint"`
	ExpirationTime any    `json:"expirationTime,omitempty"`
	Keys           struct {
		Auth   string `json:"auth"`
		P256dh string `json:"p256dh"`
	} `json:"keys"`
}

func validSubscription(sub subscription) bool {
	if !validEndpoint(sub.Endpoint) || len(sub.Endpoint) > 4096 {
		return false
	}
	auth, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(sub.Keys.Auth, "="))
	if err != nil || len(auth) != 16 {
		return false
	}
	key, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(sub.Keys.P256dh, "="))
	if err != nil {
		return false
	}
	_, err = ecdh.P256().NewPublicKey(key)
	return err == nil
}
func (s *Service) registerWeb(w http.ResponseWriter, r *http.Request) {
	if s.PublicKey == "" {
		http.Error(w, "web push is not configured", 503)
		return
	}
	var input struct {
		Subscription subscription `json:"subscription"`
		DeviceName   string       `json:"device_name"`
	}
	if !decode(w, r, &input) {
		return
	}
	if !validSubscription(input.Subscription) {
		http.Error(w, "invalid web push subscription", 400)
		return
	}
	raw, _ := json.Marshal(input.Subscription)
	id := deviceID(input.Subscription.Endpoint)
	_, err := s.DB.ExecContext(r.Context(), s.q(`INSERT INTO @web_push_devices (id,subscription,device_name) VALUES ($1,$2,$3)
 ON CONFLICT(id) DO UPDATE SET subscription=EXCLUDED.subscription,device_name=EXCLUDED.device_name,status='active',error='',updated_at=now()`), id, raw, string([]rune(input.DeviceName)[:min(120, len([]rune(input.DeviceName)))]))
	if err != nil {
		unavailable(w)
		return
	}
	writeJSON(w, map[string]string{"id": id})
}
func (s *Service) disableWeb(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Endpoint string `json:"endpoint"`
	}
	if !decode(w, r, &input) {
		return
	}
	_, err := s.DB.ExecContext(r.Context(), s.q(`UPDATE @web_push_devices SET status='disabled',updated_at=now() WHERE id=$1`), deviceID(input.Endpoint))
	if err != nil {
		unavailable(w)
		return
	}
	writeJSON(w, map[string]bool{"disabled": true})
}
func (s *Service) opened(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ID    string `json:"delivery_id"`
		Proof string `json:"open_proof"`
	}
	if !decode(w, r, &input) {
		return
	}
	if !s.validProof(input.ID, input.Proof) {
		http.Error(w, "invalid open proof", 403)
		return
	}
	res, err := s.DB.ExecContext(r.Context(), s.q(`UPDATE @notification_deliveries SET opened_at=CASE WHEN opened_at='epoch'::timestamptz THEN now() ELSE opened_at END,updated_at=now() WHERE id=$1`), input.ID)
	if err != nil {
		unavailable(w)
		return
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, map[string]bool{"recorded": true})
}
