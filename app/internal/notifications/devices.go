package notifications

import (
	"context"
	"log/slog"
	"net/http"
)

// DeviceDormancyDays is the inactivity window for this single-account app.
const DeviceDormancyDays = 14

// Never treat provider acceptance or updated_at on Expo (also written on send)
// as evidence that a person still uses an installation. to_jsonb tolerates the
// brief deployment interval before either registry ensure adds last_active_at.
const devicesSQL = `WITH registry AS (
 SELECT encode(sha256(convert_to('expo:'||expo_push_token,'UTF8')),'hex') AS id,
 'expo' AS transport, device_name, platform, app_version, status, registered_at,
 GREATEST(registered_at, COALESCE((to_jsonb(p)->>'last_active_at')::timestamptz,'epoch')) AS active_at,
 last_sent_at AS sent_at FROM @push_devices p
 UNION ALL
 SELECT encode(sha256(convert_to('web:'||id,'UTF8')),'hex'), 'web', device_name,
 'browser', '', status, registered_at, updated_at, 'epoch'::timestamptz FROM @web_push_devices
), devices AS (
 SELECT r.*, GREATEST(active_at,COALESCE(a.opened_at,'epoch')) AS last_activity_at,
 NULLIF(a.opened_at,'epoch') AS last_open,
 NULLIF(GREATEST(r.sent_at,COALESCE(a.sent_at,'epoch')),'epoch') AS last_sent
 FROM registry r CROSS JOIN LATERAL (
 SELECT (SELECT max(opened_at) FROM @notification_deliveries WHERE device_id=r.id) AS opened_at,
 (SELECT max(accepted_at) FROM @notification_deliveries WHERE device_id=r.id) AS sent_at
 ) a
) `

func (s *Service) quietDevices(ctx context.Context) error {
	// Only another device's recent observed tap warrants quieting an unused one.
	// Registration/foreground grants a grace period, but is not the other-device witness.
	rows, err := s.DB.QueryContext(ctx, s.q(devicesSQL+`SELECT id FROM devices d
 WHERE status='active' AND last_activity_at < now()-make_interval(days => $1)
 AND EXISTS (SELECT 1 FROM devices other WHERE other.id<>d.id AND other.status='active'
 AND other.last_open >= now()-make_interval(days => $1))`), DeviceDormancyDays)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err = s.changeDevice(ctx, id, "dormant"); err != nil {
			return err
		}
		slog.InfoContext(ctx, "push device dormant", "device_id", id, "inactivity_days", DeviceDormancyDays)
	}
	return nil
}

func (s *Service) changeDevice(ctx context.Context, id, status string) error {
	// A resume needs a fresh inactivity grace period. This is the same additive
	// migration as both registry ensure paths, so the UI works before Dagster runs.
	if _, err := s.DB.ExecContext(ctx, s.q(`ALTER TABLE @push_devices ADD COLUMN IF NOT EXISTS last_active_at timestamptz NOT NULL DEFAULT 'epoch'`)); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, s.q(`UPDATE @push_devices SET status=$2,
 last_active_at=CASE WHEN $2='active' THEN now() ELSE last_active_at END
 WHERE encode(sha256(convert_to('expo:'||expo_push_token,'UTF8')),'hex')=$1
 AND ($2<>'dormant' OR (status='active' AND GREATEST(registered_at,last_active_at)<now()-make_interval(days => $3)))`), id, status, DeviceDormancyDays)
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, s.q(`UPDATE @web_push_devices SET status=$2,
 updated_at=CASE WHEN $2='active' THEN now() ELSE updated_at END
 WHERE encode(sha256(convert_to('web:'||id,'UTF8')),'hex')=$1
 AND ($2<>'dormant' OR (status='active' AND updated_at<now()-make_interval(days => $3)))`), id, status, DeviceDormancyDays)
	return err
}

func (s *Service) listDevices(w http.ResponseWriter, r *http.Request) {
	rows, err := s.DB.QueryContext(r.Context(), s.q(devicesSQL+`SELECT id,transport,device_name,platform,app_version,status,registered_at,last_open,last_sent FROM devices ORDER BY registered_at,id`))
	if err != nil {
		unavailable(w)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, transport, name, platform, version, status string
		var registered, opened, sent any
		if rows.Scan(&id, &transport, &name, &platform, &version, &status, &registered, &opened, &sent) != nil {
			unavailable(w)
			return
		}
		out = append(out, map[string]any{"id": id, "transport": transport, "device_name": name, "platform": platform, "app_version": version, "status": status, "registered_at": registered, "last_open": opened, "last_sent": sent})
	}
	if rows.Err() != nil {
		unavailable(w)
		return
	}
	writeJSON(w, map[string]any{"devices": out})
}

func (s *Service) deviceStatus(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if !decode(w, r, &input) {
		return
	}
	if len(input.ID) != 64 || (input.Status != "active" && input.Status != "stopped") {
		http.Error(w, "device id and active or stopped status required", 400)
		return
	}
	var exists bool
	if err := s.DB.QueryRowContext(r.Context(), s.q(devicesSQL+`SELECT EXISTS(SELECT 1 FROM devices WHERE id=$1)`), input.ID).Scan(&exists); err != nil {
		unavailable(w)
		return
	}
	if !exists {
		http.NotFound(w, r)
		return
	}
	if err := s.changeDevice(r.Context(), input.ID, input.Status); err != nil {
		unavailable(w)
		return
	}
	slog.InfoContext(r.Context(), "push device status changed", "device_id", input.ID, "status", input.Status)
	writeJSON(w, map[string]string{"status": input.Status})
}
