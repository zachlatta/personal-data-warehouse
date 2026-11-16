package push

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMemoryStorePreservesManualStop(t *testing.T) {
	ctx, now := context.Background(), time.Now()
	store := NewMemoryStore()
	device := Device{ExpoPushToken: "ExponentPushToken[stopped-fixture]"}
	if _, err := store.Register(ctx, device, now); err != nil {
		t.Fatal(err)
	}
	store.devices[device.ExpoPushToken] = Device{ExpoPushToken: device.ExpoPushToken, Status: "stopped", RegisteredAt: now}
	if err := store.MarkFailed(ctx, device.ExpoPushToken, "DeviceNotRegistered", true, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, _ := store.Get(device.ExpoPushToken)
	if got.Status != "stopped" || got.Error != "DeviceNotRegistered" {
		t.Fatalf("provider failure replaced manual stop: %+v", got)
	}
	got, err := store.Register(ctx, device, now.Add(2*time.Minute))
	if err != nil || got.Status != "stopped" || !got.RegisteredAt.Equal(now) {
		t.Fatalf("registration replaced manual stop: %+v %v", got, err)
	}
	active, err := store.ListActive(ctx)
	if err != nil || len(active) != 0 {
		t.Fatalf("stopped device eligible for fanout: %+v %v", active, err)
	}
}

func TestPostgresStorePreservesManualStop(t *testing.T) {
	url, table := os.Getenv("PDW_PUSH_TEST_URL"), os.Getenv("PDW_PUSH_TEST_TABLE")
	if url == "" || table == "" {
		t.Skip("run through tests/test_timeline_notifications.py for managed Postgres")
	}
	store, err := NewPostgresStore(url, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.Expand = func(q string) string { return strings.ReplaceAll(q, "@push_devices", table) }
	ctx, now := context.Background(), time.Now().UTC()
	device := Device{ExpoPushToken: "ExponentPushToken[stopped-fixture]"}
	if _, err = store.Register(ctx, device, now); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.ExecContext(ctx, store.q(`UPDATE @push_devices SET status='stopped' WHERE expo_push_token=$1`), device.ExpoPushToken); err != nil {
		t.Fatal(err)
	}
	failedAt := now.Add(time.Minute).Truncate(time.Microsecond)
	if err = store.MarkFailed(ctx, device.ExpoPushToken, "DeviceNotRegistered", true, failedAt); err != nil {
		t.Fatal(err)
	}
	var status, reason string
	var lastError time.Time
	if err = store.db.QueryRowContext(ctx, store.q(`SELECT status,error,last_error_at FROM @push_devices WHERE expo_push_token=$1`), device.ExpoPushToken).Scan(&status, &reason, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != "stopped" || reason != "DeviceNotRegistered" || !lastError.Equal(failedAt) {
		t.Fatalf("provider failure lost stop or evidence: %s %s %s", status, reason, lastError)
	}
	got, err := store.Register(ctx, device, now.Add(2*time.Minute))
	if err != nil || got.Status != "stopped" {
		t.Fatalf("registration replaced manual stop: %+v %v", got, err)
	}
}
