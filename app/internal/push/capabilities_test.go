package push

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCapabilitiesKeepOnlyKnownValuesOnceSorted(t *testing.T) {
	got := NormalizeCapabilities([]string{" android_messaging_style ", "teleport", "android_messaging_style", ""})
	if strings.Join(got, ",") != CapabilityAndroidMessagingStyle {
		t.Fatalf("got %q", got)
	}
	if !HasCapability("x,android_messaging_style", CapabilityAndroidMessagingStyle) || HasCapability("", CapabilityAndroidMessagingStyle) {
		t.Fatal("stored capability list misread")
	}
}

// A phone rolled back to a build without the native chat renderer must stop
// being sent data-only chat alerts the moment it re-registers, so the
// registration replaces the capability list rather than merging into it.
func TestRegisterReplacesCapabilities(t *testing.T) {
	store := NewMemoryStore()
	mux := http.NewServeMux()
	NewHandler(store, nil, nil, nil).Register(mux, withClient("pixel"))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	post := func(body string) {
		t.Helper()
		resp, err := http.Post(srv.URL+RegisterPath, "application/json", strings.NewReader(body))
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("register failed: %v %v", err, resp)
		}
		resp.Body.Close()
	}
	post(`{"expo_push_token":"ExponentPushToken[px]","platform":"android","capabilities":["android_messaging_style"]}`)
	if d, _ := store.Get("ExponentPushToken[px]"); strings.Join(d.Capabilities, ",") != CapabilityAndroidMessagingStyle {
		t.Fatalf("capability not stored: %+v", d)
	}
	post(`{"expo_push_token":"ExponentPushToken[px]","platform":"android"}`)
	if d, _ := store.Get("ExponentPushToken[px]"); len(d.Capabilities) != 0 {
		t.Fatalf("an older build kept the capability: %+v", d)
	}
}

// Invoked by tests/test_timeline_notifications.py against a push_devices table
// created before the capabilities column existed, as production's was.
func TestPostgresStoreCapabilities(t *testing.T) {
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
	ctx := context.Background()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	d, err := store.Register(ctx, Device{ExpoPushToken: "ExponentPushToken[px]", Platform: "android", Capabilities: []string{CapabilityAndroidMessagingStyle, "unknown"}}, now)
	if err != nil {
		t.Fatalf("register against the old table: %v", err)
	}
	if strings.Join(d.Capabilities, ",") != CapabilityAndroidMessagingStyle {
		t.Fatalf("capabilities %q", d.Capabilities)
	}
	if _, err = store.Register(ctx, Device{ExpoPushToken: "ExponentPushToken[px]", Platform: "android"}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	active, err := store.ListActive(ctx)
	if err != nil || len(active) != 1 || len(active[0].Capabilities) != 0 {
		t.Fatalf("re-registration kept capabilities: %+v %v", active, err)
	}
}
