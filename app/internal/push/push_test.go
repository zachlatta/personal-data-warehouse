package push

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/auth"
)

type fakeSender struct {
	got     [][]Message
	tickets []Ticket
	err     error
}

func (f *fakeSender) Send(_ context.Context, messages []Message) ([]Ticket, error) {
	f.got = append(f.got, messages)
	if f.err != nil {
		return nil, f.err
	}
	return f.tickets, nil
}

func ticket(status, errCode string) Ticket {
	t := Ticket{Status: status}
	t.Details.Error = errCode
	return t
}

func TestValidateTokenAcceptsBothExpoShapes(t *testing.T) {
	for _, ok := range []string{"ExponentPushToken[abc]", "ExpoPushToken[xyz]"} {
		if err := ValidateToken(ok); err != nil {
			t.Fatalf("%s rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "abc", "ExponentPushToken[", "ExponentPushToken[]", "apns-device-token"} {
		if err := ValidateToken(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}

func TestNotifierFansOutAndRetiresDeadDevices(t *testing.T) {
	store := NewMemoryStore()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	for _, token := range []string{"ExponentPushToken[live]", "ExponentPushToken[dead]", "ExponentPushToken[flaky]"} {
		if _, err := store.Register(context.Background(), Device{ExpoPushToken: token}, now); err != nil {
			t.Fatal(err)
		}
	}
	sender := &fakeSender{}
	notifier := NewNotifier(store, sender, func() time.Time { return now }, nil)
	// Tickets come back in message order; the store lists in registration order.
	devices, _ := store.ListActive(context.Background())
	sender.tickets = make([]Ticket, len(devices))
	for i, d := range devices {
		switch {
		case strings.Contains(d.ExpoPushToken, "dead"):
			sender.tickets[i] = ticket("error", DeviceNotRegistered)
		case strings.Contains(d.ExpoPushToken, "flaky"):
			sender.tickets[i] = ticket("error", "MessageRateExceeded")
		default:
			sender.tickets[i] = ticket("ok", "")
		}
	}
	report, err := notifier.Notify(context.Background(), Notification{Title: "t", Body: "b", Data: map[string]any{"route": "/x"}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Devices != 3 || report.Sent != 1 || report.Failed != 2 || report.Disabled != 1 {
		t.Fatalf("unexpected report %+v", report)
	}
	if len(sender.got) != 1 || len(sender.got[0]) != 3 {
		t.Fatalf("expected one batch of three messages, got %+v", sender.got)
	}
	if sender.got[0][0].Sound != "default" || sender.got[0][0].Data["route"] != "/x" {
		t.Fatalf("message not shaped for the app: %+v", sender.got[0][0])
	}
	dead, _ := store.Get("ExponentPushToken[dead]")
	if dead.Status != StatusDisabled || dead.Error != DeviceNotRegistered {
		t.Fatalf("dead device not retired: %+v", dead)
	}
	flaky, _ := store.Get("ExponentPushToken[flaky]")
	if flaky.Status != StatusActive || flaky.Error != "MessageRateExceeded" {
		t.Fatalf("transient failure must keep the device active but record the reason: %+v", flaky)
	}
	if _, sent := store.Sent["ExponentPushToken[live]"]; !sent {
		t.Fatal("successful send not recorded")
	}
	// A retired device is out of the next fan-out.
	sender.tickets = []Ticket{ticket("ok", ""), ticket("ok", "")}
	report, _ = notifier.Notify(context.Background(), Notification{Title: "again"})
	if report.Devices != 2 {
		t.Fatalf("disabled device still in fan-out: %+v", report)
	}
}

func TestNotifierWithNoDevicesSendsNothing(t *testing.T) {
	sender := &fakeSender{}
	report, err := NewNotifier(NewMemoryStore(), sender, nil, nil).Notify(context.Background(), Notification{Title: "t"})
	if err != nil || report.Devices != 0 || len(sender.got) != 0 {
		t.Fatalf("expected a no-op, got %+v %v %d", report, err, len(sender.got))
	}
}

func TestExpoClientParsesTicketsAndErrors(t *testing.T) {
	var gotAuth string
	var gotBody []Message
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"status":"ok","id":"1"},{"status":"error","message":"gone","details":{"error":"DeviceNotRegistered"}}]}`))
	}))
	defer srv.Close()
	client := NewExpoClient("secret-token")
	client.Endpoint = srv.URL
	tickets, err := client.Send(context.Background(), []Message{{To: "ExponentPushToken[a]"}, {To: "ExponentPushToken[b]"}})
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer secret-token" {
		t.Fatalf("access token not sent: %q", gotAuth)
	}
	if len(gotBody) != 2 || gotBody[1].To != "ExponentPushToken[b]" {
		t.Fatalf("body not forwarded: %+v", gotBody)
	}
	if len(tickets) != 2 || tickets[0].Status != "ok" || tickets[1].Details.Error != DeviceNotRegistered {
		t.Fatalf("tickets not parsed: %+v", tickets)
	}

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"errors":[{"code":"PUSH_TOO_MANY_EXPERIENCE_IDS","message":"nope"}]}`))
	}))
	defer failing.Close()
	client.Endpoint = failing.URL
	if _, err := client.Send(context.Background(), []Message{{To: "ExponentPushToken[a]"}}); err == nil || !strings.Contains(err.Error(), "PUSH_TOO_MANY_EXPERIENCE_IDS") {
		t.Fatalf("top-level API error not surfaced: %v", err)
	}
}

func TestExpoClientMismatchedTicketCountIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()
	client := NewExpoClient("")
	client.Endpoint = srv.URL
	if _, err := client.Send(context.Background(), []Message{{To: "ExponentPushToken[a]"}}); err == nil {
		t.Fatal("expected an error when Expo returns fewer tickets than messages")
	}
}

func withClient(name string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := auth.WithClientNameHolder(r.Context())
			auth.SetClientName(ctx, name)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func TestRegisterEndpointStoresTheAuthenticatedClientName(t *testing.T) {
	store := NewMemoryStore()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	mux := http.NewServeMux()
	NewHandler(store, nil, func() time.Time { return now }, nil).Register(mux, withClient("zach-iphone"))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Post(srv.URL+RegisterPath, "application/json", strings.NewReader(`{"expo_push_token":"ExponentPushToken[abc]","device_name":"iPhone","platform":"ios","app_version":"1.0.0"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var body struct {
		Device Device `json:"device"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Device.ClientName != "zach-iphone" || body.Device.Platform != "ios" || body.Device.Status != StatusActive {
		t.Fatalf("unexpected device %+v", body.Device)
	}
	if stored, ok := store.Get("ExponentPushToken[abc]"); !ok || stored.DeviceName != "iPhone" {
		t.Fatalf("device not stored: %+v %v", stored, ok)
	}

	bad, _ := http.Post(srv.URL+RegisterPath, "application/json", strings.NewReader(`{"expo_push_token":"apns-hex"}`))
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for a non-Expo token, got %d", bad.StatusCode)
	}
	get, _ := http.Get(srv.URL + RegisterPath)
	if get.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for GET, got %d", get.StatusCode)
	}
	unconfigured, _ := http.Post(srv.URL+TestPath, "application/json", nil)
	if unconfigured.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("test send without a notifier should be 503, got %d", unconfigured.StatusCode)
	}
}

func TestTestEndpointReportsTheFanOut(t *testing.T) {
	store := NewMemoryStore()
	_, _ = store.Register(context.Background(), Device{ExpoPushToken: "ExponentPushToken[abc]"}, time.Now())
	sender := &fakeSender{tickets: []Ticket{ticket("ok", "")}}
	mux := http.NewServeMux()
	NewHandler(store, NewNotifier(store, sender, nil, nil), nil, nil).Register(mux, withClient("app"))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp, err := http.Post(srv.URL+TestPath, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Report Report `json:"report"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if resp.StatusCode != http.StatusOK || body.Report.Sent != 1 {
		t.Fatalf("status %d report %+v", resp.StatusCode, body.Report)
	}
}

func TestNotificationMapsEveryRichFieldOntoTheExpoMessage(t *testing.T) {
	store := NewMemoryStore()
	_, _ = store.Register(context.Background(), Device{ExpoPushToken: "ExponentPushToken[abc]"}, time.Now())
	sender := &fakeSender{tickets: []Ticket{ticket("ok", "")}}
	notifier := NewNotifier(store, sender, nil, nil)
	badge := 3
	n := Notification{
		Title:             "Wire due today",
		Subtitle:          "Invoice 4831",
		Body:              "Instructions in inbox.",
		ImageURL:          "https://example.com/receipt.png",
		Category:          CategoryLink,
		ThreadID:          "finance",
		InterruptionLevel: InterruptionTimeSensitive,
		Badge:             &badge,
		Sound:             "none",
		CollapseID:        "wire-due",
		Route:             "/timeline/gmail/abc",
		Data:              map[string]any{"kind": "digest"},
	}
	if _, err := notifier.Notify(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if len(sender.got) != 1 || len(sender.got[0]) != 1 {
		t.Fatalf("expected one message, got %+v", sender.got)
	}
	m := sender.got[0][0]
	if m.Subtitle != "Invoice 4831" || m.CategoryID != "link" || m.ThreadID != "finance" || m.InterruptionLevel != "time-sensitive" || m.CollapseID != "wire-due" {
		t.Fatalf("scalar fields not mapped: %+v", m)
	}
	if m.Badge == nil || *m.Badge != 3 {
		t.Fatalf("badge not mapped: %+v", m.Badge)
	}
	if m.Sound != "" {
		t.Fatalf("sound=none must send no sound, got %q", m.Sound)
	}
	if m.RichContent == nil || m.RichContent.Image != "https://example.com/receipt.png" {
		t.Fatalf("image not mapped into richContent: %+v", m.RichContent)
	}
	if !m.MutableContent {
		t.Fatal("an image needs mutable-content so the iOS service extension can attach it")
	}
	if m.Data["route"] != "/timeline/gmail/abc" || m.Data["kind"] != "digest" {
		t.Fatalf("route must land in data.route beside caller data: %+v", m.Data)
	}
	// The wire shape is what Expo parses; pin the JSON keys.
	raw, _ := json.Marshal(m)
	for _, key := range []string{`"subtitle"`, `"categoryId"`, `"threadId"`, `"interruptionLevel"`, `"mutableContent":true`, `"richContent":{"image":`, `"collapseId"`, `"badge":3`} {
		if !strings.Contains(string(raw), key) {
			t.Fatalf("expo wire JSON missing %s: %s", key, raw)
		}
	}
	if strings.Contains(string(raw), `"sound"`) {
		t.Fatalf("no sound must omit the key entirely: %s", raw)
	}
}

func TestNotificationDefaultsAreTheOldPlainAlert(t *testing.T) {
	m, err := Notification{Title: "t", Body: "b"}.message("ExponentPushToken[abc]")
	if err != nil {
		t.Fatal(err)
	}
	if m.Sound != "default" || m.Priority != "high" || m.MutableContent || m.RichContent != nil || m.CategoryID != "" {
		t.Fatalf("defaults changed: %+v", m)
	}
}

func TestNotificationRejectsWhatExpoWouldSilentlyDrop(t *testing.T) {
	cases := map[string]Notification{
		"empty":                  {},
		"bad interruption level": {Title: "t", InterruptionLevel: "urgent"},
		"unknown category":       {Title: "t", Category: "approve_all"},
		"http image":             {Title: "t", ImageURL: "http://example.com/a.png"},
		"relative route":         {Title: "t", Route: "mutations/1"},
		"unknown sound":          {Title: "t", Sound: "bells.wav"},
	}
	for name, n := range cases {
		if err := n.Validate(); err == nil {
			t.Fatalf("%s: expected a validation error", name)
		}
	}
	store := NewMemoryStore()
	_, _ = store.Register(context.Background(), Device{ExpoPushToken: "ExponentPushToken[abc]"}, time.Now())
	sender := &fakeSender{}
	if _, err := NewNotifier(store, sender, nil, nil).Notify(context.Background(), Notification{}); err == nil {
		t.Fatal("Notify must refuse an invalid notification before touching the provider")
	}
	if len(sender.got) != 0 {
		t.Fatal("invalid notification reached the provider")
	}
}

func TestCategoriesAreWhatTheAppRegisters(t *testing.T) {
	// The app fetches these and calls setNotificationCategoryAsync for each;
	// every category a server-side caller may name must be in the list.
	byID := map[string]Category{}
	for _, c := range Categories() {
		if c.ID == "" || len(c.Actions) == 0 {
			t.Fatalf("category without id or actions: %+v", c)
		}
		byID[c.ID] = c
		for _, a := range c.Actions {
			if a.ID == "" || a.Title == "" {
				t.Fatalf("action without id or title in %s: %+v", c.ID, a)
			}
		}
	}
	for _, want := range []string{CategoryMutationReview, CategoryLink} {
		if _, ok := byID[want]; !ok {
			t.Fatalf("category %q not published", want)
		}
	}
	review := byID[CategoryMutationReview]
	ids := map[string]bool{}
	for _, a := range review.Actions {
		ids[a.ID] = true
	}
	if !ids[ActionApprove] || !ids[ActionDeny] {
		t.Fatalf("mutation review must offer approve and deny: %+v", review.Actions)
	}
	raw, _ := json.Marshal(review)
	for _, key := range []string{`"id":"mutation_review"`, `"actions":[`, `"opens_app"`, `"destructive"`} {
		if !strings.Contains(string(raw), key) {
			t.Fatalf("category JSON missing %s: %s", key, raw)
		}
	}
}

func TestSendEndpointDeliversACallerBuiltNotification(t *testing.T) {
	store := NewMemoryStore()
	_, _ = store.Register(context.Background(), Device{ExpoPushToken: "ExponentPushToken[abc]"}, time.Now())
	sender := &fakeSender{tickets: []Ticket{ticket("ok", "")}}
	mux := http.NewServeMux()
	NewHandler(store, NewNotifier(store, sender, nil, nil), nil, nil).Register(mux, withClient("cli"))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Post(srv.URL+SendPath, "application/json", strings.NewReader(`{"title":"Hi","subtitle":"sub","body":"there","image_url":"https://example.com/i.jpg","category":"link","route":"/timeline","interruption_level":"time-sensitive","data":{"k":"v"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var body struct {
		Report Report `json:"report"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Report.Sent != 1 {
		t.Fatalf("report %+v", body.Report)
	}
	m := sender.got[0][0]
	if m.Subtitle != "sub" || m.RichContent == nil || m.CategoryID != "link" || m.Data["route"] != "/timeline" || m.Data["k"] != "v" || m.InterruptionLevel != "time-sensitive" {
		t.Fatalf("send endpoint did not forward the rich fields: %+v", m)
	}
	// The sender is recorded so a notification can be traced to who asked for it.
	if m.Data["sent_by"] != "cli" {
		t.Fatalf("sent_by should carry the bearer's client name: %+v", m.Data)
	}

	bad, _ := http.Post(srv.URL+SendPath, "application/json", strings.NewReader(`{"body":"no title","category":"nope"}`))
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid notification returned %d, want 400", bad.StatusCode)
	}
	if len(sender.got) != 1 {
		t.Fatal("invalid notification reached the provider")
	}
}

func TestCategoriesEndpointPublishesTheList(t *testing.T) {
	mux := http.NewServeMux()
	NewHandler(NewMemoryStore(), nil, nil, nil).Register(mux, withClient("app"))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp, err := http.Get(srv.URL + CategoriesPath)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Categories []Category `json:"categories"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Categories) != len(Categories()) {
		t.Fatalf("got %d categories, want %d", len(body.Categories), len(Categories()))
	}
}

func TestTestEndpointSendsARichNotification(t *testing.T) {
	store := NewMemoryStore()
	_, _ = store.Register(context.Background(), Device{ExpoPushToken: "ExponentPushToken[abc]"}, time.Now())
	sender := &fakeSender{tickets: []Ticket{ticket("ok", "")}}
	mux := http.NewServeMux()
	NewHandler(store, NewNotifier(store, sender, nil, nil), nil, nil).Register(mux, withClient("app"))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp, _ := http.Post(srv.URL+TestPath, "application/json", nil)
	resp.Body.Close()
	m := sender.got[0][0]
	if m.RichContent == nil || m.RichContent.Image == "" || m.Subtitle == "" || m.CategoryID == "" {
		t.Fatalf("the test push is how the phone proves the rich path works; it must carry an image, subtitle and category: %+v", m)
	}
}
