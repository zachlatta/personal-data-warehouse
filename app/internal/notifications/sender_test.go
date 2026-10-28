package notifications

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/push"
)

func TestVAPIDConfiguration(t *testing.T) {
	key, _ := ecdh.P256().GenerateKey(rand.Reader)
	pub, priv := base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes()), base64.RawURLEncoding.EncodeToString(key.Bytes())
	for _, c := range []struct {
		pub, priv string
		valid     bool
	}{{"", "", true}, {pub, priv, true}, {pub, "", false}, {"", priv, false}, {pub, "bad", false}, {"bad", priv, false}} {
		if (ValidateVAPID(c.pub, c.priv) == nil) != c.valid {
			t.Errorf("wrong key validation: valid=%v", c.valid)
		}
	}
	other, _ := ecdh.P256().GenerateKey(rand.Reader)
	if ValidateVAPID(pub, base64.RawURLEncoding.EncodeToString(other.Bytes())) == nil {
		t.Fatal("mismatched key pair accepted")
	}
}

func TestExpoWireAndReceipt(t *testing.T) {
	var message push.Message
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-access" {
			t.Error("missing provider authorization")
		}
		if r.URL.Path == "/receipt" {
			_, _ = w.Write([]byte(`{"data":{"ticket-1":{"status":"ok"}}}`))
			return
		}
		var batch []push.Message
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil || len(batch) != 1 {
			t.Error("invalid batch")
			return
		}
		message = batch[0]
		_, _ = w.Write([]byte(`{"data":[{"status":"ok","id":"ticket-1"}]}`))
	}))
	defer srv.Close()
	transport := Transport{Expo: &push.ExpoClient{Endpoint: srv.URL, AccessToken: "test-access", HTTP: srv.Client()}, ReceiptURL: srv.URL + "/receipt"}
	d := Delivery{ID: "delivery-1", NotificationID: "event-1", Transport: "expo", Endpoint: json.RawMessage(`{"token":"ExponentPushToken[test]"}`), Alert: Alert{Title: "Sender", Body: "Hello", Icon: "https://pdw.example/icon.png", Route: "/timeline/test/one", OpenProof: "capability", ThreadID: "timeline:slack"}}
	result := transport.Send(context.Background(), d)
	if result.Status != "accepted" || result.TicketID != "ticket-1" {
		t.Fatal(result)
	}
	if !message.MutableContent || message.RichContent.Image != d.Alert.Icon || message.Data["open_proof"] != "capability" || message.CollapseID != "event-1" || message.ThreadID != "timeline:slack" {
		t.Fatalf("wrong wire payload: %+v", message)
	}
	d.TicketID = result.TicketID
	if result = transport.Receipt(context.Background(), d); result.Status != "provider_accepted" {
		t.Fatal(result)
	}
	d.Alert.Body = strings.Repeat("x", 5000)
	if result = transport.Send(context.Background(), d); result.Status != "failed" {
		t.Fatal("oversized alert sent")
	}
}

func TestProviderErrorsAndEndpointBoundary(t *testing.T) {
	for _, c := range []struct {
		code, status string
		disable      bool
	}{{"DeviceNotRegistered", "failed", true}, {"MessageRateExceeded", "retry", false}, {"InvalidCredentials", "failed", false}, {"MessageTooBig", "failed", false}, {"anything", "failed", false}} {
		ticket := push.Ticket{Status: "error"}
		ticket.Details.Error = c.code
		got := ticketResult(ticket, false)
		if got.Status != c.status || got.Disable != c.disable {
			t.Fatal(got)
		}
	}
	for _, endpoint := range []string{"http://fcm.googleapis.com/a", "https://localhost/a", "https://fcm.googleapis.com.evil.test/a", "https://fcm.googleapis.com:443/a", "https://user@fcm.googleapis.com/a", "https://evil.test@localhost/a"} {
		if validEndpoint(endpoint) {
			t.Fatal("unsafe endpoint", endpoint)
		}
	}
	for _, endpoint := range []string{"https://fcm.googleapis.com/a", "https://updates.push.services.mozilla.com/wpush/v2/test", "https://web.push.apple.com/test"} {
		if !validEndpoint(endpoint) {
			t.Fatal("valid endpoint refused", endpoint)
		}
	}
}

func TestDecodeRejectsTrailingAndUnknownJSON(t *testing.T) {
	for _, body := range []string{`{"enabled":true}{}`, `{"enabled":true,"unknown":1}`, strings.Repeat("x", 17000)} {
		var input struct {
			Enabled bool `json:"enabled"`
		}
		if decode(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader(body)), &input) {
			t.Fatal("invalid JSON accepted")
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestEncryptedWebPushAndSubscriptionValidation(t *testing.T) {
	key, _ := ecdh.P256().GenerateKey(rand.Reader)
	browserKey, _ := ecdh.P256().GenerateKey(rand.Reader)
	enc := base64.RawURLEncoding.EncodeToString
	sub := subscription{Endpoint: "https://fcm.googleapis.com/test"}
	sub.Keys.Auth = enc(make([]byte, 16))
	sub.Keys.P256dh = enc(browserKey.PublicKey().Bytes())
	if !validSubscription(sub) {
		t.Fatal("valid subscription refused")
	}
	bad := sub
	bad.Keys.Auth = "bad"
	if validSubscription(bad) {
		t.Fatal("invalid auth accepted")
	}
	bad = sub
	bad.Keys.P256dh = enc(make([]byte, 65))
	if validSubscription(bad) {
		t.Fatal("invalid curve point accepted")
	}
	endpoint, _ := json.Marshal(sub)
	d := Delivery{Transport: "web", NotificationID: "test-id", Endpoint: endpoint, Alert: Alert{Title: "Private message", Body: "Not plaintext on the wire"}}
	for _, c := range []struct {
		code    int
		status  string
		disable bool
	}{{201, "accepted", false}, {410, "failed", true}, {429, "retry", false}, {500, "retry", false}, {400, "failed", false}} {
		transport := Transport{VAPIDPublicKey: enc(key.PublicKey().Bytes()), VAPIDPrivateKey: enc(key.Bytes()), Subscriber: "https://pdw.example"}
		transport.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			raw, _ := io.ReadAll(r.Body)
			if strings.Contains(string(raw), d.Alert.Body) || len(raw) == 0 {
				t.Error("unencrypted web push")
			}
			if r.Header.Get("Content-Encoding") != "aes128gcm" || r.Header.Get("TTL") != "3600" || !strings.HasPrefix(r.Header.Get("Authorization"), "vapid ") {
				t.Error("missing authenticated encryption headers")
			}
			return &http.Response{StatusCode: c.code, Body: io.NopCloser(strings.NewReader(""))}, nil
		})}
		got := transport.Send(context.Background(), d)
		if got.Status != c.status || got.Disable != c.disable {
			t.Fatal(got)
		}
		large := d
		large.Alert.Body = strings.Repeat("x", 5000)
		if got := transport.Send(context.Background(), large); got.Status != "failed" {
			t.Fatal("oversized web payload retried")
		}
	}
}

func TestHTTPAuthenticationBoundary(t *testing.T) {
	s := Service{Secret: []byte("test-secret")}
	mux := http.NewServeMux()
	s.Register(mux, func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unauthorized", 401) })
	})
	for _, path := range []string{APIPath, APIPath + "/settings", APIPath + "/web/register", APIPath + "/web/disable"} {
		method := "POST"
		if path == APIPath {
			method = "GET"
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader("{}")))
		if w.Code != 401 {
			t.Fatalf("unprotected %s: %d", path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", APIPath+"/opened", strings.NewReader(`{"delivery_id":"test","open_proof":"wrong"}`)))
	if w.Code != 403 {
		t.Fatal("open capability not checked independently")
	}
}

// Expo turns a message with a title into an FCM notification message, which
// Android draws itself in the background without running any app code. So a
// chat alert is data-only, but only to an Android build that says it can draw
// one: iOS, an older Android build, and a delivery queued before the platform
// was recorded all get the ordinary alert, on the high-importance channel.
func TestChatAlertWireShapeFollowsTheDevice(t *testing.T) {
	chat := Alert{Title: "Sam", Subtitle: "Messages · Sam", Body: "On my way", Icon: "https://pdw.example/icon.png", Route: "/timeline/m/1", ThreadID: "timeline:apple_messages:me:c1", OpenProof: "proof",
		Android: &AndroidStyle{Source: "Messages", ConversationID: "timeline:apple_messages:me:c1", SenderName: "Sam", SenderKey: "apple_messages:Sam", EventID: "1", AvatarURL: "https://photos.example/sam.jpg"}}
	plain := chat
	plain.Android = nil
	capable := `{"token":"ExponentPushToken[px]","platform":"android","capabilities":"android_messaging_style"}`
	cases := []struct {
		name, endpoint string
		alert          Alert
		dataOnly       bool
	}{
		{"capable android chat", capable, chat, true},
		{"capable android, not a chat", capable, plain, false},
		{"android build without the renderer", `{"token":"ExponentPushToken[px]","platform":"android","capabilities":""}`, chat, false},
		{"ios", `{"token":"ExponentPushToken[ip]","platform":"ios","capabilities":"android_messaging_style"}`, chat, false},
		{"delivery queued before platform was recorded", `{"token":"ExponentPushToken[px]"}`, chat, false},
	}
	for _, tc := range cases {
		var e struct{ Token, Platform, Capabilities string }
		_ = json.Unmarshal([]byte(tc.endpoint), &e)
		m := expoMessage(e.Token, e.Platform, e.Capabilities, Delivery{ID: "d1", NotificationID: "n1", Alert: tc.alert})
		if m.Priority != "high" || m.Data["open_proof"] != "proof" || m.Data["delivery_id"] != "d1" || m.Data["route"] != "/timeline/m/1" {
			t.Errorf("%s: lost the tap contract: %+v", tc.name, m)
		}
		if tc.dataOnly {
			style, _ := m.Data["android_style"].(map[string]any)
			if m.Title != "" || m.Body != "" || m.Subtitle != "" || m.Sound != "" || m.RichContent != nil || m.CategoryID != "" {
				t.Errorf("%s: not data-only: %+v", tc.name, m)
			}
			if style["title"] != "Sam" || style["body"] != "On my way" || style["sender_name"] != "Sam" || style["conversation_id"] != chat.ThreadID || style["avatar_url"] != "https://photos.example/sam.jpg" || style["source"] != "Messages" {
				t.Errorf("%s: style %+v", tc.name, style)
			}
			continue
		}
		if _, ok := m.Data["android_style"]; ok || m.Title != "Sam" || m.Body != "On my way" || m.ChannelID != push.AndroidChannelID || m.RichContent == nil {
			t.Errorf("%s: wrong ordinary alert: %+v", tc.name, m)
		}
	}
}
