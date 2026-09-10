package notifications

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	webpush "github.com/SherClockHolmes/webpush-go"
	"github.com/zachlatta/personal-data-warehouse/app/internal/push"
)

type Transport struct {
	Expo                                        *push.ExpoClient
	HTTP                                        *http.Client
	VAPIDPublicKey, VAPIDPrivateKey, Subscriber string
	ReceiptURL                                  string
}

func (t *Transport) client() *http.Client {
	if t.HTTP != nil {
		return t.HTTP
	}
	return &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func (t *Transport) Send(ctx context.Context, d Delivery) Result {
	a := d.Alert
	if d.Transport == "expo" {
		var e struct{ Token string }
		if json.Unmarshal(d.Endpoint, &e) != nil || push.ValidateToken(e.Token) != nil {
			return Result{Status: "failed", Error: "invalid stored Expo token"}
		}
		m := push.Message{To: e.Token, Title: a.Title, Subtitle: a.Subtitle, Body: a.Body, Priority: "high", Sound: "default", CategoryID: push.CategoryLink, CollapseID: d.NotificationID, ThreadID: a.ThreadID, Data: map[string]any{"kind": "timeline_notification", "route": a.Route, "open": a.Open, "delivery_id": d.ID, "open_proof": a.OpenProof}}
		if a.Icon != "" {
			m.MutableContent = true
			m.RichContent = &push.RichContent{Image: a.Icon}
		}
		raw, _ := json.Marshal(m)
		if len(raw) > 4000 {
			return Result{Status: "failed", Error: "notification exceeds safe APNs payload budget"}
		}
		tickets, err := t.Expo.Send(ctx, []push.Message{m})
		if err != nil || len(tickets) != 1 {
			return Result{Status: "retry", Error: "Expo request failed; acceptance unknown"}
		}
		return ticketResult(tickets[0], false)
	}
	if t.VAPIDPrivateKey == "" {
		return Result{Status: "failed", Error: "web push VAPID keys not configured"}
	}
	var sub webpush.Subscription
	if json.Unmarshal(d.Endpoint, &sub) != nil || !validEndpoint(sub.Endpoint) {
		return Result{Status: "failed", Error: "invalid stored web push endpoint"}
	}
	raw, err := json.Marshal(a)
	if err != nil || len(raw) > 3800 {
		return Result{Status: "failed", Error: "web push payload exceeds safe encrypted-payload budget"}
	}
	resp, err := webpush.SendNotificationWithContext(ctx, raw, &sub, &webpush.Options{HTTPClient: t.client(), Subscriber: t.Subscriber, VAPIDPublicKey: t.VAPIDPublicKey, VAPIDPrivateKey: t.VAPIDPrivateKey, TTL: 3600, Urgency: webpush.UrgencyHigh, Topic: d.NotificationID[:min(32, len(d.NotificationID))]})
	if err != nil {
		return Result{Status: "retry", Error: "web push request failed; acceptance unknown"}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	switch {
	case resp.StatusCode == 404 || resp.StatusCode == 410:
		return Result{Status: "failed", Error: "web push subscription expired", Disable: true}
	case resp.StatusCode/100 == 2:
		return Result{Status: "accepted"}
	case resp.StatusCode == 429 || resp.StatusCode >= 500:
		return Result{Status: "retry", Error: "web push provider temporarily unavailable"}
	default:
		return Result{Status: "failed", Error: "web push provider rejected notification"}
	}
}

func ticketResult(ticket push.Ticket, receipt bool) Result {
	if ticket.Status == "ok" {
		if receipt {
			return Result{Status: "provider_accepted"}
		}
		if ticket.ID == "" {
			return Result{Status: "unknown", Error: "Expo accepted without receipt id"}
		}
		return Result{Status: "accepted", TicketID: ticket.ID}
	}
	reason := ticket.Details.Error
	switch reason {
	case push.DeviceNotRegistered:
		return Result{Status: "failed", Error: reason, Disable: true}
	case "MessageRateExceeded":
		return Result{Status: "retry", Error: reason}
	case "MessageTooBig", "InvalidCredentials", "MismatchSenderId":
		return Result{Status: "failed", Error: reason}
	default:
		return Result{Status: "failed", Error: "Expo rejected notification"}
	}
}

func (t *Transport) Receipt(ctx context.Context, d Delivery) Result {
	raw, _ := json.Marshal(map[string]any{"ids": []string{d.TicketID}})
	endpoint := t.ReceiptURL
	if endpoint == "" {
		endpoint = "https://exp.host/--/api/v2/push/getReceipts"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return Result{Status: "waiting"}
	}
	req.Header.Set("Content-Type", "application/json")
	if t.Expo != nil && t.Expo.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+t.Expo.AccessToken)
	}
	resp, err := t.client().Do(req)
	if err != nil {
		return Result{Status: "waiting", Error: "receipt request unavailable"}
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return Result{Status: "waiting", Error: "receipt request rejected"}
	}
	var body struct {
		Data map[string]push.Ticket `json:"data"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body) != nil {
		return Result{Status: "waiting", Error: "invalid receipt response"}
	}
	ticket, ok := body.Data[d.TicketID]
	if !ok {
		return Result{Status: "waiting", Error: "receipt not yet available"}
	}
	return ticketResult(ticket, true)
}

// Subscriptions are untrusted request input: never let this authenticated API
// become an SSRF proxy to the warehouse, tailnet, or arbitrary third parties.
func validEndpoint(value string) bool {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Port() != "" || u.Fragment != "" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	return host == "fcm.googleapis.com" || host == "updates.push.services.mozilla.com" || host == "web.push.apple.com" || strings.HasSuffix(host, ".push.apple.com")
}

// ValidateVAPID fails startup for partial or mismatched keys rather than letting
// browsers register successfully with a transport that cannot deliver.
func ValidateVAPID(publicKey, privateKey string) error {
	if publicKey == "" && privateKey == "" {
		return nil
	}
	public, e1 := base64.RawURLEncoding.DecodeString(publicKey)
	private, e2 := base64.RawURLEncoding.DecodeString(privateKey)
	if e1 != nil || e2 != nil {
		return errors.New("invalid web push VAPID key encoding")
	}
	key, err := ecdh.P256().NewPrivateKey(private)
	if err != nil || subtle.ConstantTimeCompare(key.PublicKey().Bytes(), public) != 1 {
		return errors.New("invalid or mismatched web push VAPID keys")
	}
	return nil
}
