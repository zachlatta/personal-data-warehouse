package push

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"
)

// Sender is what the Notifier fans a notification out through (the Expo
// client in production, a fake in tests).
type Sender interface {
	Send(ctx context.Context, messages []Message) ([]Ticket, error)
}

// Notification is a single alert delivered to every active device. The JSON
// tags are the shape POST /api/push/send and the notify tool accept, so a
// caller iterating on notification UX writes exactly this.
type Notification struct {
	Title    string `json:"title"`
	Subtitle string `json:"subtitle,omitempty"`
	Body     string `json:"body,omitempty"`
	// ImageURL is shown as a thumbnail beside the alert and full-size when
	// it is expanded. It must be https and publicly fetchable by the phone;
	// a signed /objects/ download link qualifies.
	ImageURL string `json:"image_url,omitempty"`
	// Category selects the action buttons (see Categories).
	Category string `json:"category,omitempty"`
	// ThreadID groups related alerts in Notification Center.
	ThreadID string `json:"thread_id,omitempty"`
	// CollapseID makes a newer alert replace an older one with the same id.
	CollapseID string `json:"collapse_id,omitempty"`
	// InterruptionLevel is one of the Interruption* constants; empty means
	// active (the iOS default).
	InterruptionLevel string `json:"interruption_level,omitempty"`
	// Badge sets the app icon badge; zero clears it; nil leaves it alone.
	Badge *int `json:"badge,omitempty"`
	// Sound is "default" (also the empty value's meaning) or "none".
	Sound string `json:"sound,omitempty"`
	// Route is the in-app screen a tap opens, e.g. "/mutations/<id>" or
	// "/timeline/<adapter>/<event_id>". It is delivered as data.route.
	Route string `json:"route,omitempty"`
	// Data is arbitrary payload delivered to the app beside route; the app
	// also reads data.request_id for mutation actions.
	Data map[string]any `json:"data,omitempty"`
}

// Interruption levels, per UNNotificationInterruptionLevel.
const (
	InterruptionPassive       = "passive"
	InterruptionActive        = "active"
	InterruptionTimeSensitive = "time-sensitive"
	InterruptionCritical      = "critical"
)

var interruptionLevels = map[string]bool{
	InterruptionPassive: true, InterruptionActive: true, InterruptionTimeSensitive: true, InterruptionCritical: true,
}

// Validate rejects what the Expo service or iOS would otherwise drop
// silently: an unknown category shows an alert with no buttons, a non-https
// image is refused by App Transport Security in the extension, and so on.
func (n Notification) Validate() error {
	if strings.TrimSpace(n.Title) == "" {
		return errors.New("title is required")
	}
	if n.InterruptionLevel != "" && !interruptionLevels[n.InterruptionLevel] {
		return fmt.Errorf("interruption_level %q is not one of passive, active, time-sensitive, critical", n.InterruptionLevel)
	}
	if n.Category != "" && !categoryIDs()[n.Category] {
		return fmt.Errorf("category %q is not one the app registers (%s)", n.Category, strings.Join(categoryNames(), ", "))
	}
	if n.ImageURL != "" {
		u, err := url.Parse(n.ImageURL)
		if err != nil || u.Scheme != "https" || u.Host == "" {
			return fmt.Errorf("image_url must be an absolute https URL, got %q", n.ImageURL)
		}
	}
	if n.Route != "" && !strings.HasPrefix(n.Route, "/") {
		return fmt.Errorf("route must be an in-app path starting with /, got %q", n.Route)
	}
	switch n.Sound {
	case "", "default", "none":
	default:
		return fmt.Errorf("sound must be default or none, got %q (custom sounds are not bundled in the app)", n.Sound)
	}
	return nil
}

// message builds the Expo message for one device.
func (n Notification) message(token string) (Message, error) {
	if err := n.Validate(); err != nil {
		return Message{}, err
	}
	m := Message{
		To:                token,
		Title:             n.Title,
		Subtitle:          n.Subtitle,
		Body:              n.Body,
		Priority:          "high",
		CategoryID:        n.Category,
		ThreadID:          n.ThreadID,
		CollapseID:        n.CollapseID,
		InterruptionLevel: n.InterruptionLevel,
		Badge:             n.Badge,
	}
	if n.Sound != "none" {
		m.Sound = "default"
	}
	if n.ImageURL != "" {
		m.RichContent = &RichContent{Image: n.ImageURL}
		m.MutableContent = true
	}
	if len(n.Data) > 0 || n.Route != "" {
		m.Data = make(map[string]any, len(n.Data)+1)
		for k, v := range n.Data {
			m.Data[k] = v
		}
		if n.Route != "" {
			m.Data["route"] = n.Route
		}
	}
	return m, nil
}

// Report says what happened to one fan-out.
type Report struct {
	Devices  int      `json:"devices"`
	Sent     int      `json:"sent"`
	Failed   int      `json:"failed"`
	Disabled int      `json:"disabled"`
	Errors   []string `json:"errors,omitempty"`
}

// Notifier fans notifications out to every registered device.
type Notifier struct {
	store  Store
	sender Sender
	now    func() time.Time
	logger *slog.Logger
}

func NewNotifier(store Store, sender Sender, now func() time.Time, logger *slog.Logger) *Notifier {
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Notifier{store: store, sender: sender, now: now, logger: logger.With("component", "push")}
}

// Notify sends to every active device. It never returns an error for a
// per-device failure — those are recorded on the device row — only for a
// registry read or a whole-request failure at the provider.
func (n *Notifier) Notify(ctx context.Context, notification Notification) (Report, error) {
	if err := notification.Validate(); err != nil {
		return Report{}, err
	}
	devices, err := n.store.ListActive(ctx)
	if err != nil {
		return Report{}, err
	}
	report := Report{Devices: len(devices)}
	if len(devices) == 0 {
		return report, nil
	}
	messages := make([]Message, 0, len(devices))
	for _, device := range devices {
		message, err := notification.message(device.ExpoPushToken)
		if err != nil {
			return report, err
		}
		messages = append(messages, message)
	}
	tickets, err := n.sender.Send(ctx, messages)
	if err != nil {
		n.logger.ErrorContext(ctx, "push fan-out failed", "error", err.Error(), "devices", len(devices))
		return report, err
	}
	if len(tickets) != len(messages) {
		err := fmt.Errorf("push provider returned %d tickets for %d messages", len(tickets), len(messages))
		n.logger.ErrorContext(ctx, "push fan-out failed", "error", err.Error())
		return report, err
	}
	now := n.now()
	for index, ticket := range tickets {
		token := messages[index].To
		if ticket.Status == "ok" {
			report.Sent++
			if merr := n.store.MarkSent(ctx, token, now); merr != nil {
				n.logger.WarnContext(ctx, "push sent but could not record it", "error", merr.Error())
			}
			continue
		}
		report.Failed++
		reason := ticket.Details.Error
		if reason == "" {
			reason = ticket.Message
		}
		disable := ticket.Details.Error == DeviceNotRegistered
		if disable {
			report.Disabled++
		}
		report.Errors = append(report.Errors, reason)
		n.logger.WarnContext(ctx, "push delivery failed", "reason", reason, "disabled", disable)
		if merr := n.store.MarkFailed(ctx, token, reason, disable, now); merr != nil {
			n.logger.WarnContext(ctx, "push failed and could not record it", "error", merr.Error())
		}
	}
	return report, nil
}

// NotifyAsync runs Notify on a detached context with a bounded timeout, for
// callers (like a mutation proposal) that must not wait on the provider.
func (n *Notifier) NotifyAsync(notification Notification) {
	if n == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := n.Notify(ctx, notification); err != nil {
			n.logger.Error("background push failed", "error", err.Error(), "title", notification.Title)
		}
	}()
}
