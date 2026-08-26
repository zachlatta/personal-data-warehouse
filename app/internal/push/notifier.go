package push

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Sender is what the Notifier fans a notification out through (the Expo
// client in production, a fake in tests).
type Sender interface {
	Send(ctx context.Context, messages []Message) ([]Ticket, error)
}

// Notification is a single alert delivered to every active device.
type Notification struct {
	Title string
	Body  string
	// Data is what the app reads when the notification is tapped; the app
	// routes on data.route (e.g. "/mutations/<request_id>").
	Data map[string]any
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
		messages = append(messages, Message{
			To:       device.ExpoPushToken,
			Title:    notification.Title,
			Body:     notification.Body,
			Data:     notification.Data,
			Sound:    "default",
			Priority: "high",
		})
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
