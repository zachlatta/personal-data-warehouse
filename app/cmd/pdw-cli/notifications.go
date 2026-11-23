package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/accesstokens"
	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
)

const notificationsUsage = `usage: pdw notifications <mute|unmute|mutes>

Mute one source's phone alerts (timeline notifications) until a set time; the
mute ends by itself then. A muted source's events are recorded as "muted" in
marts_ops.notifications and never sent, not even after the mute ends. Other
sources keep paging, and mutation alerts are never muted.

  pdw notifications mute SOURCE (--until TIME | --for DURATION) [--reason TEXT]
        SOURCE is a timeline source that has paged before (slack, gmail, ...).
        --until takes an RFC 3339 time (2027-01-04T09:00:00-05:00) or a date
        (midnight UTC); --for a duration (7d, 36h). At most 90 days.
  pdw notifications unmute SOURCE
        End an active mute now.
  pdw notifications mutes [--output text|json]
        Every mute, active or ended, and when it ends.
`

const notificationMutesAPIPath = "/api/notifications/mutes"

func runNotifications(client *cliclient.Client, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, notificationsUsage)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch args[0] {
	case "mute":
		if len(args) < 2 || strings.HasPrefix(args[1], "-") {
			fmt.Fprint(stderr, notificationsUsage)
			return 2
		}
		source := strings.TrimSpace(args[1])
		fs := flag.NewFlagSet("notifications mute", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		until := fs.String("until", "", "when the mute ends")
		forDuration := fs.String("for", "", "how long the mute lasts")
		reason := fs.String("reason", "", "free-text note")
		if err := fs.Parse(args[2:]); err != nil || fs.NArg() > 0 || source == "" || (*until == "") == (*forDuration == "") {
			fmt.Fprint(stderr, notificationsUsage)
			return 2
		}
		var end time.Time
		if *until != "" {
			at, err := parseExpiry(strings.TrimSpace(*until))
			if err != nil {
				fmt.Fprintln(stderr, "pdw notifications mute: --until:", err)
				return 2
			}
			end = at
		} else {
			d, err := accesstokens.ParseDuration(*forDuration)
			if err != nil || d <= 0 {
				fmt.Fprintf(stderr, "pdw notifications mute: --for wants a duration like 7d or 36h, got %q\n", *forDuration)
				return 2
			}
			end = time.Now().Add(d)
		}
		raw, err := client.Request(ctx, http.MethodPost, notificationMutesAPIPath, map[string]string{
			"source": source, "until": end.UTC().Format(time.RFC3339), "reason": *reason,
		})
		if err != nil {
			return notificationsError(stderr, err)
		}
		var mute mutedSource
		if err := json.Unmarshal(raw, &mute); err != nil {
			fmt.Fprintln(stderr, "pdw notifications mute: decode response:", err)
			return 1
		}
		fmt.Fprintf(stdout, "Muted %s until %s.\n", mute.Source, mute.MutedUntil.UTC().Format(time.RFC3339))
		return 0
	case "unmute":
		if len(args) != 2 || strings.TrimSpace(args[1]) == "" {
			fmt.Fprint(stderr, notificationsUsage)
			return 2
		}
		source := strings.TrimSpace(args[1])
		if _, err := client.Request(ctx, http.MethodDelete, notificationMutesAPIPath+"/"+url.PathEscape(source), nil); err != nil {
			return notificationsError(stderr, err)
		}
		fmt.Fprintf(stdout, "Unmuted %s.\n", source)
		return 0
	case "mutes":
		fs := flag.NewFlagSet("notifications mutes", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		output := fs.String("output", "text", "text or json")
		if err := fs.Parse(args[1:]); err != nil || fs.NArg() > 0 {
			fmt.Fprint(stderr, notificationsUsage)
			return 2
		}
		raw, err := client.Request(ctx, http.MethodGet, notificationMutesAPIPath, nil)
		if err != nil {
			return notificationsError(stderr, err)
		}
		if *output == "json" {
			_, _ = stdout.Write(raw)
			return 0
		}
		var listed struct {
			Mutes []mutedSource `json:"mutes"`
		}
		if err := json.Unmarshal(raw, &listed); err != nil {
			fmt.Fprintln(stderr, "pdw notifications mutes: decode response:", err)
			return 1
		}
		tw := tabwriter.NewWriter(stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "SOURCE\tSTATE\tMUTED UNTIL\tREASON")
		for _, m := range listed.Mutes {
			state := "ended"
			if m.Active {
				state = "active"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", m.Source, state, m.MutedUntil.UTC().Format(time.RFC3339), m.Reason)
		}
		_ = tw.Flush()
		return 0
	default:
		fmt.Fprintf(stderr, "pdw notifications: unknown subcommand %q\n", args[0])
		fmt.Fprint(stderr, notificationsUsage)
		return 2
	}
}

type mutedSource struct {
	Source     string    `json:"source"`
	MutedUntil time.Time `json:"muted_until"`
	Reason     string    `json:"reason"`
	Active     bool      `json:"active"`
}

func notificationsError(stderr io.Writer, err error) int {
	var apiErr *cliclient.APIError
	if errors.As(err, &apiErr) {
		fmt.Fprintf(stderr, "pdw notifications: http %d: %s\n", apiErr.Status, apiErr.Message)
		return 1
	}
	fmt.Fprintln(stderr, "pdw notifications:", err)
	return 1
}
