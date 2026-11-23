package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestNotificationsMuteSendsTheSourceAndItsEnd(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"source":"slack","muted_until":"2027-01-04T14:00:00Z","reason":"vacation","active":true,"created_at":"2027-01-01T12:00:00Z","updated_at":"2027-01-01T12:00:00Z"}`))
	})
	out, stderr, code := runCLI(t, srv.URL, "", "notifications", "mute", "slack", "--until", "2027-01-04T09:00:00-05:00", "--reason", "vacation")
	if code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if srv.lastMethod != http.MethodPost || srv.lastPath != "/api/notifications/mutes" {
		t.Fatalf("%s %s", srv.lastMethod, srv.lastPath)
	}
	var body map[string]string
	if err := json.Unmarshal(srv.lastBody, &body); err != nil {
		t.Fatal(err)
	}
	// The end goes over the wire in UTC, exactly the instant asked for.
	if body["source"] != "slack" || body["until"] != "2027-01-04T14:00:00Z" || body["reason"] != "vacation" {
		t.Fatalf("body = %v", body)
	}
	if !strings.Contains(out, "slack") || !strings.Contains(out, "2027-01-04T14:00:00Z") {
		t.Fatalf("out = %q", out)
	}
}

func TestNotificationsMuteTakesADurationForItsEnd(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"source":"gmail","muted_until":"2027-01-04T14:00:00Z","active":true}`))
	})
	// "7d" is the spelling the guide teaches; days count as 24 hours.
	before := time.Now()
	if _, stderr, code := runCLI(t, srv.URL, "", "notifications", "mute", "gmail", "--for", "7d"); code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	var body map[string]string
	_ = json.Unmarshal(srv.lastBody, &body)
	until, err := time.Parse(time.RFC3339, body["until"])
	if err != nil {
		t.Fatalf("--for sent no end: %v", body)
	}
	if got := until.Sub(before); got < 7*24*time.Hour-time.Second || got > 7*24*time.Hour+time.Minute {
		t.Fatalf("--for 7d ends %v from now", got)
	}
}

func TestNotificationsMuteRefusesAMuteWithoutAnEnd(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("an argument error must not reach the server")
	})
	for _, args := range [][]string{
		{"notifications", "mute", "slack"},
		{"notifications", "mute", "--until", "2027-01-04T14:00:00Z"},
		{"notifications", "mute", "slack", "--until", "2027-01-04T14:00:00Z", "--for", "2h"},
		{"notifications", "mute", "slack", "--until", "next week"},
		{"notifications", "mute", "slack", "--for", "a week"},
		{"notifications", "mute", "slack", "--for", "-2h"},
		{"notifications", "mute", "slack", "--for", "0d"},
		{"notifications", "unmute"},
		{"notifications", "snooze", "slack"},
	} {
		if _, _, code := runCLI(t, srv.URL, "", args...); code != 2 {
			t.Fatalf("%v exited %d, want 2", args, code)
		}
	}
}

func TestNotificationsUnmuteAndList(t *testing.T) {
	srv := newStubServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"mutes":[{"source":"slack","muted_until":"2027-01-04T14:00:00Z","reason":"vacation","active":true}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"source":"slack","ended":true}`))
	})
	if _, stderr, code := runCLI(t, srv.URL, "", "notifications", "unmute", "slack"); code != 0 || srv.lastMethod != http.MethodDelete || srv.lastPath != "/api/notifications/mutes/slack" {
		t.Fatalf("unmute: code=%d %s %s stderr=%s", code, srv.lastMethod, srv.lastPath, stderr)
	}
	out, stderr, code := runCLI(t, srv.URL, "", "notifications", "mutes")
	if code != 0 || srv.lastPath != "/api/notifications/mutes" || !strings.Contains(out, "slack") || !strings.Contains(out, "2027-01-04T14:00:00Z") || !strings.Contains(out, "active") {
		t.Fatalf("mutes: code=%d out=%q stderr=%s", code, out, stderr)
	}
	out, _, code = runCLI(t, srv.URL, "", "notifications", "mutes", "--output", "json")
	if code != 0 || !json.Valid([]byte(out)) {
		t.Fatalf("mutes --output json: %q", out)
	}
}

func TestNotificationsHelpIsItsOwn(t *testing.T) {
	out, _, code := runCLI(t, "http://127.0.0.1:1", "", "notifications", "--help")
	if code != 0 || !strings.HasPrefix(out, "usage: pdw notifications") {
		t.Fatalf("code=%d out=%.120q", code, out)
	}
}
