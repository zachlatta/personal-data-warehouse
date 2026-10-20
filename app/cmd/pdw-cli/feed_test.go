package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func feedStub(t *testing.T, response string) *stubServer {
	t.Helper()
	return newStubServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":`+response+`}`)
	})
}

func TestFeedPushFromFlagsCallsFeedPush(t *testing.T) {
	srv := feedStub(t, `{"created":1,"updated":0,"items":[{"item_id":"hn:https://x.test/1","status":"created"}]}`)
	out, stderr, code := runCLI(t, srv.URL, "", "feed", "push", "--source", "hn", "--title", "Hi", "--url", "https://x.test/1",
		"--tag", "ai,news", "--tag", "go", "--score", "2.5", "--expires", "7d")
	if code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if srv.lastPath != "/api/tools/feed_push" {
		t.Fatalf("path %s", srv.lastPath)
	}
	var body struct {
		Source string           `json:"source"`
		Items  []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(srv.lastBody, &body); err != nil {
		t.Fatal(err)
	}
	item := body.Items[0]
	if body.Source != "hn" || item["title"] != "Hi" || item["score"] != 2.5 || item["expires_at"] != "7d" ||
		len(item["tags"].([]any)) != 3 {
		t.Fatalf("body: %s", srv.lastBody)
	}
	if _, sent := item["body"]; sent {
		t.Fatalf("an unset flag must not be sent: %s", srv.lastBody)
	}
	if !strings.Contains(out, "created 1, updated 0") || !strings.Contains(out, "hn:https://x.test/1") {
		t.Fatalf("out: %s", out)
	}
}

func TestFeedPushReadsEveryJSONShapeFromStdin(t *testing.T) {
	for name, stdin := range map[string]string{
		"array":    `[{"title":"a"},{"title":"b"}]`,
		"envelope": `{"source":"muse","items":[{"title":"a"},{"title":"b"}]}`,
		"lines":    "{\"title\":\"a\"}\n{\"title\":\"b\"}\n",
	} {
		srv := feedStub(t, `{"created":2,"updated":0,"items":[]}`)
		_, stderr, code := runCLI(t, srv.URL, stdin, "feed", "push", "--source", "muse")
		if code != 0 {
			t.Fatalf("%s: code=%d stderr=%s", name, code, stderr)
		}
		var body struct {
			Source string            `json:"source"`
			Items  []json.RawMessage `json:"items"`
		}
		_ = json.Unmarshal(srv.lastBody, &body)
		if body.Source != "muse" || len(body.Items) != 2 {
			t.Fatalf("%s: %s", name, srv.lastBody)
		}
	}
	srv := feedStub(t, `{}`)
	_, stderr, code := runCLI(t, srv.URL, `{"title":"one"}`, "feed", "push", "--source", "x")
	if code != 0 || !strings.Contains(string(srv.lastBody), `"items":[{"title":"one"}]`) {
		t.Fatalf("single object: code=%d %s %s", code, stderr, srv.lastBody)
	}
}

func TestFeedSubcommandsMapToTheirTools(t *testing.T) {
	for _, c := range []struct {
		args []string
		tool string
		want string
	}{
		{[]string{"list", "--source", "hn", "-n", "5"}, "feed_list", `"limit":5`},
		{[]string{"update", "hn:1", "--score", "3"}, "feed_update", `"item_id":"hn:1"`},
		{[]string{"remove", "hn:1", "hn:2", "--reason", "stale"}, "feed_remove", `"item_ids":["hn:1","hn:2"]`},
		{[]string{"rank", "hn:2", "hn:1"}, "feed_rank", `"order":["hn:2","hn:1"]`},
		{[]string{"rank", "--score", "hn:1=0.5"}, "feed_rank", `"scores":{"hn:1":0.5}`},
		{[]string{"stats", "--since", "7d"}, "feed_stats", `"since":"7d"`},
		{[]string{"events", "--type", "like,open", "--item", "hn:1"}, "feed_events", `"types":["like","open"]`},
	} {
		srv := feedStub(t, `{}`)
		_, stderr, code := runCLI(t, srv.URL, "", append([]string{"feed"}, c.args...)...)
		if code != 0 || srv.lastPath != "/api/tools/"+c.tool || !strings.Contains(string(srv.lastBody), c.want) {
			t.Fatalf("%v: code=%d path=%s body=%s stderr=%s", c.args, code, srv.lastPath, srv.lastBody, stderr)
		}
	}
}

func TestFeedArgumentErrorsNeverReachTheServer(t *testing.T) {
	srv := newStubServer(t, func(http.ResponseWriter, *http.Request) { t.Error("server reached") })
	for _, args := range [][]string{
		{"feed", "push", "--source", "x", "--bogus"},
		{"feed", "update"},
		{"feed", "remove"},
		{"feed", "rank", "--score", "nonumber"},
		{"feed", "frobnicate"},
	} {
		if _, _, code := runCLI(t, srv.URL, "", args...); code != 2 {
			t.Fatalf("%v: code=%d", args, code)
		}
	}
}

func TestFeedHelpIsItsOwn(t *testing.T) {
	out, _, code := runCLI(t, "http://127.0.0.1:1", "", "feed", "--help")
	if code != 0 || !strings.HasPrefix(out, "pdw feed - push cards") {
		t.Fatalf("code=%d out=%.80q", code, out)
	}
}
