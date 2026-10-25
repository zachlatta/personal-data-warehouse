package main

import (
	"bytes"
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
		"--tag", "ai,news", "--tag", "go", "--score", "2.5", "--expires", "7d",
		"--link", "Paper|https://arxiv.org/abs/1", "--link", "https://news.ycombinator.com/item?id=2")
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
	links := item["links"].([]any)
	if len(links) != 2 || links[0].(map[string]any)["title"] != "Paper" ||
		links[0].(map[string]any)["url"] != "https://arxiv.org/abs/1" ||
		links[1].(map[string]any)["url"] != "https://news.ycombinator.com/item?id=2" {
		t.Fatalf("links: %s", srv.lastBody)
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
		{[]string{"update", "hn:1", "--score", "3"}, "feed_update", `"item_ids":["hn:1"]`},
		{[]string{"edit", "--source", "hn", "--clear", "image_url,tags", "--reason", "broken"}, "feed_update", `"clear":["image_url","tags"],"reason":"broken","source":"hn"`},
		{[]string{"update", "hn:1", "--title", "", "--metadata", `{"a":1}`}, "feed_update", `"metadata":{"a":1},"title":""`},
		{[]string{"add", "--source", "x", "--title", "t"}, "feed_push", `"source":"x"`},
		{[]string{"get", "hn:1", "--version", "2"}, "feed_get", `"version":2`},
		{[]string{"delete", "hn:1"}, "feed_remove", `"item_ids":["hn:1"]`},
		{[]string{"restore", "hn:1", "--version", "3"}, "feed_restore", `"version":3`},
		{[]string{"restore", "--source", "hn"}, "feed_restore", `"source":"hn"`},
		{[]string{"history", "hn:1", "--full"}, "feed_history", `"snapshots":true`},
		{[]string{"history", "--source", "hn", "-n", "5"}, "feed_history", `"limit":5`},
		{[]string{"remove", "hn:1", "hn:2", "--reason", "stale"}, "feed_remove", `"item_ids":["hn:1","hn:2"]`},
		{[]string{"rank", "hn:2", "hn:1"}, "feed_rank", `"order":["hn:2","hn:1"]`},
		{[]string{"rank", "--score", "hn:1=0.5"}, "feed_rank", `"scores":{"hn:1":0.5}`},
		{[]string{"stats", "--since", "7d"}, "feed_stats", `"since":"7d"`},
		{[]string{"save", "hn:1", "--reason", "Zach asked"}, "feed_save", `"item_ids":["hn:1"],"reason":"Zach asked","unsave":false`},
		{[]string{"unsave", "hn:1"}, "feed_save", `"unsave":true`},
		{[]string{"list", "--view", "saved"}, "feed_list", `"view":"saved"`},
		{[]string{"list", "--all", "--url", "https://a.example/x?b=1,2", "--url", "https://b.example"}, "feed_list", `"urls":["https://a.example/x?b=1,2","https://b.example"]`},
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
		{"feed", "save"},
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
	if code != 0 || !strings.HasPrefix(out, "pdw feed - full control") {
		t.Fatalf("code=%d out=%.80q", code, out)
	}
}

func TestFeedMutationsCarryTheAgentFromTheEnvironment(t *testing.T) {
	srv := feedStub(t, `{"removed":1}`)
	var outBuf, errBuf bytes.Buffer
	env := map[string]string{"PDW_API_URL": srv.URL, "PDW_SECRET_TOKEN": cliTestToken, "PDW_CLIENT_NAME": "t", "PASEO_AGENT_ID": "agent-123"}
	code := run([]string{"feed", "remove", "hn:1", "--reason", "stale"}, strings.NewReader(""), &outBuf, &errBuf, func(k string) string { return env[k] })
	if code != 0 || !strings.Contains(string(srv.lastBody), `"agent":"agent-123"`) || !strings.Contains(string(srv.lastBody), `"reason":"stale"`) {
		t.Fatalf("code=%d body=%s stderr=%s", code, srv.lastBody, errBuf.String())
	}
	env["PDW_AGENT_NAME"] = "muse-picks"
	run([]string{"feed", "rank", "hn:1", "--agent", "explicit"}, strings.NewReader(""), &outBuf, &errBuf, func(k string) string { return env[k] })
	if !strings.Contains(string(srv.lastBody), `"agent":"explicit"`) {
		t.Fatalf("an explicit --agent wins: %s", srv.lastBody)
	}
	run([]string{"feed", "get", "hn:1"}, strings.NewReader(""), &outBuf, &errBuf, func(k string) string { return env[k] })
	if strings.Contains(string(srv.lastBody), "agent") {
		t.Fatalf("a read sends no provenance: %s", srv.lastBody)
	}
}

func TestFeedHistoryPrintsEachVersionWithItsDiff(t *testing.T) {
	srv := feedStub(t, `{"count":2,"versions":[`+
		`{"item_id":"hn:1","version":2,"action":"update","actor":"porygon","agent":"picker","reason":"typo","changed_fields":["title"],"diff":{"title":{"from":"Old","to":"New"}},"changed_at":"2026-10-20T12:00:00Z"},`+
		`{"item_id":"hn:1","version":1,"action":"create","actor":"porygon","changed_fields":[],"changed_at":"2026-10-20T11:00:00Z"}]}`)
	out, stderr, code := runCLI(t, srv.URL, "", "feed", "history", "hn:1")
	if code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	for _, want := range []string{"hn:1 v2  update", "by porygon / picker", "reason: typo", `title: "Old" -> "New"`, "hn:1 v1  create"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func TestFeedGetShowsARemovedCardAndADeletedOnesSnapshot(t *testing.T) {
	srv := feedStub(t, `{"item":{"item_id":"hn:1","source":"hn","key":"1","title":"T","tags":[],"score":2,"version":4,"removed_at":"2026-10-20T12:00:00Z","removed_reason":"stale","engagement":{"impressions":3}},"versions":4}`)
	out, _, code := runCLI(t, srv.URL, "", "feed", "show", "hn:1")
	if code != 0 || !strings.Contains(out, "hn:1  v4 of 4  REMOVED") || !strings.Contains(out, `title: "T"`) || !strings.Contains(out, "seen=3") {
		t.Fatalf("code=%d out=%s", code, out)
	}
	srv = feedStub(t, `{"deleted":true,"versions":3,"version":{"item_id":"hn:2","version":3,"action":"delete","actor":"postgres","changed_at":"2026-10-20T12:00:00Z","snapshot":{"item_key":"2","title":"Gone"}}}`)
	out, _, _ = runCLI(t, srv.URL, "", "feed", "get", "hn:2")
	if !strings.Contains(out, "DELETED") || !strings.Contains(out, `title: "Gone"`) {
		t.Fatalf("out=%s", out)
	}
}
