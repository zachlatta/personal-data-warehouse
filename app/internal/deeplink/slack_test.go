package deeplink

import "testing"

func TestSlack(t *testing.T) {
	for _, testCase := range []struct {
		name, team, conversation, message, thread, domain, url string
	}{
		{
			name: "channel message",
			team: "T1", conversation: "C1", message: "1593473566.000200", domain: "example",
			url: "https://example.slack.com/archives/C1/p1593473566000200",
		},
		{
			// Without thread_ts Slack opens the channel at the parent and the
			// reply is nowhere on screen.
			name: "thread reply names its thread",
			team: "T1", conversation: "C1", message: "1593473566.000200", thread: "1593473500.000100", domain: "example",
			url: "https://example.slack.com/archives/C1/p1593473566000200?thread_ts=1593473500.000100&cid=C1",
		},
		{
			name: "a thread parent is not its own reply",
			team: "T1", conversation: "C1", message: "1593473566.000200", thread: "1593473566.000200", domain: "example",
			url: "https://example.slack.com/archives/C1/p1593473566000200",
		},
		{
			name: "no known workspace domain falls back to the client url",
			team: "T1", conversation: "C1", message: "1593473566.000200",
			url: "https://app.slack.com/client/T1/C1/p1593473566000200",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			link := Slack(testCase.team, testCase.conversation, testCase.message, testCase.thread, testCase.domain)
			if link == nil {
				t.Fatal("Slack() = nil")
			}
			if link.URL != testCase.url {
				t.Fatalf("URL = %q, want %q", link.URL, testCase.url)
			}
			if link.Label != "Slack" {
				t.Fatalf("Label = %q", link.Label)
			}
			want := "slack://channel?team=" + testCase.team + "&id=" + testCase.conversation + "&message=" + testCase.message
			if link.AppURL != want {
				t.Fatalf("AppURL = %q, want %q", link.AppURL, want)
			}
		})
	}

	// A link that cannot address one message is no link: a client that treats
	// an empty url as openable sends the reviewer to the wrong place.
	for _, missing := range [][3]string{{"", "C1", "1.1"}, {"T1", "", "1.1"}, {"T1", "C1", ""}} {
		if link := Slack(missing[0], missing[1], missing[2], "", "example"); link != nil {
			t.Fatalf("Slack(%q, %q, %q) = %#v, want nil", missing[0], missing[1], missing[2], link)
		}
	}
}
