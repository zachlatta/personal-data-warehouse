package deeplink

import (
	"encoding/json"
	"testing"
)

// The phone app reads this exact shape (mobile/src/lib/deep-link.ts); a
// renamed key would silently send every chat tap back to PDW.
func TestAndroidChatWireShape(t *testing.T) {
	for _, testCase := range []struct {
		name string
		link Link
		want string
	}{
		{
			name: "bluebubbles chat",
			link: Link{Label: "BlueBubbles", Android: &AndroidChat{App: AndroidAppBlueBubbles, ChatGUID: "any;+;chat1"}},
			want: `{"url":"","label":"BlueBubbles","android":{"app":"bluebubbles","chat_guid":"any;+;chat1"}}`,
		},
		{
			name: "google messages conversation",
			link: Link{URL: "sms:+15555550100", Label: "Messages", AppURL: "sms:+15555550100", Android: &AndroidChat{App: AndroidAppGoogleMessages, Recipients: "+15555550100"}},
			want: `{"url":"sms:+15555550100","label":"Messages","app_url":"sms:+15555550100","android":{"app":"google_messages","recipients":"+15555550100"}}`,
		},
		{
			name: "no android target",
			link: Link{URL: "https://example.test", Label: "Web"},
			want: `{"url":"https://example.test","label":"Web"}`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			raw, err := json.Marshal(testCase.link)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != testCase.want {
				t.Fatalf("json = %s, want %s", raw, testCase.want)
			}
		})
	}
}
