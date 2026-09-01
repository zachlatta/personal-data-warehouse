package mutations

import (
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/deeplink"
)

func TestApplySlackMarkReadPreviewRowsAddsConversationContextAndBoundary(t *testing.T) {
	before := time.Date(2026, 8, 29, 14, 0, 0, 0, time.UTC)
	targetAt := before.Add(time.Minute)
	after := targetAt.Add(time.Minute)
	mutations := []storedMutation{{
		Provider:  SlackProvider,
		Operation: SlackMarkConversationReadOperation,
		Account:   "zrl",
		Payload: map[string]any{
			"conversation_id": "D1",
			"message_ts":      "1593473566.000200",
		},
		Preview: map[string]any{
			"slack_read": map[string]any{
				"conversation_id": "D1",
				"message_ts":      "1593473566.000200",
				"effect":          "Moves the entire conversation read cursor through this message.",
			},
		},
	}}
	detail := slackMarkReadPreviewDetail{
		Account: "zrl", TeamID: "T1", ConversationID: "D1", MessageTS: "1593473566.000200",
		ConversationName: "Marcus", ConversationType: "im", CurrentLastRead: "1593473500.000100",
		CurrentUnreadCount: 3, ContextKind: "conversation",
	}
	rows := []slackMarkReadPreviewRow{
		{Account: "zrl", ConversationID: "D1", MessageTS: "1593473500.000100", SentAt: before, ActorName: "You", Text: "Did you see this?", IsFromMe: true},
		{Account: "zrl", ConversationID: "D1", MessageTS: "1593473566.000200", SentAt: targetAt, ActorName: "Marcus", Text: "Yep — all handled.", IsTarget: true},
		{Account: "zrl", ConversationID: "D1", MessageTS: "1593473600.000300", SentAt: after, ActorName: "Marcus", Text: "One more thing after that."},
	}

	applySlackMarkReadPreviewRows(mutations, []slackMarkReadPreviewDetail{detail}, rows)

	preview := mapFromAny(mutations[0].Preview["slack_read"])
	if preview["conversation_name"] != "Marcus" || preview["conversation_type"] != "im" {
		t.Fatalf("conversation preview = %#v", preview)
	}
	if preview["current_last_read"] != "1593473500.000100" || preview["current_unread_count"] != 3 {
		t.Fatalf("current read state = %#v", preview)
	}
	messages := mapSliceFromAny(preview["messages"])
	if len(messages) != 3 {
		t.Fatalf("messages = %#v", preview["messages"])
	}
	if messages[0]["actor_name"] != "You" || messages[0]["is_from_me"] != true {
		t.Fatalf("first message = %#v", messages[0])
	}
	if messages[1]["is_target"] != true || messages[1]["text"] != "Yep — all handled." {
		t.Fatalf("target message = %#v", messages[1])
	}
	if messages[2]["position"] != "after" {
		t.Fatalf("after-boundary message = %#v", messages[2])
	}
}

func TestSlackMarkReadPreviewTargetsOnlyExactSlackOperations(t *testing.T) {
	got := slackMarkReadPreviewTargets([]storedMutation{
		{Provider: SlackProvider, Operation: SlackMarkConversationReadOperation, Account: "zrl", Payload: map[string]any{"conversation_id": "D1", "message_ts": "1.000001"}},
		{Provider: SlackProvider, Operation: "slack.send_message", Account: "zrl", Payload: map[string]any{"conversation_id": "D2", "message_ts": "2.000002"}},
	})
	if len(got) != 1 || got[0].ConversationID != "D1" || got[0].MessageTS != "1.000001" {
		t.Fatalf("targets = %#v", got)
	}
}

// previewLinkURL reads a preview's `open` whether it is still the typed link
// the builder returned or the map it becomes after a JSON round trip.
func previewLinkURL(value any) string {
	switch link := value.(type) {
	case *deeplink.Link:
		if link == nil {
			return ""
		}
		return link.URL
	case deeplink.Link:
		return link.URL
	default:
		return stringFromAny(mapFromAny(value)["url"])
	}
}

func TestApplySlackMarkReadPreviewLinksHydratesAnOlderSnapshot(t *testing.T) {
	// The shape a request proposed before links and faces existed still has.
	mutations := []Mutation{{
		ID:        "mut-1",
		Provider:  SlackProvider,
		Operation: SlackMarkConversationReadOperation,
		Account:   "example",
		Preview: map[string]any{"slack_read": map[string]any{
			"team_id":         "T1",
			"conversation_id": "C1",
			"message_ts":      "1593473566.000200",
			"thread_ts":       "1593473500.000100",
			"messages": []any{
				map[string]any{"message_ts": "1593473500.000100", "user_id": "U-ME", "actor_name": "You"},
				map[string]any{"message_ts": "1593473566.000200", "user_id": "U-MARCUS", "actor_name": "Marcus", "is_target": true},
			},
		}},
	}}

	got := applySlackMarkReadPreviewLinks(
		mutations,
		map[slackTeamKey]string{{Account: "example", TeamID: "T1"}: "example"},
		map[slackUserKey]string{{Account: "example", TeamID: "T1", UserID: "U-MARCUS"}: "https://avatars.example.test/marcus.png"},
	)

	slackRead := mapFromAny(got[0].Preview["slack_read"])
	if slackRead["team_domain"] != "example" {
		t.Fatalf("team_domain = %#v", slackRead["team_domain"])
	}
	if got := previewLinkURL(slackRead["open"]); got != "https://example.slack.com/archives/C1/p1593473566000200?thread_ts=1593473500.000100&cid=C1" {
		t.Fatalf("conversation link = %#v", got)
	}
	messages := mapSliceFromAny(slackRead["messages"])
	if len(messages) != 2 {
		t.Fatalf("messages = %#v", messages)
	}
	// The thread parent is not its own reply, so its link carries no thread.
	if got := previewLinkURL(messages[0]["open"]); got != "https://example.slack.com/archives/C1/p1593473500000100" {
		t.Fatalf("parent link = %#v", got)
	}
	if messages[1]["avatar_url"] != "https://avatars.example.test/marcus.png" {
		t.Fatalf("target avatar = %#v", messages[1]["avatar_url"])
	}
	if slackRead["avatar_url"] != "https://avatars.example.test/marcus.png" {
		t.Fatalf("row avatar = %#v", slackRead["avatar_url"])
	}
	// A speaker with no stored profile image gets no key rather than "".
	if _, ok := messages[0]["avatar_url"]; ok {
		t.Fatalf("message without an avatar carries one: %#v", messages[0])
	}
	// The stored snapshot itself is untouched.
	if _, ok := mapFromAny(mutations[0].Preview["slack_read"])["open"]; ok {
		t.Fatal("hydration mutated the input preview")
	}

	teams, users := slackMarkReadPreviewLinkTargets(mutations)
	if len(teams) != 1 || teams[0] != (slackTeamKey{Account: "example", TeamID: "T1"}) {
		t.Fatalf("teams = %#v", teams)
	}
	if len(users) != 2 {
		t.Fatalf("users = %#v", users)
	}
}

// A workspace whose domain we do not know still gets a usable link: the
// client URL routes by team id alone.
func TestApplySlackMarkReadPreviewLinksWithoutAKnownDomain(t *testing.T) {
	got := applySlackMarkReadPreviewLinks([]Mutation{{
		Provider:  SlackProvider,
		Operation: SlackMarkConversationReadOperation,
		Account:   "example",
		Preview: map[string]any{"slack_read": map[string]any{
			"team_id": "T1", "conversation_id": "C1", "message_ts": "1.2",
		}},
	}}, nil, nil)
	slackRead := mapFromAny(got[0].Preview["slack_read"])
	if url := previewLinkURL(slackRead["open"]); url != "https://app.slack.com/client/T1/C1/p12" {
		t.Fatalf("fallback link = %#v", url)
	}
}
