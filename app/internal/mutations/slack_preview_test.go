package mutations

import (
	"testing"
	"time"
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
