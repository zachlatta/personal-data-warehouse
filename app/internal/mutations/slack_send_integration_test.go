package mutations

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func seedSlackSendWorkspace(t *testing.T, store *PostgresStore, account string) {
	t.Helper()
	ctx := context.Background()
	for _, statement := range []string{
		`INSERT INTO @slack_account_identities (account, team_id, user_id) VALUES ($1, 'T1', 'UME')`,
		`INSERT INTO @slack_users (account, team_id, user_id, display_name, real_name, name, raw_json)
		 VALUES ($1, 'T1', 'UME', 'Zach', 'Zach Lata', 'zach', '{}'),
		        ($1, 'T1', 'UMARCUS', 'Marcus', 'Marcus', 'marcus', '{"profile":{"image_192":"https://avatars.example.test/marcus_192.png"}}'),
		        ($1, 'T1', 'UNEW', 'Newcomer', 'Newcomer', 'newcomer', '{}'),
		        ($1, 'T1', 'UBOT', 'Robot', 'Robot', 'robot', '{"is_bot":true}')`,
		`INSERT INTO @slack_teams (account, team_id, team_name, domain) VALUES ($1, 'T1', 'Example', 'example')`,
		`INSERT INTO @slack_conversations (account, team_id, conversation_id, conversation_type, name, raw_json)
		 VALUES ($1, 'T1', 'D1', 'im', '', '{"user":"UMARCUS","is_member":true}'),
		        ($1, 'T1', 'C1', 'public_channel', 'ops', '{"is_member":true}'),
		        ($1, 'T1', 'C2', 'public_channel', 'announce', '{"is_member":false,"is_archived":true}')`,
		`INSERT INTO @slack_messages (
			account, team_id, conversation_id, message_ts, message_datetime, thread_ts,
			parent_message_ts, user_id, bot_id, username, text,
			is_thread_parent, is_thread_reply, reply_count, is_deleted
		 ) VALUES
			($1, 'T1', 'D1', '1593473500.000100', '2026-08-29 14:00:00+00', '1593473500.000100', '', 'UME', '', '', 'Did you see this?', 0, 0, 0, 0),
			($1, 'T1', 'D1', '1593473566.000200', '2026-08-29 14:01:00+00', '1593473566.000200', '', 'UMARCUS', '', '', 'Yep — all handled.', 0, 0, 0, 0),
			($1, 'T1', 'C1', '1593473600.000300', '2026-08-29 14:02:00+00', '1593473600.000300', '', 'UMARCUS', '', '', 'Can someone look at the deploy?', 1, 0, 1, 0),
			($1, 'T1', 'C1', '1593473660.000400', '2026-08-29 14:03:00+00', '1593473600.000300', '1593473600.000300', 'UME', '', '', 'Looking.', 0, 1, 0, 0)`,
	} {
		if _, err := execContext(ctx, store.db, statement, account); err != nil {
			t.Fatalf("seed Slack workspace: %v", err)
		}
	}
}

func TestCreateRequestResolvesTheSlackSendRecipientAndContext(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	seedSlackPreviewSchema(t, store)
	account := fmt.Sprintf("slack-send-%d", time.Now().UnixNano())
	seedSlackSendWorkspace(t, store, account)

	request, err := store.CreateRequest(ctx, CreateRequestInput{
		Title: "Answer people " + account, Reason: "integration test", RequestedBy: "test",
		Mutations: []MutationInput{
			{Type: SlackSendMessageOperation, Account: account, ConversationID: "C1", ThreadTS: "1593473600.000300", Text: "Deploy is fixed."},
			{Type: SlackSendMessageOperation, Account: account, UserID: "UMARCUS", Text: "Thanks for handling that."},
			{Type: SlackSendMessageOperation, Account: account, UserID: "UNEW", Text: "Welcome!"},
			{Type: SlackSendMessageOperation, Account: account, ConversationID: "C2", Text: "Into the void."},
			{Type: SlackSendMessageOperation, Account: account, UserID: "UBOT", Text: "beep"},
		},
	})
	if err != nil {
		t.Fatalf("CreateRequest: %v", err)
	}
	if len(request.Mutations) != 5 {
		t.Fatalf("stored %d mutations", len(request.Mutations))
	}

	reply := mapFromAny(request.Mutations[0].Preview["slack_message"])
	if reply["recipient_label"] != "#ops" || reply["team_id"] != "T1" || reply["thread_found"] != true || reply["team_domain"] != "example" {
		t.Fatalf("thread reply preview = %#v", reply)
	}
	if request.Mutations[0].Title != "Reply in Slack thread in #ops" {
		t.Fatalf("thread reply title = %q", request.Mutations[0].Title)
	}
	messages := mapSliceFromAny(reply["messages"])
	if len(messages) != 2 || messages[0]["is_thread_parent"] != true || messages[1]["is_from_me"] != true {
		t.Fatalf("thread context = %#v", messages)
	}
	if got := previewLinkURL(messages[1]["open"]); !strings.Contains(got, "thread_ts=1593473600.000300") {
		t.Fatalf("reply permalink names no thread: %q", got)
	}
	if warnings := stringSliceFromAny(reply["warnings"]); len(warnings) != 0 {
		t.Fatalf("clean reply warned: %#v", warnings)
	}

	dm := mapFromAny(request.Mutations[1].Preview["slack_message"])
	if dm["recipient_label"] != "Marcus" || dm["resolved_conversation_id"] != "D1" || dm["conversation_type"] != "im" || dm["avatar_url"] != "https://avatars.example.test/marcus_192.png" {
		t.Fatalf("dm preview = %#v", dm)
	}
	if request.Mutations[1].Title != "Send Slack DM to Marcus" {
		t.Fatalf("dm title = %q", request.Mutations[1].Title)
	}
	if got := mapSliceFromAny(dm["messages"]); len(got) != 2 || got[1]["text"] != "Yep — all handled." {
		t.Fatalf("dm context = %#v", got)
	}

	fresh := mapFromAny(request.Mutations[2].Preview["slack_message"])
	// A person with no DM yet is still a verified recipient; the executor opens the DM.
	if fresh["recipient_found"] != true || fresh["resolved_conversation_id"] != "" || fresh["recipient_label"] != "Newcomer" {
		t.Fatalf("fresh dm preview = %#v", fresh)
	}
	if warnings := stringSliceFromAny(fresh["warnings"]); len(warnings) != 0 {
		t.Fatalf("fresh dm warned: %#v", warnings)
	}

	archived := mapFromAny(request.Mutations[3].Preview["slack_message"])
	warnings := stringSliceFromAny(archived["warnings"])
	if len(warnings) != 2 || !strings.Contains(warnings[0], "archived") || !strings.Contains(warnings[1], "not a member") {
		t.Fatalf("archived channel warnings = %#v", warnings)
	}

	bot := mapFromAny(request.Mutations[4].Preview["slack_message"])
	if warnings := stringSliceFromAny(bot["warnings"]); len(warnings) != 1 || !strings.Contains(warnings[0], "bot") {
		t.Fatalf("bot warnings = %#v", warnings)
	}
}

func TestUpdateSlackMessageMutationEditsTheWordsBeforeApproval(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	seedSlackPreviewSchema(t, store)
	account := fmt.Sprintf("slack-edit-%d", time.Now().UnixNano())
	seedSlackSendWorkspace(t, store, account)

	request, err := store.CreateRequest(ctx, CreateRequestInput{
		Title: "Reply " + account, Reason: "integration test", RequestedBy: "test",
		Mutations: []MutationInput{{Type: SlackSendMessageOperation, Account: account, UserID: "UMARCUS", Text: "Draft words."}},
	})
	if err != nil {
		t.Fatalf("CreateRequest: %v", err)
	}
	mutation := request.Mutations[0]
	edited, err := store.UpdateSlackMessageMutation(ctx, request.ID, mutation.ID, UpdateSlackMessageMutationInput{Text: "Final words."}, "app:web")
	if err != nil {
		t.Fatalf("UpdateSlackMessageMutation: %v", err)
	}
	if edited.Payload["text"] != "Final words." || edited.Revision != mutation.Revision+1 {
		t.Fatalf("edited = %#v", edited)
	}
	got, err := store.GetRequest(ctx, request.ID)
	if err != nil {
		t.Fatalf("GetRequest: %v", err)
	}
	stored := got.Mutations[0]
	slackMessage := mapFromAny(stored.Preview["slack_message"])
	if stored.Payload["text"] != "Final words." || slackMessage["text"] != "Final words." || slackMessage["edited"] != true {
		t.Fatalf("stored edit = %#v / %#v", stored.Payload, slackMessage)
	}
	// The recipient survived the edit, and the read path still hydrates it.
	if stored.Payload["user_id"] != "UMARCUS" || slackMessage["recipient_label"] != "Marcus" || slackMessage["team_domain"] != "example" {
		t.Fatalf("recipient after edit = %#v", slackMessage)
	}
	if _, err := store.UpdateSlackMessageMutation(ctx, request.ID, mutation.ID, UpdateSlackMessageMutationInput{Text: " "}, "app:web"); err == nil {
		t.Fatal("blank edit was accepted")
	}
	if _, err := store.ApproveRequest(ctx, request.ID, "app:web"); err != nil {
		t.Fatalf("ApproveRequest: %v", err)
	}
	if _, err := store.UpdateSlackMessageMutation(ctx, request.ID, mutation.ID, UpdateSlackMessageMutationInput{Text: "Too late."}, "app:web"); err == nil {
		t.Fatal("an approved send was edited")
	}
}
