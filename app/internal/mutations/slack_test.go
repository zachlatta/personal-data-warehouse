package mutations

import (
	"context"
	"strings"
	"testing"
)

func TestProposeMutationSlackMarkConversationRead(t *testing.T) {
	store := &recordingStore{request: Request{
		ID:        "req-slack-read",
		Status:    "pending_review",
		Mutations: []Mutation{{ID: "mut-slack-read"}},
	}}
	service := NewService(store, Config{
		BaseURL:       "https://mcp.example.test",
		SlackAccounts: []string{"zrl"},
	})

	response, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Clear Slack DM",
		Reason: "I already handled this message",
		Mutations: []map[string]any{{
			"type":            SlackMarkConversationReadOperation,
			"account":         "ZRL",
			"conversation_id": "D012ABCDEF",
			"message_ts":      "1593473566.000200",
		}},
	})
	if err != nil {
		t.Fatalf("ProposeMutation returned error: %v", err)
	}
	if response.RequestID != "req-slack-read" || response.Status != "pending_review" {
		t.Fatalf("unexpected response: %#v", response)
	}
	mutation := store.createCalls[0].Mutations[0]
	if mutation.Type != SlackMarkConversationReadOperation || mutation.Account != "zrl" {
		t.Fatalf("unexpected mutation: %#v", mutation)
	}
	if mutation.ConversationID != "D012ABCDEF" || mutation.MessageTS != "1593473566.000200" {
		t.Fatalf("unexpected Slack target: %#v", mutation)
	}
}

func TestProposeMutationSlackMarkConversationReadRejectsBadTargets(t *testing.T) {
	service := NewService(&recordingStore{}, Config{SlackAccounts: []string{"zrl"}})
	tests := []struct {
		name           string
		conversationID string
		messageTS      string
		want           string
	}{
		{name: "missing conversation", messageTS: "1593473566.000200", want: "conversation_id"},
		{name: "bad conversation", conversationID: "not-a-channel", messageTS: "1593473566.000200", want: "conversation_id"},
		{name: "missing timestamp", conversationID: "D012ABCDEF", want: "message_ts"},
		{name: "bad timestamp", conversationID: "D012ABCDEF", messageTS: "yesterday", want: "message_ts"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
				Title: "Read Slack", Reason: "test", Mutations: []map[string]any{{
					"type": SlackMarkConversationReadOperation, "account": "zrl",
					"conversation_id": test.conversationID, "message_ts": test.messageTS,
				}},
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %s error, got %v", test.want, err)
			}
		})
	}
}

func TestNormalizeSlackMarkConversationReadForStorage(t *testing.T) {
	stored, err := normalizeForStorage(CreateRequestInput{
		Reason:  "handled",
		Context: map[string]any{"source": "marts_inbox.slack_items"},
		Mutations: []MutationInput{{
			Type: SlackMarkConversationReadOperation, Account: "zrl",
			ConversationID: "D012ABCDEF", MessageTS: "1593473566.000200",
		}},
	})
	if err != nil {
		t.Fatalf("normalizeForStorage: %v", err)
	}
	if len(stored) != 1 || stored[0].Provider != SlackProvider || stored[0].Operation != SlackMarkConversationReadOperation {
		t.Fatalf("unexpected stored mutation: %#v", stored)
	}
	if stored[0].Payload["conversation_id"] != "D012ABCDEF" || stored[0].Payload["message_ts"] != "1593473566.000200" {
		t.Fatalf("unexpected payload: %#v", stored[0].Payload)
	}
	preview := mapFromAny(stored[0].Preview["slack_read"])
	if preview["effect"] == "" {
		t.Fatalf("preview must explain whole-conversation effect: %#v", stored[0].Preview)
	}
}
