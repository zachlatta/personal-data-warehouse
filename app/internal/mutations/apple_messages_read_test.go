package mutations

import (
	"context"
	"strings"
	"testing"
)

func TestNormalizeAppleMessagesChat(t *testing.T) {
	cases := map[string]string{
		"72975":               "72975",
		"(310) 555-0123":      "+13105550123",
		"Google@RBM.goog":     "google@rbm.goog",
		"chatbot@example.com": "chatbot@example.com",
	}
	for input, want := range cases {
		got, err := normalizeAppleMessagesChat(input)
		if err != nil || got != want {
			t.Fatalf("normalize(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
	for _, bad := range []string{"", "chat90010015966772932", "iMessage;+;chat1", "Ada Lovelace", "12"} {
		if got, err := normalizeAppleMessagesChat(bad); err == nil {
			t.Fatalf("normalize(%q) = %q, want an error", bad, got)
		}
	}
}

func TestProposeAppleMessagesMarkReadStoresTheConversation(t *testing.T) {
	service, store := androidMessagesService(t)
	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "Clear login codes", Reason: "Zach asked",
		Mutations: []map[string]any{
			{"type": AppleMessagesMarkReadOperation, "account": "you@example.com", "chat": "72975"},
			{"type": AppleMessagesMarkReadOperation, "account": "you@example.com", "recipient": "310-555-0123"},
		},
	}); err != nil {
		t.Fatalf("ProposeMutation: %v", err)
	}
	stored, err := normalizeForStorage(store.createCalls[0])
	if err != nil {
		t.Fatalf("normalizeForStorage: %v", err)
	}
	if len(stored) != 2 {
		t.Fatalf("stored %d", len(stored))
	}
	if stored[0].Provider != AppleMessagesProvider || stored[0].Operation != AppleMessagesMarkReadOperation || stored[0].Payload["chat"] != "72975" {
		t.Fatalf("stored[0] = %#v", stored[0])
	}
	if stored[1].Payload["chat"] != "+13105550123" || stored[1].Title != "Mark texts from +13105550123 read" {
		t.Fatalf("stored[1] = %#v", stored[1])
	}
	if !strings.Contains(stringFromAny(mapFromAny(stored[0].Preview["mark_read"])["effect"]), "read receipts") {
		t.Fatalf("preview must say what approving does: %#v", stored[0].Preview)
	}
}

func TestProposeAppleMessagesMarkReadRefusesGroupsAndUnconfiguredAccounts(t *testing.T) {
	for _, tc := range []struct {
		entry map[string]any
		want  string
	}{
		{map[string]any{"account": "you@example.com", "chat": "chat123"}, "group"},
		{map[string]any{"account": "you@example.com"}, "must include chat"},
		{map[string]any{"account": "other@example.com", "chat": "72975"}, "APPLE_MESSAGES_ACCOUNTS"},
	} {
		service, store := androidMessagesService(t)
		tc.entry["type"] = AppleMessagesMarkReadOperation
		_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{Title: "t", Reason: "r", Mutations: []map[string]any{tc.entry}})
		if err == nil || !strings.Contains(err.Error(), tc.want) || len(store.createCalls) != 0 {
			t.Fatalf("err = %v, want %q", err, tc.want)
		}
	}
}

func TestAppleMessagesChatInfoListsUnreadAndLeavesSendsAlone(t *testing.T) {
	read := Mutation{Provider: AppleMessagesProvider, Operation: AppleMessagesMarkReadOperation, Account: "you@example.com",
		Payload: map[string]any{"chat": "72975"}, Preview: map[string]any{"mark_read": map[string]any{"chat": "72975"}}}
	empty := Mutation{Provider: AppleMessagesProvider, Operation: AppleMessagesMarkReadOperation, Account: "you@example.com",
		Payload: map[string]any{"chat": "+13105550123"}}
	send := Mutation{Provider: AppleMessagesProvider, Operation: AppleMessagesSendMessageOperation, Account: "you@example.com",
		Payload: map[string]any{"recipient": "72975", "text": "hi"}}
	infos := map[appleMessagesTarget]*appleMessagesChatInfo{
		{account: "you@example.com", recipient: "72975"}:        {ok: true, unreadCount: 1, unread: []map[string]any{{"text": "Your code is 123456"}}},
		{account: "you@example.com", recipient: "+13105550123"}: {ok: true, names: []string{"Ada Lovelace"}},
	}
	out := applyAppleMessagesChatInfo([]Mutation{read, empty, send}, infos)
	view := mapFromAny(out[0].Preview["mark_read"])
	if view["unread_count"] != int64(1) || len(view["warnings"].([]string)) != 0 {
		t.Fatalf("view = %#v", view)
	}
	view = mapFromAny(out[1].Preview["mark_read"])
	if view["chat_name"] != "Ada Lovelace" || len(view["warnings"].([]string)) != 1 {
		t.Fatalf("empty view = %#v", view)
	}
	if _, ok := out[2].Preview["mark_read"]; ok {
		t.Fatal("a send must not get a mark-read preview")
	}
}

func TestMutationHelpDescribesAppleMessagesMarkRead(t *testing.T) {
	help, err := MutationHelpFor(ProposeMutationHelpInput{Type: AppleMessagesMarkReadOperation})
	if err != nil {
		t.Fatal(err)
	}
	text := strings.ToLower(strings.Join(func() []string {
		out := []string{}
		for _, entry := range help.Mutations {
			out = append(out, entry.Summary, entry.ExtraNotes)
			for _, field := range entry.Fields {
				out = append(out, field.Description)
			}
		}
		return out
	}(), " "))
	for _, want := range []string{"locked", "read receipts", "group"} {
		if !strings.Contains(text, want) {
			t.Fatalf("help does not mention %q", want)
		}
	}
}
