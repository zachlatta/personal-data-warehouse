package mutations

import (
	"context"
	"strings"
	"testing"
)

func appleMessagesService(t *testing.T) (*Service, *recordingStore) {
	t.Helper()
	store := &recordingStore{request: Request{ID: "req-1", Status: "pending_review"}}
	service := NewService(store, Config{
		BaseURL:               "https://example.test",
		AppleMessagesAccounts: []string{"you@example.com"},
	})
	return service, store
}

func TestNormalizeAppleMessagesRecipient(t *testing.T) {
	cases := map[string]string{
		"+1 (310) 555-0123":   "+13105550123",
		"310-555-0123":        "+13105550123",
		"13105550123":         "+13105550123",
		"+447700900123":       "+447700900123",
		"tel:+13105550123":    "+13105550123",
		" Someone@Example.COM": "someone@example.com",
	}
	for input, want := range cases {
		got, err := normalizeAppleMessagesRecipient(input)
		if err != nil || got != want {
			t.Fatalf("normalize(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
	for _, bad := range []string{"", "Zach Latta", "5550123", "+0123", "447700900123", "not@an"} {
		if got, err := normalizeAppleMessagesRecipient(bad); err == nil {
			t.Fatalf("normalize(%q) = %q, want an error: a guessed handle is a text to a stranger", bad, got)
		}
	}
}

func TestProposeAppleMessagesSendStoresTheExactRecipientAndText(t *testing.T) {
	service, store := appleMessagesService(t)
	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Tell them I'm late",
		Reason: "Zach asked",
		Mutations: []map[string]any{{
			"type":      AppleMessagesSendMessageOperation,
			"account":   "you@example.com",
			"recipient": "(310) 555-0123",
			"text":      "Running 10 minutes late\nSorry!  \n",
		}},
	}); err != nil {
		t.Fatalf("ProposeMutation: %v", err)
	}
	if len(store.createCalls) != 1 {
		t.Fatalf("expected one stored request, got %d", len(store.createCalls))
	}
	stored, err := normalizeForStorage(store.createCalls[0])
	if err != nil {
		t.Fatalf("normalizeForStorage: %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("expected 1 mutation, got %d", len(stored))
	}
	mutation := stored[0]
	if mutation.Provider != AppleMessagesProvider || mutation.Operation != AppleMessagesSendMessageOperation {
		t.Fatalf("unexpected provider/operation %q %q", mutation.Provider, mutation.Operation)
	}
	if got := mutation.Payload["recipient"]; got != "+13105550123" {
		t.Fatalf("payload recipient = %v", got)
	}
	if got := mutation.Payload["text"]; got != "Running 10 minutes late\nSorry!" {
		t.Fatalf("payload text = %q", got)
	}
	if got := mutation.Payload["service"]; got != "auto" {
		t.Fatalf("payload service = %v", got)
	}
	preview := mapFromAny(mutation.Preview["text_message"])
	if preview["recipient"] != "+13105550123" || preview["text"] != "Running 10 minutes late\nSorry!" {
		t.Fatalf("preview does not show the exact recipient and text: %#v", preview)
	}
	if mutation.Title != "Text +13105550123" {
		t.Fatalf("title = %q", mutation.Title)
	}
}

func TestProposeAppleMessagesSendRefusesWhatTheExecutorCannotDo(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]any
		want    string
	}{
		{"no recipient", map[string]any{"text": "hi"}, "must include recipient"},
		{"a name", map[string]any{"recipient": "Ada Lovelace", "text": "hi"}, "not a phone number"},
		{"no text", map[string]any{"recipient": "+13105550123", "text": "  "}, "must include text"},
		{"too long", map[string]any{"recipient": "+13105550123", "text": strings.Repeat("a", appleMessagesTextLimit+1)}, "at most"},
		{"bad service", map[string]any{"recipient": "+13105550123", "text": "hi", "service": "carrier pigeon"}, "service"},
		{"unconfigured account", map[string]any{"account": "other@example.com", "recipient": "+13105550123", "text": "hi"}, "APPLE_MESSAGES_ACCOUNTS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service, store := appleMessagesService(t)
			entry := map[string]any{"type": AppleMessagesSendMessageOperation, "account": "you@example.com"}
			for key, value := range tc.payload {
				entry[key] = value
			}
			_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
				Title: "t", Reason: "r", Mutations: []map[string]any{entry},
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
			if len(store.createCalls) != 0 {
				t.Fatalf("a refused proposal must store nothing")
			}
		})
	}
}

func TestAppleMessagesRecipientInfoNamesThePersonAndWarnsOnAStranger(t *testing.T) {
	known := Mutation{Provider: AppleMessagesProvider, Operation: AppleMessagesSendMessageOperation, Account: "you@example.com",
		Payload: map[string]any{"recipient": "+13105550123", "text": "edited words", "service": "auto"},
		Preview: map[string]any{"text_message": map[string]any{"recipient": "+13105550123", "text": "original words"}}}
	stranger := Mutation{Provider: AppleMessagesProvider, Operation: AppleMessagesSendMessageOperation, Account: "you@example.com",
		Payload: map[string]any{"recipient": "+13105550199", "text": "hi", "service": "auto"},
		Preview: map[string]any{"text_message": map[string]any{"recipient": "+13105550199"}}}
	infos := map[appleMessagesTarget]*appleMessagesRecipientInfo{
		{account: "you@example.com", recipient: "+13105550123"}: {names: []string{"Ada Lovelace"}, recent: []map[string]any{{"text": "see you soon"}}},
		{account: "you@example.com", recipient: "+13105550199"}: {},
	}
	out := applyAppleMessagesRecipientInfo([]Mutation{known, stranger}, infos)
	first := mapFromAny(out[0].Preview["text_message"])
	if first["recipient_name"] != "Ada Lovelace" || first["text"] != "edited words" {
		t.Fatalf("known recipient preview = %#v", first)
	}
	if warnings, _ := first["warnings"].([]string); len(warnings) != 0 {
		t.Fatalf("a known correspondent should carry no warnings: %v", warnings)
	}
	second := mapFromAny(out[1].Preview["text_message"])
	warnings, _ := second["warnings"].([]string)
	if len(warnings) != 2 {
		t.Fatalf("a stranger should be flagged twice (no card, no history): %v", warnings)
	}
}

func TestMutationHelpDocumentsAppleMessagesSend(t *testing.T) {
	help, err := MutationHelpFor(ProposeMutationHelpInput{Type: AppleMessagesSendMessageOperation})
	if err != nil {
		t.Fatalf("MutationHelpFor: %v", err)
	}
	text := strings.ToLower(strings.Join(func() []string {
		out := []string{}
		for _, entry := range help.Mutations {
			out = append(out, entry.Type, entry.Summary, entry.ExtraNotes)
			for _, field := range entry.Fields {
				out = append(out, field.Name, field.Description)
			}
		}
		return out
	}(), " "))
	for _, want := range []string{"apple_messages.send_message", "recipient", "e.164", "chat.db", "approves"} {
		if !strings.Contains(text, want) {
			t.Fatalf("help for apple_messages.send_message does not mention %q", want)
		}
	}
}
