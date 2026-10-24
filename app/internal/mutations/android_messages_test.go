package mutations

import (
	"context"
	"strings"
	"testing"
)

func androidMessagesService(t *testing.T) (*Service, *recordingStore) {
	t.Helper()
	store := &recordingStore{request: Request{ID: "req-1", Status: "pending_review"}}
	service := NewService(store, Config{
		BaseURL:                 "https://example.test",
		AppleMessagesAccounts:   []string{"you@example.com"},
		AndroidMessagesAccounts: []string{"you@example.com"},
	})
	return service, store
}

func TestNormalizeAppleMessagesRecipient(t *testing.T) {
	cases := map[string]string{
		"+1 (310) 555-0123":    "+13105550123",
		"310-555-0123":         "+13105550123",
		"13105550123":          "+13105550123",
		"+447700900123":        "+447700900123",
		"tel:+13105550123":     "+13105550123",
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

func TestProposeAndroidMessagesSendStoresTheExactRecipientAndText(t *testing.T) {
	service, store := androidMessagesService(t)
	if _, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title:  "Tell them I'm late",
		Reason: "Zach asked",
		Mutations: []map[string]any{{
			"type":      AndroidMessagesSendMessageOperation,
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
	if mutation.Provider != AndroidMessagesProvider || mutation.Operation != AndroidMessagesSendMessageOperation {
		t.Fatalf("unexpected provider/operation %q %q", mutation.Provider, mutation.Operation)
	}
	if got := mutation.Payload["recipient"]; got != "+13105550123" {
		t.Fatalf("payload recipient = %v", got)
	}
	if got := mutation.Payload["text"]; got != "Running 10 minutes late\nSorry!" {
		t.Fatalf("payload text = %q", got)
	}
	if _, ok := mutation.Payload["service"]; ok {
		t.Fatalf("payload carries a service; Messages picks RCS or SMS itself: %#v", mutation.Payload)
	}
	preview := mapFromAny(mutation.Preview["text_message"])
	if preview["recipient"] != "+13105550123" || preview["text"] != "Running 10 minutes late\nSorry!" {
		t.Fatalf("preview does not show the exact recipient and text: %#v", preview)
	}
	if !strings.Contains(stringFromAny(preview["effect"]), "Pixel") {
		t.Fatalf("preview effect does not say where it sends from: %#v", preview["effect"])
	}
	if mutation.Title != "Text +13105550123" {
		t.Fatalf("title = %q", mutation.Title)
	}
}

func TestProposeAndroidMessagesSendRefusesWhatTheExecutorCannotDo(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]any
		want    string
	}{
		{"no recipient", map[string]any{"text": "hi"}, "must include recipient"},
		{"a name", map[string]any{"recipient": "Ada Lovelace", "text": "hi"}, "not a phone number"},
		{"an iMessage email", map[string]any{"recipient": "someone@example.com", "text": "hi"}, "phone numbers only"},
		{"no text", map[string]any{"recipient": "+13105550123", "text": "  "}, "must include text"},
		{"too long", map[string]any{"recipient": "+13105550123", "text": strings.Repeat("a", androidMessagesTextLimit+1)}, "at most"},
		{"a service", map[string]any{"recipient": "+13105550123", "text": "hi", "service": "SMS"}, "service"},
		{"unconfigured account", map[string]any{"account": "other@example.com", "recipient": "+13105550123", "text": "hi"}, "ANDROID_MESSAGES_ACCOUNTS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service, store := androidMessagesService(t)
			entry := map[string]any{"type": AndroidMessagesSendMessageOperation, "account": "you@example.com"}
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

func TestProposeAppleMessagesSendIsRetired(t *testing.T) {
	service, store := androidMessagesService(t)
	_, err := service.ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "t", Reason: "r", Mutations: []map[string]any{{
			"type": AppleMessagesSendMessageOperation, "account": "you@example.com", "recipient": "+13105550123", "text": "hi",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), AndroidMessagesSendMessageOperation) {
		t.Fatalf("err = %v, want a pointer to %s", err, AndroidMessagesSendMessageOperation)
	}
	if len(store.createCalls) != 0 {
		t.Fatalf("a retired type must store nothing")
	}
}

func TestTextMessageRecipientInfoNamesThePersonAndWarnsOnAStranger(t *testing.T) {
	known := Mutation{Provider: AndroidMessagesProvider, Operation: AndroidMessagesSendMessageOperation, Account: "you@example.com",
		Payload: map[string]any{"recipient": "+13105550123", "text": "edited words"},
		Preview: map[string]any{"text_message": map[string]any{"recipient": "+13105550123", "text": "original words"}}}
	stranger := Mutation{Provider: AndroidMessagesProvider, Operation: AndroidMessagesSendMessageOperation, Account: "you@example.com",
		Payload: map[string]any{"recipient": "+13105550199", "text": "hi"},
		Preview: map[string]any{"text_message": map[string]any{"recipient": "+13105550199"}}}
	infos := map[textMessageTarget]*textMessageRecipientInfo{
		{account: "you@example.com", recipient: "+13105550123"}: {names: []string{"Ada Lovelace"}, recent: []map[string]any{{"text": "see you soon"}}},
		{account: "you@example.com", recipient: "+13105550199"}: {},
	}
	out := applyTextMessageRecipientInfo([]Mutation{known, stranger}, infos)
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

func TestUpdatedTextMessagePayloadChangesOnlyTheWords(t *testing.T) {
	mutation := Mutation{Provider: AndroidMessagesProvider, Operation: AndroidMessagesSendMessageOperation, Title: "Text +13105550123",
		Payload: map[string]any{"recipient": "+13105550123", "text": "Draft words."},
		Preview: map[string]any{"text_message": map[string]any{"recipient": "+13105550123", "text": "Draft words."}}}
	payload, preview, title, err := updatedTextMessagePayload(mutation, UpdateTextMessageMutationInput{Text: "Final words.\nSee you there.  \n"})
	if err != nil {
		t.Fatalf("updatedTextMessagePayload: %v", err)
	}
	if payload["text"] != "Final words.\nSee you there." || payload["recipient"] != "+13105550123" || title != "Text +13105550123" {
		t.Fatalf("payload = %#v, title = %q", payload, title)
	}
	message := mapFromAny(preview["text_message"])
	if message["text"] != "Final words.\nSee you there." || message["edited"] != true {
		t.Fatalf("preview = %#v", message)
	}
	if mutation.Payload["text"] != "Draft words." {
		t.Fatal("the edit mutated the original payload")
	}
	for _, bad := range []string{"", "   ", strings.Repeat("a", androidMessagesTextLimit+1)} {
		if _, _, _, err := updatedTextMessagePayload(mutation, UpdateTextMessageMutationInput{Text: bad}); err == nil {
			t.Fatalf("edit to %d characters was accepted", len(bad))
		}
	}
}

func TestMutationHelpDocumentsAndroidMessagesSend(t *testing.T) {
	help, err := MutationHelpFor(ProposeMutationHelpInput{Type: AndroidMessagesSendMessageOperation})
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
	for _, want := range []string{"android_messages.send_message", "recipient", "e.164", "pixel", "approves", "edit"} {
		if !strings.Contains(text, want) {
			t.Fatalf("help for android_messages.send_message does not mention %q", want)
		}
	}
}
