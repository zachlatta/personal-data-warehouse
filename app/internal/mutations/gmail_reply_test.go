package mutations

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A reply names its parent with reply_to_message_id (that message) or
// reply_to_thread_id (the thread's latest message); PDW derives the thread,
// In-Reply-To, References and the quote from it. Hand-built headers are what
// sent three approved replies as new threads: an In-Reply-To with no thread id
// makes Gmail start a new conversation.
func TestProposeMutationRefusesHandBuiltReplyHeaders(t *testing.T) {
	cases := map[string]map[string]any{
		"in_reply_to without a thread": {
			"message": map[string]any{
				"to": []any{"a@example.test"}, "subject": "Re: Demo", "body_text": "Yes.",
				"in_reply_to": "<ask-1@partner.test>",
			},
		},
		"references beside a thread": {
			"message": map[string]any{
				"to": []any{"a@example.test"}, "subject": "Re: Demo", "body_text": "Yes.",
				"reply_to_thread_id": "thread-1",
				"references":         []any{"<ask-1@partner.test>"},
			},
		},
		"in_reply_to on a variant": {
			"message": map[string]any{"to": []any{"a@example.test"}, "subject": "Re: Demo", "body_text": "Yes."},
			"variants": []any{
				map[string]any{"title": "Short One", "in_reply_to": "<ask-1@partner.test>"},
			},
		},
		"references inside a variant message": {
			"message": map[string]any{"to": []any{"a@example.test"}, "subject": "Re: Demo", "body_text": "Yes."},
			"variants": []any{
				map[string]any{"title": "Short One", "message": map[string]any{"references": []any{"<ask-1@partner.test>"}}},
			},
		},
	}
	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			store := &recordingStore{}
			mutation := map[string]any{"type": GmailSendEmailOperation, "account": "zach@example.test"}
			for key, value := range fields {
				mutation[key] = value
			}
			_, err := NewService(store, Config{}).ProposeMutation(context.Background(), ProposeMutationInput{
				Title: "Reply", Reason: "test", Mutations: []map[string]any{mutation},
			})
			if err == nil || !strings.Contains(err.Error(), "reply_to_message_id") {
				t.Fatalf("hand-built reply headers must be refused with the field to use, got %v", err)
			}
			if len(store.createCalls) != 0 {
				t.Fatalf("a refused proposal reached the store: %#v", store.createCalls)
			}
		})
	}
}

func TestProposeMutationRefusesAReplyThatIsAlsoAForward(t *testing.T) {
	store := &recordingStore{}
	_, err := NewService(store, Config{}).ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "Reply", Reason: "test",
		Mutations: []map[string]any{{
			"type": GmailSendEmailOperation, "account": "zach@example.test",
			"message": map[string]any{
				"to":                  []any{"a@example.test"},
				"forward_message_id":  "orig-1",
				"reply_to_message_id": "ask-1",
			},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("a reply that is also a forward must be refused, got %v", err)
	}
}

func TestProposeMutationKeepsReplyToMessageIDOnEveryVariant(t *testing.T) {
	store := &recordingStore{request: Request{ID: "req-reply", Status: "pending_review"}}
	_, err := NewService(store, Config{}).ProposeMutation(context.Background(), ProposeMutationInput{
		Title: "Reply", Reason: "test",
		Mutations: []map[string]any{{
			"type": GmailSendEmailOperation, "account": "zach@example.test",
			"message": map[string]any{
				"to": []any{"a@example.test"}, "subject": "Re: Demo", "body_text": "Yes.",
				"reply_to_message_id": " ask-1 ",
			},
			"variants": []any{
				map[string]any{"title": "Short One"},
				map[string]any{"title": "Other Parent", "reply_to_message_id": "ask-2"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("ProposeMutation: %v", err)
	}
	stored, err := normalizeForStorage(store.createCalls[0])
	if err != nil {
		t.Fatal(err)
	}
	variants := normalizeStoredEmailVariants(stored[0].Payload["variants"])
	if got := mapFromAny(variants[0]["message"])["reply_to_message_id"]; got != "ask-1" {
		t.Fatalf("variant 1 reply_to_message_id = %#v", got)
	}
	if got := mapFromAny(variants[1]["message"])["reply_to_message_id"]; got != "ask-2" {
		t.Fatalf("variant 2 reply_to_message_id = %#v", got)
	}
}

// The parent row is the message named, not the thread's latest: in each of
// the three detached replies the latest message was a private aside.
func TestApplyGmailReplyParentRowsThreadsAndQuotesThePinnedMessage(t *testing.T) {
	mutations := []storedMutation{{
		Provider: "gmail", Operation: GmailSendEmailOperation, Account: "zach@example.test",
		Payload: map[string]any{
			"delivery_mode": "send",
			"message": map[string]any{
				"to": []string{"partner@partner.test"}, "subject": "Re: Demo", "body_text": "Yes.",
				"reply_to_message_id": "ask-1",
			},
		},
		Preview: map[string]any{"email": map[string]any{"subject": "Re: Demo"}},
	}}
	parent := gmailReplyParentRow{
		Account: "zach@example.test", MessageID: "ask-1", ThreadID: "thread-ask",
		RFC822MessageID: "<ask-1@partner.test>", ReferencesHeader: "<root@partner.test>",
		FromAddress: "partner@partner.test", InternalDate: time.Date(2026, 11, 24, 15, 0, 0, 0, time.UTC),
		BodyText: "Can you join the demo?",
	}
	got, err := applyGmailReplyParentRows(mutations, []gmailReplyParentRow{parent})
	if err != nil {
		t.Fatal(err)
	}
	message := mapFromAny(got[0].Payload["message"])
	if message["reply_to_thread_id"] != "thread-ask" || message["in_reply_to"] != "<ask-1@partner.test>" {
		t.Fatalf("reply headers = %#v", message)
	}
	if refs := strings.Join(stringSliceFromAny(message["references"]), " "); refs != "<root@partner.test> <ask-1@partner.test>" {
		t.Fatalf("references = %q", refs)
	}
	if !strings.Contains(stringFromAny(message["body_text"]), "> Can you join the demo?") {
		t.Fatalf("the reply must quote the pinned message: %q", message["body_text"])
	}
	if mapFromAny(got[0].Preview["email"])["reply_to_message_id"] != "ask-1" {
		t.Fatalf("the review must name the message replied to: %#v", got[0].Preview["email"])
	}

	again, err := applyGmailReplyParentRows(got, []gmailReplyParentRow{parent})
	if err != nil {
		t.Fatal(err)
	}
	if body := stringFromAny(mapFromAny(again[0].Payload["message"])["body_text"]); strings.Count(body, "Can you join the demo?") != 1 {
		t.Fatalf("re-enrichment (a reviewer edit) must not quote twice: %q", body)
	}
}

func TestApplyGmailReplyParentRowsRefusesAnUnknownOrMismatchedParent(t *testing.T) {
	reply := func(message map[string]any) []storedMutation {
		base := map[string]any{"to": []string{"a@example.test"}, "subject": "Re: Demo", "body_text": "Yes."}
		for key, value := range message {
			base[key] = value
		}
		return []storedMutation{{
			Provider: "gmail", Operation: GmailSendEmailOperation, Account: "zach@example.test",
			Payload: map[string]any{"message": base},
		}}
	}
	rows := []gmailReplyParentRow{{
		Account: "zach@example.test", MessageID: "ask-1", ThreadID: "thread-ask", RFC822MessageID: "<ask-1@partner.test>",
	}}
	if _, err := applyGmailReplyParentRows(reply(map[string]any{"reply_to_message_id": "missing"}), rows); err == nil ||
		!strings.Contains(err.Error(), "base_gmail.messages") {
		t.Fatalf("an unsynced parent must be refused, got %v", err)
	}
	if _, err := applyGmailReplyParentRows(reply(map[string]any{"reply_to_message_id": "ask-1", "reply_to_thread_id": "thread-other"}), rows); err == nil ||
		!strings.Contains(err.Error(), "thread-ask") {
		t.Fatalf("a parent outside the named thread must be refused, got %v", err)
	}
	if _, err := applyGmailReplyParentRows(reply(map[string]any{"reply_to_thread_id": "thread-unsynced"}), rows); err == nil ||
		!strings.Contains(err.Error(), "thread-unsynced") {
		t.Fatalf("a reply to a thread the warehouse lacks must be refused, not sent detached, got %v", err)
	}
}

func TestEmailPreviewModeTreatsReplyToMessageIDAsAReply(t *testing.T) {
	if mode := emailPreviewMode(map[string]any{"reply_to_message_id": "ask-1"}); mode != "reply" {
		t.Fatalf("mode = %q", mode)
	}
}
