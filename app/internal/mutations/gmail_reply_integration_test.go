package mutations

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// Invoked by tests/test_email_attachments_e2e.py in an isolated database whose
// base_gmail.messages holds thread-demo: the partner's ask-1, then a later
// private aside-2 forwarded to a colleague. Proposals that would have sent a
// detached or wrongly quoted reply are refused; a reply to ask-1 is threaded
// and quoted from ask-1, survives a phone-shaped edit, and is approved for the
// real Python worker to send.
func TestGmailReplyToMessageProposalReviewApproval(t *testing.T) {
	url := os.Getenv("PDW_EMAIL_REPLY_TEST_URL")
	if url == "" {
		t.Skip("run through tests/test_email_attachments_e2e.py")
	}
	store, err := NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const account = "replier@example.test"
	service := NewService(store, Config{GmailAccounts: []string{account}})
	server := newAPIServer(t, store)
	ctx := context.Background()

	propose := func(message map[string]any) (ProposalResponse, error) {
		base := map[string]any{
			"to":        []string{"partner@partner.test"},
			"subject":   "Re: Friday demo",
			"body_text": "Yes, I'll be there.",
		}
		for key, value := range message {
			base[key] = value
		}
		return service.ProposeMutation(ctx, ProposeMutationInput{
			Title: "Reply about the demo", Reason: "isolated test",
			Mutations: []map[string]any{{"type": GmailSendEmailOperation, "account": account, "message": base}},
		})
	}
	for name, refused := range map[string]struct {
		message map[string]any
		want    string
	}{
		"hand-built In-Reply-To": {map[string]any{"in_reply_to": "<ask-1@partner.test>"}, "reply_to_message_id"},
		"an unsynced message":    {map[string]any{"reply_to_message_id": "missing"}, "base_gmail.messages"},
		"a message in another thread": {
			map[string]any{"reply_to_message_id": "ask-1", "reply_to_thread_id": "thread-other"}, "thread-demo",
		},
		"an unsynced thread": {map[string]any{"reply_to_thread_id": "thread-unsynced"}, "thread-unsynced"},
	} {
		if _, err := propose(refused.message); err == nil || !strings.Contains(err.Error(), refused.want) {
			t.Fatalf("%s: want a refusal naming %q, got %v", name, refused.want, err)
		}
	}

	proposal, err := propose(map[string]any{"reply_to_message_id": "ask-1"})
	if err != nil {
		t.Fatal(err)
	}
	request, err := store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if err != nil {
		t.Fatal(err)
	}
	mutation := request.Mutations[0]
	stored := mapFromAny(mutation.Payload["message"])
	if stored["reply_to_message_id"] != "ask-1" || stored["reply_to_thread_id"] != "thread-demo" ||
		stored["in_reply_to"] != "<ask-1@partner.test>" ||
		strings.Join(stringSliceFromAny(stored["references"]), " ") != "<ask-1@partner.test>" {
		t.Fatalf("stored reply = %#v", stored)
	}
	view := mapFromAny(gmailEmailView(mutation)["message"])
	quoted := stringFromAny(view["quoted_text"])
	if !strings.Contains(quoted, "Can you join the demo on Friday?") || strings.Contains(stringFromAny(stored["body_text"]), "Private aside") {
		t.Fatalf("the reply must quote ask-1, not the thread's latest aside: %q", stored["body_text"])
	}

	// A reply to the thread alone answers its latest message, and pins it:
	// a later edit must not move the reply onto a message that arrives after.
	threadReply, err := propose(map[string]any{"reply_to_thread_id": "thread-demo", "body_text": "Thread reply."})
	if err != nil {
		t.Fatal(err)
	}
	threadRequest, err := store.GetRequest(ctx, threadReply.RequestID, AllMutations)
	if err != nil {
		t.Fatal(err)
	}
	if pinned := mapFromAny(threadRequest.Mutations[0].Payload["message"]); pinned["reply_to_message_id"] != "aside-2" ||
		pinned["in_reply_to"] != "<aside-2@example.test>" {
		t.Fatalf("a thread reply must pin the thread's latest message: %#v", pinned)
	}
	if _, err := service.WithdrawMutation(ctx, WithdrawMutationInput{RequestID: threadReply.RequestID, Reason: "isolated test"}); err != nil {
		t.Fatal(err)
	}

	post := func(action string, input any) map[string]any {
		t.Helper()
		raw, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.Post(server.URL+"/api/mutations/requests/"+request.ID+action, "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		body := decodeBody(t, response)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d: %v", action, response.StatusCode, body)
		}
		return body
	}
	// The phone's edit posts the headers it was shown and never reply_to_message_id.
	edited := post("/mutations/"+mutation.ID+"/update-email", map[string]any{
		"delivery_mode": "send",
		"message": map[string]any{
			"to":                 []string{"partner@partner.test"},
			"subject":            "Re: Friday demo",
			"body_text":          "Yes, I'll be there.\n\n" + quoted + "\n",
			"body_html":          "<div>Yes, I'll be there.</div><div><br></div>" + stringFromAny(view["quoted_html"]),
			"reply_to_thread_id": "thread-demo",
			"in_reply_to":        "<ask-1@partner.test>",
			"references":         []string{"<ask-1@partner.test>"},
		},
	})
	editedMessage := mapFromAny(mapFromAny(mapFromAny(edited["mutation"])["email"])["message"])
	if editedMessage["reply_to_message_id"] != "ask-1" || editedMessage["in_reply_to"] != "<ask-1@partner.test>" {
		t.Fatalf("a reviewer edit must keep the reply's parent: %#v", editedMessage)
	}
	if strings.Count(stringFromAny(editedMessage["body_text"]), "Can you join the demo on Friday?") != 1 {
		t.Fatalf("a reviewer edit must not quote twice: %q", editedMessage["body_text"])
	}
	post("/approve", map[string]any{})
}
