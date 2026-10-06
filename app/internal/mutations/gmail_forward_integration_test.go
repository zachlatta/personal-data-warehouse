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
// base_gmail tables hold the original (account forwarder@example.test, Gmail
// message orig-1, one PDF attachment). Proposes a forward with only a note,
// edits the note through the review API the phone uses, and approves it,
// leaving the row for the real Python worker to send.
func TestGmailForwardProposalReviewApproval(t *testing.T) {
	url := os.Getenv("PDW_EMAIL_FORWARD_TEST_URL")
	if url == "" {
		t.Skip("run through tests/test_email_attachments_e2e.py")
	}
	store, err := NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := NewService(store, Config{GmailAccounts: []string{"forwarder@example.test"}})
	server := newAPIServer(t, store)
	ctx := context.Background()

	if _, err := service.ProposeMutation(ctx, ProposeMutationInput{
		Title: "Forward a message the warehouse lacks", Reason: "isolated test",
		Mutations: []map[string]any{{
			"type": GmailSendEmailOperation, "account": "forwarder@example.test",
			"message": map[string]any{"to": []string{"accountant@example.test"}, "forward_message_id": "missing"},
		}},
	}); err == nil || !strings.Contains(err.Error(), "base_gmail.messages") {
		t.Fatalf("a forward of an unsynced message must be refused, got %v", err)
	}

	proposal, err := service.ProposeMutation(ctx, ProposeMutationInput{
		Title: "Forward the invoice", Reason: "isolated test",
		Mutations: []map[string]any{{
			"type": GmailSendEmailOperation, "account": "forwarder@example.test",
			"message": map[string]any{
				"to":                 []string{"accountant@example.test"},
				"body_text":          "Can you file this?",
				"forward_message_id": "orig-1",
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if err != nil {
		t.Fatal(err)
	}
	mutation := request.Mutations[0]
	view := gmailEmailView(mutation)
	message := mapFromAny(view["message"])
	if message["subject"] != "Fwd: Invoice 4831" || message["editor_text"] != "Can you file this?" {
		t.Fatalf("forward view = %#v", message)
	}
	quoted := stringFromAny(message["quoted_text"])
	for _, want := range []string{gmailForwardMarker, "From: Vendor <billing@vendor.test>", "Subject: Invoice 4831", "Your invoice is attached."} {
		if !strings.Contains(quoted, want) {
			t.Fatalf("quoted_text missing %q: %q", want, quoted)
		}
	}
	forward := mapFromAny(view["forward"])
	files := mapSliceFromAny(forward["attachments"])
	if forward["message_id"] != "orig-1" || len(files) != 1 || files[0]["filename"] != "invoice-4831.pdf" {
		t.Fatalf("forward preview = %#v", view["forward"])
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
	// The phone's edit: its own note, then the untouched signature and quote,
	// with no forward_message_id in the body it posts.
	edited := post("/mutations/"+mutation.ID+"/update-email", map[string]any{
		"delivery_mode": "send",
		"message": map[string]any{
			"to":        []string{"accountant@example.test"},
			"subject":   message["subject"],
			"body_text": "Please file this one.\n\n" + quoted + "\n",
			"body_html": "<div>Please file this one.</div><div><br></div>" + stringFromAny(message["quoted_html"]),
		},
	})
	editedView := mapFromAny(mapFromAny(edited["mutation"])["email"])
	editedMessage := mapFromAny(editedView["message"])
	if editedMessage["forward_message_id"] != "orig-1" || editedMessage["editor_text"] != "Please file this one." {
		t.Fatalf("a reviewer edit must keep the forward: %#v", editedMessage)
	}
	if strings.Count(stringFromAny(editedMessage["body_text"]), gmailForwardMarker) != 1 {
		t.Fatalf("a reviewer edit must not forward twice: %q", editedMessage["body_text"])
	}
	post("/approve", map[string]any{})
}
