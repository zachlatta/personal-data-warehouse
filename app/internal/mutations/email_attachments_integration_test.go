package mutations

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"
)

// Invoked by pytest in an isolated disposable database. Leaves approved rows for
// the real Python worker to claim and deliver to a local Gmail HTTP test server.
func TestEmailAttachmentsProposalReviewApproval(t *testing.T) {
	url := os.Getenv("PDW_EMAIL_ATTACHMENT_TEST_URL")
	if url == "" {
		t.Skip("run through tests/test_email_attachments_e2e.py")
	}
	store, err := NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := NewService(store, Config{GmailAccounts: []string{"sender@example.test"}})
	server := newAPIServer(t, store)
	ctx := context.Background()
	attachment := map[string]any{
		"filename": "résumé.bin", "content_type": "application/octet-stream",
		"data_base64": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0, 255, 1, 128}, 25000)),
	}
	for _, mode := range []string{"send", "draft"} {
		proposal, err := service.ProposeMutation(ctx, ProposeMutationInput{
			Title: "Attachment E2E " + mode, Reason: "isolated test",
			Mutations: []map[string]any{{
				"type": GmailSendEmailOperation, "account": "sender@example.test", "delivery_mode": mode,
				"message": map[string]any{"to": []string{"recipient@example.test"}, "subject": "Attachment E2E " + mode,
					"body_text": "Plain", "body_html": "<p>HTML</p>", "attachments": []any{attachment}},
				"variants": []any{
					map[string]any{"title": "No Files", "message": map[string]any{"attachments": []any{}}},
					map[string]any{"title": "With Files"},
				},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		request, err := store.GetRequest(ctx, proposal.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		mutation := request.Mutations[0]
		if request.Status != "pending_review" {
			t.Fatal(request.Status)
		}
		variants := mapSliceFromAny(gmailEmailView(mutation)["variants"])
		if len(mapSliceFromAny(variants[0]["attachments"])) != 0 || len(mapSliceFromAny(variants[1]["attachments"])) != 1 {
			t.Fatal("variant attachment inheritance/override lost")
		}
		post := func(action string, input any, want int) map[string]any {
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
			if response.StatusCode != want {
				t.Fatalf("%s: status %d: %v", action, response.StatusCode, body)
			}
			return body
		}
		path := "/mutations/" + mutation.ID + "/update-email"
		// A >64 KiB edit exercises the real HTTP request decoder as well as JSONB.
		edited := post(path, map[string]any{"delivery_mode": mode, "selected_variant_id": "variant_2",
			"message": map[string]any{"to": []string{"recipient@example.test"}, "subject": "Reviewed " + mode,
				"body_text": "Reviewed plain", "body_html": "<p>Reviewed HTML</p>", "attachments": []any{attachment}}}, http.StatusOK)
		view := mapFromAny(mapFromAny(edited["mutation"])["email"])
		if got := mapSliceFromAny(mapFromAny(view["message"])["attachments"]); len(got) != 1 || got[0]["data_base64"] != attachment["data_base64"] {
			t.Fatal("review API lost attachment bytes")
		}
		post(path, map[string]any{"delivery_mode": mode, "message": map[string]any{
			"to": []string{"recipient@example.test"}, "subject": "Invalid", "body_text": "Invalid",
			"attachments": []any{map[string]any{"filename": "bad", "content_type": "text/plain", "data_base64": "!"}},
		}}, http.StatusConflict)
		// Older/mobile clients omit attachments while editing the body.
		post(path, map[string]any{"delivery_mode": mode, "message": map[string]any{
			"to": []string{"recipient@example.test"}, "subject": "Reviewed " + mode,
			"body_text": "Final plain", "body_html": "<p>Final HTML</p>",
		}}, http.StatusOK)
		post("/approve", map[string]any{}, http.StatusOK)
		post(path, map[string]any{"message": map[string]any{"attachments": []any{}}}, http.StatusConflict)
	}
}
