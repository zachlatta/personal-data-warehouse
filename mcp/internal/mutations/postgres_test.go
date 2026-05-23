package mutations

import "testing"

func TestNormalizeForStorageMatchesWorkerPayloads(t *testing.T) {
	mutations, err := normalizeForStorage(CreateRequestInput{
		Title:   "Do reviewed work",
		Reason:  "agent found work",
		Context: map[string]any{"source": "test"},
		Mutations: []MutationInput{
			{Type: GmailArchiveOperation, Account: "zach@example.test", ThreadIDs: []string{"thread-1", "thread-2"}},
			{Type: GmailUnarchiveOperation, Account: "zach@example.test", ThreadIDs: []string{"thread-3"}},
			{
				Type:         GmailSendEmailOperation,
				Account:      "zach@example.test",
				DeliveryMode: "draft",
				Message: map[string]any{
					"to":                 []string{"one@example.test"},
					"subject":            "Hello",
					"body_text":          "Body",
					"reply_to_thread_id": "thread-4",
				},
			},
			{Type: GooglePeopleContactsOperation, Account: "zach@example.test", Operations: []map[string]any{{"op": "delete_contact", "resource_name": "people/1"}}},
		},
	})
	if err != nil {
		t.Fatalf("normalizeForStorage returned error: %v", err)
	}
	if len(mutations) != 5 {
		t.Fatalf("mutation count = %d", len(mutations))
	}

	if mutations[0].Operation != GmailArchiveOperation || mutations[0].Provider != "gmail" {
		t.Fatalf("archive mutation metadata = %#v", mutations[0])
	}
	if got := stringSliceFromAny(mutations[0].Payload["thread_ids"]); len(got) != 1 || got[0] != "thread-1" {
		t.Fatalf("archive thread_ids = %#v", got)
	}
	if got := stringSliceFromAny(mutations[0].Payload["remove_label_ids"]); len(got) != 1 || got[0] != "INBOX" {
		t.Fatalf("archive remove_label_ids = %#v", got)
	}
	if got := stringSliceFromAny(mutations[2].Payload["add_label_ids"]); len(got) != 1 || got[0] != "INBOX" {
		t.Fatalf("unarchive add_label_ids = %#v", got)
	}

	emailPayload := mutations[3].Payload
	if emailPayload["delivery_mode"] != "draft" {
		t.Fatalf("delivery_mode = %#v", emailPayload["delivery_mode"])
	}
	message, ok := emailPayload["message"].(map[string]any)
	if !ok {
		t.Fatalf("message payload = %#v", emailPayload["message"])
	}
	if message["reply_to_thread_id"] != "thread-4" || message["subject"] != "Hello" {
		t.Fatalf("message = %#v", message)
	}

	contactOps, ok := mutations[4].Payload["operations"].([]map[string]any)
	if !ok || len(contactOps) != 1 {
		t.Fatalf("contact operations = %#v", mutations[4].Payload["operations"])
	}
	if contactOps[0]["op"] != "delete_contact" || mutations[4].Operation != ContactsBatchMutationOperation {
		t.Fatalf("contact mutation = %#v", mutations[4])
	}
}
