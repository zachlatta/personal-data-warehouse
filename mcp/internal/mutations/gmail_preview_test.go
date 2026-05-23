package mutations

import (
	"testing"
	"time"
)

func TestApplyGmailThreadPreviewRowsMergesWarehouseMessages(t *testing.T) {
	older := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	latest := time.Date(2026, 5, 20, 9, 30, 0, 0, time.UTC)
	mutations := []Mutation{{
		ID:        "mut-1",
		Provider:  "gmail",
		Operation: GmailArchiveOperation,
		Account:   "zach@example.test",
		Payload:   map[string]any{"thread_ids": []any{"thread-1"}},
		Preview: map[string]any{
			"thread_count": 1,
			"context":      map[string]any{"source": "test"},
			"threads":      []any{map[string]any{"thread_id": "thread-1"}},
		},
	}}

	got := applyGmailThreadPreviewRows(mutations, []gmailThreadPreviewRow{
		{
			Account:           "zach@example.test",
			ThreadID:          "thread-1",
			MessageID:         "message-older",
			Subject:           "Receipt received",
			FromAddress:       "HCB <receipts@hcb.example>",
			ToAddresses:       []string{"zach@example.test"},
			LabelIDs:          []string{"INBOX", "CATEGORY_UPDATES"},
			InternalDate:      older,
			Snippet:           "We received your receipt.",
			PreviewText:       "We received your receipt and attached it to the transaction.",
			MessageCount:      2,
			InboxMessageCount: 2,
		},
		{
			Account:           "zach@example.test",
			ThreadID:          "thread-1",
			MessageID:         "message-latest",
			Subject:           "Receipt received",
			FromAddress:       "HCB <receipts@hcb.example>",
			ToAddresses:       []string{"zach@example.test"},
			LabelIDs:          []string{"INBOX", "UNREAD"},
			InternalDate:      latest,
			Snippet:           "Everything is synced.",
			PreviewText:       "Everything is synced. No action is required.",
			MessageCount:      2,
			InboxMessageCount: 2,
		},
	})

	threads := mapSliceFromAny(got[0].Preview["threads"])
	if len(threads) != 1 {
		t.Fatalf("threads = %#v", got[0].Preview["threads"])
	}
	thread := threads[0]
	if thread["subject"] != "Receipt received" || thread["latest_from_address"] != "HCB <receipts@hcb.example>" {
		t.Fatalf("thread summary = %#v", thread)
	}
	if thread["latest_preview"] != "Everything is synced. No action is required." {
		t.Fatalf("latest preview = %#v", thread["latest_preview"])
	}
	if thread["message_count"] != 2 || thread["inbox_message_count"] != 2 {
		t.Fatalf("message counts = %#v / %#v", thread["message_count"], thread["inbox_message_count"])
	}
	messages := mapSliceFromAny(thread["messages"])
	if len(messages) != 2 {
		t.Fatalf("messages = %#v", thread["messages"])
	}
	if messages[0]["message_id"] != "message-older" || messages[1]["message_id"] != "message-latest" {
		t.Fatalf("message order = %#v", messages)
	}
	if stringSliceFromAny(thread["labels"])[0] != "CATEGORY_UPDATES" {
		t.Fatalf("labels = %#v", thread["labels"])
	}
	if mutations[0].Preview["threads"].([]any)[0].(map[string]any)["subject"] != nil {
		t.Fatalf("original mutation preview was mutated: %#v", mutations[0].Preview)
	}
}
